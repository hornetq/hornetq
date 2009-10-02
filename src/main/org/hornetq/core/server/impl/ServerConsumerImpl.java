/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.server.impl;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.client.management.impl.ManagementHelper;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.ManagementService;
import org.hornetq.core.management.Notification;
import org.hornetq.core.management.NotificationType;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.QueueBinding;
import org.hornetq.core.remoting.Channel;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveContinuationMessage;
import org.hornetq.core.remoting.impl.wireformat.SessionReceiveMessage;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.core.server.HandleStatus;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerConsumer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.impl.TransactionImpl;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.TypedProperties;

/**
 * Concrete implementation of a ClientConsumer.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * 
 * @version <tt>$Revision: 3783 $</tt> $Id: ServerConsumerImpl.java 3783 2008-02-25 12:15:14Z timfox $
 */
public class ServerConsumerImpl implements ServerConsumer
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerConsumerImpl.class);

   // Static ---------------------------------------------------------------------------------------

   private static final boolean trace = log.isTraceEnabled();

   private static void trace(final String message)
   {
      log.trace(message);
   }

   // Attributes -----------------------------------------------------------------------------------

   private final long id;

   private final Queue messageQueue;

   private final Filter filter;

   private final int minLargeMessageSize;

   private final ServerSession session;

   private final Executor executor;

   private final Lock lock = new ReentrantLock();

   private AtomicInteger availableCredits = new AtomicInteger(0);

   private boolean started;

   private volatile LargeMessageDeliverer largeMessageDeliverer = null;

   // We will only be sending one largeMessage at any time, however during replication you may have
   // more than one LargeMessage pending on the replicationBuffer
   private final AtomicInteger pendingLargeMessagesCounter = new AtomicInteger(0);

   /**
    * if we are a browse only consumer we don't need to worry about acknowledgemenets or being started/stopeed by the session.
    */
   private final boolean browseOnly;

   private Runnable browserDeliverer;

   private final boolean updateDeliveries;

   private final StorageManager storageManager;

   private final PagingManager pagingManager;

   private final java.util.Queue<MessageReference> deliveringRefs = new ConcurrentLinkedQueue<MessageReference>();

   private final Channel channel;

   private volatile boolean closed;

   private final boolean preAcknowledge;

   private final ManagementService managementService;

   private final Binding binding;

   // Constructors ---------------------------------------------------------------------------------
   
   
   public ServerConsumerImpl(final long id,
                             final ServerSession session,
                             final QueueBinding binding,
                             final Filter filter,
                             final boolean started,
                             final boolean browseOnly,
                             final StorageManager storageManager,
                             final PagingManager pagingManager,
                             final Channel channel,
                             final boolean preAcknowledge,
                             final boolean updateDeliveries,
                             final Executor executor,
                             final ManagementService managementService) throws Exception
   {
      
      this.id = id;

      this.filter = filter;

      this.session = session;

      this.binding = binding;

      this.messageQueue = binding.getQueue();

      this.executor = executor;

      this.started = browseOnly || started;

      this.browseOnly = browseOnly;

      this.storageManager = storageManager;

      this.channel = channel;

      this.preAcknowledge = preAcknowledge;

      this.pagingManager = pagingManager;

      this.managementService = managementService;

      this.minLargeMessageSize = session.getMinLargeMessageSize();

      this.updateDeliveries = updateDeliveries;

      if (browseOnly)
      {
         browserDeliverer = new BrowserDeliverer(messageQueue.iterator());
      }
      else
      {
         messageQueue.addConsumer(this);
      }
   }

   // ServerConsumer implementation
   // ----------------------------------------------------------------------

   public long getID()
   {
      return id;
   }

   public HandleStatus handle(final MessageReference ref) throws Exception
   {
      return doHandle(ref);
   }

   public Filter getFilter()
   {
      return filter;
   }

   public void close() throws Exception
   {
      setStarted(false);

      if (largeMessageDeliverer != null)
      {
         largeMessageDeliverer.close();
      }

      if (!browseOnly)
      {
         messageQueue.removeConsumer(this);
      }

      session.removeConsumer(this);

      LinkedList<MessageReference> refs = cancelRefs(false, null);

      Iterator<MessageReference> iter = refs.iterator();

      closed = true;

      Transaction tx = new TransactionImpl(storageManager);

      while (iter.hasNext())
      {
         MessageReference ref = iter.next();

         ref.getQueue().cancel(tx, ref);
      }

      tx.rollback();

      if (!browseOnly)
      {
         TypedProperties props = new TypedProperties();

         props.putStringProperty(ManagementHelper.HDR_ADDRESS, binding.getAddress());

         props.putStringProperty(ManagementHelper.HDR_CLUSTER_NAME, binding.getClusterName());

         props.putStringProperty(ManagementHelper.HDR_ROUTING_NAME, binding.getRoutingName());

         props.putStringProperty(ManagementHelper.HDR_FILTERSTRING, filter == null ? null : filter.getFilterString());

         props.putIntProperty(ManagementHelper.HDR_DISTANCE, binding.getDistance());

         props.putIntProperty(ManagementHelper.HDR_CONSUMER_COUNT, messageQueue.getConsumerCount());

         Notification notification = new Notification(null, NotificationType.CONSUMER_CLOSED, props);

         managementService.sendNotification(notification);
      }
   }

   public int getCountOfPendingDeliveries()
   {
      return deliveringRefs.size();
   }

   public LinkedList<MessageReference> cancelRefs(final boolean lastConsumedAsDelivered, final Transaction tx) throws Exception
   {
      boolean performACK = lastConsumedAsDelivered;

      LinkedList<MessageReference> refs = new LinkedList<MessageReference>();

      if (!deliveringRefs.isEmpty())
      {
         for (MessageReference ref : deliveringRefs)
         {
            if (performACK)
            {
               acknowledge(false, tx, ref.getMessage().getMessageID());

               performACK = false;
            }
            else
            {
               ref.decrementDeliveryCount();

               refs.add(ref);
            }
         }

         deliveringRefs.clear();
      }

      return refs;
   }

   public void setStarted(final boolean started)
   {
      lock.lock();
      try
      {
         this.started = browseOnly || started;
      }
      finally
      {
         lock.unlock();
      }
      
      // Outside the lock
      if (started)
      {
         promptDelivery();
      }
   }

   public void receiveCredits(final int credits) throws Exception
   {
      if (credits == -1)
      {
         // No flow control
         availableCredits = null;
      }
      else
      {
         int previous = availableCredits.getAndAdd(credits);

         if (trace)
         {
            log.trace("Received " + credits +
                      " credits, previous value = " +
                      previous +
                      " currentValue = " +
                      availableCredits.get());
         }

         if (previous <= 0 && previous + credits > 0)
         {
            promptDelivery();
         }
      }
   }

   public Queue getQueue()
   {
      return messageQueue;
   }

   public void acknowledge(final boolean autoCommitAcks, final Transaction tx, final long messageID) throws Exception
   {
      if (browseOnly)
      {
         return;
      }

      // Acknowledge acknowledges all refs delivered by the consumer up to and including the one explicitly
      // acknowledged

      MessageReference ref;
      do
      {
         ref = deliveringRefs.poll();

         if (ref == null)
         {
            throw new IllegalStateException(System.identityHashCode(this) + " Could not find reference on consumerID=" +
                                            id +
                                            ", messageId = " +
                                            messageID +
                                            " queue = " +
                                            messageQueue.getName() +
                                            " closed = " +
                                            closed);
         }

         if (autoCommitAcks)
         {
            ref.getQueue().acknowledge(ref);
         }
         else
         {
            ref.getQueue().acknowledge(tx, ref);
         }
      }
      while (ref.getMessage().getMessageID() != messageID);
   }

   public MessageReference getExpired(final long messageID) throws Exception
   {
      if (browseOnly)
      {
         return null;
      }

      // Expiries can come in out of sequence with respect to delivery order

      Iterator<MessageReference> iter = deliveringRefs.iterator();

      MessageReference ref = null;

      while (iter.hasNext())
      {
         MessageReference theRef = iter.next();

         if (theRef.getMessage().getMessageID() == messageID)
         {
            iter.remove();

            ref = theRef;

            break;
         }
      }

      return ref;
   }

   // Public ---------------------------------------------------------------------------------------

   /** To be used on tests only */
   public AtomicInteger getAvailableCredits()
   {
      return availableCredits;
   }

   // Private --------------------------------------------------------------------------------------

   private void promptDelivery()
   {
      lock.lock();
      try
      {
         // largeMessageDeliverer is aways set inside a lock
         // if we don't acquire a lock, we will have NPE eventually
         if (largeMessageDeliverer != null)
         {
            resumeLargeMessage();
         }
         else
         {
            if (browseOnly)
            {
               executor.execute(browserDeliverer);
            }
            else
            {
               session.promptDelivery(messageQueue);
            }
         }
      }
      finally
      {
         lock.unlock();
      }
   }

   /**
    * 
    */
   private void resumeLargeMessage()
   {
      executor.execute(resumeLargeMessageRunnable);
   }

   private HandleStatus doHandle(final MessageReference ref) throws Exception
   {
      if (availableCredits != null && availableCredits.get() <= 0)
      {         
         return HandleStatus.BUSY;
      }

      lock.lock();

      try
      {
         // If the consumer is stopped then we don't accept the message, it
         // should go back into the
         // queue for delivery later.
         if (!started)
         {
            return HandleStatus.BUSY;
         }

         // note: Since we schedule deliveries to start under replication, we use a counter of pendingLargeMessages.

         // If there is a pendingLargeMessage we can't take another message
         // This has to be checked inside the lock as the set to null is done inside the lock
         if (pendingLargeMessagesCounter.get() > 0)
         {
            return HandleStatus.BUSY;
         }

         final ServerMessage message = ref.getMessage();
         
         if (filter != null && !filter.match(message))
         {
            return HandleStatus.NO_MATCH;
         }

         if (!browseOnly)
         {
            if (!preAcknowledge)
            {
               deliveringRefs.add(ref);
            }

            ref.getQueue().referenceHandled();

            ref.incrementDeliveryCount();

            // If updateDeliveries = false (set by strict-update),
            // the updateDeliveryCount would still be updated after cancel
            if (updateDeliveries)
            {
               if (ref.getMessage().isDurable() && ref.getQueue().isDurable())
               {
                  storageManager.updateDeliveryCount(ref);
               }
            }

            if (preAcknowledge)
            {
               if (message.isLargeMessage())
               {
                  // we must hold one reference, or the file will be deleted before it could be delivered
                  message.incrementRefCount();
               }

               // With pre-ack, we ack *before* sending to the client
               ref.getQueue().acknowledge(ref);
            }

         }

         if (message.isLargeMessage())
         {
            deliverLargeMessage(ref, message);
         }
         else
         {
            deliverStandardMessage(ref, message);
         }

         return HandleStatus.HANDLED;
      }
      finally
      {
         lock.unlock();
      }
   }

   private void deliverLargeMessage(final MessageReference ref, final ServerMessage message)
   {
      pendingLargeMessagesCounter.incrementAndGet();

      final LargeMessageDeliverer localDeliverer = new LargeMessageDeliverer((LargeServerMessage)message, ref);

      // it doesn't need lock because deliverLargeMesasge is already inside the lock()
      largeMessageDeliverer = localDeliverer;
      largeMessageDeliverer.deliver();
   }

   /**
    * @param ref
    * @param message
    */
   private void deliverStandardMessage(final MessageReference ref, final ServerMessage message)
   {
      final SessionReceiveMessage packet = new SessionReceiveMessage(id, message, ref.getDeliveryCount());

      if (availableCredits != null)
      {
         availableCredits.addAndGet(-packet.getRequiredBufferSize());
      }

      channel.send(packet);
   }

   // Inner classes
   // ------------------------------------------------------------------------

   final Runnable resumeLargeMessageRunnable = new Runnable()
   {
      public void run()
      {
         lock.lock();
         try
         {
            if (largeMessageDeliverer == null || largeMessageDeliverer.deliver())
            {
               if (browseOnly)
               {
                  executor.execute(browserDeliverer);
               }
               else
               {
                  // prompt Delivery only if chunk was finished
                  session.promptDelivery(messageQueue);
               }
            }
         }
         finally
         {
            lock.unlock();
         }
      }
   };

   /** Internal encapsulation of the logic on sending LargeMessages.
    *  This Inner class was created to avoid a bunch of loose properties about the current LargeMessage being sent*/
   private class LargeMessageDeliverer
   {
      private final long sizePendingLargeMessage;

      /** The current message being processed */
      private final LargeServerMessage pendingLargeMessage;

      private final MessageReference ref;

      private volatile boolean sentFirstMessage = false;

      /** The current position on the message being processed */
      private volatile long positionPendingLargeMessage;

      public LargeMessageDeliverer(final LargeServerMessage message, final MessageReference ref)
      {
         pendingLargeMessage = message;

         // we must hold one reference, or the file will be deleted before it could be delivered
         pendingLargeMessage.incrementRefCount();

         sizePendingLargeMessage = pendingLargeMessage.getLargeBodySize();

         this.ref = ref;
      }

      public boolean deliver()
      {
         lock.lock();

         try
         {
            if (pendingLargeMessage == null)
            {
               return true;
            }

            if (availableCredits != null && availableCredits.get() <= 0)
            {
               return false;
            }
            SessionReceiveMessage initialMessage;

            if (sentFirstMessage)
            {
               initialMessage = null;
            }
            else
            {
               sentFirstMessage = true;

               HornetQBuffer headerBuffer = ChannelBuffers.buffer(pendingLargeMessage.getPropertiesEncodeSize());

               pendingLargeMessage.encodeProperties(headerBuffer);

               initialMessage = new SessionReceiveMessage(id,
                                                          headerBuffer.array(),
                                                          pendingLargeMessage.getLargeBodySize(),
                                                          ref.getDeliveryCount());
            }

            int precalculateAvailableCredits;

            if (availableCredits != null)
            {
               // Flow control needs to be done in advance.
               precalculateAvailableCredits = preCalculateFlowControl(initialMessage);
            }
            else
            {
               precalculateAvailableCredits = 0;
            }

            if (initialMessage != null)
            {
               channel.send(initialMessage);

               if (availableCredits != null)
               {
                  precalculateAvailableCredits -= initialMessage.getRequiredBufferSize();
               }
            }

            while (positionPendingLargeMessage < sizePendingLargeMessage)
            {
               if (precalculateAvailableCredits <= 0 && availableCredits != null)
               {
                  if (trace)
                  {
                     trace("deliverLargeMessage: Leaving loop of send LargeMessage because of credits");
                  }
                  return false;
               }

               SessionReceiveContinuationMessage chunk = createChunkSend();

               int chunkLen = chunk.getBody().length;

               if (availableCredits != null)
               {
                  if ((precalculateAvailableCredits -= chunk.getRequiredBufferSize()) < 0)
                  {
                     log.warn("Flowcontrol logic is not working properly, too many credits were taken");
                  }
               }

               if (trace)
               {
                  trace("deliverLargeMessage: Sending " + chunk.getRequiredBufferSize() +
                        " availableCredits now is " +
                        availableCredits);
               }

               channel.send(chunk);

               positionPendingLargeMessage += chunkLen;
            }

            if (precalculateAvailableCredits != 0)
            {
               log.warn("Flowcontrol logic is not working properly... creidts = " + precalculateAvailableCredits);
            }

            if (trace)
            {
               trace("Finished deliverLargeMessage");
            }

            close();

            return true;
         }
         finally
         {
            lock.unlock();
         }
      }

      /**
       * 
       */
      public void close()
      {
         pendingLargeMessage.releaseResources();

         int counter = pendingLargeMessage.decrementRefCount();

         if (preAcknowledge && !browseOnly)
         {
            // PreAck will have an extra reference
            counter = pendingLargeMessage.decrementRefCount();
         }

         if (!browseOnly)
         {
            // We added a reference to avoid deleting the file before it was delivered
            // if (pendingLargeMessage.decrementRefCount() == 0)
            if (counter == 0)
            {
               // The decrement was deferred to large-message, hence we need to
               // subtract the size inside largeMessage
               try
               {
                  PagingStore store = pagingManager.getPageStore(binding.getAddress());
                  store.addSize(-pendingLargeMessage.getMemoryEstimate());
               }
               catch (Exception e)
               {
                  // This shouldn't happen as the pageStore should have been initialized already.
                  log.error("Error getting pageStore", e);
               }
            }
         }

         largeMessageDeliverer = null;

         pendingLargeMessagesCounter.decrementAndGet();
      }

      /**
       * Credits flow control are calculated in advance.
       * @return
       */
      private int preCalculateFlowControl(SessionReceiveMessage firstPacket)
      {
         while (true)
         {
            final int currentCredit = availableCredits.get();
            int precalculatedCredits = 0;

            if (firstPacket != null)
            {
               precalculatedCredits = firstPacket.getRequiredBufferSize();
            }

            long chunkLen = 0;
            for (long i = positionPendingLargeMessage; precalculatedCredits < currentCredit && i < sizePendingLargeMessage; i += chunkLen)
            {
               chunkLen = (int)Math.min(sizePendingLargeMessage - i, minLargeMessageSize);
               precalculatedCredits += chunkLen + SessionReceiveContinuationMessage.SESSION_RECEIVE_CONTINUATION_BASE_SIZE;
            }

            // The calculation of credits and taking credits out has to be taken atomically.
            // Since we are not sending anything to the client during this calculation, this is unlikely to happen
            if (availableCredits.compareAndSet(currentCredit, currentCredit - precalculatedCredits))
            {
               if (trace)
               {
                  log.trace("Taking " + precalculatedCredits + " credits out on preCalculateFlowControl (largeMessage)");
               }
               return precalculatedCredits;
            }
         }
      }

      private SessionReceiveContinuationMessage createChunkSend()
      {
         SessionReceiveContinuationMessage chunk;

         int localChunkLen = 0;

         localChunkLen = (int)Math.min(sizePendingLargeMessage - positionPendingLargeMessage, minLargeMessageSize);

         HornetQBuffer bodyBuffer = ChannelBuffers.buffer(localChunkLen);

         pendingLargeMessage.encodeBody(bodyBuffer, positionPendingLargeMessage, localChunkLen);

         chunk = new SessionReceiveContinuationMessage(id,
                                                       bodyBuffer.array(),
                                                       positionPendingLargeMessage + localChunkLen < sizePendingLargeMessage,
                                                       false);

         return chunk;
      }
   }

   private class BrowserDeliverer implements Runnable
   {
      private MessageReference current = null;

      public BrowserDeliverer(final Iterator<MessageReference> iterator)
      {
         this.iterator = iterator;
      }

      private final Iterator<MessageReference> iterator;

      public void run()
      {
         // if the reference was busy during the previous iteration, handle it now
         if (current != null)
         {
            try
            {
               HandleStatus status = handle(current);
               if (status == HandleStatus.BUSY)
               {
                  return;
               }
            }
            catch (Exception e)
            {
               log.warn("Exception while browser handled from " + messageQueue + ": " + current);
               return;
            }
         }

         while (iterator.hasNext())
         {
            MessageReference ref = (MessageReference)iterator.next();
            try
            {
               HandleStatus status = handle(ref);
               if (status == HandleStatus.BUSY)
               {
                  // keep a reference on the current message reference
                  // to handle it next time the browser deliverer is executed
                  current = ref;
                  break;
               }
            }
            catch (Exception e)
            {
               log.warn("Exception while browser handled from " + messageQueue + ": " + ref);
               break;
            }
         }
      }

   }
}
