/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat
 * Middleware LLC, and individual contributors by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of individual
 * contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.server.impl;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jboss.messaging.core.buffers.ChannelBuffers;
import org.jboss.messaging.core.client.management.impl.ManagementHelper;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.management.Notification;
import org.jboss.messaging.core.management.NotificationType;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.PagingStore;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.QueueBinding;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReceiveContinuationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReceiveMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.replication.SessionReplicateDeliveryMessage;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.LargeServerMessage;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerConsumer;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;
import org.jboss.messaging.utils.TypedProperties;

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

   // private static final boolean trace = log.isTraceEnabled();
   private static final boolean trace = false;

   private static void trace(final String message)
   {
      log.trace(message);
   }

   // Attributes -----------------------------------------------------------------------------------

   private final long id;

   private final long replicatedSessionID;

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

   private final boolean updateDeliveries;

   private final StorageManager storageManager;

   private final PagingManager pagingManager;

   private final java.util.Queue<MessageReference> deliveringRefs = new ConcurrentLinkedQueue<MessageReference>();

   private final Channel channel;

   private final Channel replicatingChannel;

   private volatile boolean closed;

   private final boolean preAcknowledge;

   private final ManagementService managementService;

   private final Binding binding;

   // Constructors ---------------------------------------------------------------------------------

   public ServerConsumerImpl(final long id,
                             final long replicatedSessionID,
                             final ServerSession session,
                             final QueueBinding binding,
                             final Filter filter,
                             final boolean started,
                             final boolean browseOnly,
                             final StorageManager storageManager,
                             final PagingManager pagingManager,
                             final Channel channel,
                             final Channel replicatingChannel,
                             final boolean preAcknowledge,
                             final boolean updateDeliveries,
                             final Executor executor,
                             final ManagementService managementService) throws Exception
   {
      this.id = id;

      this.replicatedSessionID = replicatedSessionID;

      this.filter = filter;

      this.session = session;

      this.binding = binding;

      this.messageQueue = binding.getQueue();

      this.executor = executor;

      this.started = browseOnly || started;

      this.browseOnly = browseOnly;

      this.storageManager = storageManager;

      this.channel = channel;

      this.replicatingChannel = replicatingChannel;

      this.preAcknowledge = preAcknowledge;

      this.pagingManager = pagingManager;

      this.managementService = managementService;

      binding.getQueue().addConsumer(this);

      minLargeMessageSize = session.getMinLargeMessageSize();

      this.updateDeliveries = updateDeliveries;
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

      messageQueue.removeConsumer(this);

      session.removeConsumer(this);

      // log.info(System.identityHashCode(this) + " consumer close");

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

         Notification notification = new Notification(NotificationType.CONSUMER_CLOSED, props);

         managementService.sendNotification(notification);
      }

      // log.info("closed consumer with id " + id);
   }

   public LinkedList<MessageReference> cancelRefs(final boolean lastConsumedAsDelivered, final Transaction tx) throws Exception
   {
      boolean performACK = lastConsumedAsDelivered;

      LinkedList<MessageReference> refs = new LinkedList<MessageReference>();

      // log.info(System.identityHashCode(this) + " cancelling refs");

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
                                            ", messageId " +
                                            messageID +
                                            " backup " +
                                            messageQueue.isBackup() +
                                            " closed " +
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

      // Expiries can come in our of sequence with respect to delivery order

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

      if (ref == null)
      {
         throw new IllegalStateException("Could not find reference with id " + messageID +
                                         " backup " +
                                         messageQueue.isBackup() +
                                         " closed " +
                                         closed);
      }

      return ref;
   }

   public void deliverReplicated(final long messageID) throws Exception
   {
      MessageReference ref = messageQueue.removeFirstReference(id);
      
      if (ref == null)
      {
         throw new IllegalStateException("Cannot find ref when replicating delivery " + messageID +
                                         " queue" +
                                         messageQueue.getName());
      }

      // We call doHandle rather than handle, since we don't want to check available credits
      // This is because delivery and receive credits can be processed in different order on live
      // and backup, and otherwise we could have a situation where the delivery is replicated
      // but the credits haven't arrived yet, so the delivery gets rejected on backup
      HandleStatus handled = doHandle(ref);

      if (handled != HandleStatus.HANDLED)
      {
         throw new IllegalStateException("Reference " + ref +
                                         " was not handled on backup node, handleStatus = " +
                                         handled);
      }
   }

   public void failedOver()
   {
      if (messageQueue.consumerFailedOver())
      {
         if (started)
         {
            promptDelivery();
         }
      }
   }

   public void lock()
   {
      lock.lock();
   }

   public void unlock()
   {
      lock.unlock();
   }

   // Public ---------------------------------------------------------------------------------------

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
            session.promptDelivery(messageQueue);
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
      if (messageQueue.isBackup())
      {
         // We are supposed to finish largeMessageDeliverer, or use all the possible credits before we return this
         // method.
         // If we play the commands on a different order than how they were generated on the live node, we will
         // eventually still be running this largeMessage before the next message come, what would reject messages
         // from the cluster
         largeMessageDeliverer.deliver();
      }
      else
      {
         executor.execute(resumeLargeMessageRunnable);
      }
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
            if (messageQueue.isBackup())
            {
               log.warn("doHandle: rejecting message while send is pending, ignoring reference = " + ref +
                        " backup = " +
                        messageQueue.isBackup());
            }

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

      if (replicatingChannel == null)
      {
         // it doesn't need lock because deliverLargeMesasge is already inside the lock.lock()
         largeMessageDeliverer = localDeliverer;
         largeMessageDeliverer.deliver();
      }
      else
      {
         Packet replPacket = new SessionReplicateDeliveryMessage(id, message.getMessageID());
         replPacket.setChannelID(channel.getID());

         replicatingChannel.replicatePacket(replPacket, replicatedSessionID, new Runnable()
         {
            public void run()
            {
               // setting & unsetting largeMessageDeliver is done inside the lock,
               // so this needs to be locked
               lock.lock();
               try
               {
                  largeMessageDeliverer = localDeliverer;
                  if (largeMessageDeliverer.deliver())
                  {
                     promptDelivery();
                  }
               }
               finally
               {
                  lock.unlock();
               }
            }
         });
      }

   }

   /**
    * @param ref
    * @param message
    */
   private void deliverStandardMessage(final MessageReference ref, final ServerMessage message)
   {
      if (availableCredits != null)
      {
         availableCredits.addAndGet(-message.getEncodeSize());
      }

      final SessionReceiveMessage packet = new SessionReceiveMessage(id, message, ref.getDeliveryCount());

      if (replicatingChannel == null)
      {
         // Not replicated - just send now

         channel.send(packet);
      }
      else
      {
         Packet replPacket = new SessionReplicateDeliveryMessage(id, message.getMessageID());
         replPacket.setChannelID(channel.getID());

//         log.info("replicating delivery from live for queue " + messageQueue.getName() +
//                  " ref " +
//                  message.getMessageID() +
//                  " session name " +
//                  session.getName());

         replicatingChannel.replicatePacket(replPacket, replicatedSessionID, new Runnable()
         {
            public void run()
            {
//               log.info("got replicate delivery response " + messageQueue.getName() +
//                        " ref " +
//                        message.getMessageID() +
//                        " session name " +
//                        session.getName());
               channel.send(packet);
            }
         });
      }
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
               // prompt Delivery only if chunk was finished
               session.promptDelivery(messageQueue);
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

         sizePendingLargeMessage = pendingLargeMessage.getBodySize();

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

               MessagingBuffer headerBuffer = ChannelBuffers.buffer(pendingLargeMessage.getPropertiesEncodeSize());

               pendingLargeMessage.encodeProperties(headerBuffer);

               initialMessage = new SessionReceiveMessage(id, headerBuffer.array(), ref.getDeliveryCount());
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
               if (precalculateAvailableCredits <= 0)
               {
                  if (trace)
                  {
                     trace("deliverLargeMessage: Leaving loop of send LargeMessage because of credits, backup = " + messageQueue.isBackup());
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
                        availableCredits +
                        " isBackup = " +
                        messageQueue.isBackup());
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
               trace("Finished deliverLargeMessage isBackup = " + messageQueue.isBackup());
            }

            pendingLargeMessage.releaseResources();

            if (preAcknowledge && !browseOnly)
            {
               // We added a reference for pre-ack, to avoid deleting the file before it was delivered
               if (pendingLargeMessage.decrementRefCount() == 0)
               {
                  // On pre-acks for Large messages, the decrement was deferred to large-message, hence we need to
                  // subtract the size inside largeMessage
                  try
                  {
                     PagingStore store = pagingManager.getPageStore(binding.getAddress());
                     store.addSize(-pendingLargeMessage.getMemoryEstimate());
                  }
                  catch (Exception e)
                  {
                     // This shouldn't happen on getPageStore
                     log.error("Error getting pageStore", e);
                  }
               }
            }

            largeMessageDeliverer = null;

            pendingLargeMessagesCounter.decrementAndGet();

            return true;
         }
         finally
         {
            lock.unlock();
         }
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
               return precalculatedCredits;
            }
         }
      }

      private SessionReceiveContinuationMessage createChunkSend()
      {
         SessionReceiveContinuationMessage chunk;

         int localChunkLen = 0;

         localChunkLen = (int)Math.min(sizePendingLargeMessage - positionPendingLargeMessage, minLargeMessageSize);

         MessagingBuffer bodyBuffer = ChannelBuffers.buffer(localChunkLen);

         pendingLargeMessage.encodeBody(bodyBuffer, positionPendingLargeMessage, localChunkLen);

         chunk = new SessionReceiveContinuationMessage(id,
                                                       bodyBuffer.array(),
                                                       positionPendingLargeMessage + localChunkLen < sizePendingLargeMessage,
                                                       false);

         return chunk;
      }
   }
}
