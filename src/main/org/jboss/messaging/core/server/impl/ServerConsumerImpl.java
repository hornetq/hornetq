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

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.DelayedResult;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.NullResponseMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReceiveContinuationMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReceiveMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReplicateDeliveryMessage;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.LargeServerMessage;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerConsumer;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.ServerSession;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;

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
   // Constants
   // ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ServerConsumerImpl.class);

   // Static
   // ---------------------------------------------------------------------------------------

   // Attributes
   // -----------------------------------------------------------------------------------

   private final boolean trace = log.isTraceEnabled();

   private final long id;

   private final Queue messageQueue;

   private final Filter filter;

   private final int minLargeMessageSize;

   private final ServerSession session;

   private final Lock lock = new ReentrantLock();

   private AtomicInteger availableCredits = new AtomicInteger(0);

   private boolean started;

   private volatile LargeMessageSender largeMessageSender = null;

   /**
    * if we are a browse only consumer we don't need to worry about acknowledgemenets or being started/stopeed by the session.
    */
   private final boolean browseOnly;

   private final StorageManager storageManager;

   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;

   private final PostOffice postOffice;

   private final java.util.Queue<MessageReference> deliveringRefs = new ConcurrentLinkedQueue<MessageReference>();

   private final Channel channel;

   private volatile boolean closed;

   private final boolean preAcknowledge;

   // Constructors
   // ---------------------------------------------------------------------------------

   public ServerConsumerImpl(final long id,
                             final ServerSession session,
                             final Queue messageQueue,
                             final Filter filter,
                             final boolean started,
                             final boolean browseOnly,
                             final StorageManager storageManager,
                             final HierarchicalRepository<QueueSettings> queueSettingsRepository,
                             final PostOffice postOffice,
                             final Channel channel,
                             final boolean preAcknowledge)
   {
      this.id = id;

      this.messageQueue = messageQueue;

      this.filter = filter;

      this.session = session;

      this.started = browseOnly || started;

      this.browseOnly = browseOnly;

      this.storageManager = storageManager;

      this.queueSettingsRepository = queueSettingsRepository;

      this.postOffice = postOffice;

      this.channel = channel;

      this.preAcknowledge = preAcknowledge;

      messageQueue.addConsumer(this);

      minLargeMessageSize = session.getMinLargeMessageSize();
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

   public void handleClose(final Packet packet)
   {
      // We must stop delivery before replicating the packet, this ensures the close message gets processed
      // and replicated on the backup in the same order as any delivery that might be occuring gets
      // processed and replicated on the backup.
      // Otherwise we could end up with a situation where a close comes in, then a delivery comes in,
      // then close gets replicated to backup, then delivery gets replicated, but consumer is already
      // closed!
      setStarted(false);

      DelayedResult result = channel.replicatePacket(packet);

      if (result != null)
      {
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               doHandleClose(packet);
            }
         });
      }
      else
      {
         doHandleClose(packet);
      }
   }

   private void doHandleClose(final Packet packet)
   {
      Packet response = null;

      try
      {
         doClose();

         response = new NullResponseMessage();
      }
      catch (Exception e)
      {
         log.error("Failed to close producer", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }

      channel.confirm(packet);

      channel.send(response);
   }

   public void close() throws Exception
   {
      setStarted(false);

      doClose();
   }

   private void doClose() throws Exception
   {
      messageQueue.removeConsumer(this);

      session.removeConsumer(this);

      LinkedList<MessageReference> refs = cancelRefs();

      Iterator<MessageReference> iter = refs.iterator();

      closed = true;
      
      Transaction tx = new TransactionImpl(storageManager, postOffice);

      while (iter.hasNext())
      {
         MessageReference ref = iter.next();
         
         ref.cancel(tx, storageManager, postOffice, queueSettingsRepository);         
      }
      
      tx.rollback();
   }

   public LinkedList<MessageReference> cancelRefs() throws Exception
   {
      LinkedList<MessageReference> refs = new LinkedList<MessageReference>();

      if (!deliveringRefs.isEmpty())
      {
         for (MessageReference ref : deliveringRefs)
         {
            refs.add(ref);
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
            throw new IllegalStateException("Could not find reference on consumerID=" + id +
                                            ", messageId " +
                                            messageID +
                                            " backup " +
                                            messageQueue.isBackup() +
                                            " closed " +
                                            closed);
         }

         if (autoCommitAcks)
         {
            doAck(ref);
         }
         else
         {
            ref.acknowledge(tx, storageManager, postOffice, queueSettingsRepository);

            // Del count is not actually updated in storage unless it's
            // cancelled
            ref.incrementDeliveryCount();
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
      // It may not be the first in the queue - since there may be multiple producers
      // sending to the queue
      MessageReference ref = messageQueue.removeReferenceWithID(messageID);

      if (ref == null)
      {
         throw new IllegalStateException("Cannot find ref when replicating delivery " + messageID);
      }

      // We call doHandle rather than handle, since we don't want to check available credits
      // This is because delivery and receive credits can be processed in different order on live
      // and backup, and otherwise we could have a situation where the delivery is replicated
      // but the credits haven't arrived yet, so the delivery gets rejected on backup
      HandleStatus handled = doHandle(ref);

      if (handled != HandleStatus.HANDLED)
      {
         throw new IllegalStateException("Reference was not handled " + ref + " " + handled);
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

   // Public
   // -----------------------------------------------------------------------------

   // Private
   // --------------------------------------------------------------------------------------

   private void doAck(final MessageReference ref) throws Exception
   {
      ServerMessage message = ref.getMessage();

      Queue queue = ref.getQueue();

      if (message.isDurable() && queue.isDurable())
      {
         int count = message.decrementDurableRefCount();

         if (count == 0)
         {
            storageManager.deleteMessage(message.getMessageID());
         }
         else
         {
            storageManager.storeAcknowledge(queue.getPersistenceID(), message.getMessageID());
         }
      }

      queue.referenceAcknowledged(ref);
   }

   private void promptDelivery()
   {
      if (largeMessageSender != null)
      {
         if (largeMessageSender.sendLargeMessage())
         {
            // prompt Delivery only if chunk was finished
            session.promptDelivery(messageQueue);
         }
      }
      else
      {
         session.promptDelivery(messageQueue);
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
         // If there is a pendingLargeMessage we can't take another message
         // This has to be checked inside the lock as the set to null is done inside the lock
         if (largeMessageSender != null)
         {
            return HandleStatus.BUSY;
         }

         // If the consumer is stopped then we don't accept the message, it
         // should go back into the
         // queue for delivery later.
         if (!started)
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
         }
         
         if (preAcknowledge)
         {
            //With pre-ack, we ack *before* sending to the client
            doAck(ref);
         }
                  
         // TODO: get rid of the instanceof by something like message.isLargeMessage()
         if (message instanceof LargeServerMessage)
         {            
            //FIXME - please put the replication logic in the sendLargeMessage method
            
            DelayedResult result = channel.replicatePacket(new SessionReplicateDeliveryMessage(id,
                                                                                               message.getMessageID()));

            if (result == null)
            {
               sendLargeMessage(ref, message);
            }
            else
            {
               // Send when replicate delivery response comes back
               result.setResultRunner(new Runnable()
               {
                  public void run()
                  {
                     sendLargeMessage(ref, message);
                  }
               });
            }

         }
         else
         {
            sendStandardMessage(ref, message);
         }
         
         return HandleStatus.HANDLED;
      }
      finally
      {
         lock.unlock();
      }
   }

   private void sendLargeMessage(final MessageReference ref, final ServerMessage message)
   {
      largeMessageSender = new LargeMessageSender((LargeServerMessage)message, ref);

      largeMessageSender.sendLargeMessage();
   }

   /**
    * @param ref
    * @param message
    */
   private void sendStandardMessage(final MessageReference ref, final ServerMessage message)
   {
      if (availableCredits != null)
      {
         availableCredits.addAndGet(-message.getEncodeSize());
      }

      final SessionReceiveMessage packet = new SessionReceiveMessage(id, message, ref.getDeliveryCount() + 1);

      DelayedResult result = channel.replicatePacket(new SessionReplicateDeliveryMessage(id, message.getMessageID()));

      if (result == null)
      {
         // Not replicated - just send now

         channel.send(packet);
      }
      else
      {
         // Send when replicate delivery response comes back
         result.setResultRunner(new Runnable()
         {
            public void run()
            {
               channel.send(packet);
            }
         });
      }
   }

   // Inner classes
   // ------------------------------------------------------------------------

   /** Internal encapsulation of the logic on sending LargeMessages.
    *  This Inner class was created to avoid a bunch of loose properties about the current LargeMessage being sent*/
   private class LargeMessageSender
   {
      private final long sizePendingLargeMessage;

      /** The current message being processed */
      private final LargeServerMessage pendingLargeMessage;

      private final MessageReference ref;

      private volatile boolean sentFirstMessage = false;

      /** The current position on the message being processed */
      private volatile long positionPendingLargeMessage;

      private volatile SessionReceiveContinuationMessage readAheadChunk;

      public LargeMessageSender(final LargeServerMessage message, final MessageReference ref)
      {
         pendingLargeMessage = message;

         sizePendingLargeMessage = pendingLargeMessage.getBodySize();

         this.ref = ref;
      }

      public boolean sendLargeMessage()
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

            if (!sentFirstMessage)
            {
               sentFirstMessage = true;

               MessagingBuffer headerBuffer = new ByteBufferWrapper(ByteBuffer.allocate(pendingLargeMessage.getPropertiesEncodeSize()));

               pendingLargeMessage.encodeProperties(headerBuffer);

               SessionReceiveMessage initialMessage = new SessionReceiveMessage(id,
                                                                                headerBuffer.array(),
                                                                                ref.getDeliveryCount() + 1);

               channel.send(initialMessage);

               if (availableCredits != null)
               {
                  // RequiredBufferSize on this case represents the right number of bytes sent
                  availableCredits.addAndGet(-initialMessage.getRequiredBufferSize());
               }
            }

            if (readAheadChunk != null)
            {
               int chunkLen = readAheadChunk.getBody().length;

               positionPendingLargeMessage += chunkLen;

               if (availableCredits != null)
               {
                  availableCredits.addAndGet(-readAheadChunk.getRequiredBufferSize());
               }

               channel.send(readAheadChunk);

               readAheadChunk = null;
            }

            while (positionPendingLargeMessage < sizePendingLargeMessage)
            {
               if (availableCredits != null && availableCredits.get() <= 0)
               {
                  if (readAheadChunk == null)
                  {
                     readAheadChunk = createChunkSend();
                  }
                  return false;
               }

               SessionReceiveContinuationMessage chunk = createChunkSend();

               int chunkLen = chunk.getBody().length;

               if (availableCredits != null)
               {
                  availableCredits.addAndGet(-chunk.getRequiredBufferSize());
               }

               channel.send(chunk);

               positionPendingLargeMessage += chunkLen;
            }

            pendingLargeMessage.releaseResources();

            largeMessageSender = null;

            return true;
         }
         finally
         {
            lock.unlock();
         }
      }

      private SessionReceiveContinuationMessage createChunkSend()
      {
         SessionReceiveContinuationMessage chunk;

         int localChunkLen = 0;

         localChunkLen = (int)Math.min(sizePendingLargeMessage - positionPendingLargeMessage, minLargeMessageSize);

         MessagingBuffer bodyBuffer = new ByteBufferWrapper(ByteBuffer.allocate(localChunkLen));

         pendingLargeMessage.encodeBody(bodyBuffer, positionPendingLargeMessage, localChunkLen);

         chunk = new SessionReceiveContinuationMessage(id,
                                                       bodyBuffer.array(),
                                                       positionPendingLargeMessage + localChunkLen < sizePendingLargeMessage,
                                                       false);

         return chunk;
      }
   }
}
