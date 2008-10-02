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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReceiveMessage;
import org.jboss.messaging.core.server.HandleStatus;
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

   private final ServerSession session;

   private final Object startStopLock = new Object();

   private final AtomicInteger availableCredits;

   private boolean started;

   private final StorageManager storageManager;

   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;

   private final PostOffice postOffice;

   private final java.util.Queue<MessageReference> deliveringRefs = new ConcurrentLinkedQueue<MessageReference>();

   private final Channel channel;
   
   private volatile CountDownLatch waitingLatch;
   
   // Constructors
   // ---------------------------------------------------------------------------------

   public ServerConsumerImpl(final long id,
                             final ServerSession session,
                             final Queue messageQueue,
                             final Filter filter,
                             final boolean enableFlowControl,
                             final int maxRate,
                             final boolean started,
                             final StorageManager storageManager,
                             final HierarchicalRepository<QueueSettings> queueSettingsRepository,
                             final PostOffice postOffice,
                             final Channel channel)
   {
      this.id = id;

      this.messageQueue = messageQueue;

      this.filter = filter;

      this.session = session;

      this.started = started;

      if (enableFlowControl)
      {
         availableCredits = new AtomicInteger(0);
      }
      else
      {
         availableCredits = null;
      }

      this.storageManager = storageManager;

      this.queueSettingsRepository = queueSettingsRepository;

      this.postOffice = postOffice;

      this.channel = channel;

      messageQueue.addConsumer(this);
   }

   // ServerConsumer implementation
   // ----------------------------------------------------------------------

   public long getID()
   {
      return id;
   }

   public HandleStatus handle(final MessageReference ref) throws Exception
   {
      if (availableCredits != null && availableCredits.get() <= 0)
      {       
         return HandleStatus.BUSY;
      }

      if (ref.getMessage().isExpired())
      {
         ref.expire(storageManager, postOffice, queueSettingsRepository);

         return HandleStatus.HANDLED;
      }

      synchronized (startStopLock)
      {
         // If the consumer is stopped then we don't accept the message, it
         // should go back into the
         // queue for delivery later.
         if (!started)
         {
            return HandleStatus.BUSY;
         }

         ServerMessage message = ref.getMessage();

         if (filter != null && !filter.match(message))
         {
            return HandleStatus.NO_MATCH;
         }

         if (availableCredits != null)
         {
            availableCredits.addAndGet(-message.getEncodeSize());
         }

         deliveringRefs.add(ref);
         
         SessionReceiveMessage packet = new SessionReceiveMessage(id, ref.getMessage(), ref.getDeliveryCount() + 1);

         channel.send(packet);

         return HandleStatus.HANDLED;
      }
   }
   
   public void close() throws Exception
   {     
      setStarted(false);
      
      if (waitingLatch != null)
      {                
         waitingLatch.countDown();
      }

      messageQueue.removeConsumer(this);

      session.removeConsumer(this);

      cancelRefs();
   }

   public void cancelRefs() throws Exception
   {
      if (!deliveringRefs.isEmpty())
      {
         Transaction tx = new TransactionImpl(storageManager, postOffice);

         for (MessageReference ref : deliveringRefs)
         {
            tx.addAcknowledgement(ref);
         }

         deliveringRefs.clear();

         tx.rollback(queueSettingsRepository);
      }
   }

   public void setStarted(final boolean started)
   {
      synchronized (startStopLock)
      {
         this.started = started;
      }

      // Outside the lock
      if (started)
      {
         promptDelivery();
      }
   }

   public void receiveCredits(final int credits) throws Exception
   {
      if (availableCredits != null)
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
 
   public MessageReference waitForReference(final long messageID) throws Exception
   {
      if (messageQueue.isBackup())
      {
         waitingLatch = new CountDownLatch(1);
                                 
         MessageReference ref = messageQueue.waitForReferenceWithID(messageID, waitingLatch);
         
         waitingLatch = null;
         
         return ref;
      }
      else
      {
         MessageReference ref = deliveringRefs.poll();

         if (ref.getMessage().getMessageID() != messageID)
         {
            throw new IllegalStateException("Invalid order");
         }

         return ref;
      }
   }

   public void failedOver()
   {
      synchronized (startStopLock)
      {
         started = true;
      }

      if (messageQueue.consumerFailedOver())
      {
         promptDelivery();
      }
   }

   // Public
   // -----------------------------------------------------------------------------

   // Private
   // --------------------------------------------------------------------------------------

   private void promptDelivery()
   {
      session.promptDelivery(messageQueue);
   }

   // Inner classes
   // ------------------------------------------------------------------------

}
