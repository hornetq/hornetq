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

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionDeliveryCompleteMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReceiveMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReplicateDeliveryMessage;
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

import java.util.Iterator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Concrete implementation of a ClientConsumer. 
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
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

   protected final Filter filter;

   protected final ServerSession session;

   private final Object startStopLock = new Object();

   protected final AtomicInteger availableCredits;

   private boolean started;

   private final StorageManager storageManager;

   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;

   private final PostOffice postOffice;

   private final java.util.Queue<MessageReference> deliveringRefs = new ConcurrentLinkedQueue<MessageReference>();

   protected final Channel channel;

   private boolean browseOnly;

   private Iterator<MessageReference> iterator;

   private DeliveryRunner deliveryRunner = new DeliveryRunner();

   private AtomicBoolean waitingToDeliver = new AtomicBoolean(false);

   private boolean delivering = false;
   
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
                             final Channel channel,
                             final boolean browseOnly)
   {
      this.id = id;

      this.browseOnly = browseOnly;

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
      
      if(!browseOnly)
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
      if (availableCredits != null && availableCredits.get() <= 0)
      {       
         return HandleStatus.BUSY;
      }
      
      final ServerMessage message = ref.getMessage();

      if (message.isExpired())
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

         if (filter != null && !filter.match(message))
         {
            return HandleStatus.NO_MATCH;
         }

         if (availableCredits != null)
         {
            availableCredits.addAndGet(-message.getEncodeSize());
         }
         
         final SessionReceiveMessage packet = new SessionReceiveMessage(id, message, ref.getDeliveryCount() + 1);
         
         Runnable run = new Runnable()
         {
            public void run()
            {
               deliveringRefs.add(ref);
                                            
               channel.send(packet);
            }
         };
         
         channel.replicatePacket(new SessionReplicateDeliveryMessage(id, message.getMessageID()), run);
        
         return HandleStatus.HANDLED;
      }
   }
   
   public void close() throws Exception
   {     
      setStarted(false);
      
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
 
   public MessageReference getReference(final long messageID) throws Exception
   {     
      MessageReference ref = deliveringRefs.poll();
            
      return ref;      
   }
   
   public void deliver(final long messageID) throws Exception
   {
      MessageReference ref = messageQueue.removeReferenceWithID(messageID);
         
      if (ref == null)
      {
         throw new IllegalStateException("Cannot find ref to deliver " + ref);
      }
      
      HandleStatus status = handle(ref);
      
      if (status != HandleStatus.HANDLED)
      {
         throw new IllegalStateException("ref " + ref + " was not handled");
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

   public void stop() throws Exception
   {
      delivering = false;
   }

   public void start()
   {
      iterator = getQueue().list(filter).iterator();
      delivering = true;
      promptDelivery();
   }

   public void deliver(Executor executor)
   {
      if (delivering && waitingToDeliver.compareAndSet(false, true))
      {
         executor.execute(deliveryRunner);
      }
   }

   // Public
   // -----------------------------------------------------------------------------

   // Private
   // --------------------------------------------------------------------------------------

   private void promptDelivery()
   {
      if(browseOnly)
      {
         session.promptDelivery(this);
      }
      else
      {
         session.promptDelivery(messageQueue);
      }
   }

   // Inner classes
   // ------------------------------------------------------------------------
      private class DeliveryRunner implements Runnable
   {
      public void run()
      {

         waitingToDeliver.set(false);

         synchronized (ServerConsumerImpl.this)
         {
            while (delivering && iterator.hasNext() && !(availableCredits != null && availableCredits.get() <= 0))
            {
               MessageReference ref = iterator.next();
               if (availableCredits != null)
               {
                  availableCredits.addAndGet(-ref.getMessage().getEncodeSize());
               }
               channel.send(new SessionReceiveMessage(id, ref.getMessage(), 1));
            }
            //inform the client there are no more messages
            if(!iterator.hasNext() || !delivering)
            {
               channel.send(new SessionDeliveryCompleteMessage(id));
               iterator = null;
            }
         }

      }
   }
}
