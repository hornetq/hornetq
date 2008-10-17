/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.core.server.impl;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;
import org.jboss.messaging.util.SimpleString;

/**
 * Implementation of a MessageReference
 *
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @version <tt>1.3</tt>
 *
 * MessageReferenceImpl.java,v 1.3 2006/02/23 17:45:57 timfox Exp
 */
public class MessageReferenceImpl implements MessageReference
{
   private static final Logger log = Logger.getLogger(MessageReferenceImpl.class);

   // Attributes ----------------------------------------------------

   private volatile int deliveryCount;

   private long scheduledDeliveryTime;

   private ServerMessage message;

   private Queue queue;

   // Constructors --------------------------------------------------

   public MessageReferenceImpl(final MessageReferenceImpl other, final Queue queue)
   {
      this.deliveryCount = other.deliveryCount;

      this.scheduledDeliveryTime = other.scheduledDeliveryTime;

      this.message = other.message;

      this.queue = queue;
   }

   protected MessageReferenceImpl(final ServerMessage message, final Queue queue)
   {
      this.message = message;

      this.queue = queue;
   }

   // MessageReference implementation -------------------------------

   public MessageReference copy(final Queue queue)
   {
      return new MessageReferenceImpl(this, queue);
   }

   public int getDeliveryCount()
   {
      return deliveryCount;
   }

   public void setDeliveryCount(final int deliveryCount)
   {
      this.deliveryCount = deliveryCount;
   }

   public void incrementDeliveryCount()
   {
      deliveryCount++;
   }

   public long getScheduledDeliveryTime()
   {
      return scheduledDeliveryTime;
   }

   public void setScheduledDeliveryTime(final long scheduledDeliveryTime)
   {
      this.scheduledDeliveryTime = scheduledDeliveryTime;
   }

   public ServerMessage getMessage()
   {
      return message;
   }

   public Queue getQueue()
   {
      return queue;
   }

   public boolean cancel(final StorageManager persistenceManager,
                         final PostOffice postOffice,
                         final HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
   {
      if (message.isDurable() && queue.isDurable())
      {
         persistenceManager.updateDeliveryCount(this);
      }
      
      QueueSettings queueSettings = queueSettingsRepository.getMatch(queue.getName().toString());
      int maxDeliveries = queueSettings.getMaxDeliveryAttempts();

      if (maxDeliveries > 0 && deliveryCount >= maxDeliveries)
      {
         log.warn("Message has reached maximum delivery attempts, sending it to DLQ");
         sendToDLQ(persistenceManager, postOffice, queueSettingsRepository);

         return false;
      }
      else
      {
         long redeliveryDelay = queueSettings.getRedeliveryDelay();
         
         if (redeliveryDelay > 0)
         {
            scheduledDeliveryTime = System.currentTimeMillis() + redeliveryDelay;
            persistenceManager.storeMessageReferenceScheduled(queue.getPersistenceID(), message.getMessageID(), scheduledDeliveryTime);
         }
         queue.referenceCancelled();

         return true;
      }
   }

   public void sendToDLQ(final StorageManager persistenceManager,
                         final PostOffice postOffice,
                         final HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
   {
      SimpleString dlq = queueSettingsRepository.getMatch(queue.getName().toString()).getDLQ();

      if (dlq != null)
      {
         Binding dlqBinding = postOffice.getBinding(dlq);

         if (dlqBinding == null)
         {
            dlqBinding = postOffice.addBinding(dlq, dlq, null, true, false);
         }

         move(dlqBinding, persistenceManager, postOffice, false);
      }
      else
      {
         throw new IllegalStateException("No DLQ configured for queue " + queue.getName() + ", so dropping it");
      }
   }

   public void expire(final StorageManager persistenceManager,
                      final PostOffice postOffice,
                      final HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
   {
      SimpleString expiryQueue = queueSettingsRepository.getMatch(queue.getName().toString()).getExpiryQueue();

      if (expiryQueue != null)
      {
         Binding expiryBinding = postOffice.getBinding(expiryQueue);

         if (expiryBinding == null)
         {
            expiryBinding = postOffice.addBinding(expiryQueue, expiryQueue, null, true, false);
         }

         move(expiryBinding, persistenceManager, postOffice, true);
      }
      else
      {
         log.warn("Message has expired, no expiry queue is configured so dropping it");

         Transaction tx = new TransactionImpl(persistenceManager, postOffice);
         tx.addAcknowledgement(this);
         tx.commit();
      }

   }

   public void move(final Binding otherBinding, final StorageManager persistenceManager, final PostOffice postOffice) throws Exception
   {
      move(otherBinding, persistenceManager, postOffice, false);
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "Reference[" + getMessage().getMessageID() +
             "]:" +
             (getMessage().isDurable() ? "RELIABLE" : "NON-RELIABLE");
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void move(final Binding otherBinding,
                     final StorageManager persistenceManager,
                     final PostOffice postOffice,
                     final boolean expiry) throws Exception
   {
      Transaction tx = new TransactionImpl(persistenceManager, postOffice);

      ServerMessage copyMessage = makeCopy(expiry, persistenceManager);
      copyMessage.setDestination(otherBinding.getAddress());

      tx.addMessage(copyMessage);

      tx.addAcknowledgement(this);

      tx.commit();
   }

   private ServerMessage makeCopy(final boolean expiry, final StorageManager pm) throws Exception
   {
      /*
       We copy the message and send that to the dlq/expiry queue - this is
       because otherwise we may end up with a ref with the same message id in the
       queue more than once which would barf - this might happen if the same message had been
       expire from multiple subscriptions of a topic for example
       We set headers that hold the original message destination, expiry time
       and original message id
      */

      ServerMessage copy = message.copy();

      // FIXME - this won't work with replication!!!!!!!!!!!
      long newMessageId = pm.generateUniqueID();

      copy.setMessageID(newMessageId);

      SimpleString originalQueue = copy.getDestination();
      copy.putStringProperty(MessageImpl.HDR_ORIGIN_QUEUE, originalQueue);

      // reset expiry
      copy.setExpiration(0);
      if (expiry)
      {
         long actualExpiryTime = System.currentTimeMillis();

         copy.putLongProperty(MessageImpl.HDR_ACTUAL_EXPIRY_TIME, actualExpiryTime);
      }

      return copy;
   }

   // Inner classes -------------------------------------------------

}