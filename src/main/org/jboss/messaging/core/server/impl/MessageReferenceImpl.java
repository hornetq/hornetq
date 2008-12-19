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

import static org.jboss.messaging.core.message.impl.MessageImpl.HDR_ACTUAL_EXPIRY_TIME;
import static org.jboss.messaging.core.message.impl.MessageImpl.HDR_ORIGIN_QUEUE;
import static org.jboss.messaging.core.message.impl.MessageImpl.HDR_ORIG_MESSAGE_ID;

import java.util.List;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.Bindings;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;
import org.jboss.messaging.util.DataConstants;
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
   
   // Static --------------------------------------------------------
   


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

   public int getMemoryEstimate()
   {
      // from few tests I have done, deliveryCount and scheduledDelivery will use  two longs (because of alignment)
      // and each of the references (messages and queue) will use the equivalent to two longs (because of long pointers).
      // Anyway.. this is just an estimate
      return DataConstants.SIZE_LONG * 4;
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

   public boolean cancel(final StorageManager storageManager,
                         final PostOffice postOffice,
                         final HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
   {
      if (message.isDurable() && queue.isDurable())
      {
         storageManager.updateDeliveryCount(this);
      }

      QueueSettings queueSettings = queueSettingsRepository.getMatch(queue.getName().toString());
      int maxDeliveries = queueSettings.getMaxDeliveryAttempts();

      if (maxDeliveries > 0 && deliveryCount >= maxDeliveries)
      {
         log.warn("Message has reached maximum delivery attempts, sending it to Dead Letter Address");
         sendToDeadLetterAddress(storageManager, postOffice, queueSettingsRepository);

         return false;
      }
      else
      {
         long redeliveryDelay = queueSettings.getRedeliveryDelay();

         if (redeliveryDelay > 0)
         {
            scheduledDeliveryTime = System.currentTimeMillis() + redeliveryDelay;

            storageManager.updateScheduledDeliveryTime(this);
         }
         queue.referenceCancelled();

         return true;
      }
   }

   public void sendToDeadLetterAddress(final StorageManager persistenceManager,
                                       final PostOffice postOffice,
                                       final HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
   {
      SimpleString deadLetterAddress = queueSettingsRepository.getMatch(queue.getName().toString())
                                                              .getDeadLetterAddress();
      if (deadLetterAddress != null)
      {
         Bindings bindingList = postOffice.getBindingsForAddress(deadLetterAddress);
         
         if (bindingList.getBindings().isEmpty())
         {
            log.warn("Message has exceeded max delivery attempts. No bindings for Dead Letter Address " + deadLetterAddress +
                     " so dropping it");
         }
         else
         {
            move(deadLetterAddress, persistenceManager, postOffice, false);
         }
      }
      else
      {
         log.warn("Message has exceeded max delivery attempts. No Dead Letter Address configured for queue " + queue.getName() +
                  " so dropping it");

         Transaction tx = new TransactionImpl(persistenceManager, postOffice);
         tx.addAcknowledgement(this);
         tx.commit();
      }
   }

   public void expire(final StorageManager persistenceManager,
                      final PostOffice postOffice,
                      final HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
   {
      SimpleString expiryAddress = queueSettingsRepository.getMatch(queue.getName().toString()).getExpiryAddress();

      if (expiryAddress != null)
      {
         Bindings bindingList = postOffice.getBindingsForAddress(expiryAddress);
         
         if (bindingList.getBindings().isEmpty())
         {
            log.warn("Message has expired. No bindings for Expiry Address " + expiryAddress + " so dropping it");
         }
         else
         {
            move(expiryAddress, persistenceManager, postOffice, true);
         }
      }
      else
      {
         log.warn("Message has expired. No expiry queue configured for queue " + queue.getName() + " so dropping it");

         Transaction tx = new TransactionImpl(persistenceManager, postOffice);
         tx.addAcknowledgement(this);
         tx.commit();
      }

   }
   
   public void expire(final Transaction tx,
                      final StorageManager storageManager,
                      final PostOffice postOffice,
                      final HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
   {
      SimpleString expiryAddress = queueSettingsRepository.getMatch(queue.getName().toString()).getExpiryAddress();

      if (expiryAddress != null)
      {
         Bindings bindingList = postOffice.getBindingsForAddress(expiryAddress);
         
         if (bindingList.getBindings().isEmpty())
         {
            log.warn("Message has expired. No bindings for Expiry Address " + expiryAddress + " so dropping it");
         }
         else
         {
            move(expiryAddress, tx, storageManager, true);
         }
      }
      else
      {
         log.warn("Message has expired. No expiry queue configured for queue " + queue.getName() + " so dropping it");

         tx.addAcknowledgement(this);
      }
   }

   public void move(final SimpleString toAddress, final StorageManager persistenceManager, final PostOffice postOffice) throws Exception
   {
      move(toAddress, persistenceManager, postOffice, false);
   }
   
   public void move(final SimpleString toAddress, final Transaction tx, final StorageManager persistenceManager, final boolean expiry) throws Exception
   {
      ServerMessage copyMessage = makeCopy(expiry, persistenceManager);

      copyMessage.setDestination(toAddress);

      tx.addMessage(copyMessage);

      tx.addAcknowledgement(this);
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

   private void move(final SimpleString address,
                     final StorageManager persistenceManager,
                     final PostOffice postOffice,
                     final boolean expiry) throws Exception
   {
      Transaction tx = new TransactionImpl(persistenceManager, postOffice);

      // FIXME: JBMESSAGING-1468
      ServerMessage copyMessage = makeCopy(expiry, persistenceManager);

      copyMessage.setDestination(address);

      tx.addMessage(copyMessage);

      tx.addAcknowledgement(this);

      tx.commit();
   }

   private ServerMessage makeCopy(final boolean expiry, final StorageManager pm) throws Exception
   {
      /*
       We copy the message and send that to the dla/expiry queue - this is
       because otherwise we may end up with a ref with the same message id in the
       queue more than once which would barf - this might happen if the same message had been
       expire from multiple subscriptions of a topic for example
       We set headers that hold the original message destination, expiry time
       and original message id
      */

      ServerMessage copy = message.copy();

      // (JBMESSAGING-1468)
      // FIXME - this won't work with replication!!!!!!!!!!!
      // FIXME - this won't work with LargeMessages also!!!!
      long newMessageId = pm.generateUniqueID();

      copy.setMessageID(newMessageId);

      SimpleString originalQueue = copy.getDestination();
      copy.putStringProperty(HDR_ORIGIN_QUEUE, originalQueue);
      copy.putLongProperty(HDR_ORIG_MESSAGE_ID, message.getMessageID());

      // reset expiry
      copy.setExpiration(0);
      if (expiry)
      {
         long actualExpiryTime = System.currentTimeMillis();

         copy.putLongProperty(HDR_ACTUAL_EXPIRY_TIME, actualExpiryTime);
      }

      return copy;
   }

   // Inner classes -------------------------------------------------

}