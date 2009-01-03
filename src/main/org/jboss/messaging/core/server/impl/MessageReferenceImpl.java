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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Bindings;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.TransactionOperation;
import org.jboss.messaging.core.transaction.TransactionPropertyIndexes;
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
      // from few tests I have done, deliveryCount and scheduledDelivery will use two longs (because of alignment)
      // and each of the references (messages and queue) will use the equivalent to two longs (because of long
      // pointers).
      // Anyway.. this is just an estimate

      // TODO - doesn't the object itself have an overhead? - I thought was usually one Long per Object?
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

   private boolean cancel(final StorageManager storageManager,
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

   public void sendToDeadLetterAddress(final StorageManager storageManager,
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
            move(deadLetterAddress, storageManager, postOffice, queueSettingsRepository, false);
         }
      }
      else
      {
         log.warn("Message has exceeded max delivery attempts. No Dead Letter Address configured for queue " + queue.getName() +
                  " so dropping it");

         Transaction tx = new TransactionImpl(storageManager, postOffice);

         acknowledge(tx, storageManager, postOffice, queueSettingsRepository);

         tx.commit();
      }
   }

   public void expire(final StorageManager storageManager,
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
            move(expiryAddress, storageManager, postOffice, queueSettingsRepository, true);
         }
      }
      else
      {
         log.warn("Message has expired. No expiry queue configured for queue " + queue.getName() + " so dropping it");

         Transaction tx = new TransactionImpl(storageManager, postOffice);

         acknowledge(tx, storageManager, postOffice, queueSettingsRepository);

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
            move(expiryAddress, tx, storageManager, postOffice, queueSettingsRepository, true);
         }
      }
      else
      {
         log.warn("Message has expired. No expiry queue configured for queue " + queue.getName() + " so dropping it");

         acknowledge(tx, storageManager, postOffice, queueSettingsRepository);
      }
   }

   public void move(final SimpleString toAddress,
                    final StorageManager storageManager,
                    final PostOffice postOffice,
                    final HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
   {
      move(toAddress, storageManager, postOffice, queueSettingsRepository, false);
   }

   public void move(final SimpleString toAddress,
                    final Transaction tx,
                    final StorageManager storageManager,
                    final PostOffice postOffice,
                    final HierarchicalRepository<QueueSettings> queueSettingsRepository,
                    final boolean expiry) throws Exception
   {
      ServerMessage copyMessage = makeCopy(expiry, storageManager);

      copyMessage.setDestination(toAddress);

      postOffice.route(copyMessage, tx);

      acknowledge(tx, storageManager, postOffice, queueSettingsRepository);
   }

   public void acknowledge(final Transaction tx,
                           final StorageManager storageManager,
                           final PostOffice postOffice,
                           final HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
   {
      if (message.isDurable() && queue.isDurable())
      {
         // Need to lock on the message to prevent a race where the ack and
         // delete
         // records get recorded in the log in the wrong order

         // TODO For now - we just use synchronized - can probably do better
         // locking

         synchronized (message)
         {
            int count = message.decrementDurableRefCount();
            
            if (count == 0)
            {
               storageManager.deleteMessageTransactional(tx.getID(), queue.getPersistenceID(), message.getMessageID());
            }
            else
            {
               storageManager.storeAcknowledgeTransactional(tx.getID(),
                                                            queue.getPersistenceID(),
                                                            message.getMessageID());
            }

            tx.setContainsPersistent(true);
         }
      }

      tx.addOperation(new AcknowledgeOperation(storageManager, postOffice, queueSettingsRepository));
   }

   public void cancel(final Transaction tx,
                      final StorageManager storageManager,
                      final PostOffice postOffice,
                      final HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
   { 
      message.decrementDurableRefCount();
      
      tx.addOperation(new AcknowledgeOperation(storageManager, postOffice, queueSettingsRepository));
   }

   public void reacknowledge(final Transaction tx,
                             final StorageManager storageManager,
                             final PostOffice postOffice,
                             final HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
   {
      if (message.isDurable() && queue.isDurable())
      {
         tx.setContainsPersistent(true);
      }

      tx.addOperation(new AcknowledgeOperation(storageManager, postOffice, queueSettingsRepository));
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
                     final StorageManager storageManager,
                     final PostOffice postOffice,
                     final HierarchicalRepository<QueueSettings> queueSettingsRepository,
                     final boolean expiry) throws Exception
   {
      Transaction tx = new TransactionImpl(storageManager, postOffice);

      // FIXME: JBMESSAGING-1468
      ServerMessage copyMessage = makeCopy(expiry, storageManager);

      copyMessage.setDestination(address);

      postOffice.route(copyMessage, tx);

      acknowledge(tx, storageManager, postOffice, queueSettingsRepository);

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

   private class AcknowledgeOperation implements TransactionOperation
   {
      final StorageManager storageManager;

      final PostOffice postOffice;

      final HierarchicalRepository<QueueSettings> queueSettingsRepository;

      AcknowledgeOperation(final StorageManager storageManager,
                           final PostOffice postOffice,
                           final HierarchicalRepository<QueueSettings> queueSettingsRepository)
      {
         this.storageManager = storageManager;

         this.postOffice = postOffice;

         this.queueSettingsRepository = queueSettingsRepository;
      }

      public void afterCommit(final Transaction tx) throws Exception
      {
      }

      public void afterPrepare(final Transaction tx) throws Exception
      {
      }

      public void afterRollback(final Transaction tx) throws Exception
      {         
         if (message.isDurable() && queue.isDurable())
         {
            message.incrementDurableRefCount();
         }

         if (cancel(storageManager, postOffice, queueSettingsRepository))
         {
            Map<Queue, LinkedList<MessageReference>> queueMap = (Map<Queue, LinkedList<MessageReference>>)tx.getProperty(TransactionPropertyIndexes.QUEUE_MAP_INDEX);

            if (queueMap == null)
            {
               queueMap = new HashMap<Queue, LinkedList<MessageReference>>();

               tx.putProperty(TransactionPropertyIndexes.QUEUE_MAP_INDEX, queueMap);
            }

            Queue queue = MessageReferenceImpl.this.getQueue();

            LinkedList<MessageReference> toCancel = queueMap.get(queue);

            if (toCancel == null)
            {
               toCancel = new LinkedList<MessageReference>();

               queueMap.put(queue, toCancel);
            }

            toCancel.add(MessageReferenceImpl.this);

            AtomicInteger rollbackCount = (AtomicInteger)tx.getProperty(TransactionPropertyIndexes.ROLLBACK_COUNTER_INDEX);

            if (rollbackCount.decrementAndGet() == 0)
            {
               for (Map.Entry<Queue, LinkedList<MessageReference>> entry : queueMap.entrySet())
               {
                  LinkedList<MessageReference> refs = entry.getValue();
                 
                  entry.getKey().addListFirst(refs);
               }
            }
         }
      }

      public void beforeCommit(final Transaction tx) throws Exception
      {
         queue.referenceAcknowledged(MessageReferenceImpl.this);
      }

      public void beforePrepare(final Transaction tx) throws Exception
      {
      }

      public void beforeRollback(final Transaction tx) throws Exception
      {
         AtomicInteger rollbackCount = (AtomicInteger)tx.getProperty(TransactionPropertyIndexes.ROLLBACK_COUNTER_INDEX);

         if (rollbackCount == null)
         {
            rollbackCount = new AtomicInteger(0);

            tx.putProperty(TransactionPropertyIndexes.ROLLBACK_COUNTER_INDEX, rollbackCount);
         }

         rollbackCount.incrementAndGet();
      }

   }

}