/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.server.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.list.PriorityLinkedList;
import org.jboss.messaging.core.list.impl.PriorityLinkedListImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.Consumer;
import org.jboss.messaging.core.server.DistributionPolicy;
import org.jboss.messaging.core.server.HandleStatus;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ScheduledDeliveryHandler;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;
import org.jboss.messaging.util.SimpleString;

/**
 * Implementation of a Queue TODO use Java 5 concurrent queue
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="clebert.suconic@jboss.com">Clebert Suconic</a>
 */
public class QueueImpl implements Queue
{
   private static final Logger log = Logger.getLogger(QueueImpl.class);

   private static final boolean trace = log.isTraceEnabled();

   public static final int NUM_PRIORITIES = 10;

   private volatile long persistenceID = -1;

   private final SimpleString name;

   private volatile Filter filter;

   private final boolean clustered;

   private final boolean durable;

   private final boolean temporary;

   private final PostOffice postOffice;

   private final PriorityLinkedList<MessageReference> messageReferences = new PriorityLinkedListImpl<MessageReference>(NUM_PRIORITIES);

   private final ScheduledDeliveryHandler scheduledDeliveryHandler;

   private volatile DistributionPolicy distributionPolicy = new RoundRobinDistributionPolicy();

   private boolean direct;

   private boolean promptDelivery;

   private AtomicInteger sizeBytes = new AtomicInteger(0);

   private AtomicInteger messagesAdded = new AtomicInteger(0);

   private AtomicInteger deliveringCount = new AtomicInteger(0);

   private AtomicBoolean waitingToDeliver = new AtomicBoolean(false);

   private final Runnable deliverRunner = new DeliverRunner();

   private final PagingManager pagingManager;

   private volatile boolean backup;

   private int consumersToFailover = -1;

   public QueueImpl(final long persistenceID,
                    final SimpleString name,
                    final Filter filter,
                    final boolean clustered,
                    final boolean durable,
                    final boolean temporary,
                    final ScheduledExecutorService scheduledExecutor,
                    final PostOffice postOffice)
   {
      this.persistenceID = persistenceID;

      this.name = name;

      this.filter = filter;

      this.clustered = clustered;

      this.durable = durable;

      this.temporary = temporary;

      this.postOffice = postOffice;

      if (postOffice == null)
      {
         this.pagingManager = null;
      }
      else
      {
         this.pagingManager = postOffice.getPagingManager();
      }

      direct = true;

      scheduledDeliveryHandler = new ScheduledDeliveryHandlerImpl(scheduledExecutor);
   }

   // Queue implementation
   // -------------------------------------------------------------------
  
   public boolean isClustered()
   {
      return clustered;
   }

   public boolean isDurable()
   {
      return durable;
   }

   public boolean isTemporary()
   {
      return temporary;
   }

   public SimpleString getName()
   {
      return name;
   }

   public HandleStatus addLast(final MessageReference ref)
   {
      HandleStatus status = add(ref, false);

      return status;
   }

   public HandleStatus addFirst(final MessageReference ref)
   {
      return add(ref, true);
   }

   public synchronized void addListFirst(final LinkedList<MessageReference> list)
   {
      ListIterator<MessageReference> iter = list.listIterator(list.size());

      while (iter.hasPrevious())
      {
         MessageReference ref = iter.previous();

         ServerMessage msg = ref.getMessage();

         if (!scheduledDeliveryHandler.checkAndSchedule(ref, backup))
         {
            messageReferences.addFirst(ref, msg.getPriority());
         }
      }

      deliver();
   }

   public void deliverAsync(final Executor executor)
   {
      // Prevent too many executors running at once

      if (waitingToDeliver.compareAndSet(false, true))
      {
         executor.execute(deliverRunner);
      }
   }

   // Only used in testing - do not call directly!
   public synchronized void deliverNow()
   {
      deliver();
   }

   public void addConsumer(final Consumer consumer)
   {
      distributionPolicy.addConsumer(consumer);
   }

   public synchronized boolean removeConsumer(final Consumer consumer) throws Exception
   {
      boolean removed = distributionPolicy.removeConsumer(consumer);

      if (!distributionPolicy.hasConsumers())
      {
         promptDelivery = false;
      }

      return removed;
   }

   public synchronized int getConsumerCount()
   {
      return distributionPolicy.getConsumerCount();
   }

   public synchronized List<MessageReference> list(final Filter filter)
   {
      if (filter == null)
      {
         return new ArrayList<MessageReference>(messageReferences.getAll());
      }
      else
      {
         ArrayList<MessageReference> list = new ArrayList<MessageReference>();

         for (MessageReference ref : messageReferences.getAll())
         {
            if (filter.match(ref.getMessage()))
            {
               list.add(ref);
            }
         }

         return list;
      }
   }

   public synchronized MessageReference removeReferenceWithID(final long id)
   {
      Iterator<MessageReference> iterator = messageReferences.iterator();

      MessageReference removed = null;

      while (iterator.hasNext())
      {
         MessageReference ref = iterator.next();

         if (ref.getMessage().getMessageID() == id)
         {
            iterator.remove();

            removed = ref;

            try
            {
               referenceRemoved(removed);
            }
            catch (Exception e)
            {
               log.warn(e.getMessage(), e);
            }

            break;
         }
      }
      
      if (removed == null)
      {
         //Look in scheduled deliveries
         removed = scheduledDeliveryHandler.removeReferenceWithID(id);
      }

      return removed;
   }

   // Remove message from queue, add it to the scheduled delivery list without affect reference counting
   public void rescheduleDelivery(final long id, final long scheduledDeliveryTime)
   {
      Iterator<MessageReference> iterator = messageReferences.iterator();
      while (iterator.hasNext())
      {
         MessageReference ref = iterator.next();

         if (ref.getMessage().getMessageID() == id)
         {
            iterator.remove();

            ref.setScheduledDeliveryTime(scheduledDeliveryTime);

            if (!scheduledDeliveryHandler.checkAndSchedule(ref, backup))
            {
               messageReferences.addFirst(ref, ref.getMessage().getPriority());
            }

            break;
         }
      }
   }

   public synchronized MessageReference getReference(final long id)
   {
      Iterator<MessageReference> iterator = messageReferences.iterator();

      while (iterator.hasNext())
      {
         MessageReference ref = iterator.next();

         if (ref.getMessage().getMessageID() == id)
         {
            return ref;
         }
      }

      return null;
   }

   public long getPersistenceID()
   {
      return persistenceID;
   }

   public void setPersistenceID(final long id)
   {
      this.persistenceID = id;
   }

   public Filter getFilter()
   {
      return filter;
   }

   public synchronized int getMessageCount()
   {
      return messageReferences.size() + getScheduledCount() + getDeliveringCount();
   }

   public synchronized int getScheduledCount()
   {
      return scheduledDeliveryHandler.getScheduledCount();
   }

   public synchronized List<MessageReference> getScheduledMessages()
   {
      return scheduledDeliveryHandler.getScheduledReferences();
   }

   public int getDeliveringCount()
   {
      return deliveringCount.get();
   }

   public void referenceAcknowledged(MessageReference ref) throws Exception
   {

      referenceRemoved(ref);

   }

   public void referenceCancelled()
   {
      deliveringCount.decrementAndGet();
   }

   public int getSizeBytes()
   {
      return sizeBytes.get();
   }

   public DistributionPolicy getDistributionPolicy()
   {
      return distributionPolicy;
   }

   public void setDistributionPolicy(final DistributionPolicy distributionPolicy)
   {
      this.distributionPolicy = distributionPolicy;
   }

   public int getMessagesAdded()
   {
      return messagesAdded.get();
   }

   public synchronized void deleteAllReferences(final StorageManager storageManager) throws Exception
   {
      Transaction tx = new TransactionImpl(storageManager, postOffice);

      Iterator<MessageReference> iter = messageReferences.iterator();

      while (iter.hasNext())
      {
         MessageReference ref = iter.next();

         deliveringCount.incrementAndGet();

         tx.addAcknowledgement(ref);

         iter.remove();
      }

      List<MessageReference> cancelled = scheduledDeliveryHandler.cancel();
      for (MessageReference messageReference : cancelled)
      {
         deliveringCount.incrementAndGet();

         tx.addAcknowledgement(messageReference);
      }

      tx.commit();
   }

   public synchronized boolean deleteReference(final long messageID, final StorageManager storageManager) throws Exception
   {
      boolean deleted = false;

      Transaction tx = new TransactionImpl(storageManager, postOffice);

      Iterator<MessageReference> iter = messageReferences.iterator();

      while (iter.hasNext())
      {
         MessageReference ref = iter.next();
         if (ref.getMessage().getMessageID() == messageID)
         {
            deliveringCount.incrementAndGet();
            tx.addAcknowledgement(ref);
            iter.remove();
            deleted = true;
            break;
         }
      }

      tx.commit();

      return deleted;
   }

   public synchronized boolean expireMessage(final long messageID,
                                final StorageManager storageManager,
                                final PostOffice postOffice,
                                final HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
   {
      Iterator<MessageReference> iter = messageReferences.iterator();

      while (iter.hasNext())
      {
         MessageReference ref = iter.next();
         if (ref.getMessage().getMessageID() == messageID)
         {
            deliveringCount.incrementAndGet();
            ref.expire(storageManager, postOffice, queueSettingsRepository);
            iter.remove();
            return true;
         }
      }
      return false;
   }

   /**
    * todo- at present we need the whole method syncronized until the iterator is fail safe.
    */
   public synchronized void expireMessages(final StorageManager storageManager,
                                final PostOffice postOffice,
                                final HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
   {
      Iterator<MessageReference> iter = messageReferences.iterator();

      while (iter.hasNext())
      {
         MessageReference ref = iter.next();
         if (ref.getMessage().isExpired())
         {
            deliveringCount.incrementAndGet();
            ref.expire(storageManager, postOffice, queueSettingsRepository);
            iter.remove();
         }
      }
   }

   public boolean sendMessageToDeadLetterAddress(final long messageID,
                                   final StorageManager storageManager,
                                   final PostOffice postOffice,
                                   final HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
   {
      Iterator<MessageReference> iter = messageReferences.iterator();

      while (iter.hasNext())
      {
         MessageReference ref = iter.next();
         if (ref.getMessage().getMessageID() == messageID)
         {
            deliveringCount.incrementAndGet();
            ref.sendToDeadLetterAddress(storageManager, postOffice, queueSettingsRepository);
            iter.remove();
            return true;
         }
      }
      return false;
   }

   public boolean moveMessage(final long messageID,
                              final Binding toBinding,
                              final StorageManager storageManager,
                              final PostOffice postOffice) throws Exception
   {
      Iterator<MessageReference> iter = messageReferences.iterator();

      while (iter.hasNext())
      {
         MessageReference ref = iter.next();
         if (ref.getMessage().getMessageID() == messageID)
         {
            deliveringCount.incrementAndGet();
            ref.move(toBinding, storageManager, postOffice);
            iter.remove();
            return true;
         }
      }
      return false;
   }

   public boolean changeMessagePriority(final long messageID,
                                        final byte newPriority,
                                        final StorageManager storageManager,
                                        final PostOffice postOffice,
                                        final HierarchicalRepository<QueueSettings> queueSettingsRepository) throws Exception
   {
      List<MessageReference> refs = list(null);
      for (MessageReference ref : refs)
      {
         ServerMessage message = ref.getMessage();
         if (message.getMessageID() == messageID)
         {
            message.setPriority(newPriority);
            // delete and add the reference so that it
            // goes to the right queues for the new priority
            deleteReference(messageID, storageManager);
            addLast(ref);
            return true;
         }
      }
      return false;
   }

   public boolean isBackup()
   {
      return backup;
   }

   public synchronized void setBackup()
   {
      this.backup = true;

      this.direct = false;
   }

   public MessageReference removeFirst()
   {
      return messageReferences.removeFirst();
   }

   public synchronized boolean activate()
   {
      consumersToFailover = distributionPolicy.getConsumerCount();

      if (consumersToFailover == 0)
      {
         backup = false;

         return true;
      }
      else
      {
         return false;
      }
   }

   public synchronized void activateNow(final Executor executor)
   {
      if (backup)
      {
         log.info("Timed out waiting for all consumers to reconnect to queue " + name +
                  " so queue will be activated now");

         backup = false;
         
         scheduledDeliveryHandler.reSchedule();

         deliverAsync(executor);
      }
   }

   public synchronized boolean consumerFailedOver()
   {
      consumersToFailover--;

      if (consumersToFailover == 0)
      {
         // All consumers for the queue have failed over, can re-activate it now

         backup = false;

         scheduledDeliveryHandler.reSchedule();

         return true;
      }
      else
      {
         return false;
      }
   }

   // Public
   // -----------------------------------------------------------------------------

   public boolean equals(Object other)
   {
      if (this == other)
      {
         return true;
      }

      QueueImpl qother = (QueueImpl)other;

      return name.equals(qother.name);
   }

   public int hashCode()
   {
      return name.hashCode();
   }

   // Private
   // ------------------------------------------------------------------------------

   /*
    * Attempt to deliver all the messages in the queue
    */
   private void deliver()
   {
      // We don't do actual delivery if the queue is on a backup node - this is
      // because it's async and could get out of step
      // with the live node. Instead, when we replicate the delivery we remove
      // the ref from the queue

      if (backup)
      {
         return;
      }

      MessageReference reference;

      Iterator<MessageReference> iterator = null;

      while (true)
      {
         if (iterator == null)
         {
            reference = messageReferences.peekFirst();
         }
         else
         {
            if (iterator.hasNext())
            {
               reference = iterator.next();
            }
            else
            {
               reference = null;
            }
         }

         if (reference == null)
         {
            if (iterator == null)
            {
               // We delivered all the messages - go into direct delivery
               direct = true;

               promptDelivery = false;
            }
            return;
         }

         HandleStatus status = deliver(reference);

         if (status == HandleStatus.HANDLED)
         {
            if (iterator == null)
            {
               messageReferences.removeFirst();
            }
            else
            {
               iterator.remove();
            }
         }
         else if (status == HandleStatus.BUSY)
         {
            // All consumers busy - give up
            break;
         }
         else if (status == HandleStatus.NO_MATCH && iterator == null)
         {
            // Consumers not all busy - but filter not accepting - iterate
            // back
            // through the queue
            iterator = messageReferences.iterator();
         }
      }
   }

   private synchronized HandleStatus add(final MessageReference ref, final boolean first)
   {
      if (!first)
      {
         messagesAdded.incrementAndGet();

         sizeBytes.addAndGet(ref.getMessage().getEncodeSize());
      }

      if (scheduledDeliveryHandler.checkAndSchedule(ref, backup))
      {
         return HandleStatus.HANDLED;
      }

      boolean add = false;

      if (direct && !backup)
      {
         // Deliver directly

         HandleStatus status = deliver(ref);

         if (status == HandleStatus.HANDLED)
         {
            // Ok
         }
         else if (status == HandleStatus.BUSY)
         {
            add = true;
         }
         else if (status == HandleStatus.NO_MATCH)
         {
            add = true;
         }

         if (add)
         {
            direct = false;
         }
      }
      else
      {
         add = true;
      }

      if (add)
      {
         if (first)
         {
            messageReferences.addFirst(ref, ref.getMessage().getPriority());
         }
         else
         {
            messageReferences.addLast(ref, ref.getMessage().getPriority());
         }

         if (!direct && promptDelivery)
         {
            // We have consumers with filters which don't match, so we need
            // to prompt delivery every time
            // a new message arrives - this is why you really shouldn't use
            // filters with queues - in most cases
            // it's an ant-pattern since it would cause a queue scan on each
            // message
            deliver();
         }
      }

      return HandleStatus.HANDLED;
   }

   private HandleStatus deliver(final MessageReference reference)
   {
      HandleStatus status = distributionPolicy.distribute(reference);

      if (status == HandleStatus.HANDLED)
      {
         deliveringCount.incrementAndGet();
         return HandleStatus.HANDLED;
      }
      else if (status == HandleStatus.NO_MATCH)
      {
         promptDelivery = true;
      }

      return status;
   }

   /**
    * To be called when a reference is removed from the queue.
    * @param ref
    * @throws Exception
    */
   private void referenceRemoved(MessageReference ref) throws Exception
   {
      deliveringCount.decrementAndGet();

      sizeBytes.addAndGet(-ref.getMessage().getEncodeSize());

      if (ref.getMessage().decrementRefCount() == 0)
      {
         pagingManager.messageDone(ref.getMessage());
      }
   }

   // Inner classes
   // --------------------------------------------------------------------------

   private class DeliverRunner implements Runnable
   {
      public void run()
      {
         // Must be set to false *before* executing to avoid race
         waitingToDeliver.set(false);

         synchronized (QueueImpl.this)
         {
            deliver();
         }
      }
   }

}
