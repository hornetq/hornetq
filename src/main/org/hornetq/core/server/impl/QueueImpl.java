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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.core.filter.Filter;
import org.hornetq.core.list.PriorityLinkedList;
import org.hornetq.core.list.impl.PriorityLinkedListImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.Consumer;
import org.hornetq.core.server.HandleStatus;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ScheduledDeliveryHandler;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.cluster.impl.Redistributor;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionOperation;
import org.hornetq.core.transaction.TransactionPropertyIndexes;
import org.hornetq.core.transaction.impl.TransactionImpl;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.ConcurrentSet;
import org.hornetq.utils.SimpleString;

/**
 * Implementation of a Queue
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="clebert.suconic@jboss.com">Clebert Suconic</a>
 */
public class QueueImpl implements Queue
{
   private static final Logger log = Logger.getLogger(QueueImpl.class);

   public static final int REDISTRIBUTOR_BATCH_SIZE = 100;

   public static final int NUM_PRIORITIES = 10;

   private final long id;

   private final SimpleString name;

   private volatile Filter filter;

   private final boolean durable;

   private final boolean temporary;

   private final PostOffice postOffice;

   private final PriorityLinkedList<MessageReference> messageReferences = new PriorityLinkedListImpl<MessageReference>(NUM_PRIORITIES);

   private List<MessageHandler> handlers = new ArrayList<MessageHandler>();

   private final ConcurrentSet<MessageReference> expiringMessageReferences = new ConcurrentHashSet<MessageReference>();

   private final ScheduledDeliveryHandler scheduledDeliveryHandler;

   private boolean direct;

   private boolean promptDelivery;

   private final AtomicInteger messagesAdded = new AtomicInteger(0);

   protected final AtomicInteger deliveringCount = new AtomicInteger(0);

   private final AtomicBoolean waitingToDeliver = new AtomicBoolean(false);

   private boolean paused;

   private final Runnable deliverRunner = new DeliverRunner();

   private final Semaphore lock = new Semaphore(1);

   private final StorageManager storageManager;

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private final ScheduledExecutorService scheduledExecutor;
   
   private final SimpleString address;

   private Redistributor redistributor;

   private final Set<ScheduledFuture<?>> futures = new ConcurrentHashSet<ScheduledFuture<?>>();

   private ScheduledFuture<?> future;

   // We cache the consumers here since we don't want to include the redistributor

   private final Set<Consumer> consumerSet = new HashSet<Consumer>();

   private final ConcurrentMap<SimpleString, Consumer> groups = new ConcurrentHashMap<SimpleString, Consumer>();

   private volatile SimpleString expiryAddress;

   private int pos;

   public QueueImpl(final long id,
                    final SimpleString address,
                    final SimpleString name,
                    final Filter filter,
                    final boolean durable,
                    final boolean temporary,
                    final ScheduledExecutorService scheduledExecutor,
                    final PostOffice postOffice,
                    final StorageManager storageManager,
                    final HierarchicalRepository<AddressSettings> addressSettingsRepository)
   {
      this.id = id;

      this.address = address;

      this.name = name;

      this.filter = filter;

      this.durable = durable;

      this.temporary = temporary;

      this.postOffice = postOffice;

      this.storageManager = storageManager;

      this.addressSettingsRepository = addressSettingsRepository;

      this.scheduledExecutor = scheduledExecutor;

      direct = true;

      scheduledDeliveryHandler = new ScheduledDeliveryHandlerImpl(scheduledExecutor);

      if (addressSettingsRepository != null)
      {
         expiryAddress = addressSettingsRepository.getMatch(address.toString()).getExpiryAddress();
      }
      else
      {
         expiryAddress = null;
      }
   }

   // Bindable implementation -------------------------------------------------------------------------------------

   public SimpleString getRoutingName()
   {
      return name;
   }

   public SimpleString getUniqueName()
   {
      return name;
   }

   public boolean isExclusive()
   {
      return false;
   }

   public void route(final ServerMessage message, final RoutingContext context) throws Exception
   {
      context.addQueue(this);
   }

   // Queue implementation ----------------------------------------------------------------------------------------

   public void lockDelivery()
   {
      try
      {
         lock.acquire();
      }
      catch (InterruptedException e)
      {
         log.warn(e.getMessage(), e);
      }
   }

   public void unlockDelivery()
   {
      lock.release();
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

   public long getID()
   {
      return id;
   }

   public Filter getFilter()
   {
      return filter;
   }

   public void addLast(final MessageReference ref)
   {
      add(ref, false);
   }

   public void addFirst(final MessageReference ref)
   {
      add(ref, true);
   }

   public void deliverAsync(final Executor executor)
   {
      // Prevent too many executors running at once

      if (waitingToDeliver.compareAndSet(false, true))
      {
         executor.execute(deliverRunner);
      }
   }

   public synchronized void deliverNow()
   {
      deliverRunner.run();
   }

   public synchronized void addConsumer(final Consumer consumer) throws Exception
   {
      cancelRedistributor();

      MessageHandler handler;

      if (consumer.getFilter() != null)
      {
         handler = new FilterMessageHandler(consumer, messageReferences.iterator());
      }
      else
      {
         handler = new NullFilterMessageHandler(consumer);
      }

      handlers.add(handler);

      consumerSet.add(consumer);
   }

   public synchronized boolean removeConsumer(final Consumer consumer) throws Exception
   {
      boolean removed = this.removeHandlerGivenConsumer(consumer);

      if (handlers.isEmpty())
      {
         promptDelivery = false;
      }

      consumerSet.remove(consumer);

      if (removed)
      {
         for (SimpleString groupID : groups.keySet())
         {
            if (consumer == groups.get(groupID))
            {
               groups.remove(groupID);
            }
         }
      }

      return removed;
   }

   public synchronized void addRedistributor(final long delay, final Executor executor)
   {
      if (future != null)
      {
         future.cancel(false);

         futures.remove(future);
      }

      if (redistributor != null)
      {
         // Just prompt delivery
         deliverAsync(executor);
      }

      if (delay > 0)
      {
         if (consumerSet.isEmpty())
         {
            DelayedAddRedistributor dar = new DelayedAddRedistributor(executor);

            future = scheduledExecutor.schedule(dar, delay, TimeUnit.MILLISECONDS);

            futures.add(future);
         }
      }
      else
      {
         internalAddRedistributor(executor);
      }
   }

   public synchronized void cancelRedistributor() throws Exception
   {
      if (redistributor != null)
      {
         redistributor.stop();

         redistributor = null;

         removeHandlerGivenConsumer(redistributor);
      }

      if (future != null)
      {
         future.cancel(false);

         future = null;
      }
   }

   public synchronized int getConsumerCount()
   {
      return consumerSet.size();
   }

   public synchronized Set<Consumer> getConsumers()
   {
      return consumerSet;
   }

   public synchronized boolean hasMatchingConsumer(final ServerMessage message)
   {
      for (MessageHandler handler : handlers)
      {
         Consumer consumer = handler.getConsumer();

         if (consumer instanceof Redistributor)
         {
            continue;
         }

         Filter filter = consumer.getFilter();

         if (filter == null)
         {
            return true;
         }
         else
         {
            if (filter.match(message))
            {
               return true;
            }
         }
      }
      return false;
   }

   public Iterator<MessageReference> iterator()
   {
      return new Iterator<MessageReference>()
      {
         private final Iterator<MessageReference> iterator = messageReferences.iterator();

         public boolean hasNext()
         {
            return iterator.hasNext();
         }

         public MessageReference next()
         {
            return iterator.next();
         }

         public void remove()
         {
            throw new UnsupportedOperationException("iterator is immutable");
         }
      };
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

   public synchronized MessageReference removeReferenceWithID(final long id) throws Exception
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

            removeExpiringReference(removed);

            break;
         }
      }

      if (removed == null)
      {
         // Look in scheduled deliveries
         removed = scheduledDeliveryHandler.removeReferenceWithID(id);
      }

      return removed;
   }

   public synchronized MessageReference removeFirstReference(final long id) throws Exception
   {
      MessageReference ref = messageReferences.peekFirst();

      if (ref != null && ref.getMessage().getMessageID() == id)
      {
         messageReferences.removeFirst();

         return ref;
      }
      else
      {
         ref = scheduledDeliveryHandler.removeReferenceWithID(id);
      }

      return ref;
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

   public void acknowledge(final MessageReference ref) throws Exception
   {
      ServerMessage message = ref.getMessage();

      boolean durableRef = message.isDurable() && durable;

      if (durableRef)
      {
         storageManager.storeAcknowledge(id, message.getMessageID());
      }

      postAcknowledge(ref);
   }

   public void acknowledge(final Transaction tx, final MessageReference ref) throws Exception
   {
      ServerMessage message = ref.getMessage();

      boolean durableRef = message.isDurable() && durable;

      if (durableRef)
      {
         storageManager.storeAcknowledgeTransactional(tx.getID(), id, message.getMessageID());

         tx.setContainsPersistent();
      }

      getRefsOperation(tx).addAck(ref);
   }

   public void reacknowledge(final Transaction tx, final MessageReference ref) throws Exception
   {
      ServerMessage message = ref.getMessage();

      if (message.isDurable() && durable)
      {
         tx.setContainsPersistent();
      }

      getRefsOperation(tx).addAck(ref);
   }

   private final RefsOperation getRefsOperation(final Transaction tx)
   {
      synchronized (tx)
      {
         RefsOperation oper = (RefsOperation)tx.getProperty(TransactionPropertyIndexes.REFS_OPERATION);

         if (oper == null)
         {
            oper = new RefsOperation();

            tx.putProperty(TransactionPropertyIndexes.REFS_OPERATION, oper);

            tx.addOperation(oper);
         }

         return oper;
      }
   }

   public void cancel(final Transaction tx, final MessageReference reference) throws Exception
   {
      getRefsOperation(tx).addAck(reference);
   }

   public synchronized void cancel(final MessageReference reference) throws Exception
   {
      if (checkDLQ(reference))
      {
         if (!scheduledDeliveryHandler.checkAndSchedule(reference))
         {
            messageReferences.addFirst(reference, reference.getMessage().getPriority());
         }
      }
   }

   public void expire(final MessageReference ref) throws Exception
   {
      if (expiryAddress != null)
      {
         move(expiryAddress, ref, true);
      }
      else
      {
         acknowledge(ref);
      }
      
      storageManager.completeOperations();
   }

   public void setExpiryAddress(final SimpleString expiryAddress)
   {
      this.expiryAddress = expiryAddress;
   }

   public void referenceHandled()
   {
      deliveringCount.incrementAndGet();
   }

   public int getMessagesAdded()
   {
      return messagesAdded.get();
   }

   public int deleteAllReferences() throws Exception
   {
      return deleteMatchingReferences(null);
   }

   public int deleteMatchingReferences(final Filter filter) throws Exception
   {
      int count = 0;
      
      synchronized(this)
      {
   
         Transaction tx = new TransactionImpl(storageManager);
   
         Iterator<MessageReference> iter = messageReferences.iterator();
   
         while (iter.hasNext())
         {
            MessageReference ref = iter.next();
   
            if (filter == null || filter.match(ref.getMessage()))
            {
               deliveringCount.incrementAndGet();
               acknowledge(tx, ref);
               iter.remove();
               count++;
            }
         }
   
         List<MessageReference> cancelled = scheduledDeliveryHandler.cancel();
         for (MessageReference messageReference : cancelled)
         {
            if (filter == null || filter.match(messageReference.getMessage()))
            {
               deliveringCount.incrementAndGet();
               acknowledge(tx, messageReference);
               count++;
            }
         }
   
         tx.commit();
      }
      
      storageManager.waitOnOperations(-1);

      return count;
   }

   public synchronized boolean deleteReference(final long messageID) throws Exception
   {
      boolean deleted = false;

      Transaction tx = new TransactionImpl(storageManager);

      Iterator<MessageReference> iter = messageReferences.iterator();

      while (iter.hasNext())
      {
         MessageReference ref = iter.next();
         if (ref.getMessage().getMessageID() == messageID)
         {
            deliveringCount.incrementAndGet();
            acknowledge(tx, ref);
            iter.remove();
            deleted = true;
            break;
         }
      }

      tx.commit();

      return deleted;
   }

   public synchronized boolean expireReference(final long messageID) throws Exception
   {
      Iterator<MessageReference> iter = messageReferences.iterator();

      while (iter.hasNext())
      {
         MessageReference ref = iter.next();
         if (ref.getMessage().getMessageID() == messageID)
         {
            deliveringCount.incrementAndGet();
            expire(ref);
            iter.remove();
            return true;
         }
      }
      return false;
   }

   public synchronized int expireReferences(final Filter filter) throws Exception
   {
      Transaction tx = new TransactionImpl(storageManager);

      int count = 0;
      Iterator<MessageReference> iter = messageReferences.iterator();

      while (iter.hasNext())
      {
         MessageReference ref = iter.next();
         if (filter == null || filter.match(ref.getMessage()))
         {
            deliveringCount.incrementAndGet();
            expire(tx, ref);
            iter.remove();
            count++;
         }
      }

      tx.commit();

      return count;
   }

   public synchronized void expireReferences() throws Exception
   {
      for (MessageReference expiringMessageReference : expiringMessageReferences)
      {
         if (expiringMessageReference.getMessage().isExpired())
         {
            expireReference(expiringMessageReference.getMessage().getMessageID());
         }
      }
   }

   public synchronized boolean sendMessageToDeadLetterAddress(final long messageID) throws Exception
   {
      Iterator<MessageReference> iter = messageReferences.iterator();

      while (iter.hasNext())
      {
         MessageReference ref = iter.next();
         if (ref.getMessage().getMessageID() == messageID)
         {
            deliveringCount.incrementAndGet();
            sendToDeadLetterAddress(ref);
            iter.remove();
            return true;
         }
      }
      return false;
   }

   public synchronized boolean moveReference(final long messageID, final SimpleString toAddress) throws Exception
   {
      Iterator<MessageReference> iter = messageReferences.iterator();

      while (iter.hasNext())
      {
         MessageReference ref = iter.next();
         if (ref.getMessage().getMessageID() == messageID)
         {
            iter.remove();
            deliveringCount.incrementAndGet();
            move(toAddress, ref);
            return true;
         }
      }
      return false;
   }

   public synchronized int moveReferences(final Filter filter, final SimpleString toAddress) throws Exception
   {
      Transaction tx = new TransactionImpl(storageManager);

      int count = 0;
      Iterator<MessageReference> iter = messageReferences.iterator();

      while (iter.hasNext())
      {
         MessageReference ref = iter.next();
         if (filter == null || filter.match(ref.getMessage()))
         {
            deliveringCount.incrementAndGet();
            move(toAddress, tx, ref, false);
            iter.remove();
            count++;
         }
      }

      List<MessageReference> cancelled = scheduledDeliveryHandler.cancel();
      for (MessageReference ref : cancelled)
      {
         if (filter == null || filter.match(ref.getMessage()))
         {
            deliveringCount.incrementAndGet();
            move(toAddress, tx, ref, false);
            acknowledge(tx, ref);
            count++;
         }
      }

      tx.commit();

      return count;
   }

   public synchronized boolean changeReferencePriority(final long messageID, final byte newPriority) throws Exception
   {
      Iterator<MessageReference> iter = messageReferences.iterator();

      while (iter.hasNext())
      {
         MessageReference ref = iter.next();
         if (ref.getMessage().getMessageID() == messageID)
         {
            iter.remove();
            ref.getMessage().setPriority(newPriority);
            addLast(ref);
            return true;
         }
      }

      return false;
   }

   // Public
   // -----------------------------------------------------------------------------

   @Override
   public boolean equals(final Object other)
   {
      if (this == other)
      {
         return true;
      }

      QueueImpl qother = (QueueImpl)other;

      return name.equals(qother.name);
   }

   @Override
   public int hashCode()
   {
      return name.hashCode();
   }

   @Override
   public String toString()
   {
      return "QueueImpl(name=" + name.toString() + ")";
   }

   // Private
   // ------------------------------------------------------------------------------

   private boolean removeHandlerGivenConsumer(final Consumer consumer)
   {
      Iterator<MessageHandler> iter = handlers.iterator();

      boolean removed = false;

      while (iter.hasNext())
      {
         MessageHandler handler = iter.next();

         if (handler.getConsumer() == consumer)
         {
            iter.remove();

            if (pos >= handlers.size())
            {
               pos = 0;
            }
            removed = true;

            break;
         }
      }

      return removed;
   }

   private void internalAddRedistributor(final Executor executor)
   {
      // create the redistributor only once if there are no local consumers
      if (consumerSet.isEmpty() && redistributor == null)
      {
         redistributor = new Redistributor(this, storageManager, postOffice, executor, REDISTRIBUTOR_BATCH_SIZE);

         handlers.add(new NullFilterMessageHandler(redistributor));

         redistributor.start();

         deliverAsync(executor);
      }
   }

   public boolean checkDLQ(final MessageReference reference) throws Exception
   {
      ServerMessage message = reference.getMessage();

      if (message.isDurable() && durable)
      {
         storageManager.updateDeliveryCount(reference);
         storageManager.waitOnOperations();
      }

      AddressSettings addressSettings = addressSettingsRepository.getMatch(address.toString());

      int maxDeliveries = addressSettings.getMaxDeliveryAttempts();

      if (maxDeliveries > 0 && reference.getDeliveryCount() >= maxDeliveries)
      {
         sendToDeadLetterAddress(reference);
         storageManager.waitOnOperations();

         return false;
      }
      else
      {
         long redeliveryDelay = addressSettings.getRedeliveryDelay();

         if (redeliveryDelay > 0)
         {
            reference.setScheduledDeliveryTime(System.currentTimeMillis() + redeliveryDelay);

            storageManager.updateScheduledDeliveryTime(reference);
         }

         deliveringCount.decrementAndGet();

         return true;
      }
   }

   private void move(final SimpleString toAddress, final MessageReference ref) throws Exception
   {
      move(toAddress, ref, false);
   }

   private void move(final SimpleString toAddress,
                     final Transaction tx,
                     final MessageReference ref,
                     final boolean expiry) throws Exception
   {
      ServerMessage copyMessage = makeCopy(ref, expiry);

      copyMessage.setDestination(toAddress);

      postOffice.route(copyMessage, tx);

      acknowledge(tx, ref);
   }

   private ServerMessage makeCopy(final MessageReference ref, final boolean expiry) throws Exception
   {
      ServerMessage message = ref.getMessage();
      /*
       We copy the message and send that to the dla/expiry queue - this is
       because otherwise we may end up with a ref with the same message id in the
       queue more than once which would barf - this might happen if the same message had been
       expire from multiple subscriptions of a topic for example
       We set headers that hold the original message destination, expiry time
       and original message id
      */

      long newID = storageManager.generateUniqueID();

      ServerMessage copy = message.makeCopyForExpiryOrDLA(newID, expiry);

      return copy;
   }

   private void expire(final Transaction tx, final MessageReference ref) throws Exception
   {
      SimpleString expiryAddress = addressSettingsRepository.getMatch(address.toString()).getExpiryAddress();

      if (expiryAddress != null)
      {
         Bindings bindingList = postOffice.getBindingsForAddress(expiryAddress);

         if (bindingList.getBindings().isEmpty())
         {
            log.warn("Message has expired. No bindings for Expiry Address " + expiryAddress + " so dropping it");
         }
         else
         {
            move(expiryAddress, tx, ref, true);
         }
      }
      else
      {
         log.warn("Message has expired. No expiry queue configured for queue " + name + " so dropping it");

         acknowledge(tx, ref);
      }
   }

   private void sendToDeadLetterAddress(final MessageReference ref) throws Exception
   {
      SimpleString deadLetterAddress = addressSettingsRepository.getMatch(address.toString()).getDeadLetterAddress();

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

            log.warn("Message has reached maximum delivery attempts, sending it to Dead Letter Address " + deadLetterAddress +
                     " from " +
                     name);
            move(deadLetterAddress, ref, false);
         }
      }
      else
      {
         log.warn("Message has exceeded max delivery attempts. No Dead Letter Address configured for queue " + name +
                  " so dropping it");

         acknowledge(ref);
      }
   }

   private void move(final SimpleString address, final MessageReference ref, final boolean expiry) throws Exception
   {
      Transaction tx = new TransactionImpl(storageManager);

      ServerMessage copyMessage = makeCopy(ref, expiry);

      copyMessage.setDestination(address);

      postOffice.route(copyMessage, tx);

      acknowledge(tx, ref);

      tx.commit();
   }

   private MessageHandler getHandlerRoundRobin()
   {
      MessageHandler handler = handlers.get(pos);

      pos++;

      if (pos == handlers.size())
      {
         pos = 0;
      }

      return handler;
   }

   private boolean checkExpired(final MessageReference reference)
   {
      if (reference.getMessage().isExpired())
      {
         reference.handled();

         try
         {
            expire(reference);
         }
         catch (Exception e)
         {
            log.error("Failed to expire ref", e);
         }

         return true;
      }
      else
      {
         return false;
      }
   }

   /*
    * Attempt to deliver all the messages in the queue
    */
   private synchronized void deliver()
   {
      if (paused || handlers.isEmpty())
      {
         return;
      }

      direct = false;

      int startPos = pos;
      int totalCount = handlers.size();
      int nullCount = 0;
      int busyCount = 0;
      while (true)
      {
         MessageHandler handler = getHandlerRoundRobin();

         Consumer consumer = handler.getConsumer();

         MessageReference reference = handler.peek(consumer);

         if (reference == null)
         {
            nullCount++;
         }
         else
         {
            if (checkExpired(reference))
            {
               handler.remove();
            }
            else
            {
               final SimpleString groupID = reference.getMessage().getSimpleStringProperty(MessageImpl.HDR_GROUP_ID);

               boolean tryHandle = true;

               if (groupID != null)
               {
                  Consumer groupConsumer = groups.putIfAbsent(groupID, consumer);

                  if (groupConsumer != null && groupConsumer != consumer)
                  {
                     tryHandle = false;

                     busyCount++;
                  }
               }

               if (tryHandle)
               {
                  HandleStatus status = handle(reference, consumer);

                  if (status == HandleStatus.HANDLED)
                  {
                     handler.remove();
                  }
                  else if (status == HandleStatus.BUSY)
                  {
                     busyCount++;

                     handler.reset();

                     // if (groupID != null )
                     // {
                     // // group id being set seems to make delivery stop
                     // // FIXME !!! why??
                     // break;
                     // }
                  }
                  else if (status == HandleStatus.NO_MATCH)
                  {
                     // if consumer filter reject the message make sure it won't be assigned the message group
                     if (groupID != null)
                     {
                        groups.remove(groupID);
                     }
                  }
               }
            }
         }

         if (pos == startPos)
         {
            // We've done all the consumers

            if (nullCount + busyCount == totalCount)
            {
               if (nullCount == totalCount)
               {
                  // We delivered all the messages - go into direct delivery
                  direct = true;

                  promptDelivery = false;
               }

               break;
            }

            nullCount = busyCount = 0;
         }
      }
   }

   private synchronized boolean directDeliver(final MessageReference reference)
   {
      if (paused || handlers.isEmpty())
      {
         return false;
      }

      int startPos = pos;
      int busyCount = 0;
      boolean setPromptDelivery = false;
      while (true)
      {
         MessageHandler handler = getHandlerRoundRobin();

         Consumer consumer = handler.getConsumer();

         if (!checkExpired(reference))
         {
            SimpleString groupID = reference.getMessage().getSimpleStringProperty(MessageImpl.HDR_GROUP_ID);

            boolean tryHandle = true;

            if (groupID != null)
            {
               Consumer groupConsumer = groups.putIfAbsent(groupID, consumer);

               if (groupConsumer != null && groupConsumer != consumer)
               {
                  tryHandle = false;
               }
            }

            if (tryHandle)
            {
               HandleStatus status = handle(reference, consumer);

               if (status == HandleStatus.HANDLED)
               {
                  return true;
               }
               else if (status == HandleStatus.BUSY)
               {
                  busyCount++;

                  if (groupID != null)
                  {
                     // If the group has been assigned a consumer there is no point in trying others

                     return false;
                  }
               }
               else if (status == HandleStatus.NO_MATCH)
               {
                  // if consumer filter reject the message make sure it won't be assigned the message group
                  if (groupID != null)
                  {
                     groups.remove(groupID);
                  }

                  setPromptDelivery = true;
               }
            }
         }

         if (pos == startPos)
         {
            if (setPromptDelivery)
            {
               promptDelivery = true;
            }

            return false;
         }
      }
   }

   protected synchronized void add(final MessageReference ref, final boolean first)
   {
      if (!first)
      {
         messagesAdded.incrementAndGet();
      }

      if (scheduledDeliveryHandler.checkAndSchedule(ref))
      {
         return;
      }

      boolean add = false;

      if (direct && !paused)
      {
         // Deliver directly

         boolean delivered = directDeliver(ref);

         if (!delivered)
         {
            add = true;

            direct = false;
         }
      }
      else
      {
         add = true;
      }

      if (add)
      {
         if (ref.getMessage().getExpiration() != 0)
         {
            expiringMessageReferences.addIfAbsent(ref);
         }

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
   }

   private synchronized HandleStatus handle(final MessageReference reference, final Consumer consumer)
   {
      HandleStatus status;
      try
      {
         status = consumer.handle(reference);
      }
      catch (Throwable t)
      {
         log.warn("removing consumer which did not handle a message, consumer=" + consumer + ", message=" + reference,
                  t);

         // If the consumer throws an exception we remove the consumer
         try
         {
            removeConsumer(consumer);
         }
         catch (Exception e)
         {
            log.error("Failed to remove consumer", e);
         }
         return HandleStatus.BUSY;
      }

      if (status == null)
      {
         throw new IllegalStateException("ClientConsumer.handle() should never return null");
      }

      return status;
   }

   private void removeExpiringReference(final MessageReference ref)
   {
      if (ref.getMessage().getExpiration() > 0)
      {
         expiringMessageReferences.remove(ref);
      }
   }

   private void postAcknowledge(final MessageReference ref)
   {
      final ServerMessage message = ref.getMessage();

      QueueImpl queue = (QueueImpl)ref.getQueue();

      boolean durableRef = message.isDurable() && queue.durable;

      if (durableRef)
      {
         int count = message.decrementDurableRefCount();

         if (count == 0)
         {
            // Note - we MUST store the delete after the preceeding ack has been committed to storage, we cannot combine
            // the last ack and delete into a single delete.
            // This is because otherwise we could have a situation where the same message is being acked concurrently
            // from two different queues on different sessions.
            // One decrements the ref count, then the other stores a delete, the delete gets committed, but the first
            // ack isn't committed, then the server crashes and on
            // recovery the message is deleted even though the other ack never committed

            // also note then when this happens as part of a trasaction its the tx commt of the ack that is important
            // not this
            try
            {
               storageManager.deleteMessage(message.getMessageID());
            }
            catch (Exception e)
            {
               log.warn("Unable to remove message id = " + message.getMessageID() + " please remove manually");
            }
         }
      }

      queue.removeExpiringReference(ref);

      queue.deliveringCount.decrementAndGet();

      message.decrementRefCount(ref);
   }

   void postRollback(final LinkedList<MessageReference> refs)
   {
      synchronized (this)
      {
         direct = false;

         for (MessageReference ref : refs)
         {
            add(ref, true);
         }

         deliver();
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

         lockDelivery();
         try
         {
            deliver();
         }
         finally
         {
            unlockDelivery();
         }
      }
   }

   private final class RefsOperation implements TransactionOperation
   {
      List<MessageReference> refsToAck = new ArrayList<MessageReference>();

      synchronized void addAck(final MessageReference ref)
      {
         refsToAck.add(ref);
      }

      public void beforeCommit(final Transaction tx) throws Exception
      {
      }

      public void afterPrepare(final Transaction tx)
      {
      }

      public void afterRollback(final Transaction tx)
      {
         Map<QueueImpl, LinkedList<MessageReference>> queueMap = new HashMap<QueueImpl, LinkedList<MessageReference>>();

         for (MessageReference ref : refsToAck)
         {
            try
            {
               if (ref.getQueue().checkDLQ(ref))
               {
                  LinkedList<MessageReference> toCancel = queueMap.get(ref.getQueue());
   
                  if (toCancel == null)
                  {
                     toCancel = new LinkedList<MessageReference>();
   
                     queueMap.put((QueueImpl)ref.getQueue(), toCancel);
                  }
   
                  toCancel.addFirst(ref);
               }
            }
            catch (Exception e)
            {
               log.warn("Error on checkDLQ", e);
            }
         }

         for (Map.Entry<QueueImpl, LinkedList<MessageReference>> entry : queueMap.entrySet())
         {
            LinkedList<MessageReference> refs = entry.getValue();

            QueueImpl queue = entry.getKey();

            synchronized (queue)
            {
               queue.postRollback(refs);
            }
         }
      }

      public void afterCommit(final Transaction tx)
      {
         for (MessageReference ref : refsToAck)
         {
            synchronized (ref.getQueue())
            {
               postAcknowledge(ref);
            }
         }
      }

      public void beforePrepare(final Transaction tx) throws Exception
      {
      }

      public void beforeRollback(final Transaction tx) throws Exception
      {
      }
   }

   private class DelayedAddRedistributor implements Runnable
   {
      private final Executor executor;

      DelayedAddRedistributor(final Executor executor)
      {
         this.executor = executor;
      }

      public void run()
      {
         synchronized (QueueImpl.this)
         {
            internalAddRedistributor(executor);

            futures.remove(this);
         }
      }
   }

   public synchronized void pause()
   {
      paused = true;
   }

   public synchronized void resume()
   {
      paused = false;

      deliver();
   }

   public synchronized boolean isPaused()
   {
      return paused;
   }

   private static interface MessageHandler
   {
      MessageReference peek(Consumer consumer);

      void remove();

      void reset();

      Consumer getConsumer();
   }

   private class FilterMessageHandler implements MessageHandler
   {
      private final Consumer consumer;

      private Iterator<MessageReference> iterator;

      private MessageReference lastReference;

      private boolean resetting;

      public FilterMessageHandler(final Consumer consumer, final Iterator<MessageReference> iterator)
      {
         this.consumer = consumer;

         this.iterator = iterator;
      }

      public MessageReference peek(final Consumer consumer)
      {
         if (resetting)
         {
            resetting = false;

            return lastReference;
         }

         MessageReference reference;

         if (iterator.hasNext())
         {
            reference = iterator.next();
         }
         else
         {
            reference = null;

            if (consumer.getFilter() != null)
            {
               // we have iterated on the whole queue for
               // messages which matches the consumer filter.
               // we reset its iterator in case new messages are added to the queue
               iterator = messageReferences.iterator();
            }
         }
         lastReference = reference;

         return reference;
      }

      public void remove()
      {
         iterator.remove();
      }

      public void reset()
      {
         resetting = true;
      }

      public Consumer getConsumer()
      {
         return consumer;
      }
   }

   private class NullFilterMessageHandler implements MessageHandler
   {
      private final Consumer consumer;

      NullFilterMessageHandler(final Consumer consumer)
      {
         this.consumer = consumer;
      }

      public MessageReference peek(final Consumer consumer)
      {
         return messageReferences.peekFirst();
      }

      public void remove()
      {
         messageReferences.removeFirst();
      }

      public void reset()
      {
         // no-op
      }

      public Consumer getConsumer()
      {
         return consumer;
      }
   }
}
