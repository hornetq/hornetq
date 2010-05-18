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
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.logging.Logger;
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
import org.hornetq.utils.PriorityLinkedList;
import org.hornetq.utils.PriorityLinkedListImpl;
import org.hornetq.utils.concurrent.HQIterator;

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

   private final PriorityLinkedList<MessageReference> messageReferences = new PriorityLinkedListImpl<MessageReference>(true,
                                                                                                                       QueueImpl.NUM_PRIORITIES);

   private final List<ConsumerHolder> consumerList = new ArrayList<ConsumerHolder>();

   private final ScheduledDeliveryHandler scheduledDeliveryHandler;

   private final AtomicLong messagesAdded = new AtomicLong(0);

   protected final AtomicInteger deliveringCount = new AtomicInteger(0);

   private boolean paused;

   private final Runnable deliverRunner = new DeliverRunner();

   private final StorageManager storageManager;

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private final ScheduledExecutorService scheduledExecutor;

   private final SimpleString address;

   private Redistributor redistributor;

   private final Set<ScheduledFuture<?>> futures = new ConcurrentHashSet<ScheduledFuture<?>>();

   private ScheduledFuture<?> future;

   // We cache the consumers here since we don't want to include the redistributor

   private final Set<Consumer> consumerSet = new HashSet<Consumer>();

   private final Map<SimpleString, Consumer> groups = new HashMap<SimpleString, Consumer>();

   private volatile SimpleString expiryAddress;

   private int pos;

   private final Executor executor;

   private static class ConsumerHolder
   {
      ConsumerHolder(final Consumer consumer)
      {
         this.consumer = consumer;
      }

      final Consumer consumer;

      volatile HQIterator<MessageReference> iter;
   }

   public QueueImpl(final long id,
                    final SimpleString address,
                    final SimpleString name,
                    final Filter filter,
                    final boolean durable,
                    final boolean temporary,
                    final ScheduledExecutorService scheduledExecutor,
                    final PostOffice postOffice,
                    final StorageManager storageManager,
                    final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                    final Executor executor)
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

      scheduledDeliveryHandler = new ScheduledDeliveryHandlerImpl(scheduledExecutor);

      if (addressSettingsRepository != null)
      {
         expiryAddress = addressSettingsRepository.getMatch(address.toString()).getExpiryAddress();
      }
      else
      {
         expiryAddress = null;
      }

      this.executor = executor;
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
      addLast(ref, false);
   }

   public void addLast(final MessageReference ref, final boolean direct)
   {
      messagesAdded.incrementAndGet();

      add(ref, false, direct);
   }

   public void addFirst(final MessageReference ref)
   {
      add(ref, true, false);
   }

   public void deliverAsync()
   {
      executor.execute(deliverRunner);
   }

   public Executor getExecutor()
   {
      return executor;
   }

   public synchronized void deliverNow()
   {
      deliver();
   }

   public synchronized void addConsumer(final Consumer consumer) throws Exception
   {
      cancelRedistributor();

      consumerList.add(new ConsumerHolder(consumer));

      consumerSet.add(consumer);
   }

   public synchronized void removeConsumer(final Consumer consumer) throws Exception
   {
      Iterator<ConsumerHolder> iter = consumerList.iterator();

      while (iter.hasNext())
      {
         ConsumerHolder holder = iter.next();

         if (holder.consumer == consumer)
         {
            iter.remove();

            break;
         }
      }

      if (pos > 0 && pos >= consumerList.size())
      {
         pos = consumerList.size() - 1;
      }

      consumerSet.remove(consumer);

      List<SimpleString> gids = new ArrayList<SimpleString>();

      for (SimpleString groupID : groups.keySet())
      {
         if (consumer == groups.get(groupID))
         {
            gids.add(groupID);
         }
      }

      for (SimpleString gid : gids)
      {
         groups.remove(gid);
      }
   }

   public synchronized void addRedistributor(final long delay)
   {
      if (future != null)
      {
         future.cancel(false);

         futures.remove(future);
      }

      if (redistributor != null)
      {
         // Just prompt delivery
         deliverAsync();
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

         Iterator<ConsumerHolder> iter = consumerList.iterator();

         while (iter.hasNext())
         {
            ConsumerHolder holder = iter.next();

            if (holder.consumer == redistributor)
            {
               iter.remove();

               break;
            }
         }

         if (pos > 0 && pos >= consumerList.size())
         {
            pos = consumerList.size() - 1;
         }
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
      for (ConsumerHolder holder : consumerList)
      {
         Consumer consumer = holder.consumer;

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
         private final HQIterator<MessageReference> iterator = messageReferences.iterator();

         private MessageReference next;

         public boolean hasNext()
         {
            if (next == null)
            {
               next = iterator.next();
            }

            return next != null;
         }

         public MessageReference next()
         {
            MessageReference n = next;

            next = null;

            return n;
         }

         public void remove()
         {
            iterator.remove();
         }
      };
   }

   public MessageReference removeReferenceWithID(final long id) throws Exception
   {
      Iterator<MessageReference> iterator = iterator();

      MessageReference removed = null;

      while (iterator.hasNext())
      {
         MessageReference ref = iterator.next();

         if (ref.getMessage().getMessageID() == id)
         {
            iterator.remove();

            removed = ref;

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

   public MessageReference getReference(final long id)
   {
      Iterator<MessageReference> iterator = iterator();

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
   }

   public void setExpiryAddress(final SimpleString expiryAddress)
   {
      this.expiryAddress = expiryAddress;
   }

   public void referenceHandled()
   {
      deliveringCount.incrementAndGet();
   }

   public long getMessagesAdded()
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

      Transaction tx = new TransactionImpl(storageManager);

      Iterator<MessageReference> iter = iterator();

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

      List<MessageReference> cancelled = scheduledDeliveryHandler.cancel(filter);
      for (MessageReference messageReference : cancelled)
      {
         deliveringCount.incrementAndGet();
         acknowledge(tx, messageReference);
         count++;
      }

      tx.commit();

      return count;
   }

   public boolean deleteReference(final long messageID) throws Exception
   {
      boolean deleted = false;

      Transaction tx = new TransactionImpl(storageManager);

      Iterator<MessageReference> iter = iterator();

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

   public boolean expireReference(final long messageID) throws Exception
   {
      Iterator<MessageReference> iter = iterator();

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

   public int expireReferences(final Filter filter) throws Exception
   {
      Transaction tx = new TransactionImpl(storageManager);

      int count = 0;
      Iterator<MessageReference> iter = iterator();

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

   public void expireReferences() throws Exception
   {
      Iterator<MessageReference> iter = iterator();

      while (iter.hasNext())
      {
         MessageReference ref = iter.next();
         if (ref.getMessage().isExpired())
         {
            deliveringCount.incrementAndGet();
            expire(ref);
            iter.remove();
         }
      }
   }

   public boolean sendMessageToDeadLetterAddress(final long messageID) throws Exception
   {
      Iterator<MessageReference> iter = iterator();

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

   public int sendMessagesToDeadLetterAddress(Filter filter) throws Exception
   {
      int count = 0;
      Iterator<MessageReference> iter = iterator();

      while (iter.hasNext())
      {
         MessageReference ref = iter.next();
         if (filter == null || filter.match(ref.getMessage()))
         {
            deliveringCount.incrementAndGet();
            sendToDeadLetterAddress(ref);
            iter.remove();
            count++;
         }
      }
      return count;
   }

   public boolean moveReference(final long messageID, final SimpleString toAddress) throws Exception
   {
      Iterator<MessageReference> iter = iterator();

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

   public int moveReferences(final Filter filter, final SimpleString toAddress) throws Exception
   {
      Transaction tx = new TransactionImpl(storageManager);

      int count = 0;
      Iterator<MessageReference> iter = iterator();

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

      List<MessageReference> cancelled = scheduledDeliveryHandler.cancel(filter);
      for (MessageReference ref : cancelled)
      {
         deliveringCount.incrementAndGet();
         move(toAddress, tx, ref, false);
         acknowledge(tx, ref);
         count++;
      }

      tx.commit();

      return count;
   }

   public boolean changeReferencePriority(final long messageID, final byte newPriority) throws Exception
   {
      Iterator<MessageReference> iter = iterator();

      while (iter.hasNext())
      {
         MessageReference ref = iter.next();
         if (ref.getMessage().getMessageID() == messageID)
         {
            iter.remove();
            ref.getMessage().setPriority(newPriority);
            addLast(ref, false);
            return true;
         }
      }

      return false;
   }

   public int changeReferencesPriority(final Filter filter, final byte newPriority) throws Exception
   {
      Iterator<MessageReference> iter = iterator();

      int count = 0;
      while (iter.hasNext())
      {
         MessageReference ref = iter.next();
         if (filter == null || filter.match(ref.getMessage()))
         {
            count++;
            iter.remove();
            ref.getMessage().setPriority(newPriority);
            addLast(ref, false);
         }
      }
      return count;
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
      return "QueueImpl[name=" + name.toString() + "]@" + Integer.toHexString(System.identityHashCode(this));
   }

   // Private
   // ------------------------------------------------------------------------------

   private void internalAddRedistributor(final Executor executor)
   {
      // create the redistributor only once if there are no local consumers
      if (consumerSet.isEmpty() && redistributor == null)
      {
         redistributor = new Redistributor(this,
                                           storageManager,
                                           postOffice,
                                           executor,
                                           QueueImpl.REDISTRIBUTOR_BATCH_SIZE);

         consumerList.add(new ConsumerHolder(redistributor));

         redistributor.start();

         deliverAsync();
      }
   }

   public boolean checkDLQ(final MessageReference reference) throws Exception
   {
      ServerMessage message = reference.getMessage();

      if (message.isDurable() && durable)
      {
         storageManager.updateDeliveryCount(reference);
      }

      AddressSettings addressSettings = addressSettingsRepository.getMatch(address.toString());

      int maxDeliveries = addressSettings.getMaxDeliveryAttempts();

      if (maxDeliveries > 0 && reference.getDeliveryCount() >= maxDeliveries)
      {
         sendToDeadLetterAddress(reference);

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

      copyMessage.setAddress(toAddress);

      postOffice.route(copyMessage, tx, false);

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
       We set headers that hold the original message address, expiry time
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
            QueueImpl.log.warn("Message has expired. No bindings for Expiry Address " + expiryAddress +
                               " so dropping it");
         }
         else
         {
            move(expiryAddress, tx, ref, true);
         }
      }
      else
      {
         QueueImpl.log.warn("Message has expired. No expiry queue configured for queue " + name + " so dropping it");

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
            QueueImpl.log.warn("Message has exceeded max delivery attempts. No bindings for Dead Letter Address " + deadLetterAddress +
                               " so dropping it");
         }
         else
         {

            QueueImpl.log.warn("Message has reached maximum delivery attempts, sending it to Dead Letter Address " + deadLetterAddress +
                               " from " +
                               name);
            move(deadLetterAddress, ref, false);
         }
      }
      else
      {
         QueueImpl.log.warn("Message has exceeded max delivery attempts. No Dead Letter Address configured for queue " + name +
                            " so dropping it");

         acknowledge(ref);
      }
   }

   private void move(final SimpleString address, final MessageReference ref, final boolean expiry) throws Exception
   {
      Transaction tx = new TransactionImpl(storageManager);

      ServerMessage copyMessage = makeCopy(ref, expiry);

      copyMessage.setAddress(address);

      postOffice.route(copyMessage, tx, false);

      acknowledge(tx, ref);

      tx.commit();
   }

   private synchronized void deliver()
   {
      if (paused || consumerList.isEmpty())
      {
         return;
      }

      int busyCount = 0;

      int nullRefCount = 0;

      int size = consumerList.size();

      int startPos = pos;

      // Deliver at most 1000 messages in one go, to prevent tying this thread up for too long
      int loop = Math.min(messageReferences.size(), 1000);

      for (int i = 0; i < loop; i++)
      {
         ConsumerHolder holder = consumerList.get(pos);

         Consumer consumer = holder.consumer;

         MessageReference ref;

         if (holder.iter == null)
         {
            ref = messageReferences.removeFirst();
         }
         else
         {
            ref = holder.iter.next();
         }

         if (ref == null)
         {
            nullRefCount++;
         }
         else
         {
            if (checkExpired(ref))
            {
               if (holder.iter != null)
               {
                  holder.iter.remove();
               }

               continue;
            }

            Consumer groupConsumer = null;

            // If a group id is set, then this overrides the consumer chosen round-robin

            SimpleString groupID = ref.getMessage().getSimpleStringProperty(Message.HDR_GROUP_ID);

            if (groupID != null)
            {
               groupConsumer = groups.get(groupID);

               if (groupConsumer != null)
               {
                  consumer = groupConsumer;
               }
            }

            HandleStatus status = handle(ref, consumer);

            if (status == HandleStatus.HANDLED)
            {
               if (holder.iter != null)
               {
                  holder.iter.remove();
               }

               if (groupID != null && groupConsumer == null)
               {
                  groups.put(groupID, consumer);
               }
            }
            else if (status == HandleStatus.BUSY)
            {
               if (holder.iter == null)
               {
                  // Put the ref back

                  messageReferences.addFirst(ref, ref.getMessage().getPriority());
               }

               busyCount++;
            }
            else if (status == HandleStatus.NO_MATCH)
            {
               if (holder.iter == null)
               {
                  // Put the ref back

                  messageReferences.addFirst(ref, ref.getMessage().getPriority());

                  holder.iter = messageReferences.iterator();

                  // Skip past the one we just put back

                  holder.iter.next();
               }
            }
         }

         pos++;

         if (pos == size)
         {
            pos = 0;
         }

         if (pos == startPos)
         {
            // Round robin'd all

            if (nullRefCount + busyCount == size)
            {
               break;
            }

            nullRefCount = busyCount = 0;
         }
      }

      if (messageReferences.size() > 0 && busyCount != size)
      {
         // More messages to deliver so need to prompt another runner - note we don't
         // prompt another one if all consumers are busy

         executor.execute(deliverRunner);
      }

   }

   /*
    * This method delivers the reference on the callers thread - this can give us better latency in the case there is nothing in the queue
    */
   private synchronized boolean deliverDirect(final MessageReference ref)
   {
      if (paused || consumerList.isEmpty())
      {
         return false;
      }
      
      if (checkExpired(ref))
      {
         return true;
      }
      
      int startPos = pos;
      
      int size = consumerList.size();

      while (true)
      {
         ConsumerHolder holder = consumerList.get(pos);

         Consumer consumer = holder.consumer;

         Consumer groupConsumer = null;

         // If a group id is set, then this overrides the consumer chosen round-robin

         SimpleString groupID = ref.getMessage().getSimpleStringProperty(Message.HDR_GROUP_ID);

         if (groupID != null)
         {
            groupConsumer = groups.get(groupID);

            if (groupConsumer != null)
            {
               consumer = groupConsumer;
            }
         }
         
         pos++;

         if (pos == size)
         {
            pos = 0;
         }

         HandleStatus status = handle(ref, consumer);

         if (status == HandleStatus.HANDLED)
         {            
            if (groupID != null && groupConsumer == null)
            {
               groups.put(groupID, consumer);
            }
            
            return true;
         }

         if (pos == startPos)
         {
            // Tried them all

            return false;
         }
      }
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
            QueueImpl.log.error("Failed to expire ref", e);
         }

         return true;
      }
      else
      {
         return false;
      }
   }

   protected void add(final MessageReference ref, final boolean first, final boolean direct)
   {
      if (scheduledDeliveryHandler.checkAndSchedule(ref))
      {
         return;
      }

      if (direct && messageReferences.isEmpty())
      {
         if (deliverDirect(ref))
         {
            return;
         }
      }

      int refs;

      if (first)
      {
         refs = messageReferences.addFirst(ref, ref.getMessage().getPriority());
      }
      else
      {
         refs = messageReferences.addLast(ref, ref.getMessage().getPriority());
      }

      /*
       * We only prompt delivery if there are no messages waiting for delivery - this prevents many executors being
       * unnecessarily queued up
       * During delivery toDeliver is decremented before the message is delivered, therefore if it's delivering the last
       * message, then we cannot have a situation where this delivery is not prompted and message remains stranded in the
       * queue
       */
      if (refs == 1)
      {
         deliverAsync();
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
         QueueImpl.log.warn("removing consumer which did not handle a message, consumer=" + consumer +
                            ", message=" +
                            reference, t);

         // If the consumer throws an exception we remove the consumer
         try
         {
            removeConsumer(consumer);
         }
         catch (Exception e)
         {
            QueueImpl.log.error("Failed to remove consumer", e);
         }
         return HandleStatus.BUSY;
      }

      if (status == null)
      {
         throw new IllegalStateException("ClientConsumer.handle() should never return null");
      }

      return status;
   }

   private void postAcknowledge(final MessageReference ref) throws Exception
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
               QueueImpl.log.warn("Unable to remove message id = " + message.getMessageID() + " please remove manually",
                                  e);
            }
         }
      }

      queue.deliveringCount.decrementAndGet();

      message.decrementRefCount();
   }

   void postRollback(final LinkedList<MessageReference> refs)
   {
      synchronized (this)
      {
         for (MessageReference ref : refs)
         {
            add(ref, true, false);
         }

         deliverAsync();
      }
   }

   // Inner classes
   // --------------------------------------------------------------------------

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
               QueueImpl.log.warn("Error on checkDLQ", e);
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

      public void afterCommit(final Transaction tx) throws Exception
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

      deliverAsync();
   }

   public synchronized boolean isPaused()
   {
      return paused;
   }

   private class DeliverRunner implements Runnable
   {
      public void run()
      {
         try
         {
            deliver();
         }
         catch (Exception e)
         {
            log.error("Failed to deliver", e);
         }
      }
   }
}
