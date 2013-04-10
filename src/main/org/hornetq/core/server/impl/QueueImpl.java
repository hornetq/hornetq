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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.paging.cursor.PagedReference;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.DuplicateIDCache;
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
import org.hornetq.core.settings.HierarchicalRepositoryChangeListener;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionOperation;
import org.hornetq.core.transaction.TransactionPropertyIndexes;
import org.hornetq.core.transaction.impl.BindingsTransactionImpl;
import org.hornetq.core.transaction.impl.TransactionImpl;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.Future;
import org.hornetq.utils.LinkedListIterator;
import org.hornetq.utils.PriorityLinkedList;
import org.hornetq.utils.PriorityLinkedListImpl;
import org.hornetq.utils.ReusableLatch;

/**
 * Implementation of a Queue
 *
 * Completely non blocking between adding to queue and delivering to consumers.
 *
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="clebert.suconic@jboss.com">Clebert Suconic</a>
 */
public class QueueImpl implements Queue
{
   private static final Logger log = Logger.getLogger(QueueImpl.class);

   private static final boolean isTrace = log.isTraceEnabled();

   public static final int REDISTRIBUTOR_BATCH_SIZE = 100;

   public static final int NUM_PRIORITIES = 10;

   public static final int MAX_DELIVERIES_IN_LOOP = 1000;

   public static final int CHECK_QUEUE_SIZE_PERIOD = 100;

   /** If The system gets slow for any reason, this is the maximum time an Delivery or
       or depage executor should be hanging on
   */
   private static final int DELIVERY_TIMEOUT = 1000;

   private static final int FLUSH_TIMEOUT = 10000;

   private final long id;

   private final SimpleString name;

   private final Filter filter;

   private final boolean durable;

   private final boolean temporary;

   private final PostOffice postOffice;

   private volatile boolean queueDestroyed = false;
   
   private final PageSubscription pageSubscription;

   private final LinkedListIterator<PagedReference> pageIterator;

   // Messages will first enter intermediateMessageReferences
   // Before they are added to messageReferences
   // This is to avoid locking the queue on the producer
   private final ConcurrentLinkedQueue<MessageReference> intermediateMessageReferences = new ConcurrentLinkedQueue<MessageReference>();

   // This is where messages are stored
   private final PriorityLinkedList<MessageReference> messageReferences = new PriorityLinkedListImpl<MessageReference>(QueueImpl.NUM_PRIORITIES);

   // The quantity of pagedReferences on messageReferences priority list
   private final AtomicInteger pagedReferences = new AtomicInteger(0);

   // The estimate of memory being consumed by this queue. Used to calculate instances of messages to depage
   private final AtomicInteger queueMemorySize = new AtomicInteger(0);

   // used to control if we should recalculate certain positions inside deliverAsync
   private volatile boolean consumersChanged = true;

   private final List<ConsumerHolder> consumerList = new CopyOnWriteArrayList<ConsumerHolder>();

   private final ScheduledDeliveryHandler scheduledDeliveryHandler;

   private long messagesAdded;

   protected final AtomicInteger deliveringCount = new AtomicInteger(0);

   private boolean paused;

   private static final int MAX_SCHEDULED_RUNNERS = 2;

   // We don't ever need more than two DeliverRunner on the executor's list
   // that is getting the worse scenario possible when one runner is almost finishing before the second started
   // for that we keep a counter of scheduled instances
   private final AtomicInteger scheduledRunners = new AtomicInteger(0);

   private final Runnable deliverRunner = new DeliverRunner();

   private volatile boolean depagePending = false;

   private final StorageManager storageManager;

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private final ScheduledExecutorService scheduledExecutor;

   private final SimpleString address;

   private Redistributor redistributor;

   private final Set<ScheduledFuture<?>> futures = new ConcurrentHashSet<ScheduledFuture<?>>();

   private ScheduledFuture<?> redistributorFuture;

   private ScheduledFuture<?> checkQueueSizeFuture;

   // We cache the consumers here since we don't want to include the redistributor

   private final Set<Consumer> consumerSet = new HashSet<Consumer>();

   private final Map<SimpleString, Consumer> groups = new HashMap<SimpleString, Consumer>();

   private volatile SimpleString expiryAddress;

   private int pos;

   private final Executor executor;

   private boolean internalQueue;

   private volatile long lastDirectDeliveryCheck = 0;

   private volatile boolean directDeliver = true;

   private AddressSettingsRepositoryListener addressSettingsRepositoryListener;

   private ExpiryScanner expiryScanner = new ExpiryScanner();
   
   private final ReusableLatch deliveriesInTransit = new ReusableLatch(0);

   /**
    * This is to avoid multi-thread races on calculating direct delivery,
    * to guarantee ordering will be always be correct
    */
   private final Object directDeliveryGuard = new Object();


   public String debug()
   {
      StringWriter str = new StringWriter();
      PrintWriter out = new PrintWriter(str);

      out.println("queueMemorySize=" + queueMemorySize);

      for (ConsumerHolder holder : consumerList)
      {
         out.println("consumer: " + holder.consumer.debug());
      }

      for (MessageReference reference : intermediateMessageReferences)
      {
         out.print("Intermediate reference:" + reference);
      }

      if (intermediateMessageReferences.isEmpty())
      {
         out.println("No intermediate references");
      }

      boolean foundRef = false;
      Iterator<MessageReference> iter = messageReferences.iterator();
      while (iter.hasNext())
      {
         foundRef = true;
         out.println("reference = " + iter.next());
      }

      if (!foundRef)
      {
         out.println("No permanent references on queue");
      }



      System.out.println(str.toString());

      return str.toString();
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
      this(id,
           address,
           name,
           filter,
           null,
           durable,
           temporary,
           scheduledExecutor,
           postOffice,
           storageManager,
           addressSettingsRepository,
           executor);
   }

   public QueueImpl(final long id,
                    final SimpleString address,
                    final SimpleString name,
                    final Filter filter,
                    final PageSubscription pageSubscription,
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

      this.pageSubscription = pageSubscription;

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
         addressSettingsRepositoryListener = new AddressSettingsRepositoryListener();
         addressSettingsRepository.registerListener(addressSettingsRepositoryListener);
      }
      else
      {
         expiryAddress = null;
      }

      if (pageSubscription != null)
      {
         pageSubscription.setQueue(this);
         this.pageIterator = pageSubscription.iterator();
      }
      else
      {
         this.pageIterator = null;
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
      context.addQueue(address, this);
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

   public SimpleString getAddress()
   {
      return address;
   }

   public long getID()
   {
      return id;
   }

   public PageSubscription getPageSubscription()
   {
      return pageSubscription;
   }

   public Filter getFilter()
   {
      return filter;
   }

   /* Called when a message is cancelled back into the queue */
   public synchronized void addHead(final MessageReference ref)
   {
      flushDeliveriesInTransit();
      if (scheduledDeliveryHandler.checkAndSchedule(ref, false))
      {
         return;
      }

      internalAddHead(ref);

      directDeliver = false;
   }

   /* Called when a message is cancelled back into the queue */
   public synchronized void addHead(final LinkedList<MessageReference> refs)
   {
      flushDeliveriesInTransit();
      for (MessageReference ref: refs)
      {
         addHead(ref);
      }

      resetAllIterators();

      deliverAsync();
   }

   public synchronized void reload(final MessageReference ref)
   {
      queueMemorySize.addAndGet(ref.getMessageMemoryEstimate());
      if (!scheduledDeliveryHandler.checkAndSchedule(ref, true))
      {
         internalAddTail(ref);
      }

      directDeliver = false;

      messagesAdded++;
   }

   public void addTail(final MessageReference ref)
   {
      addTail(ref, false);
   }

   public void addTail(final MessageReference ref, final boolean direct)
   {
      if (scheduledDeliveryHandler.checkAndSchedule(ref, true))
      {
         synchronized (this)
         {
            messagesAdded++;
         }

         return;
      }

      synchronized (directDeliveryGuard)
      {
         // The checkDirect flag is periodically set to true, if the delivery is specified as direct then this causes the
         // directDeliver flag to be re-computed resulting in direct delivery if the queue is empty
         // We don't recompute it on every delivery since executing isEmpty is expensive for a ConcurrentQueue
         if (!directDeliver &&
             direct &&
            System.currentTimeMillis() - lastDirectDeliveryCheck > CHECK_QUEUE_SIZE_PERIOD)
         {
            lastDirectDeliveryCheck = System.currentTimeMillis();

            if (intermediateMessageReferences.isEmpty() &&
                messageReferences.isEmpty() &&
                !pageIterator.hasNext() &&
                !pageSubscription.isPaging())
            {
               // We must block on the executor to ensure any async deliveries have completed or we might get out of order
               // deliveries
               if (flushExecutor() && flushDeliveriesInTransit())
               {
                  // Go into direct delivery mode
                  directDeliver = true;
               }
            }
         }
      }

      if (direct && directDeliver && deliverDirect(ref))
      {
         return;
      }

      // We only add queueMemorySize if not being delivered directly
      queueMemorySize.addAndGet(ref.getMessageMemoryEstimate());

      intermediateMessageReferences.add(ref);

      directDeliver = false;

      // Delivery async will both poll for intermediate reference and deliver to clients
      deliverAsync();
   }

   /**
    * This will wait for any pending deliveries to finish
    */
   private boolean flushDeliveriesInTransit()
   {
      try
      {

         if (deliveriesInTransit.await(DELIVERY_TIMEOUT))
         {
            return true;
         }
         else
         {
            log.warn("Queue " + getName() + ", on address=" + address + ", is taking too long to flush deliveries. Watch out for frozen clients.");
            return false;
         }
      }
      catch (Exception e)
      {
         log.warn(e.getMessage(), e);
         return false;
      }
   }

   public void forceDelivery()
   {
      if (pageSubscription != null && pageSubscription.isPaging())
      {
         if (isTrace)
         {
            log.trace("Force delivery scheduling depage");
         }
         scheduleDepage(false);
      }

      if (isTrace)
      {
         log.trace("Force delivery deliverying async");
      }

      deliverAsync();
   }

   public void deliverAsync()
   {
      if (scheduledRunners.get() < MAX_SCHEDULED_RUNNERS)
      {
         scheduledRunners.incrementAndGet();
         try
         {
            getExecutor().execute(deliverRunner);
         }
         catch (RejectedExecutionException ignored)
         {
            // no-op
            scheduledRunners.decrementAndGet();
         }
      }

   }

   public void close() throws Exception
   {
      if (checkQueueSizeFuture != null)
      {
         checkQueueSizeFuture.cancel(false);
      }

      getExecutor().execute(new Runnable(){
         public void run()
         {
            try
            {
               cancelRedistributor();
            }
            catch (Exception e)
            {
               // nothing that could be done anyway.. just logging
               log.warn(e.getMessage(), e);
            }
         }
      });

      if (addressSettingsRepository != null)
      {
         addressSettingsRepository.unRegisterListener(addressSettingsRepositoryListener);
      }
   }

   public Executor getExecutor()
   {
      if (pageSubscription != null && pageSubscription.isPaging())
      {
         // When in page mode, we don't want to have concurrent IO on the same PageStore
         return pageSubscription.getExecutor();
      }
      else
      {
         return executor;
      }
   }

   /* Only used on tests */
   public void deliverNow()
   {
      deliverAsync();

      flushExecutor();
   }

   public boolean flushExecutor()
   {
      boolean ok = internalFlushExecutor(10000);

      if (!ok)
      {
         log.warn("Couldn't finish waiting executors. Try increasing the thread pool size", new Exception ("trace"));
      }

      return ok;
   }

   private boolean internalFlushExecutor(long timeout)
   {
      Future future = new Future();

      getExecutor().execute(future);

      boolean result = future.await(timeout);

      if (!result)
      {
         log.warn("Queue " + this.getName() + " was busy for more than " + timeout + " miliseconds. There are possibly consumers hanging on a network operation");
      }
      
      return result;
   }

   public synchronized void addConsumer(final Consumer consumer) throws Exception
   {
      if (log.isDebugEnabled())
      {
         log.debug(this + " adding consumer " + consumer);
      }

      flushDeliveriesInTransit();

      consumersChanged = true;

      cancelRedistributor();

      consumerList.add(new ConsumerHolder(consumer));

      consumerSet.add(consumer);
   }

   public synchronized void removeConsumer(final Consumer consumer)
   {
      consumersChanged = true;

       for (ConsumerHolder holder : consumerList) {
           if (holder.consumer == consumer) {
               if (holder.iter != null) {
                   holder.iter.close();
               }
               consumerList.remove(holder);
               break;
           }
       }

      if (pos > 0 && pos >= consumerList.size())
      {
         pos = consumerList.size() - 1;
      }

      consumerSet.remove(consumer);

      LinkedList<SimpleString> groupsToRemove = null;

      for (SimpleString groupID : groups.keySet())
      {
         if (consumer == groups.get(groupID))
         {
            if (groupsToRemove == null)
            {
               groupsToRemove = new LinkedList<SimpleString>();
            }
            groupsToRemove.add(groupID);
         }
      }

      // We use an auxiliary List here to avoid concurrent modification exceptions on the keySet
      // while the iteration is being done.
      // Since that's a simple HashMap there's no Iterator's support with a remove operation
      if (groupsToRemove != null)
      {
         for (SimpleString groupID: groupsToRemove)
         {
            groups.remove(groupID);
         }
      }
   }

   public synchronized void addRedistributor(final long delay)
   {
      if (redistributorFuture != null)
      {
         redistributorFuture.cancel(false);

         futures.remove(redistributorFuture);
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

            redistributorFuture = scheduledExecutor.schedule(dar, delay, TimeUnit.MILLISECONDS);

            futures.add(redistributorFuture);
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
         Redistributor redistributorToRemove = redistributor;
         redistributor = null;

         removeConsumer(redistributorToRemove);
      }

      if (redistributorFuture != null)
      {
         redistributorFuture.cancel(false);

         redistributorFuture = null;
      }
   }

   @Override
   protected void finalize() throws Throwable
   {
      if (checkQueueSizeFuture != null)
      {
         checkQueueSizeFuture.cancel(false);
      }

      cancelRedistributor();

      super.finalize();
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

   public LinkedListIterator<MessageReference> iterator()
   {
      return new SynchronizedIterator(messageReferences.iterator());
   }

   public TotalQueueIterator totalIterator()
   {
      return new TotalQueueIterator();
   }

   public synchronized MessageReference removeReferenceWithID(final long id) throws Exception
   {
      LinkedListIterator<MessageReference> iterator = iterator();

      try
      {

         MessageReference removed = null;

         while (iterator.hasNext())
         {
            MessageReference ref = iterator.next();

            if (ref.getMessage().getMessageID() == id)
            {
               iterator.remove();
               refRemoved(ref);

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
      finally
      {
         iterator.close();
      }
   }

   public synchronized MessageReference getReference(final long id)
   {
      LinkedListIterator<MessageReference> iterator = iterator();

      try
      {

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
      finally
      {
         iterator.close();
      }
   }

   public long getMessageCount()
   {
      return getMessageCount(FLUSH_TIMEOUT);
   }

   public long getMessageCount(final long timeout)
   {
      if (timeout > 0)
      {
         internalFlushExecutor(timeout);
      }
      return getInstantMessageCount();
   }
   
   public long getInstantMessageCount()
   {
      synchronized (this)
      {
         if (pageSubscription != null)
         {
            // messageReferences will have depaged messages which we need to discount from the counter as they are
            // counted on the pageSubscription as well
            return messageReferences.size() + getScheduledCount() +
                   deliveringCount.get() +
                   pageSubscription.getMessageCount();
         }
         else
         {
            return messageReferences.size() + getScheduledCount() + deliveringCount.get();
         }
      }
   }

   public synchronized int getScheduledCount()
   {
      return scheduledDeliveryHandler.getScheduledCount();
   }

   public synchronized List<MessageReference> getScheduledMessages()
   {
      return scheduledDeliveryHandler.getScheduledReferences();
   }
   
   public Map<String, List<MessageReference>> getDeliveringMessages()
   {
      
      List<ConsumerHolder> consumerListClone = cloneConsumersList();

      Map<String, List<MessageReference>> mapReturn = new HashMap<String, List<MessageReference>>();
      
      for (ConsumerHolder holder : consumerListClone)
      {
         LinkedList<MessageReference> msgs = new LinkedList<MessageReference>();
         holder.consumer.getDeliveringMessages(msgs);
         if (msgs.size() > 0)
         {
            mapReturn.put(holder.consumer.toManagementString(), msgs);
         }
      }
      
      return mapReturn;
   }

   public int getDeliveringCount()
   {
      return deliveringCount.get();
   }

   public void acknowledge(final MessageReference ref) throws Exception
   {
      if (ref.isPaged())
      {
         pageSubscription.ack((PagedReference)ref);
         postAcknowledge(ref);
      }
      else
      {
         ServerMessage message = ref.getMessage();

         boolean durableRef = message.isDurable() && durable;

         if (durableRef)
         {
            storageManager.storeAcknowledge(id, message.getMessageID());
         }
         postAcknowledge(ref);
      }

   }

   public void acknowledge(final Transaction tx, final MessageReference ref) throws Exception
   {
      if (ref.isPaged())
      {
         pageSubscription.ackTx(tx, (PagedReference)ref);

         getRefsOperation(tx).addAck(ref);
      }
      else
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
   }

   public void reacknowledge(final Transaction tx, final MessageReference ref) throws Exception
   {
      ServerMessage message = ref.getMessage();

      if (message.isDurable() && durable)
      {
         tx.setContainsPersistent();
      }

      getRefsOperation(tx).addAck(ref);

      // https://issues.jboss.org/browse/HORNETQ-609
      deliveringCount.incrementAndGet();
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

   public void cancel(final Transaction tx, final MessageReference reference)
   {
      getRefsOperation(tx).addAck(reference);
   }

   public synchronized void cancel(final MessageReference reference, final long timeBase) throws Exception
   {
      deliveringCount.decrementAndGet();
      if (checkRedelivery(reference, timeBase))
      {
         if (!scheduledDeliveryHandler.checkAndSchedule(reference, false))
         {
            internalAddHead(reference);
         }

         resetAllIterators();
      }
   }

   public void expire(final MessageReference ref) throws Exception
   {
      if (expiryAddress != null)
      {
         if (isTrace)
         {
            log.trace("moving expired reference " + ref + " to address = " + expiryAddress + " from queue=" + this.getName());
         }
         move(expiryAddress, ref, true, false);
      }
      else
      {
         if (isTrace)
         {
            log.trace("expiry is null, just acking expired message for reference " + ref + " from queue=" + this.getName());
         }
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
      return getMessagesAdded(FLUSH_TIMEOUT);
   }

   public long getMessagesAdded(final long timeout)
   {
      if (timeout > 0) internalFlushExecutor(timeout);
      return getInstantMessagesAdded();
   }

   public synchronized long getInstantMessagesAdded()
   {
      if (pageSubscription != null)
      {
         return messagesAdded + pageSubscription.getCounter().getValue() - pagedReferences.get();
      }
      else
      {
         return messagesAdded;
      }
   }

   public int deleteAllReferences() throws Exception
   {
      return deleteMatchingReferences(null);
   }

   public synchronized int deleteMatchingReferences(final Filter filter) throws Exception
   {
          return iterQueue(filter, new QueueIterateAction()
                                  {
                                     @Override
                                     public void actMessage(Transaction tx, MessageReference ref) throws Exception
                                     {
                                        deliveringCount.incrementAndGet();
                                        acknowledge(tx, ref);
                                        refRemoved(ref);
                                     }
                                  });
   }


   /**
    * This is a generic method for any method interacting on the Queue to move or delete messages
    * Instead of duplicate the feature we created an abstract class where you pass the logic for each message.
    * Too bad there's not such thing as a function pointer in Java (as there is in scala).
    * @param filter
    * @param messageAction
    * @return
    * @throws Exception
    */
   private synchronized int iterQueue(final Filter filter, QueueIterateAction messageAction) throws Exception
   {
      int count = 0;
      int txCount = 0;

      Transaction tx = new TransactionImpl(storageManager);

      LinkedListIterator<MessageReference> iter = iterator();
      try
      {

         while (iter.hasNext())
         {
            MessageReference ref = iter.next();

            if (ref.isPaged() && queueDestroyed)
            {
               // this means the queue is being removed
               // hence paged references are just going away through
               // page cleanup
               continue;
            }

            if (filter == null || filter.match(ref.getMessage()))
            {
               messageAction.actMessage(tx, ref);
               iter.remove();
               txCount++;
               count++;
            }
         }

         if (txCount > 0)
         {
            tx.commit();

            tx = new TransactionImpl(storageManager);

            txCount = 0;
         }

         List<MessageReference> cancelled = scheduledDeliveryHandler.cancel(filter);
         for (MessageReference messageReference : cancelled)
         {
            messageAction.actMessage(tx, messageReference);
            count++;
            txCount++;
         }

         if (txCount > 0)
         {
            tx.commit();
            tx = new TransactionImpl(storageManager);
            txCount = 0;
         }


         if (pageIterator != null && !queueDestroyed)
         {
            // System.out.println("QueueMemorySize before depage = " + queueMemorySize.get());
            while (pageIterator.hasNext())
            {
               PagedReference reference = pageIterator.next();
               pageIterator.remove();

               if (filter == null || filter.match(reference.getMessage()))
               {
                  count++;
                  txCount++;
                  messageAction.actMessage(tx, reference);
               }
               else
               {
                  addTail(reference, false);
               }

               if (txCount > 0 && txCount % 500 == 0)
               {
                  tx.commit();
                  tx = new TransactionImpl(storageManager);
                  txCount = 0;
               }
            }
         }

         if (txCount > 0)
         {
            tx.commit();
            tx = null;
         }



         if (filter != null && !queueDestroyed && pageSubscription != null)
         {
            scheduleDepage(false);
         }

         return count;
      }
      finally
      {
         iter.close();
      }
   }


   public void destroyPaging() throws Exception
   {
      // it could be null on embedded or certain unit tests
      if (pageSubscription != null)
      {
         pageSubscription.destroy();
         pageSubscription.cleanupEntries(true);
      }
   }

   public synchronized boolean deleteReference(final long messageID) throws Exception
   {
      boolean deleted = false;

      Transaction tx = new TransactionImpl(storageManager);

      LinkedListIterator<MessageReference> iter = iterator();
      try
      {

         while (iter.hasNext())
         {
            MessageReference ref = iter.next();
            if (ref.getMessage().getMessageID() == messageID)
            {
               deliveringCount.incrementAndGet();
               acknowledge(tx, ref);
               iter.remove();
               refRemoved(ref);
               deleted = true;
               break;
            }
         }

         tx.commit();

         return deleted;
      }
      finally
      {
         iter.close();
      }
   }

   public synchronized boolean expireReference(final long messageID) throws Exception
   {
      if (expiryAddress != null && expiryAddress.equals(this.address))
      {
         // check expire with itself would be silly (waste of time)
         if (log.isDebugEnabled())
            log.debug("Cannot expire from " + address + " into " + expiryAddress);
         return false;
      }

      LinkedListIterator<MessageReference> iter = iterator();
      try
      {

         while (iter.hasNext())
         {
            MessageReference ref = iter.next();
            if (ref.getMessage().getMessageID() == messageID)
            {
               deliveringCount.incrementAndGet();
               expire(ref);
               iter.remove();
               refRemoved(ref);
               return true;
            }
         }
         return false;
      }
      finally
      {
         iter.close();
      }
   }

   public synchronized int expireReferences(final Filter filter) throws Exception
   {
      if (expiryAddress != null && expiryAddress.equals(this.address))
      {
         // check expire with itself would be silly (waste of time)
         if (log.isDebugEnabled())
            log.debug("Cannot expire from " + address + " into " + expiryAddress);
         return 0;
      }

      Transaction tx = new TransactionImpl(storageManager);

      int count = 0;
      LinkedListIterator<MessageReference> iter = iterator();

      try
      {

         while (iter.hasNext())
         {
            MessageReference ref = iter.next();
            if (filter == null || filter.match(ref.getMessage()))
            {
               deliveringCount.incrementAndGet();
               expire(tx, ref);
               iter.remove();
               refRemoved(ref);
               count++;
            }
         }

         tx.commit();

         return count;
      }
      finally
      {
         iter.close();
      }
   }
   
   public void deleteQueue() throws Exception
   {
      synchronized (this)
      {
         this.queueDestroyed = true;
      }
      
      Transaction tx = new BindingsTransactionImpl(storageManager);
      
      try
      {
         postOffice.removeBinding(name, tx);
         
         deleteAllReferences();
   
         destroyPaging();
   
         if (isDurable())
         {
            storageManager.deleteQueueBinding(tx.getID(), getID());
            tx.setContainsPersistent();
         }
         
         tx.commit();
      }
      catch (Exception e)
      {
         tx.rollback();
         throw e;
      }

   }

   public void expireReferences()
   {
      if (expiryAddress != null && expiryAddress.equals(this.address))
      {
         // check expire with itself would be silly (waste of time)
         if (log.isDebugEnabled())
            log.debug("Cannot expire from " + address + " into " + expiryAddress);
         return;
      }

      if (!queueDestroyed && expiryScanner.scannerRunning.get() == 0)
      {
         expiryScanner.scannerRunning.incrementAndGet();
         getExecutor().execute(expiryScanner);
      }
   }

   class ExpiryScanner implements Runnable
   {
      public AtomicInteger scannerRunning = new AtomicInteger(0);

      public void run()
      {
         synchronized (QueueImpl.this)
         {
            if (queueDestroyed)
            {
               return;
            }
            
            LinkedListIterator<MessageReference> iter = iterator();

            try
            {
               boolean expired = false;
               boolean hasElements = false;
               while (postOffice.isStarted() && iter.hasNext())
               {
                  hasElements = true;
                  MessageReference ref = iter.next();
                  try
                  {
                     if (ref.getMessage().isExpired())
                     {
                        deliveringCount.incrementAndGet();
                        expired = true;
                        expire(ref);
                        iter.remove();
                        refRemoved(ref);
                     }
                  }
                  catch (Exception e)
                  {
                     log.debug("Error expiry reference " + ref, e);
                  }

               }

               // If empty we need to schedule depaging to make sure we would depage expired messages as well
               if ((!hasElements || expired) && pageIterator != null && pageIterator.hasNext())
               {
                  scheduleDepage(true);
               }
            }
            finally
            {
               try
               {
                  iter.close();
               }
               catch (Throwable ignored)
               {
               }
               scannerRunning.decrementAndGet();
            }
         }
      }
   }

   public synchronized boolean sendMessageToDeadLetterAddress(final long messageID) throws Exception
   {
      LinkedListIterator<MessageReference> iter = iterator();

      try
      {
         while (iter.hasNext())
         {
            MessageReference ref = iter.next();
            if (ref.getMessage().getMessageID() == messageID)
            {
               deliveringCount.incrementAndGet();
               sendToDeadLetterAddress(ref);
               iter.remove();
               refRemoved(ref);
               return true;
            }
         }
         return false;
      }
      finally
      {
         iter.close();
      }
   }

   public synchronized int sendMessagesToDeadLetterAddress(Filter filter) throws Exception
   {
      int count = 0;
      LinkedListIterator<MessageReference> iter = iterator();

      try
      {
         while (iter.hasNext())
         {
            MessageReference ref = iter.next();
            if (filter == null || filter.match(ref.getMessage()))
            {
               deliveringCount.incrementAndGet();
               sendToDeadLetterAddress(ref);
               iter.remove();
               refRemoved(ref);
               count++;
            }
         }
         return count;
      }
      finally
      {
         iter.close();
      }
   }

   public boolean moveReference(final long messageID, final SimpleString toAddress) throws Exception
   {
      return moveReference(messageID, toAddress, false);
   }

   public synchronized boolean moveReference(final long messageID,
                                             final SimpleString toAddress,
                                             final boolean rejectDuplicate) throws Exception
   {
      LinkedListIterator<MessageReference> iter = iterator();

      try
      {
         while (iter.hasNext())
         {
            MessageReference ref = iter.next();
            if (ref.getMessage().getMessageID() == messageID)
            {
               iter.remove();
               refRemoved(ref);
               deliveringCount.incrementAndGet();
               try
               {
                  move(toAddress, ref, false, rejectDuplicate);
               }
               catch (Exception e)
               {
                  deliveringCount.decrementAndGet();
                  throw e;
               }
               return true;
            }
         }
         return false;
      }
      finally
      {
         iter.close();
      }
   }

   public int moveReferences(final Filter filter, final SimpleString toAddress) throws Exception
   {
      return moveReferences(filter, toAddress, false);
   }

   public synchronized int moveReferences(final Filter filter,
                                          final SimpleString toAddress,
                                          final boolean rejectDuplicates) throws Exception
   {
      final DuplicateIDCache targetDuplicateCache = postOffice.getDuplicateIDCache(toAddress);

      return iterQueue(filter, new QueueIterateAction()
      {
         @Override
         public void actMessage(Transaction tx, MessageReference ref) throws Exception
         {
            boolean ignored = false;

            deliveringCount.incrementAndGet();

            if (rejectDuplicates)
            {
               byte[] duplicateBytes = ref.getMessage().getDuplicateIDBytes();
               if (duplicateBytes != null)
               {
                  if (targetDuplicateCache.contains(duplicateBytes))
                  {
                     log.info("Message with duplicate ID " + ref.getMessage().getDuplicateProperty() +
                        " was already set at " +
                        toAddress +
                        ". Move from " +
                        QueueImpl.this.address +
                        " being ignored and message removed from " +
                        QueueImpl.this.address);
                     acknowledge(tx, ref);
                     ignored = true;
                  }
               }
            }

            if (!ignored)
            {
               move(toAddress, tx, ref, false, rejectDuplicates);
            }
         }
      });
   }

   public synchronized boolean changeReferencePriority(final long messageID, final byte newPriority) throws Exception
   {
      LinkedListIterator<MessageReference> iter = iterator();

      try
      {

         while (iter.hasNext())
         {
            MessageReference ref = iter.next();
            if (ref.getMessage().getMessageID() == messageID)
            {
               iter.remove();
               refRemoved(ref);
               ref.getMessage().setPriority(newPriority);
               addTail(ref, false);
               return true;
            }
         }

         return false;
      }
      finally
      {
         iter.close();
      }
   }

   public synchronized int changeReferencesPriority(final Filter filter, final byte newPriority) throws Exception
   {
      LinkedListIterator<MessageReference> iter = iterator();

      try
      {
         int count = 0;
         while (iter.hasNext())
         {
            MessageReference ref = iter.next();
            if (filter == null || filter.match(ref.getMessage()))
            {
               count++;
               iter.remove();
               refRemoved(ref);
               ref.getMessage().setPriority(newPriority);
               addTail(ref, false);
            }
         }
         return count;
      }
      finally
      {
         iter.close();
      }
   }

   public synchronized void resetAllIterators()
   {
      for (ConsumerHolder holder : this.consumerList)
      {
         if (holder.iter != null)
         {
            holder.iter.close();
         }
         holder.iter = null;
      }
   }

   public synchronized void pause()
   {
      try
      {
         this.flushDeliveriesInTransit();
      }
      catch (Exception e)
      {
         log.warn(e.getMessage(), e);
      }
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

   public boolean isDirectDeliver()
   {
      return directDeliver;
   }

   /**
    * @return the internalQueue
    */
   public boolean isInternalQueue()
   {
      return internalQueue;
   }

   /**
    * @param internalQueue the internalQueue to set
    */
   public void setInternalQueue(boolean internalQueue)
   {
      this.internalQueue = internalQueue;
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
      if (!(other instanceof QueueImpl)) return false;

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
      return "QueueImpl[name=" + name.toString() + ", postOffice=" + this.postOffice + "]@" + Integer.toHexString(System.identityHashCode(this));
   }

   // Private
   // ------------------------------------------------------------------------------

   /**
    * @param ref
    */
   private void internalAddTail(final MessageReference ref)
   {
      refAdded(ref);
      messageReferences.addTail(ref, ref.getMessage().getPriority());
   }

   /**
    * @param ref
    */
   private void internalAddHead(final MessageReference ref)
   {
      queueMemorySize.addAndGet(ref.getMessageMemoryEstimate());
      refAdded(ref);
      messageReferences.addHead(ref, ref.getMessage().getPriority());
   }

   private synchronized void doInternalPoll()
   {

      int added = 0;
      MessageReference ref;

      while ((ref = intermediateMessageReferences.poll()) != null)
      {
         internalAddTail(ref);

         messagesAdded++;
         if (added++ > MAX_DELIVERIES_IN_LOOP)
         {
            // if we just keep polling from the intermediate we could starve in case there's a sustained load
            deliverAsync();
            return;
         }
      }
   }

   // This method will deliver as many messages as possible until all consumers are busy or there are no more matching
   // or available messages
   private void deliver()
   {
      if (log.isDebugEnabled())
      {
         log.debug(this + " doing deliver. messageReferences=" + messageReferences.size());
      }

      doInternalPoll();

      // Either the iterator is empty or the consumer is busy
      int noDelivery = 0;

      int size = 0;

      int endPos = -1;

      int handled = 0;

      long timeout = System.currentTimeMillis() + DELIVERY_TIMEOUT;

      while (true)
      {
         if (handled == MAX_DELIVERIES_IN_LOOP)
         {
            // Schedule another one - we do this to prevent a single thread getting caught up in this loop for too
            // long

            deliverAsync();

            return;
         }

         if (System.currentTimeMillis() > timeout)
         {
            if (isTrace)
            {
               log.trace("delivery has been running for too long. Scheduling another delivery task now");
            }

            deliverAsync();

            return;
         }


         MessageReference ref;

         Consumer handledconsumer = null;

         synchronized (this)
         {

            // Need to do these checks inside the synchronized
            if (paused || consumerList.isEmpty())
            {
               return;
            }

            if (messageReferences.size() == 0)
            {
               break;
            }

            if (endPos < 0 || consumersChanged)
            {
               consumersChanged = false;

               size = consumerList.size();

               endPos = pos -1;

               if (endPos < 0)
               {
                  endPos = size -1;
                  noDelivery = 0;
               }
            }

            ConsumerHolder holder = consumerList.get(pos);

            Consumer consumer = holder.consumer;

            if (holder.iter == null)
            {
               holder.iter = messageReferences.iterator();
            }

            if (holder.iter.hasNext())
            {
               ref = holder.iter.next();
            }
            else
            {
               ref = null;
            }
            if (ref == null)
            {
               noDelivery++;
            }
            else
            {
               if (checkExpired(ref))
               {
                  if (isTrace)
                  {
                     log.trace("Reference " + ref + " being expired");
                  }
                  holder.iter.remove();

                  refRemoved(ref);

                  handled++;

                  continue;
               }

               Consumer groupConsumer = null;

               if (isTrace)
               {
                  log.trace("Queue " + this.getName() + " is delivering reference " + ref);
               }

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

                  deliveriesInTransit.countUp();

                  handledconsumer = consumer;

                  holder.iter.remove();

                  refRemoved(ref);

                  if (groupID != null && groupConsumer == null)
                  {
                     groups.put(groupID, consumer);
                  }

                  handled++;
               }
               else if (status == HandleStatus.BUSY)
               {
                  holder.iter.repeat();

                  noDelivery++;
               }
               else if (status == HandleStatus.NO_MATCH)
               {
                  // nothing to be done on this case, the iterators will just jump next
               }
            }

            if (pos == endPos)
            {
               // Round robin'd all

               if (noDelivery == size)
               {
                  if (handledconsumer != null)
                  {
                     // this shouldn't really happen,
                     // however I'm keeping this as an assertion case future developers ever change the logic here on this class
                     log.warn("Internal error! Delivery logic has identified a non delivery and still handled a consumer!");
                  }
                  else
                  {
                     if (log.isDebugEnabled())
                     {
                        log.debug(this + "::All the consumers were busy, giving up now");
                     }
                     break;
                  }
               }

               noDelivery = 0;
            }

            pos++;

            if (pos == size)
            {
               pos = 0;
            }
         }

         if (handledconsumer != null)
         {
            proceedDeliver(handledconsumer, ref);
         }

      }

      if (pageIterator != null && messageReferences.size() == 0 && pageSubscription.isPaging() && pageIterator.hasNext() && !depagePending)
      {
         scheduleDepage(false);
      }
   }


   /**
    * @param ref
    */
   private void refRemoved(MessageReference ref)
   {
      queueMemorySize.addAndGet(-ref.getMessageMemoryEstimate());
      if (ref.isPaged())
      {
         pagedReferences.decrementAndGet();
      }
   }

   /**
    * @param ref
    */
   protected void refAdded(final MessageReference ref)
   {
      if (ref.isPaged())
      {
         pagedReferences.incrementAndGet();
      }
   }

   private void scheduleDepage(final boolean scheduleExpiry)
   {
      if (!depagePending)
      {
         if (isTrace)
         {
            log.trace("Scheduling depage for queue " + this.getName());
         }
         depagePending = true;
         pageSubscription.getExecutor().execute(new DepageRunner(scheduleExpiry));
      }
   }

   private void depage(final boolean scheduleExpiry)
   {
      depagePending = false;

      if (paused || pageIterator == null)
      {
         return;
      }

      long maxSize = pageSubscription.getPagingStore().getPageSizeBytes();


      long timeout = System.currentTimeMillis() + DELIVERY_TIMEOUT;

      if (isTrace)
      {
         log.trace("QueueMemorySize before depage on queue=" + this.getName() + " is " + queueMemorySize.get());
      }

      this.directDeliver = false;

      int depaged = 0;
      while (timeout > System.currentTimeMillis() && queueMemorySize.get() < maxSize && pageIterator.hasNext())
      {
         depaged++;
         PagedReference reference = pageIterator.next();
         if (isTrace)
         {
            log.trace("Depaging reference " + reference + " on queue " + this.getName());
         }
         addTail(reference, false);
         pageIterator.remove();
      }

      if (log.isDebugEnabled())
      {
         if (depaged == 0 && queueMemorySize.get() >= maxSize)
         {
            log.debug("Couldn't depage any message as the maxSize on the queue was achieved. " + "There are too many pending messages to be acked in reference to the page configuration");
         }

         if (log.isDebugEnabled())
         {
            log.debug("Queue Memory Size after depage on queue=" + this.getName() +
                      " is " +
                      queueMemorySize.get() +
                      " with maxSize = " +
                      maxSize +
                      ". Depaged " +
                      depaged +
                      " messages, pendingDelivery=" +  messageReferences.size() + ", intermediateMessageReferences= " + intermediateMessageReferences.size() +
                      ", queueDelivering=" + deliveringCount.get());

         }
      }

      deliverAsync();

      if (depaged>0 && scheduleExpiry)
      {
         // This will just call an executor
         expireReferences();
      }
   }

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

         consumersChanged = true;

         redistributor.start();

         deliverAsync();
      }
   }

   public boolean checkRedelivery(final MessageReference reference, final long timeBase) throws Exception
   {
      ServerMessage message = reference.getMessage();

      if (internalQueue)
      {
         if (isTrace)
         {
            log.trace("Queue " + this.getName() + " is an internal queue, no checkRedelivery");
         }
         // no DLQ check on internal queues
         return true;
      }

      if (!internalQueue && message.isDurable() && durable && !reference.isPaged())
      {
         storageManager.updateDeliveryCount(reference);
      }

      AddressSettings addressSettings = addressSettingsRepository.getMatch(address.toString());

      int maxDeliveries = addressSettings.getMaxDeliveryAttempts();

      // First check DLA
      if (maxDeliveries > 0 && reference.getDeliveryCount() >= maxDeliveries)
      {
         if (isTrace)
         {
            log.trace("Sending reference " + reference + " to DLA = " + addressSettings.getDeadLetterAddress() +  " since ref.getDeliveryCount=" + reference.getDeliveryCount() + "and maxDeliveries=" + maxDeliveries + " from queue=" + this.getName());
         }
         sendToDeadLetterAddress(reference, addressSettings.getDeadLetterAddress());

         return false;
      }
      else
      {
         // Second check Redelivery Delay
         long redeliveryDelay = addressSettings.getRedeliveryDelay();

         if (redeliveryDelay > 0)
         {
            if (isTrace)
            {
               log.trace("Setting redeliveryDelay=" + redeliveryDelay + " on reference=" + reference);
            }
            reference.setScheduledDeliveryTime(timeBase + redeliveryDelay);

            if (!reference.isPaged() && message.isDurable() && durable)
            {
               storageManager.updateScheduledDeliveryTime(reference);
            }
         }

         deliveringCount.decrementAndGet();

         return true;
      }
   }

   /** Used on testing only **/
   public int getNumberOfReferences()
   {
      return messageReferences.size();
   }

   private void move(final SimpleString toAddress,
                     final Transaction tx,
                     final MessageReference ref,
                     final boolean expiry,
                     final boolean rejectDuplicate) throws Exception
   {
      ServerMessage copyMessage = makeCopy(ref, expiry);

      copyMessage.setAddress(toAddress);

      postOffice.route(copyMessage, tx, false, rejectDuplicate);

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
            move(expiryAddress, tx, ref, true, true);
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
      sendToDeadLetterAddress(ref, addressSettingsRepository.getMatch(address.toString()).getDeadLetterAddress());
   }

   private void sendToDeadLetterAddress(final MessageReference ref, final  SimpleString deadLetterAddress) throws Exception
   {
      if (deadLetterAddress != null)
      {
         Bindings bindingList = postOffice.getBindingsForAddress(deadLetterAddress);

         if (bindingList.getBindings().isEmpty())
         {
            QueueImpl.log.warn("Message " + ref + " has exceeded max delivery attempts. No bindings for Dead Letter Address " + deadLetterAddress +
                               " so dropping it");
            acknowledge(ref);
         }
         else
         {

            QueueImpl.log.warn("Message " + ref + " has reached maximum delivery attempts, sending it to Dead Letter Address " + deadLetterAddress +
                               " from " +
                               name);
            move(deadLetterAddress, ref, false, false);
         }
      }
      else
      {
         QueueImpl.log.warn("Message has exceeded max delivery attempts. No Dead Letter Address configured for queue " + name +
                            " so dropping it");

         acknowledge(ref);
      }
   }

   private void move(final SimpleString address, final MessageReference ref, final boolean expiry, final boolean rejectDuplicate) throws Exception
   {
      Transaction tx = new TransactionImpl(storageManager);

      ServerMessage copyMessage = makeCopy(ref, expiry);

      copyMessage.setAddress(address);

      postOffice.route(copyMessage, tx, false, rejectDuplicate);

      acknowledge(tx, ref);

      tx.commit();
   }

   /*
    * This method delivers the reference on the callers thread - this can give us better latency in the case there is nothing in the queue
    * note: you require to synchronize this at the callers
    */
   private boolean deliverDirect(final MessageReference ref)
   {
      synchronized (this)
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

               messagesAdded++;

               deliveriesInTransit.countUp();
               proceedDeliver(consumer, ref);
               return true;
            }

            if (pos == startPos)
            {
               // Tried them all
               break;
            }
         }
         return false;
      }
   }

   private void proceedDeliver(Consumer consumer, MessageReference reference)
   {
      try
      {
         consumer.proceedDeliver(reference);
      }
      catch (Throwable t)
      {
         log.warn("removing consumer which did not handle a message, consumer=" + consumer + ", message=" + reference);

         synchronized (this)
         {
            flushDeliveriesInTransit();
            // If the consumer throws an exception we remove the consumer
            try
            {
               removeConsumer(consumer);
            }
            catch (Exception e)
            {
               log.warn(e.getMessage(), e);
            }

            // The message failed to be delivered, hence we try again
            addHead(reference);
         }
      }
      finally
      {
         deliveriesInTransit.countDown();
      }
   }

   private boolean checkExpired(final MessageReference reference)
   {
      if (reference.getMessage().isExpired())
      {
         if (isTrace)
         {
            log.trace("Reference " + reference + " is expired");
         }
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
   
   private List<ConsumerHolder> cloneConsumersList()
   {
      List<ConsumerHolder> consumerListClone;
      
      synchronized (this)
      {
         consumerListClone = new ArrayList<ConsumerHolder>(consumerList.size());
         consumerListClone.addAll(consumerList);
      }
      return consumerListClone;
   }

   // Protected as testcases may change this behaviour
   protected void postAcknowledge(final MessageReference ref)
   {
      QueueImpl queue = (QueueImpl)ref.getQueue();

      queue.deliveringCount.decrementAndGet();

      if (ref.isPaged())
      {
         // nothing to be done
         return;
      }

      final ServerMessage message = ref.getMessage();

      boolean durableRef = message.isDurable() && queue.durable;

      try
      {
         message.decrementRefCount();
      }
      catch (Exception e)
      {
         QueueImpl.log.warn("Unable to decrement reference counting", e);
      }

      if (durableRef)
      {
         int count = message.decrementDurableRefCount();

         if (count == 0)
         {
            // Note - we MUST store the delete after the preceding ack has been committed to storage, we cannot combine
            // the last ack and delete into a single delete.
            // This is because otherwise we could have a situation where the same message is being acked concurrently
            // from two different queues on different sessions.
            // One decrements the ref count, then the other stores a delete, the delete gets committed, but the first
            // ack isn't committed, then the server crashes and on
            // recovery the message is deleted even though the other ack never committed

            // also note then when this happens as part of a transaction it is the tx commit of the ack that is
            // important not this

            // Also note that this delete shouldn't sync to disk, or else we would build up the executor's queue
            // as we can't delete each messaging with sync=true while adding messages transactionally.
            // There is a startup check to remove non referenced messages case these deletes fail
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
   }

   void postRollback(final LinkedList<MessageReference> refs)
   {
      addHead(refs);
   }

   // Inner classes
   // --------------------------------------------------------------------------

   private static class ConsumerHolder
   {
      ConsumerHolder(final Consumer consumer)
      {
         this.consumer = consumer;
      }

      final Consumer consumer;

      LinkedListIterator<MessageReference> iter;
   }

   private final class RefsOperation implements TransactionOperation
   {
      List<MessageReference> refsToAck = new ArrayList<MessageReference>();

      List<ServerMessage> pagedMessagesToPostACK = null;

      synchronized void addAck(final MessageReference ref)
      {
         refsToAck.add(ref);
         if (ref.isPaged())
         {
            if (pagedMessagesToPostACK == null)
            {
               pagedMessagesToPostACK = new ArrayList<ServerMessage>();
            }
            pagedMessagesToPostACK.add(ref.getMessage());
         }
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

         long timeBase = System.currentTimeMillis();

         for (MessageReference ref : refsToAck)
         {
            if (log.isTraceEnabled())
            {
               log.trace("rolling back " + ref);
            }
            try
            {
               if (ref.getQueue().checkRedelivery(ref, timeBase))
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

      public void afterCommit(final Transaction tx)
      {
         for (MessageReference ref : refsToAck)
         {
            synchronized (ref.getQueue())
            {
               postAcknowledge(ref);
            }
         }

         if (pagedMessagesToPostACK != null)
         {
            for (ServerMessage msg : pagedMessagesToPostACK)
            {
               try
               {
                  msg.decrementRefCount();
               }
               catch (Exception e)
               {
                  log.warn(e.getMessage(), e);
               }
            }
         }
      }

      public void beforePrepare(final Transaction tx) throws Exception
      {
      }

      public void beforeRollback(final Transaction tx) throws Exception
      {
      }

      public List<MessageReference> getRelatedMessageReferences()
      {
         return refsToAck;
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
         finally
         {
            scheduledRunners.decrementAndGet();
         }
      }
   }

   private class DepageRunner implements Runnable
   {
      final boolean scheduleExpiry;

      public DepageRunner(boolean scheduleExpiry)
      {
         this.scheduleExpiry = scheduleExpiry;
      }

      public void run()
      {
         try
         {
            depage(scheduleExpiry);
         }
         catch (Exception e)
         {
            log.error("Failed to deliver", e);
         }
      }
   }

   /**
    * This will determine the actions that could be done while iterate the queue through iterQueue
    *
    * @author clebertsuconic
    */
   abstract class QueueIterateAction
   {
      public abstract void actMessage(Transaction tx, MessageReference ref) throws Exception;
   }

   /* For external use we need to use a synchronized version since the list is not thread safe */
   private class SynchronizedIterator implements LinkedListIterator<MessageReference>
   {
      private final LinkedListIterator<MessageReference> iter;

      SynchronizedIterator(LinkedListIterator<MessageReference> iter)
      {
         this.iter = iter;
      }

      public void close()
      {
         synchronized (QueueImpl.this)
         {
            iter.close();
         }
      }

      public void repeat()
      {
         synchronized (QueueImpl.this)
         {
            iter.repeat();
         }
      }

      public boolean hasNext()
      {
         synchronized (QueueImpl.this)
         {
            return iter.hasNext();
         }
      }

      public MessageReference next()
      {
         synchronized (QueueImpl.this)
         {
            return iter.next();
         }
      }

      public void remove()
      {
         synchronized (QueueImpl.this)
         {
            iter.remove();
         }
      }

   }

   private class AddressSettingsRepositoryListener implements HierarchicalRepositoryChangeListener
   {
      @Override
      public void onChange()
      {
         expiryAddress = addressSettingsRepository.getMatch(address.toString()).getExpiryAddress();
      }
   }

   // Readonly (no remove) iterator over the messages in the queue, in order of
   // paging store, intermediateMessageReferences and MessageReferences
   private class TotalQueueIterator implements LinkedListIterator<MessageReference>
   {
      LinkedListIterator<PagedReference> pageIter = null;

      Iterator<MessageReference> interIterator = null;

      LinkedListIterator<MessageReference> messagesIterator = null;

      public TotalQueueIterator()
      {
         if (pageSubscription != null)
         {
            pageIter = pageSubscription.iterator();
         }
         interIterator = intermediateMessageReferences.iterator();
         messagesIterator = new SynchronizedIterator(messageReferences.iterator());
      }

      @Override
      public boolean hasNext()
      {
         if (pageIter != null)
         {
            if (pageIter.hasNext())
            {
               return true;
            }
         }
         if (interIterator.hasNext())
         {
            return true;
         }
         return messagesIterator.hasNext();

      }

      @Override
      public MessageReference next()
      {
         if (pageIter != null)
         {
            if (pageIter.hasNext())
            {
               return pageIter.next();
            }
         }
         if (interIterator.hasNext())
         {
            return interIterator.next();
         }
         return messagesIterator.next();
      }

      @Override
      public void remove()
      {
      }

      @Override
      public void repeat()
      {
      }

      @Override
      public void close()
      {
         if (pageIter != null)
            pageIter.close();
         messagesIterator.close();
      }
   }
}
