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
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.artemis.utils.critical.CriticalAnalyzer;
import org.apache.activemq.artemis.utils.critical.CriticalComponentImpl;
import org.apache.activemq.artemis.utils.critical.EmptyCriticalAnalyzer;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.management.CoreNotificationType;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.paging.cursor.PagedReference;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.DuplicateIDCache;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.impl.PostOfficeImpl;
import org.hornetq.core.remoting.server.RemotingService;
import org.hornetq.core.server.Consumer;
import org.hornetq.core.server.HandleStatus;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.QueueFactory;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ScheduledDeliveryHandler;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.cluster.impl.Redistributor;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.HierarchicalRepositoryChangeListener;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.settings.impl.SlowConsumerPolicy;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionOperationAbstract;
import org.hornetq.core.transaction.TransactionPropertyIndexes;
import org.hornetq.core.transaction.impl.BindingsTransactionImpl;
import org.hornetq.core.transaction.impl.TransactionImpl;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.utils.FutureLatch;
import org.hornetq.utils.LinkedListIterator;
import org.hornetq.utils.PriorityLinkedList;
import org.hornetq.utils.PriorityLinkedListImpl;
import org.hornetq.utils.ReusableLatch;
import org.hornetq.utils.TypedProperties;

/**
 * Implementation of a Queue
 * <p/>
 * Completely non blocking between adding to queue and delivering to consumers.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="clebert.suconic@jboss.com">Clebert Suconic</a>
 */
public class QueueImpl extends CriticalComponentImpl implements Queue
{
   protected static final int CRITICAL_PATHS = 4;
   protected static final int CRITICAL_PATH_ADD_TAIL = 0;
   protected static final int CRITICAL_PATH_ADD_HEAD = 1;
   protected static final int CRITICAL_DELIVER = 2;
   protected static final int CRITICAL_CONSUMER = 3;

   private static final boolean isTrace = HornetQServerLogger.LOGGER.isTraceEnabled();

   public static final int REDISTRIBUTOR_BATCH_SIZE = 100;

   public static final int NUM_PRIORITIES = 10;

   public static final int MAX_DELIVERIES_IN_LOOP = 1000;

   public static final int CHECK_QUEUE_SIZE_PERIOD = 100;

   /**
    * If The system gets slow for any reason, this is the maximum time an Delivery or
    * or depage executor should be hanging on
    */
   public static final int DELIVERY_TIMEOUT = 1000;

   private static final int FLUSH_TIMEOUT = 10000;

   private final long id;

   private final SimpleString name;

   private volatile Filter filter;

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

   protected final ScheduledDeliveryHandler scheduledDeliveryHandler;

   private volatile long messagesAdded;

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

   private final ExpiryScanner expiryScanner = new ExpiryScanner();

   private final ReusableLatch deliveriesInTransit = new ReusableLatch(0);

   private AtomicLong queueRateCheckTime = new AtomicLong(System.currentTimeMillis());

   private AtomicLong messagesAddedSnapshot = new AtomicLong(0);

   private ScheduledFuture slowConsumerReaperFuture;

   private SlowConsumerReaperRunnable slowConsumerReaperRunnable;

   private long messagesAcknowledged;

   private final QueueFactory factory;

   /**
    * This is to avoid multi-thread races on calculating direct delivery,
    * to guarantee ordering will be always be correct
    */
   private final Object directDeliveryGuard = new Object();

   /**
    * For testing only
    * */
   public List<SimpleString> getGroupsUsed()
   {
      final CountDownLatch flush = new CountDownLatch(1);
      executor.execute(new Runnable()
      {
         public void run()
         {
            flush.countDown();
         }
      });
      try
      {
         flush.await(10, TimeUnit.SECONDS);
      }
      catch (Exception ignored)
      {
      }

      synchronized (this)
      {
         ArrayList<SimpleString> groupsUsed = new ArrayList<SimpleString>();
         groupsUsed.addAll(groups.keySet());
         return groupsUsed;
      }
   }

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

      synchronized (this)
      {
         Iterator<MessageReference> iter = messageReferences.iterator();
         while (iter.hasNext())
         {
            foundRef = true;
            out.println("reference = " + iter.next());
         }
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
           executor,
           null,
           null);
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
                    final Executor executor,
                    final QueueFactory factory,
                    final CriticalAnalyzer analyzer)
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
           executor,
           factory,
           analyzer);
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
      this(id, address, name, filter, pageSubscription, durable, temporary, scheduledExecutor, postOffice, storageManager, addressSettingsRepository, executor, null, null);
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
                    final Executor executor,
                    final QueueFactory factory,
                    final CriticalAnalyzer analyzer)
   {
      super(analyzer == null ? EmptyCriticalAnalyzer.getInstance() : analyzer, CRITICAL_PATHS);

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

      this.factory = factory;

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

   public void unproposed(final SimpleString groupID)
   {
      if (groupID.toString().endsWith("." + this.getName()))
      {
         // this means this unproposed belongs to this routing, so we will
         // remove this group

         // This is removing the name and a . added, giving us the original groupID used
         // this is because a groupID is stored per queue, and only this queue is expiring at this point
         final SimpleString groupIDToRemove = (SimpleString)groupID.subSequence(0, groupID.length() - getName().length() - 1);
         // using an executor so we don't want to hold anyone just because of this
         getExecutor().execute(new Runnable()
         {
            public void run()
            {
               synchronized (QueueImpl.this)
               {
                  if (groups.remove(groupIDToRemove) != null)
                  {
                     HornetQServerLogger.LOGGER.debug("Removing group after unproposal " + groupID + " from queue " + QueueImpl.this);
                  }
                  else
                  {
                     HornetQServerLogger.LOGGER.debug("Couldn't remove Removing group " + groupIDToRemove + " after unproposal on queue " + QueueImpl.this);
                  }
               }
            }
         });
      }
   }

   /* Called when a message is cancelled back into the queue */
   public void addHead(final MessageReference ref)
   {
      enterCritical(CRITICAL_PATH_ADD_HEAD);
      synchronized (this)
      {
         try
         {
            flushDeliveriesInTransit();
            if (scheduledDeliveryHandler.checkAndSchedule(ref, false))
            {
               return;
            }

            internalAddHead(ref);

            directDeliver = false;
         }
         finally
         {
            leaveCritical(CRITICAL_PATH_ADD_HEAD);
         }
      }
   }

   /* Called when a message is cancelled back into the queue */
   public void addHead(final List<MessageReference> refs)
   {
      flushDeliveriesInTransit();
      for (MessageReference ref : refs)
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

      if (!ref.isPaged())
      {
         messagesAdded++;
      }
   }

   public void addTail(final MessageReference ref)
   {
      addTail(ref, false);
   }

   public void addTail(final MessageReference ref, final boolean direct)
   {
      enterCritical(CRITICAL_PATH_ADD_TAIL);
      try
      {
         if (scheduledDeliveryHandler.checkAndSchedule(ref, true))
         {
            synchronized (this)
            {
               if (!ref.isPaged())
               {
                  messagesAdded++;
               }
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

         if (direct && directDeliver && deliveriesInTransit.getCount() == 0 && deliverDirect(ref))
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
      finally
      {
         leaveCritical(CRITICAL_PATH_ADD_TAIL);
      }
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
            HornetQServerLogger.LOGGER.timeoutFlushInTransit(getName().toString(), getAddress().toString());
            return false;
         }
      }
      catch (Exception e)
      {
         HornetQServerLogger.LOGGER.warn(e.getMessage(), e);
         return false;
      }
   }

   public void forceDelivery()
   {
      if (pageSubscription != null && pageSubscription.isPaging())
      {
         if (isTrace)
         {
            HornetQServerLogger.LOGGER.trace("Force delivery scheduling depage");
         }
         scheduleDepage(false);
      }

      if (isTrace)
      {
         HornetQServerLogger.LOGGER.trace("Force delivery deliverying async");
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

         checkDepage();
      }

   }

   public void close() throws Exception
   {
      if (checkQueueSizeFuture != null)
      {
         checkQueueSizeFuture.cancel(false);
      }

      getExecutor().execute(new Runnable()
      {
         public void run()
         {
            try
            {
               cancelRedistributor();
            }
            catch (Exception e)
            {
               // nothing that could be done anyway.. just logging
               HornetQServerLogger.LOGGER.warn(e.getMessage(), e);
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
         HornetQServerLogger.LOGGER.errorFlushingExecutorsOnQueue();
      }

      return ok;
   }

   private boolean internalFlushExecutor(long timeout)
   {
      FutureLatch future = new FutureLatch();

      getExecutor().execute(future);

      boolean result = future.await(timeout);

      if (!result)
      {
         HornetQServerLogger.LOGGER.queueBusy(this.name.toString(), timeout);
      }
      return result;
   }

   public void addConsumer(final Consumer consumer) throws Exception
   {
      enterCritical(CRITICAL_CONSUMER);
      try
      {
         synchronized (this)
         {
            if (HornetQServerLogger.LOGGER.isDebugEnabled())
            {
               HornetQServerLogger.LOGGER.debug(this + " adding consumer " + consumer);
            }

            flushDeliveriesInTransit();

            consumersChanged = true;

            cancelRedistributor();

            consumerList.add(new ConsumerHolder(consumer));

            consumerSet.add(consumer);
         }
      }
      finally
      {
         leaveCritical(CRITICAL_CONSUMER);
      }
   }

   public void removeConsumer(final Consumer consumer)
   {
      enterCritical(CRITICAL_CONSUMER);
      try
      {
         synchronized (this)
         {
            consumersChanged = true;

            for (ConsumerHolder holder : consumerList)
            {
               if (holder.consumer == consumer)
               {
                  if (holder.iter != null)
                  {
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
               for (SimpleString groupID : groupsToRemove)
               {
                  groups.remove(groupID);
               }
            }
         }
      }
      finally
      {
         leaveCritical(CRITICAL_CONSUMER);
      }
   }

   public synchronized void addRedistributor(final long delay)
   {
      if (redistributorFuture != null)
      {
         redistributorFuture.cancel(false);
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
      return new HashSet<Consumer>(consumerSet);
   }

   public boolean hasMatchingConsumer(final ServerMessage message)
   {
      for (ConsumerHolder holder : consumerList)
      {
         Consumer consumer = holder.consumer;

         if (consumer instanceof Redistributor)
         {
            continue;
         }

         Filter filter1 = consumer.getFilter();

         if (filter1 == null)
         {
            return true;
         }
         else
         {
            if (filter1.match(message))
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

   public synchronized MessageReference removeReferenceWithID(final long id1) throws Exception
   {
      LinkedListIterator<MessageReference> iterator = iterator();

      try
      {

         MessageReference removed = null;

         while (iterator.hasNext())
         {
            MessageReference ref = iterator.next();

            if (ref.getMessage().getMessageID() == id1)
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
            removed = scheduledDeliveryHandler.removeReferenceWithID(id1);
         }

         return removed;
      }
      finally
      {
         iterator.close();
      }
   }

   public synchronized MessageReference getReference(final long id1) throws HornetQException
   {
      LinkedListIterator<MessageReference> iterator = iterator();

      try
      {

         while (iterator.hasNext())
         {
            MessageReference ref = iterator.next();

            if (ref.getMessage().getMessageID() == id1)
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
         List<MessageReference> msgs = holder.consumer.getDeliveringMessages();
         if (msgs != null && msgs.size() > 0)
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
      incDelivering();
   }

   private RefsOperation getRefsOperation(final Transaction tx)
   {
      return getRefsOperation(tx, false);
   }

   private RefsOperation getRefsOperation(final Transaction tx, boolean ignoreRedlieveryCheck)
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

         if (ignoreRedlieveryCheck)
         {
            oper.setIgnoreRedeliveryCheck();
         }

         return oper;
      }
   }

   public void cancel(final Transaction tx, final MessageReference reference)
   {
      cancel(tx, reference, false);
   }

   public void cancel(final Transaction tx, final MessageReference reference, boolean ignoreRedeliveryCheck)
   {
      getRefsOperation(tx, ignoreRedeliveryCheck).addAck(reference);
   }

   public synchronized void cancel(final MessageReference reference, final long timeBase) throws Exception
   {
      if (checkRedelivery(reference, timeBase, false))
      {
         if (!scheduledDeliveryHandler.checkAndSchedule(reference, false))
         {
            internalAddHead(reference);
         }

         resetAllIterators();
      }
      else
      {
         decDelivering();
      }
   }

   public void expire(final MessageReference ref) throws Exception
   {
      SimpleString messageExpiryAddress = expiryAddressFromMessageAddress(ref);
      if (messageExpiryAddress == null)
      {
         messageExpiryAddress = expiryAddressFromAddressSettings(ref);
      }

      if (messageExpiryAddress != null)
      {
         if (isTrace)
         {
            HornetQServerLogger.LOGGER.trace("moving expired reference " + ref + " to address = " + messageExpiryAddress
                  + " from queue=" + this.getName());
         }
         move(messageExpiryAddress, ref, true, false);
      }
      else
      {
         if (isTrace)
         {
            HornetQServerLogger.LOGGER
                  .trace("expiry is null, just acking expired message for reference " + ref + " from queue=" + this.getName());
         }
         acknowledge(ref);
      }
   }

   private SimpleString expiryAddressFromMessageAddress(MessageReference ref)
   {
      SimpleString messageAddress = extractAddress(ref.getMessage());
      SimpleString expiryAddress = null;

      if (messageAddress == null || messageAddress.equals(getAddress()))
      {
         expiryAddress = getExpiryAddress();
      }

      return expiryAddress;
   }

   private SimpleString expiryAddressFromAddressSettings(MessageReference ref)
   {
      SimpleString messageAddress = extractAddress(ref.getMessage());
      SimpleString expiryAddress = null;

      if (messageAddress != null)
      {
         AddressSettings addressSettings = addressSettingsRepository.getMatch(messageAddress.toString());

         expiryAddress = addressSettings.getExpiryAddress();
      }

      return expiryAddress;
   }

   private SimpleString extractAddress(ServerMessage message)
   {
      if (message.containsProperty(Message.HDR_ORIG_MESSAGE_ID))
      {
         return message.getSimpleStringProperty(Message.HDR_ORIGINAL_ADDRESS);
      }
      else
      {
         return message.getAddress();
      }
   }

   public SimpleString getExpiryAddress()
   {
      return this.expiryAddress;
   }

   public void referenceHandled()
   {
      incDelivering();
   }

   public long getMessagesAdded()
   {
      if (pageSubscription != null)
      {
         return messagesAdded + pageSubscription.getCounter().getValueAdded();
      }
      else
      {
         return messagesAdded;
      }
   }

   public long getMessagesAcknowledged()
   {
      return messagesAcknowledged;
   }

   public int deleteAllReferences() throws Exception
   {
      return deleteMatchingReferences(null);
   }

   public synchronized int deleteMatchingReferences(final Filter filter1) throws Exception
   {
      return iterQueue(filter1, new QueueIterateAction()
      {
         @Override
         public void actMessage(Transaction tx, MessageReference ref) throws Exception
         {
            incDelivering();
            acknowledge(tx, ref);
            refRemoved(ref);
         }
      });
   }


   /**
    * This is a generic method for any method interacting on the Queue to move or delete messages
    * Instead of duplicate the feature we created an abstract class where you pass the logic for
    * each message. Too bad there's not such thing as a function pointer in Java (as there is in
    * scala).
    *
    * @param filter1
    * @param messageAction
    * @return
    * @throws Exception
    */
   private synchronized int iterQueue(final Filter filter1, QueueIterateAction messageAction) throws Exception
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

            if (filter1 == null || filter1.match(ref.getMessage()))
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

         List<MessageReference> cancelled = scheduledDeliveryHandler.cancel(filter1);
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

               if (filter1 == null || filter1.match(reference.getMessage()))
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
               incDelivering();
               acknowledge(tx, ref);
               iter.remove();
               refRemoved(ref);
               deleted = true;
               break;
            }
         }

         if (!deleted)
         {
            // Look in scheduled deliveries
            deleted = scheduledDeliveryHandler.removeReferenceWithID(messageID) != null ? true : false;
         }

         tx.commit();

         return deleted;
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
         postOffice.removeBinding(name, tx, true);

         deleteAllReferences();

         destroyPaging();

         if (isDurable())
         {
            storageManager.deleteQueueBinding(tx.getID(), getID());
            tx.setContainsPersistent();
         }

         if (slowConsumerReaperFuture != null)
         {
            slowConsumerReaperFuture.cancel(false);
         }

         tx.commit();
      }
      catch (Exception e)
      {
         tx.rollback();
         throw e;
      }
      finally
      {
         if (factory != null)
         {
            factory.queueRemoved(this);
         }
      }

   }

   public synchronized boolean expireReference(final long messageID) throws Exception
   {
      if (expiryAddress != null && expiryAddress.equals(this.address))
      {
         // check expire with itself would be silly (waste of time)
         if (HornetQServerLogger.LOGGER.isDebugEnabled())
            HornetQServerLogger.LOGGER.debug("Cannot expire from " + address + " into " + expiryAddress);
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
               incDelivering();
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
         if (HornetQServerLogger.LOGGER.isDebugEnabled())
            HornetQServerLogger.LOGGER.debug("Cannot expire from " + address + " into " + expiryAddress);
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
               incDelivering();
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

   public void expireReferences()
   {
      if (expiryAddress != null && expiryAddress.equals(this.address))
      {
         // check expire with itself would be silly (waste of time)
         if (HornetQServerLogger.LOGGER.isDebugEnabled())
            HornetQServerLogger.LOGGER.debug("Cannot expire from " + address + " into " + expiryAddress);
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
                        incDelivering();
                        expired = true;
                        expire(ref);
                        iter.remove();
                        refRemoved(ref);
                     }
                  }
                  catch (Exception e)
                  {
                     HornetQServerLogger.LOGGER.errorExpiringReferencesOnQueue(e, ref);
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
               incDelivering();
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
               incDelivering();
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
               incDelivering();
               try
               {
                  move(toAddress, ref, false, rejectDuplicate);
               }
               catch (Exception e)
               {
                  decDelivering();
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

            incDelivering();

            if (rejectDuplicates)
            {
               byte[] duplicateBytes = ref.getMessage().getDuplicateIDBytes();
               if (duplicateBytes != null)
               {
                  if (targetDuplicateCache.contains(duplicateBytes))
                  {
                     HornetQServerLogger.LOGGER.messageWithDuplicateID(ref.getMessage().getDuplicateProperty(), toAddress, address, address);
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

   public synchronized void resetMessagesAdded()
   {
      messagesAdded = 0;
   }

   public synchronized void pause()
   {
      try
      {
         this.flushDeliveriesInTransit();
      }
      catch (Exception e)
      {
         HornetQServerLogger.LOGGER.warn(e.getMessage(), e);
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

   private synchronized void internalAddTail(final MessageReference ref)
   {
      refAdded(ref);
      messageReferences.addTail(ref, getPriority(ref));
   }

   /**
    * The caller of this method requires synchronized on the queue.
    * I'm not going to add synchronized to this method just for a precaution,
    * as I'm not 100% sure this won't cause any extra runtime.
    *
    * @param ref
    */
   private void internalAddHead(final MessageReference ref)
   {
      queueMemorySize.addAndGet(ref.getMessageMemoryEstimate());
      refAdded(ref);

      int priority = getPriority(ref);

      messageReferences.addHead(ref, priority);
   }

   private int getPriority(MessageReference ref)
   {
      try
      {
         return ref.getMessage().getPriority();
      }
      catch (Throwable e)
      {
         HornetQServerLogger.LOGGER.warn(e.getMessage(), e);
         return 4; // the default one in case of failure
      }
   }

   private synchronized void doInternalPoll()
   {

      int added = 0;
      MessageReference ref;

      while ((ref = intermediateMessageReferences.poll()) != null)
      {
         internalAddTail(ref);

         if (!ref.isPaged())
         {
            messagesAdded++;
         }
         if (added++ > MAX_DELIVERIES_IN_LOOP)
         {
            // if we just keep polling from the intermediate we could starve in case there's a sustained load
            deliverAsync();
            return;
         }
      }
   }

   /**
    * This method will deliver as many messages as possible until all consumers are busy or there
    * are no more matching or available messages.
    */
   private void deliver()
   {
      if (HornetQServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQServerLogger.LOGGER.debug(this + " doing deliver. messageReferences=" + messageReferences.size());
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
               HornetQServerLogger.LOGGER.trace("delivery has been running for too long. Scheduling another delivery task now");
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

               endPos = pos - 1;

               if (endPos < 0)
               {
                  endPos = size - 1;
                  noDelivery = 0;
               }
            }

            ConsumerHolder holder = consumerList.get(pos);

            Consumer consumer = holder.consumer;
            Consumer groupConsumer = null;

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
                     HornetQServerLogger.LOGGER.trace("Reference " + ref + " being expired");
                  }
                  holder.iter.remove();

                  refRemoved(ref);

                  handled++;

                  continue;
               }


               if (isTrace)
               {
                  HornetQServerLogger.LOGGER.trace("Queue " + this.getName() + " is delivering reference " + ref);
               }

               // If a group id is set, then this overrides the consumer chosen round-robin

               SimpleString groupID = extractGroupID(ref);

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
                     HornetQServerLogger.LOGGER.warn("Internal error! Delivery logic has identified a non delivery and still handled a consumer!");
                  }
                  else
                  {
                     if (HornetQServerLogger.LOGGER.isDebugEnabled())
                     {
                        HornetQServerLogger.LOGGER.debug(this + "::All the consumers were busy, giving up now");
                     }
                     break;
                  }
               }

               noDelivery = 0;
            }

            // Only move onto the next position if the consumer on the current position was used.
            // When using group we don't need to load balance to the next position
            if (groupConsumer == null)
            {
               pos++;
            }

            if (pos >= size)
            {
               pos = 0;
            }
         }

         if (handledconsumer != null)
         {
            proceedDeliver(handledconsumer, ref);
         }
      }

      checkDepage();
   }

   private void checkDepage()
   {
      if (pageIterator != null && pageSubscription.isPaging() && !depagePending && needsDepage() && pageIterator.hasNext())
      {
         scheduleDepage(false);
      }
   }

   /**
    * This is a common check we do before scheduling depaging.. or while depaging.
    * Before scheduling a depage runnable we verify if it fits / needs depaging.
    * We also check for while needsDepage While depaging.
    * This is just to avoid a copy & paste dependency
    * @return
    */
   private boolean needsDepage()
   {
      return queueMemorySize.get() < pageSubscription.getPagingStore().getMaxSize() || pageSubscription.getPagingStore().getMaxSize() == -1;
   }

   private SimpleString extractGroupID(MessageReference ref)
   {
      if (internalQueue)
      {
         return null;
      }
      else
      {
         try
         {
            // But we don't use the groupID on internal queues (clustered queues) otherwise the group map would leak forever
            return ref.getMessage().getSimpleStringProperty(Message.HDR_GROUP_ID);
         }
         catch (Throwable e)
         {
            HornetQServerLogger.LOGGER.warn(e.getMessage(), e);
            return null;
         }
      }
   }

   protected void refRemoved(MessageReference ref)
   {
      queueMemorySize.addAndGet(-ref.getMessageMemoryEstimate());
      if (ref.isPaged())
      {
         pagedReferences.decrementAndGet();
      }
   }

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
            HornetQServerLogger.LOGGER.trace("Scheduling depage for queue " + this.getName());
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
         HornetQServerLogger.LOGGER.trace("QueueMemorySize before depage on queue=" + this.getName() + " is " + queueMemorySize.get());
      }

      this.directDeliver = false;

      int depaged = 0;
      while (timeout > System.currentTimeMillis() && needsDepage() && pageIterator.hasNext())
      {
         depaged++;
         PagedReference reference = pageIterator.next();
         if (isTrace)
         {
            HornetQServerLogger.LOGGER.trace("Depaging reference " + reference + " on queue " + this.getName());
         }
         addTail(reference, false);
         pageIterator.remove();
      }

      if (HornetQServerLogger.LOGGER.isDebugEnabled())
      {
         if (depaged == 0 && queueMemorySize.get() >= maxSize)
         {
            HornetQServerLogger.LOGGER.debug("Couldn't depage any message as the maxSize on the queue was achieved. " + "There are too many pending messages to be acked in reference to the page configuration");
         }

         if (HornetQServerLogger.LOGGER.isDebugEnabled())
         {
            HornetQServerLogger.LOGGER.debug("Queue Memory Size after depage on queue=" + this.getName() +
                                                " is " +
                                                queueMemorySize.get() +
                                                " with maxSize = " +
                                                maxSize +
                                                ". Depaged " +
                                                depaged +
                                                " messages, pendingDelivery=" + messageReferences.size() + ", intermediateMessageReferences= " + intermediateMessageReferences.size() +
                                                ", queueDelivering=" + deliveringCount.get());

         }
      }

      deliverAsync();

      if (depaged > 0 && scheduleExpiry)
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
         if (isTrace)
         {
            HornetQServerLogger.LOGGER.trace("QueueImpl::Adding redistributor on queue " + this.toString());
         }
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

   public boolean checkRedelivery(final MessageReference reference, final long timeBase, final boolean ignoreRedeliveryDelay) throws Exception
   {
      ServerMessage message = reference.getMessage();

      if (internalQueue)
      {
         if (isTrace)
         {
            HornetQServerLogger.LOGGER.trace("Queue " + this.getName() + " is an internal queue, no checkRedelivery");
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
      long redeliveryDelay = addressSettings.getRedeliveryDelay();
      int deliveryCount = reference.getDeliveryCount();

      // First check DLA
      if (maxDeliveries > 0 && deliveryCount >= maxDeliveries)
      {
         if (isTrace)
         {
            HornetQServerLogger.LOGGER.trace("Sending reference " + reference + " to DLA = " + addressSettings.getDeadLetterAddress() + " since ref.getDeliveryCount=" + reference.getDeliveryCount() + "and maxDeliveries=" + maxDeliveries + " from queue=" + this.getName());
         }
         sendToDeadLetterAddress(reference, addressSettings.getDeadLetterAddress());

         return false;
      }
      else
      {
         // Second check Redelivery Delay
         if (!ignoreRedeliveryDelay && redeliveryDelay > 0)
         {
            redeliveryDelay = calculateRedeliveryDelay(addressSettings, deliveryCount);

            if (isTrace)
            {
               HornetQServerLogger.LOGGER.trace("Setting redeliveryDelay=" + redeliveryDelay + " on reference=" + reference);
            }

            reference.setScheduledDeliveryTime(timeBase + redeliveryDelay);

            if (!reference.isPaged() && message.isDurable() && durable)
            {
               storageManager.updateScheduledDeliveryTime(reference);
            }
         }

         decDelivering();

         return true;
      }
   }

   /**
    * Used on testing only *
    */
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

      ServerMessage copy = message.makeCopyForExpiryOrDLA(newID, ref, expiry);

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
            HornetQServerLogger.LOGGER.errorExpiringReferencesNoBindings(expiryAddress);
         }
         else
         {
            move(expiryAddress, tx, ref, true, true);
         }
      }
      else
      {
         HornetQServerLogger.LOGGER.errorExpiringReferencesNoQueue(name);

         acknowledge(tx, ref);
      }
   }


   private void sendToDeadLetterAddress(final MessageReference ref) throws Exception
   {
      sendToDeadLetterAddress(ref, addressSettingsRepository.getMatch(address.toString()).getDeadLetterAddress());
   }

   private void sendToDeadLetterAddress(final MessageReference ref, final SimpleString deadLetterAddress) throws Exception
   {
      if (deadLetterAddress != null)
      {
         Bindings bindingList = postOffice.getBindingsForAddress(deadLetterAddress);

         if (bindingList.getBindings().isEmpty())
         {
            HornetQServerLogger.LOGGER.messageExceededMaxDelivery(ref, deadLetterAddress);
            acknowledge(ref);
         }
         else
         {
            HornetQServerLogger.LOGGER.messageExceededMaxDeliverySendtoDLA(ref, deadLetterAddress, name);
            move(deadLetterAddress, ref, false, false);
         }
      }
      else
      {
         HornetQServerLogger.LOGGER.messageExceededMaxDeliveryNoDLA(name);

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

            SimpleString groupID = extractGroupID(ref);

            if (groupID != null)
            {
               groupConsumer = groups.get(groupID);

               if (groupConsumer != null)
               {
                  consumer = groupConsumer;
               }
            }

            // Only move onto the next position if the consumer on the current position was used.
            if (groupConsumer == null)
            {
               pos++;
            }

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
         HornetQServerLogger.LOGGER.removingBadConsumer(t, consumer, reference);

         synchronized (this)
         {
            // If the consumer throws an exception we remove the consumer
            try
            {
               removeConsumer(consumer);
            }
            catch (Exception e)
            {
               HornetQServerLogger.LOGGER.errorRemovingConsumer(e);
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
      try
      {
         if (reference.getMessage().isExpired())
         {
            if (isTrace)
            {
               HornetQServerLogger.LOGGER.trace("Reference " + reference + " is expired");
            }
            reference.handled();

            try
            {
               expire(reference);
            }
            catch (Exception e)
            {
               HornetQServerLogger.LOGGER.errorExpiringRef(e);
            }

            return true;
         }
         else
         {
            return false;
         }
      }
      catch (Throwable e)
      {
         HornetQServerLogger.LOGGER.warn(e.getMessage(), e);
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
         HornetQServerLogger.LOGGER.removingBadConsumer(t, consumer, reference);

         // If the consumer throws an exception we remove the consumer
         try
         {
            removeConsumer(consumer);
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.errorRemovingConsumer(e);
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

      queue.decDelivering();

      if (ref.isPaged())
      {
         // nothing to be done
         return;
      }

      ServerMessage message;

      try
      {
         message = ref.getMessage();
      }
      catch (Throwable e)
      {
         HornetQServerLogger.LOGGER.warn(e.getMessage(), e);
         message = null;
      }

      boolean durableRef = message != null && message.isDurable() && queue.durable;

      decrementRefCount(ref);

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
               HornetQServerLogger.LOGGER.errorRemovingMessage(e, message.getMessageID());
            }
         }
      }
   }

   void postRollback(final LinkedList<MessageReference> refs)
   {
      addHead(refs);
   }

   private long calculateRedeliveryDelay(final AddressSettings addressSettings, final int deliveryCount)
   {
      long redeliveryDelay = addressSettings.getRedeliveryDelay();
      long maxRedeliveryDelay = addressSettings.getMaxRedeliveryDelay();
      double redeliveryMultiplier = addressSettings.getRedeliveryMultiplier();

      int tmpDeliveryCount = deliveryCount > 0 ? deliveryCount - 1 : 0;
      long delay = (long)(redeliveryDelay * (Math.pow(redeliveryMultiplier, tmpDeliveryCount)));

      if (delay > maxRedeliveryDelay)
      {
         delay = maxRedeliveryDelay;
      }

      return delay;
   }

   public float getRate()
   {
      long locaMessageAdded = getMessagesAdded();
      float timeSlice = ((System.currentTimeMillis() - queueRateCheckTime.getAndSet(System.currentTimeMillis())) / 1000.0f);
      if (timeSlice == 0)
      {
         messagesAddedSnapshot.getAndSet(locaMessageAdded);
         return 0.0f;
      }
      return BigDecimal.valueOf((locaMessageAdded - messagesAddedSnapshot.getAndSet(locaMessageAdded)) / timeSlice).setScale(2, BigDecimal.ROUND_UP).floatValue();
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

   public final class RefsOperation extends TransactionOperationAbstract
   {
      List<MessageReference> refsToAck = new ArrayList<MessageReference>();

      List<MessageReference> pagedMessagesToPostACK = null;

      /**
       * It will ignore redelivery check, which is used during consumer.close
       * to not perform reschedule redelivery check
       */
      protected boolean ignoreRedeliveryCheck = false;


      // once turned on, we shouldn't turn it off, that's why no parameters
      public void setIgnoreRedeliveryCheck()
      {
         ignoreRedeliveryCheck = true;
      }

      synchronized void addAck(final MessageReference ref)
      {
         refsToAck.add(ref);
         if (ref.isPaged())
         {
            if (pagedMessagesToPostACK == null)
            {
               pagedMessagesToPostACK = new ArrayList<MessageReference>();
            }
            pagedMessagesToPostACK.add(ref);
         }
      }

      @Override
      public void afterRollback(final Transaction tx)
      {
         Map<QueueImpl, LinkedList<MessageReference>> queueMap = new HashMap<QueueImpl, LinkedList<MessageReference>>();

         long timeBase = System.currentTimeMillis();

         for (MessageReference ref : refsToAck)
         {
            ref.setConsumerId(null);

            if (HornetQServerLogger.LOGGER.isTraceEnabled())
            {
               HornetQServerLogger.LOGGER.trace("rolling back " + ref);
            }
            try
            {
               // if ignore redelivery check, we just perform redelivery straight
               if (ref.getQueue().checkRedelivery(ref, timeBase, ignoreRedeliveryCheck))
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
               HornetQServerLogger.LOGGER.errorCheckingDLQ(e);
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

      @Override
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
            for (MessageReference refmsg : pagedMessagesToPostACK)
            {
               decrementRefCount(refmsg);
            }
         }
      }

      @Override
      public synchronized List<MessageReference> getRelatedMessageReferences()
      {
         List<MessageReference> listRet = new LinkedList<MessageReference>();
         listRet.addAll(listRet);
         return listRet;
      }

      @Override
      public synchronized List<MessageReference> getListOnConsumer(long consumerID)
      {
         List<MessageReference> list = new LinkedList<MessageReference>();
         for (MessageReference ref : refsToAck)
         {
            if (ref.getConsumerId() != null && ref.getConsumerId().equals(consumerID))
            {
               list.add(ref);
            }
         }

         return list;
      }


   }

   private void decrementRefCount(MessageReference refmsg)
   {
      try
      {
         refmsg.getMessage().decrementRefCount();
      }
      catch (Exception e)
      {
         HornetQServerLogger.LOGGER.warn(e.getMessage(), e);
      }
   }

   private class DelayedAddRedistributor implements Runnable
   {
      private final Executor executor1;

      DelayedAddRedistributor(final Executor executor)
      {
         this.executor1 = executor;
      }

      public void run()
      {
         synchronized (QueueImpl.this)
         {
            internalAddRedistributor(executor1);
         }
      }
   }

   private final class DeliverRunner implements Runnable
   {
      private final Object deliveryGuard = new Object();

      public void run()
      {
         try
         {
            enterCritical(CRITICAL_DELIVER);
            try
            {
               // during the transition between paging and nonpaging, we could have this using a different executor
               // and at this short period we could have more than one delivery thread running in async mode
               // this will avoid that possibility
               synchronized (deliveryGuard)
               {
                  deliver();
               }
            }
            finally
            {
               leaveCritical(CRITICAL_DELIVER);
            }
         }
         catch (Exception e)
         {
            HornetQServerLogger.LOGGER.errorDelivering(e);
         }
         finally
         {
            scheduledRunners.decrementAndGet();
         }
      }
   }

   private final class DepageRunner implements Runnable
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
            HornetQServerLogger.LOGGER.errorDelivering(e);
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

   //Readonly (no remove) iterator over the messages in the queue, in order of
   //paging store, intermediateMessageReferences and MessageReferences
   private class TotalQueueIterator implements LinkedListIterator<MessageReference>
   {
      LinkedListIterator<PagedReference> pageIter = null;
      LinkedListIterator<MessageReference> messagesIterator = null;

      public TotalQueueIterator()
      {
         if (pageSubscription != null)
         {
            pageIter = pageSubscription.iterator();
         }
         messagesIterator = new SynchronizedIterator(messageReferences.iterator());
      }

      @Override
      public boolean hasNext()
      {
         if (messagesIterator.hasNext())
         {
            return true;
         }
         if (pageIter != null)
         {
            if (pageIter.hasNext())
            {
               return true;
            }
         }

         return false;
      }

      @Override
      public MessageReference next()
      {
         if (messagesIterator.hasNext())
         {
            return messagesIterator.next();
         }
         if (pageIter != null)
         {
            if (pageIter.hasNext())
            {
               return pageIter.next();
            }
         }

         throw new NoSuchElementException();
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
         if (pageIter != null) pageIter.close();
         messagesIterator.close();
      }
   }

   private int incDelivering()
   {
      return deliveringCount.incrementAndGet();
   }

   private void decDelivering()
   {
      deliveringCount.decrementAndGet();
   }

   private void configureExpiry(final AddressSettings settings)
   {
      this.expiryAddress = settings == null ? null : settings.getExpiryAddress();
   }

   private void configureSlowConsumerReaper(final AddressSettings settings)
   {
      if (settings == null || settings.getSlowConsumerThreshold() == AddressSettings.DEFAULT_SLOW_CONSUMER_THRESHOLD)
      {
         if (slowConsumerReaperFuture != null)
         {
            slowConsumerReaperFuture.cancel(false);
            slowConsumerReaperFuture = null;
            slowConsumerReaperRunnable = null;
            if (HornetQServerLogger.LOGGER.isDebugEnabled())
            {
               HornetQServerLogger.LOGGER.debug("Cancelled slow-consumer-reaper thread for queue \"" + getName() + "\"");
            }
         }
      }
      else
      {
         if (slowConsumerReaperRunnable == null)
         {
            scheduleSlowConsumerReaper(settings);
         }
         else if (slowConsumerReaperRunnable.checkPeriod != settings.getSlowConsumerCheckPeriod() ||
            slowConsumerReaperRunnable.threshold != settings.getSlowConsumerThreshold() ||
            !slowConsumerReaperRunnable.policy.equals(settings.getSlowConsumerPolicy()))
         {
            slowConsumerReaperFuture.cancel(false);
            scheduleSlowConsumerReaper(settings);
         }
      }
   }

   void scheduleSlowConsumerReaper(AddressSettings settings)
   {
      slowConsumerReaperRunnable = new SlowConsumerReaperRunnable(settings.getSlowConsumerCheckPeriod(),
                                                                  settings.getSlowConsumerThreshold(),
                                                                  settings.getSlowConsumerPolicy());

      slowConsumerReaperFuture = scheduledExecutor.scheduleWithFixedDelay(slowConsumerReaperRunnable,
                                                                          settings.getSlowConsumerCheckPeriod(),
                                                                          settings.getSlowConsumerCheckPeriod(),
                                                                          TimeUnit.SECONDS);

      if (HornetQServerLogger.LOGGER.isDebugEnabled())
      {
         HornetQServerLogger.LOGGER.debug("Scheduled slow-consumer-reaper thread for queue \"" + getName() +
                                             "\"; slow-consumer-check-period=" + settings.getSlowConsumerCheckPeriod() +
                                             ", slow-consumer-threshold=" + settings.getSlowConsumerThreshold() +
                                             ", slow-consumer-policy=" + settings.getSlowConsumerPolicy());
      }
   }

   private class AddressSettingsRepositoryListener implements HierarchicalRepositoryChangeListener
   {
      @Override
      public void onChange()
      {
         AddressSettings settings = addressSettingsRepository.getMatch(address.toString());
         configureExpiry(settings);
         configureSlowConsumerReaper(settings);
      }
   }

   private final class SlowConsumerReaperRunnable implements Runnable
   {
      private SlowConsumerPolicy policy;
      private float threshold;
      private long checkPeriod;

      public SlowConsumerReaperRunnable(long checkPeriod, float threshold, SlowConsumerPolicy policy)
      {
         this.checkPeriod = checkPeriod;
         this.policy = policy;
         this.threshold = threshold;
      }

      @Override
      public void run()
      {
         float queueRate = getRate();
         if (HornetQServerLogger.LOGGER.isDebugEnabled())
         {
            HornetQServerLogger.LOGGER.debug(getAddress() + ":" + getName() + " has " + getConsumerCount() + " consumer(s) and is receiving messages at a rate of " + queueRate + " msgs/second.");
         }
         for (Consumer consumer : getConsumers())
         {
            if (consumer instanceof ServerConsumerImpl)
            {
               ServerConsumerImpl serverConsumer = (ServerConsumerImpl) consumer;
               float consumerRate = serverConsumer.getRate();
               if (queueRate < threshold)
               {
                  if (HornetQServerLogger.LOGGER.isDebugEnabled())
                  {
                     HornetQServerLogger.LOGGER.debug("Insufficient messages received on queue \"" + getName() + "\" to satisfy slow-consumer-threshold. Skipping inspection of consumer.");
                  }
               }
               else if (consumerRate < threshold)
               {
                  RemotingConnection connection = null;
                  RemotingService remotingService = ((PostOfficeImpl) postOffice).getServer().getRemotingService();

                  for (RemotingConnection potentialConnection : remotingService.getConnections())
                  {
                     if (potentialConnection.getID().toString().equals(serverConsumer.getConnectionID()))
                     {
                        connection = potentialConnection;
                     }
                  }

                  if (connection != null)
                  {
                     HornetQServerLogger.LOGGER.slowConsumerDetected(serverConsumer.getSessionID(), serverConsumer.getID(), getName().toString(), connection.getRemoteAddress(), threshold, consumerRate);
                     if (policy.equals(SlowConsumerPolicy.KILL))
                     {
                        remotingService.removeConnection(connection.getID());
                        connection.fail(HornetQMessageBundle.BUNDLE.connectionsClosedByManagement(connection.getRemoteAddress()));
                     }
                     else if (policy.equals(SlowConsumerPolicy.NOTIFY))
                     {
                        TypedProperties props = new TypedProperties();

                        props.putIntProperty(ManagementHelper.HDR_CONSUMER_COUNT, getConsumerCount());

                        props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, address);

                        if (connection != null)
                        {
                           props.putSimpleStringProperty(ManagementHelper.HDR_REMOTE_ADDRESS, SimpleString.toSimpleString(connection.getRemoteAddress()));

                           if (connection.getID() != null)
                           {
                              props.putSimpleStringProperty(ManagementHelper.HDR_CONNECTION_NAME, SimpleString.toSimpleString(connection.getID().toString()));
                           }
                        }

                        props.putLongProperty(ManagementHelper.HDR_CONSUMER_NAME, serverConsumer.getID());

                        props.putSimpleStringProperty(ManagementHelper.HDR_SESSION_NAME, SimpleString.toSimpleString(serverConsumer.getSessionID()));

                        Notification notification = new Notification(null, CoreNotificationType.CONSUMER_SLOW, props);

                        ManagementService managementService = ((PostOfficeImpl) postOffice).getServer().getManagementService();
                        try
                        {
                           managementService.sendNotification(notification);
                        }
                        catch (Exception e)
                        {
                           HornetQServerLogger.LOGGER.failedToSendSlowConsumerNotification(notification, e);
                        }
                     }
                  }
               }
            }
         }
      }
   }
}

