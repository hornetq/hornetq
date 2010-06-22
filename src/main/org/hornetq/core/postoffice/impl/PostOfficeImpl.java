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

package org.hornetq.core.postoffice.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.PageTransactionInfo;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.impl.PageTransactionInfoImpl;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.AddressManager;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.BindingType;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.BindingsFactory;
import org.hornetq.core.postoffice.DuplicateIDCache;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.QueueInfo;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.QueueFactory;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.RoutingContextImpl;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.core.server.management.Notification;
import org.hornetq.core.server.management.NotificationListener;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionOperation;
import org.hornetq.core.transaction.TransactionPropertyIndexes;
import org.hornetq.core.transaction.Transaction.State;
import org.hornetq.core.transaction.impl.TransactionImpl;
import org.hornetq.utils.TypedProperties;
import org.hornetq.utils.UUIDGenerator;

/**
 * A PostOfficeImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="csuconic@redhat.com">Clebert Suconic</a>
 */
public class PostOfficeImpl implements PostOffice, NotificationListener, BindingsFactory
{
   private static final Logger log = Logger.getLogger(PostOfficeImpl.class);

   public static final SimpleString HDR_RESET_QUEUE_DATA = new SimpleString("_HQ_RESET_QUEUE_DATA");

   private final AddressManager addressManager;

   private final QueueFactory queueFactory;

   private final StorageManager storageManager;

   private final PagingManager pagingManager;

   private volatile boolean started;

   private final ManagementService managementService;

   private final Reaper reaperRunnable = new Reaper();

   private volatile Thread reaperThread;

   private final long reaperPeriod;

   private final int reaperPriority;

   private final ConcurrentMap<SimpleString, DuplicateIDCache> duplicateIDCaches = new ConcurrentHashMap<SimpleString, DuplicateIDCache>();

   private final int idCacheSize;

   private final boolean persistIDCache;

   private final Map<SimpleString, QueueInfo> queueInfos = new HashMap<SimpleString, QueueInfo>();

   private final Object notificationLock = new Object();

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private final HornetQServer server;

   public PostOfficeImpl(final HornetQServer server,
                         final StorageManager storageManager,
                         final PagingManager pagingManager,
                         final QueueFactory bindableFactory,
                         final ManagementService managementService,
                         final long reaperPeriod,
                         final int reaperPriority,
                         final boolean enableWildCardRouting,
                         final int idCacheSize,
                         final boolean persistIDCache,
                         final HierarchicalRepository<AddressSettings> addressSettingsRepository)

   {
      this.storageManager = storageManager;

      queueFactory = bindableFactory;

      this.managementService = managementService;

      this.pagingManager = pagingManager;

      this.reaperPeriod = reaperPeriod;

      this.reaperPriority = reaperPriority;

      if (enableWildCardRouting)
      {
         addressManager = new WildcardAddressManager(this);
      }
      else
      {
         addressManager = new SimpleAddressManager(this);
      }

      this.idCacheSize = idCacheSize;

      this.persistIDCache = persistIDCache;

      this.addressSettingsRepository = addressSettingsRepository;

      this.server = server;
   }

   // HornetQComponent implementation ---------------------------------------

   public synchronized void start() throws Exception
   {
      managementService.addNotificationListener(this);

      if (pagingManager != null)
      {
         pagingManager.setPostOffice(this);
      }

      // Injecting the postoffice (itself) on queueFactory for paging-control
      queueFactory.setPostOffice(this);

      // The flag started needs to be set before starting the Reaper Thread
      // This is to avoid thread leakages where the Reaper would run beyong the life cycle of the PostOffice
      started = true;

      startExpiryScanner();
   }

   public synchronized void stop() throws Exception
   {
      started = false;

      managementService.removeNotificationListener(this);

      reaperRunnable.stop();

      if (reaperThread != null)
      {
         reaperThread.join();

         reaperThread = null;
      }

      addressManager.clear();

      queueInfos.clear();
   }

   public boolean isStarted()
   {
      return started;
   }

   // NotificationListener implementation -------------------------------------

   public void onNotification(final Notification notification)
   {
      synchronized (notificationLock)
      {
         NotificationType type = notification.getType();

         switch (type)
         {
            case BINDING_ADDED:
            {
               TypedProperties props = notification.getProperties();

               if (!props.containsProperty(ManagementHelper.HDR_BINDING_TYPE))
               {
                  throw new IllegalArgumentException("Binding type not specified");
               }

               Integer bindingType = props.getIntProperty(ManagementHelper.HDR_BINDING_TYPE);

               if (bindingType == BindingType.DIVERT_INDEX)
               {
                  // We don't propagate diverts
                  return;
               }

               SimpleString routingName = props.getSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME);

               SimpleString clusterName = props.getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);

               SimpleString address = props.getSimpleStringProperty(ManagementHelper.HDR_ADDRESS);

               if (!props.containsProperty(ManagementHelper.HDR_BINDING_ID))
               {
                  throw new IllegalArgumentException("ID is null");
               }

               long id = props.getLongProperty(ManagementHelper.HDR_BINDING_ID);

               SimpleString filterString = props.getSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING);

               if (!props.containsProperty(ManagementHelper.HDR_DISTANCE))
               {
                  throw new IllegalArgumentException("Distance is null");
               }

               int distance = props.getIntProperty(ManagementHelper.HDR_DISTANCE);

               QueueInfo info = new QueueInfo(routingName, clusterName, address, filterString, id, distance);

               queueInfos.put(clusterName, info);

               break;
            }
            case BINDING_REMOVED:
            {
               TypedProperties props = notification.getProperties();

               if (!props.containsProperty(ManagementHelper.HDR_CLUSTER_NAME))
               {
                  throw new IllegalStateException("No cluster name");
               }

               SimpleString clusterName = props.getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);

               QueueInfo info = queueInfos.remove(clusterName);

               if (info == null)
               {
                  throw new IllegalStateException("Cannot find queue info for queue " + clusterName);
               }

               break;
            }
            case CONSUMER_CREATED:
            {
               TypedProperties props = notification.getProperties();

               if (!props.containsProperty(ManagementHelper.HDR_CLUSTER_NAME))
               {
                  throw new IllegalStateException("No cluster name");
               }

               SimpleString clusterName = props.getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);

               SimpleString filterString = props.getSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING);

               QueueInfo info = queueInfos.get(clusterName);

               if (info == null)
               {
                  throw new IllegalStateException("Cannot find queue info for queue " + clusterName);
               }

               info.incrementConsumers();

               if (filterString != null)
               {
                  List<SimpleString> filterStrings = info.getFilterStrings();

                  if (filterStrings == null)
                  {
                     filterStrings = new ArrayList<SimpleString>();

                     info.setFilterStrings(filterStrings);
                  }

                  filterStrings.add(filterString);
               }

               if (!props.containsProperty(ManagementHelper.HDR_DISTANCE))
               {
                  throw new IllegalStateException("No distance");
               }

               int distance = props.getIntProperty(ManagementHelper.HDR_DISTANCE);

               if (distance > 0)
               {
                  SimpleString queueName = props.getSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME);

                  if (queueName == null)
                  {
                     throw new IllegalStateException("No queue name");
                  }

                  Binding binding = getBinding(queueName);

                  if (binding != null)
                  {
                     // We have a local queue
                     Queue queue = (Queue)binding.getBindable();

                     AddressSettings addressSettings = addressSettingsRepository.getMatch(binding.getAddress()
                                                                                                 .toString());

                     long redistributionDelay = addressSettings.getRedistributionDelay();

                     if (redistributionDelay != -1)
                     {
                        queue.addRedistributor(redistributionDelay);
                     }
                  }
               }

               break;
            }
            case CONSUMER_CLOSED:
            {
               TypedProperties props = notification.getProperties();

               SimpleString clusterName = props.getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);

               if (clusterName == null)
               {
                  throw new IllegalStateException("No cluster name");
               }

               SimpleString filterString = props.getSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING);

               QueueInfo info = queueInfos.get(clusterName);

               if (info == null)
               {
                  return;
               }

               info.decrementConsumers();

               if (filterString != null)
               {
                  List<SimpleString> filterStrings = info.getFilterStrings();

                  filterStrings.remove(filterString);
               }

               if (info.getNumberOfConsumers() == 0)
               {
                  if (!props.containsProperty(ManagementHelper.HDR_DISTANCE))
                  {
                     throw new IllegalStateException("No cluster name");
                  }

                  int distance = props.getIntProperty(ManagementHelper.HDR_DISTANCE);

                  if (distance == 0)
                  {
                     SimpleString queueName = props.getSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME);

                     if (queueName == null)
                     {
                        throw new IllegalStateException("No queue name");
                     }

                     Binding binding = getBinding(queueName);

                     if (binding == null)
                     {
                        throw new IllegalStateException("No queue " + queueName);
                     }

                     Queue queue = (Queue)binding.getBindable();

                     AddressSettings addressSettings = addressSettingsRepository.getMatch(binding.getAddress()
                                                                                                 .toString());

                     long redistributionDelay = addressSettings.getRedistributionDelay();

                     if (redistributionDelay != -1)
                     {
                        queue.addRedistributor(redistributionDelay);
                     }
                  }
               }

               break;
            }
            default:
            {
               break;
            }
         }
      }
   }

   // PostOffice implementation -----------------------------------------------

   // TODO - needs to be synchronized to prevent happening concurrently with activate().
   // (and possible removeBinding and other methods)
   // Otherwise can have situation where createQueue comes in before failover, then failover occurs
   // and post office is activated but queue remains unactivated after failover so delivery never occurs
   // even though failover is complete
   public synchronized void addBinding(final Binding binding) throws Exception
   {
      addressManager.addBinding(binding);

      TypedProperties props = new TypedProperties();

      props.putIntProperty(ManagementHelper.HDR_BINDING_TYPE, binding.getType().toInt());

      props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, binding.getAddress());

      props.putSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME, binding.getClusterName());

      props.putSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME, binding.getRoutingName());

      props.putLongProperty(ManagementHelper.HDR_BINDING_ID, binding.getID());

      props.putIntProperty(ManagementHelper.HDR_DISTANCE, binding.getDistance());

      Filter filter = binding.getFilter();

      if (filter != null)
      {
         props.putSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING, filter.getFilterString());
      }

      String uid = UUIDGenerator.getInstance().generateStringUUID();

      managementService.sendNotification(new Notification(uid, NotificationType.BINDING_ADDED, props));
   }

   public synchronized Binding removeBinding(final SimpleString uniqueName) throws Exception
   {
      Binding binding = addressManager.removeBinding(uniqueName);

      if (binding == null)
      {
         throw new HornetQException(HornetQException.QUEUE_DOES_NOT_EXIST);
      }

      if (addressManager.getBindingsForRoutingAddress(binding.getAddress()) == null)
      {
         pagingManager.deletePageStore(binding.getAddress());
         
         managementService.unregisterAddress(binding.getAddress());
      }
      
      if (binding.getType() == BindingType.LOCAL_QUEUE)
      {
         managementService.unregisterQueue(uniqueName, binding.getAddress());
      }
      else if (binding.getType() == BindingType.DIVERT)
      {
         managementService.unregisterDivert(uniqueName);
      }

      TypedProperties props = new TypedProperties();

      props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, binding.getAddress());

      props.putSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME, binding.getClusterName());

      props.putSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME, binding.getRoutingName());

      props.putIntProperty(ManagementHelper.HDR_DISTANCE, binding.getDistance());

      managementService.sendNotification(new Notification(null, NotificationType.BINDING_REMOVED, props));

      return binding;
   }

   public Bindings getBindingsForAddress(final SimpleString address)
   {
      Bindings bindings = addressManager.getBindingsForRoutingAddress(address);

      if (bindings == null)
      {
         bindings = createBindings();
      }

      return bindings;
   }

   public Binding getBinding(final SimpleString name)
   {
      return addressManager.getBinding(name);
   }

   public Bindings getMatchingBindings(final SimpleString address)
   {
      return addressManager.getMatchingBindings(address);
   }

   public void route(final ServerMessage message, final boolean direct) throws Exception
   {
      route(message, (Transaction)null, direct);
   }

   public void route(final ServerMessage message, final Transaction tx, final boolean direct) throws Exception
   {
      route(message, new RoutingContextImpl(tx), direct);
   }
   
   public void route(final ServerMessage message, final RoutingContext context, final boolean direct) throws Exception
   {
      // Sanity check
      if (message.getRefCount() > 0)
      {
         throw new IllegalStateException("Message cannot be routed more than once");
      }

      SimpleString address = message.getAddress();
      
      setPagingStore(message);

      Object duplicateID = message.getObjectProperty(Message.HDR_DUPLICATE_DETECTION_ID);

      DuplicateIDCache cache = null;

      byte[] duplicateIDBytes = null;

      if (duplicateID != null)
      {
         cache = getDuplicateIDCache(message.getAddress());

         if (duplicateID instanceof SimpleString)
         {
            duplicateIDBytes = ((SimpleString)duplicateID).getData();
         }
         else
         {
            duplicateIDBytes = (byte[])duplicateID;
         }

         if (cache.contains(duplicateIDBytes))
         {
            if (context.getTransaction() == null)
            {
               PostOfficeImpl.log.warn("Duplicate message detected - message will not be routed");
            }
            else
            {
               PostOfficeImpl.log.warn("Duplicate message detected - transaction will be rejected");

               context.getTransaction().markAsRollbackOnly(null);
            }

            return;
         }
      }

      boolean startedTx = false;

      if (cache != null)
      {
         if (context.getTransaction() == null)
         {
            // We need to store the duplicate id atomically with the message storage, so we need to create a tx for this

            Transaction newTX = new TransactionImpl(storageManager);

            context.setTransaction(newTX);

            startedTx = true;
         }

         cache.addToCache(duplicateIDBytes, context.getTransaction());
      }

      if (context.getTransaction() == null)
      {
         if (message.page(true))
         {
            return;
         }
      }
      else
      {
         Transaction tx = context.getTransaction();
         boolean depage = tx.getProperty(TransactionPropertyIndexes.IS_DEPAGE) != null;

         if (!depage && message.storeIsPaging())
         {
            
            getPageOperation(tx).addMessageToPage(message);
            if (startedTx)
            {
               tx.commit();
            }
            
            return;
         }
      }

      Bindings bindings = addressManager.getBindingsForRoutingAddress(address);

      if (bindings != null)
      {
         bindings.route(message, context);
      }
      if (context.getQueueCount() == 0)
      {
         // Send to DLA if appropriate

         AddressSettings addressSettings = addressSettingsRepository.getMatch(address.toString());

         boolean sendToDLA = addressSettings.isSendToDLAOnNoRoute();

         if (sendToDLA)
         {
            // Send to the DLA for the address

            SimpleString dlaAddress = addressSettings.getDeadLetterAddress();

            if (dlaAddress == null)
            {
               PostOfficeImpl.log.warn("Did not route to any bindings for address " + address +
                                       " and sendToDLAOnNoRoute is true " +
                                       "but there is no DLA configured for the address, the message will be ignored.");
            }
            else
            {
               message.setOriginalHeaders(message, false);

               message.setAddress(dlaAddress);

               route(message, context.getTransaction(), false);
            }
         }
      }
      else
      {
         processRoute(message, context, direct);
      }

      if (startedTx)
      {
         context.getTransaction().commit();
      }
   }

   public MessageReference reroute(final ServerMessage message, final Queue queue, final Transaction tx) throws Exception
   {
      setPagingStore(message);

      MessageReference reference = message.createReference(queue);

      if (message.containsProperty(Message.HDR_SCHEDULED_DELIVERY_TIME))
      {
         Long scheduledDeliveryTime = message.getLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME);
         reference.setScheduledDeliveryTime(scheduledDeliveryTime);
      }

      message.incrementDurableRefCount();

      message.incrementRefCount();

      if (tx == null)
      {
         queue.addLast(reference, false);
      }
      else
      {
         List<MessageReference> refs = new ArrayList<MessageReference>(1);

         refs.add(reference);

         tx.addOperation(new AddOperation(refs));
      }

      return reference;
   }

   public boolean redistribute(final ServerMessage message, final Queue originatingQueue, final Transaction tx) throws Exception
   {
      Bindings bindings = addressManager.getBindingsForRoutingAddress(message.getAddress());

      boolean res = false;

      if (bindings != null)
      {
         RoutingContext context = new RoutingContextImpl(tx);

         boolean routed = bindings.redistribute(message, originatingQueue, context);

         if (routed)
         {
            processRoute(message, context, false);

            res = true;
         }
      }

      return res;
   }

   public PagingManager getPagingManager()
   {
      return pagingManager;
   }

   public DuplicateIDCache getDuplicateIDCache(final SimpleString address)
   {
      DuplicateIDCache cache = duplicateIDCaches.get(address);

      if (cache == null)
      {
         cache = new DuplicateIDCacheImpl(address, idCacheSize, storageManager, persistIDCache);

         DuplicateIDCache oldCache = duplicateIDCaches.putIfAbsent(address, cache);

         if (oldCache != null)
         {
            cache = oldCache;
         }
      }

      return cache;
   }

   public Object getNotificationLock()
   {
      return notificationLock;
   }

   public void sendQueueInfoToQueue(final SimpleString queueName, final SimpleString address) throws Exception
   {
      // We send direct to the queue so we can send it to the same queue that is bound to the notifications adress -
      // this is crucial for ensuring
      // that queue infos and notifications are received in a contiguous consistent stream
      Binding binding = addressManager.getBinding(queueName);

      if (binding == null)
      {
         throw new IllegalStateException("Cannot find queue " + queueName);
      }

      Queue queue = (Queue)binding.getBindable();

      // Need to lock to make sure all queue info and notifications are in the correct order with no gaps
      synchronized (notificationLock)
      {
         // First send a reset message

         ServerMessage message = new ServerMessageImpl(storageManager.generateUniqueID(), 50);

         message.setAddress(queueName);
         message.putBooleanProperty(PostOfficeImpl.HDR_RESET_QUEUE_DATA, true);
         routeQueueInfo(message, queue, false);

         for (QueueInfo info : queueInfos.values())
         {
            if (info.getAddress().startsWith(address))
            {
               message = createQueueInfoMessage(NotificationType.BINDING_ADDED, queueName);

               message.putStringProperty(ManagementHelper.HDR_ADDRESS, info.getAddress());
               message.putStringProperty(ManagementHelper.HDR_CLUSTER_NAME, info.getClusterName());
               message.putStringProperty(ManagementHelper.HDR_ROUTING_NAME, info.getRoutingName());
               message.putLongProperty(ManagementHelper.HDR_BINDING_ID, info.getID());
               message.putStringProperty(ManagementHelper.HDR_FILTERSTRING, info.getFilterString());
               message.putIntProperty(ManagementHelper.HDR_DISTANCE, info.getDistance());

               routeQueueInfo(message, queue, true);

               int consumersWithFilters = info.getFilterStrings() != null ? info.getFilterStrings().size() : 0;

               for (int i = 0; i < info.getNumberOfConsumers() - consumersWithFilters; i++)
               {
                  message = createQueueInfoMessage(NotificationType.CONSUMER_CREATED, queueName);

                  message.putStringProperty(ManagementHelper.HDR_ADDRESS, info.getAddress());
                  message.putStringProperty(ManagementHelper.HDR_CLUSTER_NAME, info.getClusterName());
                  message.putStringProperty(ManagementHelper.HDR_ROUTING_NAME, info.getRoutingName());
                  message.putIntProperty(ManagementHelper.HDR_DISTANCE, info.getDistance());

                  routeQueueInfo(message, queue, true);
               }

               if (info.getFilterStrings() != null)
               {
                  for (SimpleString filterString : info.getFilterStrings())
                  {
                     message = createQueueInfoMessage(NotificationType.CONSUMER_CREATED, queueName);

                     message.putStringProperty(ManagementHelper.HDR_ADDRESS, info.getAddress());
                     message.putStringProperty(ManagementHelper.HDR_CLUSTER_NAME, info.getClusterName());
                     message.putStringProperty(ManagementHelper.HDR_ROUTING_NAME, info.getRoutingName());
                     message.putStringProperty(ManagementHelper.HDR_FILTERSTRING, filterString);
                     message.putIntProperty(ManagementHelper.HDR_DISTANCE, info.getDistance());

                     routeQueueInfo(message, queue, true);
                  }
               }
            }
         }
      }

   }

   // Private -----------------------------------------------------------------

   private void setPagingStore(final ServerMessage message) throws Exception
   {
      PagingStore store = pagingManager.getPageStore(message.getAddress());
      message.setPagingStore(store);
   }

   private void routeQueueInfo(final ServerMessage message, final Queue queue, final boolean applyFilters) throws Exception
   {
      if (!applyFilters || queue.getFilter() == null || queue.getFilter().match(message))
      {
         RoutingContext context = new RoutingContextImpl(null);

         queue.route(message, context);

         processRoute(message, context, false);
      }
   }

   private void processRoute(final ServerMessage message, final RoutingContext context, final boolean direct) throws Exception
   {
      final List<MessageReference> refs = new ArrayList<MessageReference>();

      Transaction tx = context.getTransaction();

      for (Queue queue : context.getNonDurableQueues())
      {
         MessageReference reference = message.createReference(queue);

         refs.add(reference);

         if (message.containsProperty(Message.HDR_SCHEDULED_DELIVERY_TIME))
         {
            Long scheduledDeliveryTime = message.getLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME);

            reference.setScheduledDeliveryTime(scheduledDeliveryTime);
         }

         message.incrementRefCount();
      }

      Iterator<Queue> iter = context.getDurableQueues().iterator();

      while (iter.hasNext())
      {
         Queue queue = iter.next();

         MessageReference reference = message.createReference(queue);

         refs.add(reference);

         if (message.containsProperty(Message.HDR_SCHEDULED_DELIVERY_TIME))
         {
            Long scheduledDeliveryTime = message.getLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME);

            reference.setScheduledDeliveryTime(scheduledDeliveryTime);
         }

         if (message.isDurable())
         {
            int durableRefCount = message.incrementDurableRefCount();

            if (durableRefCount == 1)
            {
               if (tx != null)
               {
                  storageManager.storeMessageTransactional(tx.getID(), message);
               }
               else
               {
                  storageManager.storeMessage(message);
               }
            }

            if (tx != null)
            {
               storageManager.storeReferenceTransactional(tx.getID(), queue.getID(), message.getMessageID());

               tx.setContainsPersistent();
            }
            else
            {
               storageManager.storeReference(queue.getID(), message.getMessageID(), !iter.hasNext());
            }

            if (message.containsProperty(Message.HDR_SCHEDULED_DELIVERY_TIME))
            {
               if (tx != null)
               {
                  storageManager.updateScheduledDeliveryTimeTransactional(tx.getID(), reference);
               }
               else
               {
                  storageManager.updateScheduledDeliveryTime(reference);
               }
            }
         }

         message.incrementRefCount();
      }

      if (tx != null)
      {
         tx.addOperation(new AddOperation(refs));
      }
      else
      {
         // This will use the same thread if there are no pending operations
         // avoiding a context switch on this case
         storageManager.afterCompleteOperations(new IOAsyncTask()
         {
            public void onError(final int errorCode, final String errorMessage)
            {
               PostOfficeImpl.log.warn("It wasn't possible to add references due to an IO error code " + errorCode +
                                       " message = " +
                                       errorMessage);
            }

            public void done()
            {
               addReferences(refs, direct);
            }
         });
      }
   }

   /**
    * @param refs
    */
   private void addReferences(final List<MessageReference> refs, final boolean direct)
   {
      for (MessageReference ref : refs)
      {
         ref.getQueue().addLast(ref, direct);
      }
   }

   private synchronized void startExpiryScanner()
   {
      if (reaperPeriod > 0)
      {
         reaperThread = new Thread(reaperRunnable, "hornetq-expiry-reaper-thread");

         reaperThread.setPriority(reaperPriority);

         reaperThread.start();
      }
   }

   private ServerMessage createQueueInfoMessage(final NotificationType type, final SimpleString queueName)
   {
      ServerMessage message = new ServerMessageImpl(storageManager.generateUniqueID(), 50);

      message.setAddress(queueName);

      String uid = UUIDGenerator.getInstance().generateStringUUID();

      message.putStringProperty(ManagementHelper.HDR_NOTIFICATION_TYPE, new SimpleString(type.toString()));
      message.putLongProperty(ManagementHelper.HDR_NOTIFICATION_TIMESTAMP, System.currentTimeMillis());

      message.putStringProperty(new SimpleString("foobar"), new SimpleString(uid));

      return message;
   }

   private final PageMessageOperation getPageOperation(final Transaction tx)
   {
      // you could have races on the case two sessions using the same XID
      // so this whole operation needs to be atomic per TX
      synchronized (tx)
      {
         PageMessageOperation oper = (PageMessageOperation)tx.getProperty(TransactionPropertyIndexes.PAGE_MESSAGES_OPERATION);

         if (oper == null)
         {
            oper = new PageMessageOperation();

            tx.putProperty(TransactionPropertyIndexes.PAGE_MESSAGES_OPERATION, oper);

            tx.addOperation(oper);
         }

         return oper;
      }
   }

   private class Reaper implements Runnable
   {
      private volatile boolean closed = false;

      public synchronized void stop()
      {
         closed = true;

         notify();
      }

      public synchronized void run()
      {
         if (closed)
         {
            // This shouldn't happen in a regular scenario
            PostOfficeImpl.log.warn("Reaper thread being restarted");
            closed = false;
         }

         // The reaper thread should be finished case the PostOffice is gone
         // This is to avoid leaks on PostOffice between stops and starts
         while (isStarted())
         {
            long toWait = reaperPeriod;

            long start = System.currentTimeMillis();

            while (!closed && toWait > 0)
            {
               try
               {
                  wait(toWait);
               }
               catch (InterruptedException e)
               {
               }

               long now = System.currentTimeMillis();

               toWait -= now - start;

               start = now;
            }

            if (closed)
            {
               return;
            }

            Map<SimpleString, Binding> nameMap = addressManager.getBindings();

            List<Queue> queues = new ArrayList<Queue>();

            for (Binding binding : nameMap.values())
            {
               if (binding.getType() == BindingType.LOCAL_QUEUE)
               {
                  Queue queue = (Queue)binding.getBindable();

                  queues.add(queue);
               }
            }

            for (Queue queue : queues)
            {
               try
               {
                  queue.expireReferences();
               }
               catch (Exception e)
               {
                  PostOfficeImpl.log.error("failed to expire messages for queue " + queue.getName(), e);
               }
            }
         }
      }
   }

   private class PageMessageOperation implements TransactionOperation
   {
      private final List<ServerMessage> messagesToPage = new ArrayList<ServerMessage>();

      void addMessageToPage(final ServerMessage message)
      {
         messagesToPage.add(message);
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.TransactionOperation#getDistinctQueues()
       */
      public Collection<Queue> getDistinctQueues()
      {
         return Collections.emptySet();
      }

      public void afterCommit(final Transaction tx)
      {
         // If part of the transaction goes to the queue, and part goes to paging, we can't let depage start for the
         // transaction until all the messages were added to the queue
         // or else we could deliver the messages out of order

         PageTransactionInfo pageTransaction = (PageTransactionInfo)tx.getProperty(TransactionPropertyIndexes.PAGE_TRANSACTION);

         if (pageTransaction != null)
         {
            pageTransaction.commit();
         }
      }

      public void afterPrepare(final Transaction tx)
      {
      }

      public void afterRollback(final Transaction tx)
      {
         PageTransactionInfo pageTransaction = (PageTransactionInfo)tx.getProperty(TransactionPropertyIndexes.PAGE_TRANSACTION);

         if (tx.getState() == State.PREPARED && pageTransaction != null)
         {
            pageTransaction.rollback();
         }
      }

      public void beforeCommit(final Transaction tx) throws Exception
      {
         if (tx.getState() != Transaction.State.PREPARED)
         {
            pageMessages(tx);
         }
      }

      public void beforePrepare(final Transaction tx) throws Exception
      {
         pageMessages(tx);
      }

      public void beforeRollback(final Transaction tx) throws Exception
      {
      }

      private void pageMessages(final Transaction tx) throws Exception
      {
         if (!messagesToPage.isEmpty())
         {
            PageTransactionInfo pageTransaction = (PageTransactionInfo)tx.getProperty(TransactionPropertyIndexes.PAGE_TRANSACTION);

            if (pageTransaction == null)
            {
               pageTransaction = new PageTransactionInfoImpl(tx.getID());

               tx.putProperty(TransactionPropertyIndexes.PAGE_TRANSACTION, pageTransaction);

               // To avoid a race condition where depage happens before the transaction is completed, we need to inform
               // the pager about this transaction is being processed
               pagingManager.addTransaction(pageTransaction);
            }

            boolean pagingPersistent = false;

            Set<PagingStore> pagingStoresToSync = new HashSet<PagingStore>();

            // We only need to add the dupl id header once per transaction
            boolean first = true;
            for (ServerMessage message : messagesToPage)
            {
               if (message.page(tx.getID(), first))
               {
                  if (message.isDurable())
                  {
                     // We only create pageTransactions if using persistent messages
                     pageTransaction.increment();
                     pagingPersistent = true;
                     pagingStoresToSync.add(message.getPagingStore());
                  }
               }
               else
               {
                  // This could happen when the PageStore left the pageState

                  // TODO is this correct - don't we lose transactionality here???
                  route(message, false);
               }
               first = false;
            }

            if (pagingPersistent)
            {
               tx.setContainsPersistent();

               if (!pagingStoresToSync.isEmpty())
               {
                  for (PagingStore store : pagingStoresToSync)
                  {
                     store.sync();
                  }

                  storageManager.storePageTransaction(tx.getID(), pageTransaction);
               }
            }
         }
      }
   }

   private class AddOperation implements TransactionOperation
   {
      private final List<MessageReference> refs;

      AddOperation(final List<MessageReference> refs)
      {
         this.refs = refs;
      }

      public void afterCommit(final Transaction tx)
      {
         for (MessageReference ref : refs)
         {
            ref.getQueue().addLast(ref, false);
         }
      }

      public void afterPrepare(final Transaction tx)
      {
      }

      public void afterRollback(final Transaction tx)
      {
      }

      public void beforeCommit(final Transaction tx) throws Exception
      {
      }

      public void beforePrepare(final Transaction tx) throws Exception
      {
      }

      public void beforeRollback(final Transaction tx) throws Exception
      {
         // Reverse the ref counts, and paging sizes

         for (MessageReference ref : refs)
         {
            ServerMessage message = ref.getMessage();

            if (message.isDurable() && ref.getQueue().isDurable())
            {
               message.decrementDurableRefCount();
            }

            message.decrementRefCount();
         }
      }
   }

   public Bindings createBindings()
   {
      return new BindingsImpl(server.getGroupingHandler());
   }
}
