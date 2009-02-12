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

package org.jboss.messaging.core.postoffice.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.client.management.impl.ManagementHelper;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.management.Notification;
import org.jboss.messaging.core.management.NotificationListener;
import org.jboss.messaging.core.management.NotificationType;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.paging.PageTransactionInfo;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.impl.PageTransactionInfoImpl;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.AddressManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.BindingType;
import org.jboss.messaging.core.postoffice.Bindings;
import org.jboss.messaging.core.postoffice.DuplicateIDCache;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.postoffice.QueueInfo;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.SendLock;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.cluster.RemoteQueueBinding;
import org.jboss.messaging.core.server.impl.SendLockImpl;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.TransactionOperation;
import org.jboss.messaging.core.transaction.TransactionPropertyIndexes;
import org.jboss.messaging.core.transaction.Transaction.State;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;
import org.jboss.messaging.util.JBMThreadFactory;
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.TypedProperties;

/**
 * A PostOfficeImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="csuconic@redhat.com">Clebert Suconic</a>
 */
public class PostOfficeImpl implements PostOffice, NotificationListener
{
   private static final Logger log = Logger.getLogger(PostOfficeImpl.class);

   public static final SimpleString HDR_RESET_QUEUE_DATA = new SimpleString("_JBM_RESET_QUEUE_DATA");

   private static final List<MessageReference> emptyList = Collections.<MessageReference> emptyList();

   private final AddressManager addressManager;

   private final QueueFactory queueFactory;

   private final boolean checkAllowable;

   private final StorageManager storageManager;

   private final PagingManager pagingManager;

   private volatile boolean started;

   private volatile boolean backup;

   private final ManagementService managementService;

   private final Map<SimpleString, SendLock> addressLocks = new HashMap<SimpleString, SendLock>();

   private ScheduledThreadPoolExecutor messageExpiryExecutor;

   private final long messageExpiryScanPeriod;

   private final int messageExpiryThreadPriority;

   private final ConcurrentMap<SimpleString, DuplicateIDCache> duplicateIDCaches = new ConcurrentHashMap<SimpleString, DuplicateIDCache>();

   private final int idCacheSize;

   private final boolean persistIDCache;

   // Each queue has a transient ID which lasts the lifetime of its binding. This is used in clustering when routing
   // messages to particular queues on nodes. We could
   // use the queue name on the node to identify it. But sometimes we need to route to maybe 10s of thousands of queues
   // on a particular node, and all would
   // have to be specified in the message. Specify 10000 ints takes up a lot less space than 10000 arbitrary queue names
   // The drawback of this approach is we only allow up to 2^32 queues in memory at any one time
   private int transientIDSequence;

   private Set<Integer> transientIDs = new HashSet<Integer>();

   private Map<SimpleString, QueueInfo> queueInfos = new HashMap<SimpleString, QueueInfo>();

   private final Object notificationLock = new Object();

   public PostOfficeImpl(final StorageManager storageManager,
                         final PagingManager pagingManager,
                         final QueueFactory bindableFactory,
                         final ManagementService managementService,
                         final long messageExpiryScanPeriod,
                         final int messageExpiryThreadPriority,
                         final boolean checkAllowable,
                         final boolean enableWildCardRouting,
                         final boolean backup,
                         final int idCacheSize,
                         final boolean persistIDCache)
   {
      this.storageManager = storageManager;

      this.queueFactory = bindableFactory;

      this.managementService = managementService;

      this.checkAllowable = checkAllowable;

      this.pagingManager = pagingManager;

      this.messageExpiryScanPeriod = messageExpiryScanPeriod;

      this.messageExpiryThreadPriority = messageExpiryThreadPriority;

      if (enableWildCardRouting)
      {
         addressManager = new WildcardAddressManager();
      }
      else
      {
         addressManager = new SimpleAddressManager();
      }

      this.backup = backup;

      this.idCacheSize = idCacheSize;

      this.persistIDCache = persistIDCache;
   }

   // MessagingComponent implementation ---------------------------------------

   public void start() throws Exception
   {
      managementService.addNotificationListener(this);

      if (pagingManager != null)
      {
         pagingManager.setPostOffice(this);
      }

      // Injecting the postoffice (itself) on queueFactory for paging-control
      queueFactory.setPostOffice(this);

      if (messageExpiryScanPeriod > 0)
      {
         MessageExpiryRunner messageExpiryRunner = new MessageExpiryRunner();

         messageExpiryExecutor = new ScheduledThreadPoolExecutor(1, new JBMThreadFactory("JBM-scheduled-threads",
                                                                                         messageExpiryThreadPriority));
         messageExpiryExecutor.scheduleAtFixedRate(messageExpiryRunner,
                                                   messageExpiryScanPeriod,
                                                   messageExpiryScanPeriod,
                                                   TimeUnit.MILLISECONDS);
      }
      started = true;
   }

   public void stop() throws Exception
   {
      managementService.removeNotificationListener(this);

      if (messageExpiryExecutor != null)
      {
         messageExpiryExecutor.shutdown();
      }

      addressManager.clear();

      // Release all the locks
      for (SendLock lock : addressLocks.values())
      {
         lock.close();
      }

      addressLocks.clear();

      started = false;
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
         
         switch (type.toInt())
         {
            case NotificationType.BINDING_ADDED_INDEX:
            {
               TypedProperties props = notification.getProperties();
   
               Integer bindingType = (Integer)props.getProperty(ManagementHelper.HDR_BINDING_TYPE);
   
               if (bindingType == null)
               {
                  throw new IllegalArgumentException("Binding type not specified");
               }
               
               if (bindingType == BindingType.DIVERT_INDEX)
               {
                  // We don't propagate diverts
                  return;
               }
   
               SimpleString routingName = (SimpleString)props.getProperty(ManagementHelper.HDR_ROUTING_NAME);
   
               SimpleString clusterName = (SimpleString)props.getProperty(ManagementHelper.HDR_CLUSTER_NAME);
   
               SimpleString address = (SimpleString)props.getProperty(ManagementHelper.HDR_ADDRESS);
   
               Integer transientID = (Integer)props.getProperty(ManagementHelper.HDR_BINDING_ID);
   
               SimpleString filterString = (SimpleString)props.getProperty(ManagementHelper.HDR_FILTERSTRING);
               
               Integer distance = (Integer)props.getProperty(ManagementHelper.HDR_DISTANCE);
   
               QueueInfo info = new QueueInfo(routingName, clusterName, address, filterString, transientID, distance);
   
               queueInfos.put(clusterName, info);
               
               break;
            }            
            case NotificationType.BINDING_REMOVED_INDEX:
            {
               TypedProperties props = notification.getProperties();
   
               SimpleString clusterName = (SimpleString)props.getProperty(ManagementHelper.HDR_CLUSTER_NAME);
   
               if (clusterName == null)
               {
                  throw new IllegalStateException("No cluster name");
               }
   
               QueueInfo info = queueInfos.remove(clusterName);
   
               if (info == null)
               {
                  throw new IllegalStateException("Cannot find queue info for queue " + clusterName);
               }
               
               break;
            }
            case NotificationType.CONSUMER_CREATED_INDEX:
            { 
               TypedProperties props = notification.getProperties();
   
               SimpleString clusterName = (SimpleString)props.getProperty(ManagementHelper.HDR_CLUSTER_NAME);
   
               if (clusterName == null)
               {
                  throw new IllegalStateException("No cluster name");
               }
                                 
               SimpleString filterString = (SimpleString)props.getProperty(ManagementHelper.HDR_FILTERSTRING);
   
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
               }
                              
               break;
            }
            case NotificationType.CONSUMER_CLOSED_INDEX:
            {   
               TypedProperties props = notification.getProperties();
   
               SimpleString clusterName = (SimpleString)props.getProperty(ManagementHelper.HDR_CLUSTER_NAME);
   
               if (clusterName == null)
               {
                  throw new IllegalStateException("No cluster name");
               }
   
               SimpleString filterString = (SimpleString)props.getProperty(ManagementHelper.HDR_FILTERSTRING);
   
               QueueInfo info = queueInfos.get(clusterName);
   
               if (info == null)
               {
                  throw new IllegalStateException("Cannot find queue info for queue " + clusterName);
               }

               info.decrementConsumers();
   
               if (filterString != null)
               {
                  List<SimpleString> filterStrings = info.getFilterStrings();
                     
                  filterStrings.remove(filterString);
               }
               
//               if (redistributeMessages)
//               {                  
//                  int distance = (Integer)props.getProperty(ManagementHelper.HDR_DISTANCE);
//                  
//                  if (distance == 0)
//                  {
//                     if (info.getNumberOfConsumers() == 0)
//                     {
//                        Bindings bindings = getBindingsForAddress(info.getAddress());
//                        
//                        Collection<Binding> theBindings = bindings.getBindings();
//                        
//                        for (Binding binding: theBindings)
//                        {
//                           if (binding.getFilter() != null && !binding.getFilter().match(message) ?????????????
//                        }
//                     }
//                  }
//               }
               
               
               break;
            }
         }
      }
   }

   private boolean redistributeMessages;
   
   // PostOffice implementation -----------------------------------------------

   public synchronized boolean addDestination(final SimpleString address, final boolean durable) throws Exception
   {
      boolean added = addressManager.addDestination(address);

      if (added)
      {
         if (durable)
         {
            storageManager.addDestination(address);
         }

         managementService.registerAddress(address);
      }

      return added;
   }

   public synchronized boolean removeDestination(final SimpleString address, final boolean durable) throws Exception
   {
      boolean removed = addressManager.removeDestination(address);

      if (removed)
      {
         if (durable)
         {
            storageManager.deleteDestination(address);
         }
         managementService.unregisterAddress(address);
      }

      addressLocks.remove(address);

      return removed;
   }

   public synchronized boolean containsDestination(final SimpleString address)
   {
      return addressManager.containsDestination(address);
   }

   public Set<SimpleString> listAllDestinations()
   {
      return addressManager.getDestinations();
   }

   // TODO - needs to be synchronized to prevent happening concurrently with activate().
   // (and possible removeBinding and other methods)
   // Otherwise can have situation where createQueue comes in before failover, then failover occurs
   // and post office is activated but queue remains unactivated after failover so delivery never occurs
   // even though failover is complete
   public synchronized void addBinding(final Binding binding) throws Exception
   {
      binding.setID(generateTransientID());

      addBindingInMemory(binding);

      TypedProperties props = new TypedProperties();

      props.putIntProperty(ManagementHelper.HDR_BINDING_TYPE, binding.getType().toInt());

      props.putStringProperty(ManagementHelper.HDR_ADDRESS, binding.getAddress());

      props.putStringProperty(ManagementHelper.HDR_CLUSTER_NAME, binding.getClusterName());

      props.putStringProperty(ManagementHelper.HDR_ROUTING_NAME, binding.getRoutingName());

      props.putIntProperty(ManagementHelper.HDR_BINDING_ID, binding.getID());

      props.putIntProperty(ManagementHelper.HDR_DISTANCE, binding.getDistance());

      Filter filter = binding.getFilter();
      
      if (filter != null)
      {
         props.putStringProperty(ManagementHelper.HDR_FILTERSTRING, filter.getFilterString());
      }

      managementService.sendNotification(new Notification(NotificationType.BINDING_ADDED, props));
   }

   public synchronized Binding removeBinding(final SimpleString uniqueName) throws Exception
   {
      Binding binding = removeBindingInMemory(uniqueName);

      if (binding.getType() == BindingType.LOCAL_QUEUE)
      {
         managementService.unregisterQueue(uniqueName, binding.getAddress());
      }

      TypedProperties props = new TypedProperties();

      props.putStringProperty(ManagementHelper.HDR_ADDRESS, binding.getAddress());

      props.putStringProperty(ManagementHelper.HDR_CLUSTER_NAME, binding.getClusterName());

      props.putIntProperty(ManagementHelper.HDR_DISTANCE, binding.getDistance());

      managementService.sendNotification(new Notification(NotificationType.BINDING_REMOVED, props));

      releaseTransientID(binding.getID());

      return binding;
   }

   public Bindings getBindingsForAddress(final SimpleString address)
   {
      Bindings bindings = addressManager.getBindings(address);

      if (bindings == null)
      {
         bindings = new BindingsImpl();
      }

      return bindings;
   }

   public Binding getBinding(final SimpleString name)
   {
      return addressManager.getBinding(name);
   }

   public void route(final ServerMessage message, Transaction tx) throws Exception
   {
      SimpleString address = message.getDestination();
            
      if (checkAllowable)
      {
         if (!addressManager.containsDestination(address))
         {
            throw new MessagingException(MessagingException.ADDRESS_DOES_NOT_EXIST,
                                         "Cannot route to address " + address);
         }
      }

      byte[] duplicateID = (byte[])message.getProperty(MessageImpl.HDR_DUPLICATE_DETECTION_ID);
      
      DuplicateIDCache cache = null;

      if (duplicateID != null)
      {
         cache = getDuplicateIDCache(message.getDestination());

         if (cache.contains(duplicateID))
         {
            if (tx == null)
            {
               log.warn("Duplicate message detected - message will not be routed");
            }
            else
            {
               log.warn("Duplicate message detected - transaction will be rejected");

               tx.markAsRollbackOnly(null);
            }

            return;
         }
      }

      boolean startedTx = false;

      if (cache != null)
      {
         cache.addToCache(duplicateID, tx);

         if (tx == null)
         {
            // We need to store the duplicate id atomically with the message storage, so we need to create a tx for this

            tx = new TransactionImpl(storageManager);

            startedTx = true;
         }
      }

      if (tx == null)
      {
         if (pagingManager.page(message, true))
         {
            return;
         }
      }
      else
      {
         SimpleString destination = message.getDestination();

         boolean depage = tx.getProperty(TransactionPropertyIndexes.IS_DEPAGE) != null;

         if (!depage && pagingManager.isPaging(destination))
         {
            getPageOperation(tx).addMessageToPage(message);

            return;
         }
      }

      Bindings bindings = addressManager.getBindings(address);

      if (bindings != null)
      {
         bindings.route(message, tx);
      }

      if (startedTx)
      {
         tx.commit();
      }
   }

   public void route(final ServerMessage message) throws Exception
   {
      route(message, null);
   }

   public PagingManager getPagingManager()
   {
      return pagingManager;
   }

   public List<Queue> activate()
   {
      backup = false;

      pagingManager.activate();

      Map<SimpleString, Binding> nameMap = addressManager.getBindings();

      List<Queue> queues = new ArrayList<Queue>();

      for (Binding binding : nameMap.values())
      {
         if (binding.getType() == BindingType.LOCAL_QUEUE)
         {
            Queue queue = (Queue)binding.getBindable();

            boolean activated = queue.activate();

            if (!activated)
            {
               queues.add(queue);
            }
         }
      }

      return queues;
   }

   public synchronized SendLock getAddressLock(final SimpleString address)
   {
      SendLock lock = addressLocks.get(address);

      if (lock == null)
      {
         lock = new SendLockImpl();

         addressLocks.put(address, lock);
      }

      return lock;
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

         ServerMessage message = new ServerMessageImpl(storageManager.generateUniqueID());
         message.setBody(new ByteBufferWrapper(ByteBuffer.allocate(0)));
         message.setDestination(queueName);
         message.putBooleanProperty(HDR_RESET_QUEUE_DATA, true);
         queue.preroute(message, null);
         queue.route(message, null);

         for (QueueInfo info : queueInfos.values())
         {
            if (info.getAddress().startsWith(address))
            {
               message = createQueueInfoMessage(NotificationType.BINDING_ADDED, queueName);

               message.putStringProperty(ManagementHelper.HDR_ADDRESS, info.getAddress());
               message.putStringProperty(ManagementHelper.HDR_CLUSTER_NAME, info.getClusterName());
               message.putStringProperty(ManagementHelper.HDR_ROUTING_NAME, info.getRoutingName());
               message.putIntProperty(ManagementHelper.HDR_BINDING_ID, info.getID());
               message.putStringProperty(ManagementHelper.HDR_FILTERSTRING, info.getFilterString());
               message.putIntProperty(ManagementHelper.HDR_DISTANCE, info.getDistance());

               routeDirect(queue, message);

               int consumersWithFilters = info.getFilterStrings() != null ? info.getFilterStrings().size() : 0;

               for (int i = 0; i < info.getNumberOfConsumers() - consumersWithFilters; i++)
               {
                  message = createQueueInfoMessage(NotificationType.CONSUMER_CREATED, queueName);

                  message.putStringProperty(ManagementHelper.HDR_ADDRESS, info.getAddress());
                  message.putStringProperty(ManagementHelper.HDR_CLUSTER_NAME, info.getClusterName());
                  message.putIntProperty(ManagementHelper.HDR_DISTANCE, info.getDistance());

                  routeDirect(queue, message);
               }

               if (info.getFilterStrings() != null)
               {
                  for (SimpleString filterString : info.getFilterStrings())
                  {
                     message = createQueueInfoMessage(NotificationType.CONSUMER_CREATED, queueName);

                     message.putStringProperty(ManagementHelper.HDR_ADDRESS, info.getAddress());
                     message.putStringProperty(ManagementHelper.HDR_CLUSTER_NAME, info.getClusterName());
                     message.putStringProperty(ManagementHelper.HDR_FILTERSTRING, filterString);
                     message.putIntProperty(ManagementHelper.HDR_DISTANCE, info.getDistance());

                     routeDirect(queue, message);
                  }
               }
            }
         }
      }

   }

   // Private -----------------------------------------------------------------

   private void routeDirect(final Queue queue, final ServerMessage message) throws Exception
   {
      if (queue.getFilter() == null || queue.getFilter().match(message))
      {
         queue.preroute(message, null);
         queue.route(message, null);
      }
   }

   private ServerMessage createQueueInfoMessage(final NotificationType type, final SimpleString queueName)
   {
      ServerMessage message = new ServerMessageImpl(storageManager.generateUniqueID());
      message.setBody(new ByteBufferWrapper(ByteBuffer.allocate(0)));

      message.setDestination(queueName);

      message.putStringProperty(ManagementHelper.HDR_NOTIFICATION_TYPE, new SimpleString(type.toString()));
      message.putLongProperty(ManagementHelper.HDR_NOTIFICATION_TIMESTAMP, System.currentTimeMillis());

      return message;
   }

   private int generateTransientID()
   {
      int start = transientIDSequence;
      do
      {
         int id = transientIDSequence++;

         if (!transientIDs.contains(id))
         {
            transientIDs.add(id);

            return id;
         }
      }
      while (transientIDSequence != start);

      throw new IllegalStateException("Run out of queue ids!");
   }

   private void releaseTransientID(final int id)
   {
      transientIDs.remove(id);
   }

   private final PageMessageOperation getPageOperation(final Transaction tx)
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

   private void addBindingInMemory(final Binding binding) throws Exception
   {
      boolean exists = addressManager.addBinding(binding);

      if (!exists)
      {
         managementService.registerAddress(binding.getAddress());
      }

      if (binding.getType() == BindingType.LOCAL_QUEUE)
      {
         Queue queue = (Queue)binding.getBindable();

         if (backup)
         {
            queue.setBackup();
         }

         managementService.registerQueue(queue, binding.getAddress(), storageManager);
      }
   }

   private Binding removeBindingInMemory(final SimpleString bindingName) throws Exception
   {
      Binding binding = addressManager.removeBinding(bindingName);

      if (binding != null)
      {
         managementService.unregisterAddress(binding.getAddress());
      }

      return binding;
   }

   private class MessageExpiryRunner implements Runnable
   {
      public void run()
      {
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
               queue.expireMessages();
            }
            catch (Exception e)
            {
               log.error("failed to expire messages for queue " + queue.getName(), e);
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

      public void afterCommit(final Transaction tx) throws Exception
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

      public void afterPrepare(final Transaction tx) throws Exception
      {
      }

      public void afterRollback(final Transaction tx) throws Exception
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

            HashSet<SimpleString> pagedDestinationsToSync = new HashSet<SimpleString>();

            // We only need to add the dupl id header once per transaction
            boolean first = true;
            for (ServerMessage message : messagesToPage)
            {
               // http://wiki.jboss.org/wiki/JBossMessaging2Paging
               // Explained under Transaction On Paging. (This is the item B)
               if (pagingManager.page(message, tx.getID(), first))
               {
                  if (message.isDurable())
                  {
                     // We only create pageTransactions if using persistent messages
                     pageTransaction.increment();
                     pagingPersistent = true;
                     pagedDestinationsToSync.add(message.getDestination());
                  }
               }
               else
               {
                  // This could happen when the PageStore left the pageState

                  // TODO is this correct - don't we lose transactionality here???
                  route(message, null);
               }
               first = false;
            }

            if (pagingPersistent)
            {
               tx.putProperty(TransactionPropertyIndexes.CONTAINS_PERSISTENT, true);

               if (!pagedDestinationsToSync.isEmpty())
               {
                  pagingManager.sync(pagedDestinationsToSync);
                  storageManager.storePageTransaction(tx.getID(), pageTransaction);
               }
            }
         }
      }

   }

}
