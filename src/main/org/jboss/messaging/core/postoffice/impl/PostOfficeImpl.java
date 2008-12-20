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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.PagingStore;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.AddressManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.Bindings;
import org.jboss.messaging.core.postoffice.DuplicateIDCache;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.SendLock;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.SendLockImpl;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.util.JBMThreadFactory;
import org.jboss.messaging.util.Pair;
import org.jboss.messaging.util.SimpleString;

/**
 * A PostOfficeImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="csuconic@redhat.com">Clebert Suconic</a>
 */
public class PostOfficeImpl implements PostOffice
{
   private static final Logger log = Logger.getLogger(PostOfficeImpl.class);
   
   private static final List<MessageReference> emptyList = Collections.<MessageReference>emptyList();
   
   private final AddressManager addressManager;

   private final QueueFactory queueFactory;

   private final boolean checkAllowable;

   private final StorageManager storageManager;

   private final PagingManager pagingManager;

   private volatile boolean started;

   private volatile boolean backup;

   private final ManagementService managementService;

   private final ResourceManager resourceManager;

   private final Map<SimpleString, SendLock> addressLocks = new HashMap<SimpleString, SendLock>();

   private ScheduledThreadPoolExecutor messageExpiryExecutor;

   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;

   private final long messageExpiryScanPeriod;

   private final int messageExpiryThreadPriority;

   private final ConcurrentMap<SimpleString, DuplicateIDCache> duplicateIDCaches = new ConcurrentHashMap<SimpleString, DuplicateIDCache>();

   private final int idCacheSize;

   private final boolean persistIDCache;
   
   public PostOfficeImpl(final StorageManager storageManager,
                         final PagingManager pagingManager,
                         final QueueFactory queueFactory,
                         final ManagementService managementService,
                         final HierarchicalRepository<QueueSettings> queueSettingsRepository,
                         final long messageExpiryScanPeriod,
                         final int messageExpiryThreadPriority,
                         final boolean checkAllowable,
                         final ResourceManager resourceManager,
                         final boolean enableWildCardRouting,
                         final boolean backup,
                         final int idCacheSize,
                         final boolean persistIDCache)
   {
      this.storageManager = storageManager;

      this.queueFactory = queueFactory;

      this.managementService = managementService;

      this.checkAllowable = checkAllowable;

      this.pagingManager = pagingManager;

      this.resourceManager = resourceManager;

      this.queueSettingsRepository = queueSettingsRepository;

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
      if (pagingManager != null)
      {
         pagingManager.setPostOffice(this);

         pagingManager.start();
      }

      // Injecting the postoffice (itself) on queueFactory for paging-control
      queueFactory.setPostOffice(this);

      load();

      if (messageExpiryScanPeriod > 0)
      {
         MessageExpiryRunner messageExpiryRunner = new MessageExpiryRunner();
         messageExpiryRunner.setPriority(3);
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
      if (messageExpiryExecutor != null)
      {
         messageExpiryExecutor.shutdown();
      }

      pagingManager.stop();

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
   // TODO - more subtle locking could be used -this is a bit heavy handed
   public synchronized Binding addBinding(final SimpleString address,
                                          final SimpleString queueName,
                                          final Filter filter,
                                          final boolean durable,
                                          final boolean temporary,
                                          final boolean exclusive) throws Exception
   {
      Binding binding = createBinding(address, queueName, filter, durable, temporary, exclusive);

      addBindingInMemory(binding);

      if (durable)
      {
         storageManager.addBinding(binding);
      }

      return binding;
   }

   public synchronized Binding removeBinding(final SimpleString queueName) throws Exception
   {
      Binding binding = removeQueueInMemory(queueName);

      if (binding.getQueue().isDurable())
      {
         storageManager.deleteBinding(binding);
      }

      managementService.unregisterQueue(queueName, binding.getAddress());

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

   public Binding getBinding(final SimpleString queueName)
   {
      return addressManager.getBinding(queueName);
   }

   public void deliver(final List<MessageReference> references)
   {
      for (MessageReference ref : references)
      {
         ref.getQueue().add(ref);
      }
   }

   public List<MessageReference> reroute(final ServerMessage message) throws Exception
   {
      SimpleString address = message.getDestination();

      Bindings bindings = addressManager.getBindings(address);

      List<MessageReference> references = null;
      
      if (bindings != null)
      {
         references = bindings.route(message);

         computePaging(address, message, references);
      }
      
      return references;
   }
   
   public List<MessageReference> route(final ServerMessage message, final Transaction tx, final boolean deliver) throws Exception
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

      Bindings bindings = addressManager.getBindings(address);

      List<MessageReference> references = null;
      
      if (bindings != null)
      {
         references = bindings.route(message);

         computePaging(address, message, references);
      }

      if (message.getDurableRefCount() != 0)
      {
         if (tx == null)
         {
            storageManager.storeMessage(message);
         }
         else
         {
            storageManager.storeMessageTransactional(tx.getID(), message);
         }
      }

      if (references != null)
      {         
         Long scheduledDeliveryTime = (Long)message.getProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME);

         if (scheduledDeliveryTime != null)
         {
            for (MessageReference ref : references)
            {
               ref.setScheduledDeliveryTime(scheduledDeliveryTime);

               if (ref.getMessage().isDurable() && ref.getQueue().isDurable())
               {
                  if (tx != null)
                  {
                     storageManager.updateScheduledDeliveryTimeTransactional(tx.getID(), ref);
                  }
                  else
                  {
                     storageManager.updateScheduledDeliveryTime(ref);
                  }
               }
            }
         }
      
         if (deliver)
         {
            deliver(references);
         }
                  
         return references;
      }
      else
      {
         return emptyList;
      }
   }
   
   public void route(final ServerMessage message, final Transaction tx) throws Exception
   {
      if (tx == null)
      {
         if (!pagingManager.page(message))
         {
            route(message, null, true);
         }
      }
      else
      {
         tx.addMessage(message);
      }
   }

   public PagingManager getPagingManager()
   {
      return pagingManager;
   }

   public List<Queue> activate()
   {
      backup = false;

      Map<SimpleString, Binding> nameMap = addressManager.getBindings();

      List<Queue> queues = new ArrayList<Queue>();

      for (Binding binding : nameMap.values())
      {
         Queue queue = binding.getQueue();

         boolean activated = queue.activate();

         if (!activated)
         {
            queues.add(queue);
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

   public int numMappings()
   {
      return addressManager.numMappings();
   }

   // Private -----------------------------------------------------------------
 
   /**
    * Add sizes on Paging
    * @param address
    * @param message
    * @param references
    * @throws Exception
    */
   private void computePaging(SimpleString address, final ServerMessage message, List<MessageReference> references) throws Exception
   {
      if (!references.isEmpty())
      {
         PagingStore store = pagingManager.getPageStore(address);

         store.addSize(message.getMemoryEstimate());

         for (MessageReference ref : references)
         {
            store.addSize(ref.getMemoryEstimate());
         }
      }
   }

   private Binding createBinding(final SimpleString address,
                                 final SimpleString name,
                                 final Filter filter,
                                 final boolean durable,
                                 final boolean temporary,
                                 final boolean exclusive) throws Exception
   {
      Queue queue = queueFactory.createQueue(-1, name, filter, durable, false);

      if (backup)
      {
         queue.setBackup();
      }

      Binding binding = new BindingImpl(address, queue, exclusive);

      return binding;
   }

   private void addBindingInMemory(final Binding binding) throws Exception
   {
      boolean exists = addressManager.addMapping(binding.getAddress(), binding);
      if (!exists)
      {
         managementService.registerAddress(binding.getAddress());
      }

      managementService.registerQueue(binding.getQueue(), binding.getAddress(), storageManager);

      addressManager.addBinding(binding);
   }

   private Binding removeQueueInMemory(final SimpleString queueName) throws Exception
   {
      Binding binding = addressManager.removeBinding(queueName);

      if (addressManager.removeMapping(binding.getAddress(), queueName))
      {
         managementService.unregisterAddress(binding.getAddress());
      }

      return binding;
   }

   private void load() throws Exception
   {
      List<Binding> bindings = new ArrayList<Binding>();

      List<SimpleString> dests = new ArrayList<SimpleString>();

      storageManager.loadBindings(queueFactory, bindings, dests);

      // Destinations must be added first to ensure flow controllers exist
      // before queues are created
      for (SimpleString destination : dests)
      {
         addDestination(destination, true);
      }

      Map<Long, Queue> queues = new HashMap<Long, Queue>();

      for (Binding binding : bindings)
      {
         addBindingInMemory(binding);

         queues.put(binding.getQueue().getPersistenceID(), binding.getQueue());
      }

      Map<SimpleString, List<Pair<SimpleString, Long>>> duplicateIDMap = new HashMap<SimpleString, List<Pair<SimpleString, Long>>>();

      storageManager.loadMessageJournal(this, queues, resourceManager, duplicateIDMap);

      for (Map.Entry<SimpleString, List<Pair<SimpleString, Long>>> entry : duplicateIDMap.entrySet())
      {
         SimpleString address = entry.getKey();

         DuplicateIDCache cache = getDuplicateIDCache(address);

         if (persistIDCache)
         {
            cache.load(entry.getValue());
         }
      }

      // This is necessary as if the server was previously stopped while a depage was being executed,
      // it needs to resume the depage process on those destinations
      pagingManager.reloadStores();
      pagingManager.startGlobalDepage();
   }

   private class MessageExpiryRunner extends Thread
   {
      @Override
      public void run()
      {
         Map<SimpleString, Binding> nameMap = addressManager.getBindings();

         List<Queue> queues = new ArrayList<Queue>();

         for (Binding binding : nameMap.values())
         {
            Queue queue = binding.getQueue();
            queues.add(queue);
         }
         for (Queue queue : queues)
         {
            try
            {
               queue.expireMessages(storageManager, PostOfficeImpl.this, queueSettingsRepository);
            }
            catch (Exception e)
            {
               log.error("failed to expire messages for queue " + queue.getName(), e);
            }
         }
      }
   }
}
