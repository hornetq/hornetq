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

package org.hornetq.core.paging.impl;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.PageTransactionInfo;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.PagingStoreFactory;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;

/**
 *
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:andy.taylor@jboss.org>Andy Taylor</a>
 *
 */
public class PagingManagerImpl implements PagingManager
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private volatile boolean started = false;

   /**
    * Lock used at the start of synchronization between a live server and its backup.
    * Synchronization will lock all {@link PagingStore} instances, and so any operation here that
    * requires a lock on a {@link PagingStore} instance needs to take a read-lock on
    * {@link #syncLock} to avoid dead-locks.
    */
   private final ReentrantReadWriteLock syncLock = new ReentrantReadWriteLock();
   private final ConcurrentMap<SimpleString, PagingStore> stores = new ConcurrentHashMap<SimpleString, PagingStore>();

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private final PagingStoreFactory pagingStoreFactory;

   private final StorageManager storageManager;

   private final ConcurrentMap<Long, PageTransactionInfo> transactions =
            new ConcurrentHashMap<Long, PageTransactionInfo>();

   // Static
   // --------------------------------------------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(PagingManagerImpl.class);
   private static boolean isTrace = log.isTraceEnabled();

   // Constructors
   // --------------------------------------------------------------------------------------------------------------------

   public PagingManagerImpl(final PagingStoreFactory pagingSPI,
                            final StorageManager storageManager,
                            final HierarchicalRepository<AddressSettings> addressSettingsRepository)
   {
      pagingStoreFactory = pagingSPI;
      this.addressSettingsRepository = addressSettingsRepository;
      addressSettingsRepository.registerListener(this);
      this.storageManager = storageManager;
   }

   // Public
   // ---------------------------------------------------------------------------------------------------------------------------


   // Hierarchical changes listener

   /* (non-Javadoc)
    * @see org.hornetq.core.settings.HierarchicalRepositoryChangeListener#onChange()
    */
   public void onChange()
   {
      reaplySettings();
   }

   // PagingManager implementation
   // -----------------------------------------------------------------------------------------------------

   public void reaplySettings()
   {
    for (PagingStore store : stores.values())
      {
         AddressSettings settings = this.addressSettingsRepository.getMatch(store.getAddress().toString());
         store.applySetting(settings);
      }
   }

   public SimpleString[] getStoreNames()
   {
      Set<SimpleString> names = stores.keySet();
      return names.toArray(new SimpleString[names.size()]);
   }

   public void reloadStores() throws Exception
   {
      lock();
      try
      {
         List<PagingStore> reloadedStores = pagingStoreFactory.reloadStores(addressSettingsRepository);

         for (PagingStore store : reloadedStores)
         {
            store.start();
            stores.put(store.getStoreName(), store);
         }
      }
      finally
      {
         unlock();
      }

   }

   public void deletePageStore(final SimpleString storeName) throws Exception
   {
      syncLock.readLock().lock();
      try
      {
         PagingStore store = stores.remove(storeName);
         if (store != null)
         {
            store.stop();
         }
      }
      finally
      {
         syncLock.readLock().unlock();
      }
   }

   /** stores is a ConcurrentHashMap, so we don't need to synchronize this method */
   public PagingStore getPageStore(final SimpleString storeName) throws Exception
   {
      PagingStore store = stores.get(storeName);

      if (store != null)
      {
         return store;
      }
      return newStore(storeName);
   }

   /** this will be set by the postOffice itself.
    *  There is no way to set this on the constructor as the PagingManager is constructed before the postOffice.
    *  (There is a one-to-one relationship here) */
   public void setPostOffice(final PostOffice postOffice)
   {
      pagingStoreFactory.setPostOffice(postOffice);
   }

   public void addTransaction(final PageTransactionInfo pageTransaction)
   {
      if (isTrace)
      {
         log.trace("Adding pageTransaction " + pageTransaction.getTransactionID());
      }
      transactions.put(pageTransaction.getTransactionID(), pageTransaction);
   }

   public void removeTransaction(final long id)
   {
      if (isTrace)
      {
         log.trace("Removing pageTransaction " +id);
      }
      transactions.remove(id);
   }

   public PageTransactionInfo getTransaction(final long id)
   {
      if (isTrace)
      {
         log.trace("looking up pageTX = " + id);
      }
      return transactions.get(id);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.PagingManager#getTransactions()
    */
   public Map<Long, PageTransactionInfo> getTransactions()
   {
      return transactions;
   }


   // HornetQComponent implementation
   // ------------------------------------------------------------------------------------------------

   public boolean isStarted()
   {
      return started;
   }

   public void start() throws Exception
   {
      lock();
      try
      {
         if (started)
         {
            return;
         }

         pagingStoreFactory.setPagingManager(this);

         pagingStoreFactory.setStorageManager(storageManager);

         reloadStores();

         started = true;
      }
      finally
      {
         unlock();
      }
   }

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;
      }
      started = false;

      lock();
      try
      {

         for (PagingStore store : stores.values())
         {
            store.stop();
         }

         pagingStoreFactory.stop();
      }
      finally
      {
         unlock();
      }
   }

   public void processReload() throws Exception
   {
      for (PagingStore store: stores.values())
      {
         store.processReload();
      }
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private PagingStore newStore(final SimpleString address) throws Exception
   {
      syncLock.readLock().lock();
      try {
         PagingStore store = stores.get(address);
         if (store == null)
         {
            store = pagingStoreFactory.newStore(address, addressSettingsRepository.getMatch(address.toString()));
            store.start();
            stores.put(address, store);
         }
         return store;
      }
      finally
      {
         syncLock.readLock().unlock();
      }
   }

   public void unlock()
   {
      syncLock.writeLock().unlock();
   }

   public void lock()
   {
      syncLock.writeLock().lock();
   }
}
