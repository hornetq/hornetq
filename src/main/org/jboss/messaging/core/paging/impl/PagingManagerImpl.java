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

package org.jboss.messaging.core.paging.impl;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.paging.PageTransactionInfo;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.PagingStore;
import org.jboss.messaging.core.paging.PagingStoreFactory;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.utils.SimpleString;

/**
 *  <p>Look at the <a href="http://wiki.jboss.org/wiki/JBossMessaging2Paging">WIKI</a> for more information.</p>
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

   private final long maxGlobalSize;

   private volatile boolean backup;

   private final AtomicLong totalMemoryBytes = new AtomicLong(0);

   private final AtomicBoolean globalMode = new AtomicBoolean(false);

   private final ConcurrentMap<SimpleString, PagingStore> stores = new ConcurrentHashMap<SimpleString, PagingStore>();

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private final PagingStoreFactory pagingStoreFactory;

   private final StorageManager storageManager;

   private final long globalPageSize;

   private final boolean syncNonTransactional;

   private final ConcurrentMap</*TransactionID*/Long, PageTransactionInfo> transactions = new ConcurrentHashMap<Long, PageTransactionInfo>();

   // Static
   // --------------------------------------------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(PagingManagerImpl.class);

   // Constructors
   // --------------------------------------------------------------------------------------------------------------------

   public PagingManagerImpl(final PagingStoreFactory pagingSPI,
                            final StorageManager storageManager,
                            final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                            final long maxGlobalSize,
                            final long globalPageSize,
                            final boolean syncNonTransactional,
                            final boolean backup)
   {
      pagingStoreFactory = pagingSPI;
      this.addressSettingsRepository = addressSettingsRepository;
      this.storageManager = storageManager;
      this.globalPageSize = globalPageSize;
      this.maxGlobalSize = maxGlobalSize;
      this.syncNonTransactional = syncNonTransactional;
      this.backup = backup;
   }

   // Public
   // ---------------------------------------------------------------------------------------------------------------------------

   // PagingManager implementation
   // -----------------------------------------------------------------------------------------------------

   public void activate()
   {
      backup = false;

      startGlobalDepage();
   }

   public boolean isBackup()
   {
      return backup;
   }

   public boolean isGlobalPageMode()
   {
      return globalMode.get();
   }

   public void setGlobalPageMode(final boolean globalMode)
   {
      this.globalMode.set(globalMode);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.paging.PagingManager#reloadStores()
    */
   public synchronized void reloadStores() throws Exception
   {
      List<PagingStore> destinations = pagingStoreFactory.reloadStores(addressSettingsRepository);

      for (PagingStore store : destinations)
      {
         store.start();
         stores.put(store.getStoreName(), store);
      }

   }

   /**
    * @param destination
    * @return
    */
   private synchronized PagingStore createPageStore(final SimpleString storeName) throws Exception
   {
      PagingStore store = stores.get(storeName);

      if (store == null)
      {
         store = newStore(storeName);

         store.start();

         stores.put(storeName, store);
      }

      return store;
   }

   /** stores is a ConcurrentHashMap, so we don't need to synchronize this method */
   public PagingStore getPageStore(final SimpleString storeName) throws Exception
   {
      PagingStore store = stores.get(storeName);

      if (store == null)
      {
         store = createPageStore(storeName);
      }

      return store;
   }

   /** this will be set by the postOffice itself.
    *  There is no way to set this on the constructor as the PagingManager is constructed before the postOffice.
    *  (There is a one-to-one relationship here) */
   public void setPostOffice(final PostOffice postOffice)
   {
      pagingStoreFactory.setPostOffice(postOffice);
   }

   public long getGlobalPageSize()
   {
      return globalPageSize;
   }

   public boolean isPaging(final SimpleString destination) throws Exception
   {
      return getPageStore(destination).isPaging();
   }

   public boolean page(final ServerMessage message, final long transactionId, final boolean duplicateDetection) throws Exception
   {
      // The sync on transactions is done on commit only
      return getPageStore(message.getDestination()).page(new PagedMessageImpl(message, transactionId),
                                                         false,
                                                         duplicateDetection);
   }

   public boolean page(final ServerMessage message, final boolean duplicateDetection) throws Exception
   {
      // If non Durable, there is no need to sync as there is no requirement for persistence for those messages in case
      // of crash
      return getPageStore(message.getDestination()).page(new PagedMessageImpl(message),
                                                         syncNonTransactional && message.isDurable(),
                                                         duplicateDetection);
   }

   public void addTransaction(final PageTransactionInfo pageTransaction)
   {
      transactions.put(pageTransaction.getTransactionID(), pageTransaction);
   }

   public void removeTransaction(final long id)
   {
      transactions.remove(id);
   }

   public PageTransactionInfo getTransaction(final long id)
   {
      return transactions.get(id);
   }

   public void sync(final Collection<SimpleString> destinationsToSync) throws Exception
   {
      for (SimpleString destination : destinationsToSync)
      {
         getPageStore(destination).sync();
      }
   }

   // MessagingComponent implementation
   // ------------------------------------------------------------------------------------------------

   public boolean isStarted()
   {
      return started;
   }

   public synchronized void start() throws Exception
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

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;
      }

      started = false;

      for (PagingStore store : stores.values())
      {
         store.stop();
      }

      pagingStoreFactory.stop();

      totalMemoryBytes.set(0);

      globalMode.set(false);
   }

   public void startGlobalDepage()
   {
      if (!started)
      {
         // If stop the server while depaging, the server may call a rollback,
         // the rollback may addSizes back and that would fire a globalDepage.
         // Because of that we must ignore any startGlobalDepage calls, 
         // and this check needs to be done outside of the lock
         return;
      }
      synchronized (this)
      {
         if (!isBackup())
         {
            setGlobalPageMode(true);
            for (PagingStore store : stores.values())
            {
               store.startDepaging(pagingStoreFactory.getGlobalDepagerExecutor());
            }
         }
      }
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.paging.PagingManager#getGlobalSize()
    */
   public long getTotalMemory()
   {
      return totalMemoryBytes.get();
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.paging.PagingManager#addGlobalSize(long)
    */
   public long addSize(final long size)
   {
      return totalMemoryBytes.addAndGet(size);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.paging.PagingManager#getMaxGlobalSize()
    */
   public long getMaxMemory()
   {
      return maxGlobalSize;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private PagingStore newStore(final SimpleString destinationName) throws Exception
   {
      return pagingStoreFactory.newStore(destinationName, addressSettingsRepository.getMatch(destinationName.toString()));
   }

   // Inner classes -------------------------------------------------

}
