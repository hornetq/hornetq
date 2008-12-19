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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.paging.LastPageRecord;
import org.jboss.messaging.core.paging.PageTransactionInfo;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.PagingStore;
import org.jboss.messaging.core.paging.PagingStoreFactory;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.util.SimpleString;

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

   private final AtomicLong globalSize = new AtomicLong(0);

   private final AtomicBoolean globalMode = new AtomicBoolean(false);

   private final ConcurrentMap<SimpleString, PagingStore> stores = new ConcurrentHashMap<SimpleString, PagingStore>();

   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;

   private final PagingStoreFactory pagingSPI;

   private final StorageManager storageManager;

   private final long defaultPageSize;

   private final boolean syncNonTransactional;

   private final ConcurrentMap</*TransactionID*/Long, PageTransactionInfo> transactions = new ConcurrentHashMap<Long, PageTransactionInfo>();

   // Static
   // --------------------------------------------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(PagingManagerImpl.class);

   // This is just a debug tool method.
   // During debugs you could make log.trace as log.info, and change the
   // variable isTrace above
   private static void trace(final String message)
   {
      // log.trace(message);
      log.info(message);
   }

   // Constructors
   // --------------------------------------------------------------------------------------------------------------------

   public PagingManagerImpl(final PagingStoreFactory pagingSPI,
                            final StorageManager storageManager,
                            final HierarchicalRepository<QueueSettings> queueSettingsRepository,
                            final long maxGlobalSize,
                            final long defaultPageSize,
                            final boolean syncNonTransactional)
   {
      this.pagingSPI = pagingSPI;
      this.queueSettingsRepository = queueSettingsRepository;
      this.storageManager = storageManager;
      this.defaultPageSize = defaultPageSize;
      this.maxGlobalSize = maxGlobalSize;
      this.syncNonTransactional = syncNonTransactional;
   }

   // Public
   // ---------------------------------------------------------------------------------------------------------------------------

   // PagingManager implementation
   // -----------------------------------------------------------------------------------------------------

   public boolean isGlobalPageMode()
   {
      return globalMode.get();
   }

   public void setGlobalPageMode(final boolean globalMode)
   {
      this.globalMode.set(globalMode);
   }

   /**
    * @param destination
    * @return
    */
   public synchronized PagingStore createPageStore(final SimpleString storeName) throws Exception
   {
      PagingStore store = stores.get(storeName);

      if (store == null)
      {
         store = newStore(storeName);

         PagingStore oldStore = stores.putIfAbsent(storeName, store);

         if (oldStore != null)
         {
            store = oldStore;
         }
         else
         {
            store.start();
         }
      }

      return store;
   }

   public PagingStore getPageStore(final SimpleString storeName) throws Exception
   {
      PagingStore store = stores.get(storeName);

      if (store == null)
      {
         throw new IllegalStateException("Store " + storeName + " not found on paging");
      }

      return store;
   }

   /** this will be set by the postOffice itself.
    *  There is no way to set this on the constructor as the PagingManager is constructed before the postOffice.
    *  (There is a one-to-one relationship here) */
   public void setPostOffice(final PostOffice postOffice)
   {
      pagingSPI.setPostOffice(postOffice);
   }

   public long getDefaultPageSize()
   {
      return defaultPageSize;
   }

   public void setLastPageRecord(final LastPageRecord lastPage) throws Exception
   {
      trace("LastPage loaded was " + lastPage.getLastId() + " recordID = " + lastPage.getRecordId());

      getPageStore(lastPage.getDestination()).setLastPageRecord(lastPage);
   }

   public boolean isPaging(final SimpleString destination) throws Exception
   {
      return getPageStore(destination).isPaging();
   }

   public void messageDone(final ServerMessage message) throws Exception
   {
      getPageStore(message.getDestination()).addSize(-message.getMemoryEstimate());
   }

   public boolean addSize(final ServerMessage message) throws Exception
   {
      return getPageStore(message.getDestination()).addSize(message.getMemoryEstimate());
   }

   public void removeSize(final ServerMessage message) throws Exception
   {
      getPageStore(message.getDestination()).addSize(-message.getMemoryEstimate());
   }

   public void removeSize(final MessageReference reference) throws Exception
   {
      getPageStore(reference.getMessage().getDestination()).addSize(-reference.getMemoryEstimate());
   }

   public boolean page(final ServerMessage message, final long transactionId) throws Exception
   {
      // The sync on transactions is done on commit only
      return getPageStore(message.getDestination()).page(new PagedMessageImpl(message, transactionId), false);
   }

   public boolean page(final ServerMessage message) throws Exception
   {
      // If non Durable, there is no need to sync as there is no requirement for persistence for those messages in case
      // of crash
      return getPageStore(message.getDestination()).page(new PagedMessageImpl(message),
                                                         syncNonTransactional && message.isDurable());
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

      pagingSPI.setPagingManager(this);

      pagingSPI.setStorageManager(storageManager);

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

      pagingSPI.stop();
   }

   public synchronized void startGlobalDepage()
   {
      setGlobalPageMode(true);
      for (PagingStore store : stores.values())
      {
         store.startDepaging(pagingSPI.getGlobalDepagerExecutor());
      }
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.paging.PagingManager#getGlobalSize()
    */
   public long getGlobalSize()
   {
      return globalSize.get();
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.paging.PagingManager#addGlobalSize(long)
    */
   public long addGlobalSize(final long size)
   {
      return globalSize.addAndGet(size);
   }

   /* (non-Javadoc)
    * @see org.jboss.messaging.core.paging.PagingManager#getMaxGlobalSize()
    */
   public long getMaxGlobalSize()
   {
      return maxGlobalSize;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private PagingStore newStore(final SimpleString destinationName)
   {
      return pagingSPI.newStore(destinationName, queueSettingsRepository.getMatch(destinationName.toString()));
   }

   // Inner classes -------------------------------------------------

}
