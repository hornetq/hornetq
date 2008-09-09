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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.paging.LastPageRecord;
import org.jboss.messaging.core.paging.PageMessage;
import org.jboss.messaging.core.paging.PageTransactionInfo;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.PagingStore;
import org.jboss.messaging.core.paging.PagingStoreFactory;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.postoffice.impl.PostOfficeImpl;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.util.SimpleString;

/**
 *  <p>Look at the <a href="http://wiki.jboss.org/auth/wiki/JBossMessaging2Paging">WIKI</a> for more information.</p>
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class PagingManagerImpl implements PagingManager
{

   
   // Constants -----------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private volatile boolean started = false;
   
   private final ConcurrentMap<SimpleString, PagingStore> stores = new ConcurrentHashMap<SimpleString, PagingStore>();
   
   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;
   
   private final PagingStoreFactory pagingSPI;
   
   private final StorageManager storageManager;

   private PostOffice postOffice;
   
   private final ConcurrentMap</*TransactionID*/ Long , PageTransactionInfo> transactions = new ConcurrentHashMap<Long, PageTransactionInfo>();
   

   
   // Static --------------------------------------------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(PostOfficeImpl.class);
   
   //private static final boolean isTrace = log.isTraceEnabled();
   private static final boolean isTrace = true;
   
   // This is just a debug tool method.
   // During debugs you could make log.trace as log.info, and change the variable isTrace above
   private static void trace(String message)
   {
      //log.trace(message);
      log.info(message);
   }
   
   
   // Constructors --------------------------------------------------------------------------------------------------------------------
   
   public PagingManagerImpl(final PagingStoreFactory pagingSPI, StorageManager storageManager, 
                            final HierarchicalRepository<QueueSettings> queueSettingsRepository)
   {
      this.pagingSPI = pagingSPI;
      this.queueSettingsRepository = queueSettingsRepository;
      this.storageManager = storageManager;
   }
   
   // Public ---------------------------------------------------------------------------------------------------------------------------
   
   // PagingManager implementation -----------------------------------------------------------------------------------------------------
   
   public PagingStore getPageStore(final SimpleString storeName) throws Exception
   {
      validateStarted();
      
      PagingStore store = stores.get(storeName);
      if (store == null)
      {
         
         store = newStore(storeName);
         
         PagingStore oldStore = stores.putIfAbsent(storeName, store);
         
         if (oldStore != null)
         {
            store = oldStore;
         }
         
         store.start();
      }

      return store;
      
   }
   
   /** this will be set by the postOffice itself.
    *  There is no way to set this on the constructor as the PagingManager is constructed before the postOffice.
    *  (There is a one-to-one relationship here) */
   public void setPostOffice(PostOffice postOffice)
   {
      this.postOffice = postOffice;
   }

   public void clearLastPageRecord(LastPageRecord lastRecord) throws Exception
   {
      trace("Clearing lastRecord information " + lastRecord.getLastId());
      storageManager.storeDelete(lastRecord.getRecordId());
   }
   
   /**
    * This method will remove files from the page system and add them into the journal, doing it transactionally
    * 
    * A Transaction will be opened only if persistent messages are used.
    * If persistent messages are also used, it will update eventual PageTransactions
    */
   public boolean onDepage(int pageId, final SimpleString destination, PagingStore pagingStore, final PageMessage[] data) throws Exception
   {
      log.info("Depaging....");
      
      /// Depage has to be done atomically, in case of failure it should be back to where it was
      final long depageTransactionID = storageManager.generateTransactionID();
      
      LastPageRecord lastPage = pagingStore.getLastRecord(); 
      
      if (lastPage == null)
      {
         lastPage = new LastPageRecordImpl(pageId, destination);
         pagingStore.setLastRecord(lastPage);
      }
      else
      {
         if (pageId <= lastPage.getLastId())
         {
            log.warn("Page " + pageId + " was already processed, ignoring the page");
            return true;
         }
      }

      lastPage.setLastId(pageId);
      storageManager.storeLastPage(depageTransactionID, lastPage);
      
      HashSet<PageTransactionInfo> pageTransactionsToUpdate = new HashSet<PageTransactionInfo>();

      final List<MessageReference> refsToAdd = new ArrayList<MessageReference>();
      
      for (PageMessage msg: data)
      {
         final long transactionIdDuringPaging = msg.getTransactionID();
         if (transactionIdDuringPaging > 0)
         {
            final PageTransactionInfo pageTransactionInfo = transactions.get(transactionIdDuringPaging);
            
            // http://wiki.jboss.org/auth/wiki/JBossMessaging2Paging
            // This is the Step D described on the "Transactions on Paging" section
            if (pageTransactionInfo == null)
            {
               if (isTrace)
               {
                  trace("Transaction " + msg.getTransactionID() + " not found, ignoring message " + msg.getMessage().getMessageID());
               }
               continue;
            }
            
            // This is to avoid a race condition where messages are depaged before the commit arrived
            pageTransactionInfo.waitCompletion();

            /// Update information about transactions
            if (msg.getMessage().isDurable())
            {
               pageTransactionInfo.decrement();
               pageTransactionsToUpdate.add(pageTransactionInfo);
            }
         }
         
         msg.getMessage().setMessageID(storageManager.generateID());

         refsToAdd.addAll(postOffice.route(msg.getMessage()));
         
         if (msg.getMessage().getDurableRefCount() != 0)
         {
            storageManager.storeMessageTransactional(depageTransactionID, msg.getMessage());
         }
      }
      
      
      for (PageTransactionInfo pageWithTransaction: pageTransactionsToUpdate)
      {
         if (pageWithTransaction.getNumberOfMessages() == 0)
         { 
            // http://wiki.jboss.org/auth/wiki/JBossMessaging2Paging
            // numberOfReads==numberOfWrites -> We delete the record
            storageManager.storeDeleteTransactional(depageTransactionID, pageWithTransaction.getRecordID());
            this.transactions.remove(pageWithTransaction.getTransactionID());
         }
         else
         {
            storageManager.storePageTransaction(depageTransactionID, pageWithTransaction);
         }
      }
      
      storageManager.commit(depageTransactionID);

      for (MessageReference ref : refsToAdd)
      {
         ref.getQueue().addLast(ref);
      }
      
      return pagingStore.getAddressSize() < pagingStore.getMaxSizeBytes(); 
   }
   
   public void setLastPage(LastPageRecord lastPage) throws Exception
   {
      System.out.println("LastPage loaded was " + lastPage.getLastId() + " recordID = " + lastPage.getRecordId());
      this.getPageStore(lastPage.getDestination()).setLastRecord(lastPage);
   }

   public boolean isPaging(SimpleString destination) throws Exception
   {
      return this.getPageStore(destination).isPaging();
   }
   
   public void messageDone(ServerMessage message) throws Exception
   {
      addSize(message.getDestination(), message.getEncodeSize() * -1);
   }
   
   public long addSize(final ServerMessage message) throws Exception
   {
      return addSize(message.getDestination(), message.getEncodeSize());      
   }
   
   public boolean page(ServerMessage message, long transactionId)
         throws Exception
   {
      return this.getPageStore(message.getDestination()).page(new PageMessageImpl(message, transactionId));
   }


   public boolean page(ServerMessage message) throws Exception
   {
      return this.getPageStore(message.getDestination()).page(new PageMessageImpl(message));
   }

   
   public void addTransaction(PageTransactionInfo pageTransaction)
   {
      this.transactions.put(pageTransaction.getTransactionID(), pageTransaction);
   }

   public void completeTransaction(long transactionId)
   {
      PageTransactionInfo pageTrans = this.transactions.get(transactionId);
      
      // If nothing was paged.. we just remove the information to avoid memory leaks
      if (pageTrans.getNumberOfMessages() == 0)
      {
         this.transactions.remove(pageTrans);
      }
   }
   
   public void sync(Collection<SimpleString> destinationsToSync) throws Exception
   {
      for (SimpleString destination: destinationsToSync)
      {
         this.getPageStore(destination).sync();
      }
   }
   
   // MessagingComponent implementation ------------------------------------------------------------------------------------------------
   
   public boolean isStarted()
   {
      return started;
   }
   
   public void start() throws Exception
   {
      this.started = true;      
   }
   
   public void stop() throws Exception
   {
      this.started = false;
      
      for (PagingStore store: stores.values())
      {
         store.stop();
      }
   }
   
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   private PagingStore newStore(final SimpleString destinationName)
   {
      return pagingSPI.newStore(destinationName, this.queueSettingsRepository.getMatch(destinationName.toString()));
   }
   
   private void validateStarted()
   {
      if (!started)
      {
         throw new IllegalStateException("PagingManager is not started");
      }
   }

   
   private long addSize(final SimpleString destination, final long size) throws Exception
   {
      final PagingStore store = this.getPageStore(destination);
      
      final long maxSize = store.getMaxSizeBytes();
      
      final long pageSize = store.getPageSizeBytes();

      if (store.isDropWhenMaxSize() && size > 0)
      {
         if (store.getAddressSize() + size > maxSize)
         {
            if (!store.isDroppedMessage())
            {
               store.setDroppedMessage(true);
               log.warn("Messages are being dropped on adress " + store.getStoreName());
            }
            
            return -1l;
         }
         else
         {
            return store.addAddressSize(size);
         }
      }
      else
      {
         final long addressSize = store.addAddressSize(size);

         if (size > 0)
         {
            if (maxSize > 0 && addressSize > maxSize)
            {
               if (store.startPaging())
               {
                  if (isTrace)
                  {
                     trace("Starting paging on " + destination + ", size = " + addressSize + ", maxSize=" + maxSize);
                  }
               }
            }
         }
         else
         {
            if ( maxSize > 0 && addressSize < (maxSize - pageSize))
            {
               if (store.startDepaging())
               {
                  log.info("Starting depaging Thread, size = " + addressSize);
               }
            }
         }
         
         return addressSize;
      }
   }

   
   // Inner classes -------------------------------------------------
   
}
