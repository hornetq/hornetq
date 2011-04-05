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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.Pair;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.PageTransactionInfo;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.cursor.PagePosition;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionOperationAbstract;
import org.hornetq.core.transaction.TransactionPropertyIndexes;
import org.hornetq.utils.DataConstants;

/**
 *
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class PageTransactionInfoImpl implements PageTransactionInfo
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(PageTransactionInfoImpl.class);

   // Attributes ----------------------------------------------------

   private long transactionID;

   private volatile long recordID = -1;

   private volatile boolean committed = false;
   
   private volatile boolean useRedelivery = false;

   private volatile boolean rolledback = false;

   private AtomicInteger numberOfMessages = new AtomicInteger(0);
   
   private List<Pair<PageSubscription, PagePosition>> lateDeliveries;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PageTransactionInfoImpl(final long transactionID)
   {
      this();
      this.transactionID = transactionID;
   }

   public PageTransactionInfoImpl()
   {
   }

   // Public --------------------------------------------------------

   public long getRecordID()
   {
      return recordID;
   }

   public void setRecordID(final long recordID)
   {
      this.recordID = recordID;
   }

   public long getTransactionID()
   {
      return transactionID;
   }

   public void onUpdate(final int update, final StorageManager storageManager, PagingManager pagingManager)
   {
      int sizeAfterUpdate = numberOfMessages.addAndGet(-update);
      if (sizeAfterUpdate == 0 && storageManager != null)
      {
         try
         {
            storageManager.deletePageTransactional(this.recordID);
         }
         catch (Exception e)
         {
            log.warn("Can't delete page transaction id=" + this.recordID);
         }

         pagingManager.removeTransaction(this.transactionID);
      }
   }

   public void increment()
   {
      numberOfMessages.incrementAndGet();
   }
   
   public void increment(final int size)
   {
      numberOfMessages.addAndGet(size);
   }

   public int getNumberOfMessages()
   {
      return numberOfMessages.get();
   }

   // EncodingSupport implementation

   public synchronized void decode(final HornetQBuffer buffer)
   {
      transactionID = buffer.readLong();
      numberOfMessages.set(buffer.readInt());
      committed = true;
   }

   public synchronized void encode(final HornetQBuffer buffer)
   {
      buffer.writeLong(transactionID);
      buffer.writeInt(numberOfMessages.get());
   }

   public synchronized int getEncodeSize()
   {
      return DataConstants.SIZE_LONG + DataConstants.SIZE_INT;
   }

   public synchronized void commit()
   {
      if (lateDeliveries != null)
      {
         // This is to make sure deliveries that were touched before the commit arrived will be delivered
         for (Pair<PageSubscription, PagePosition> pos : lateDeliveries)
         {
            pos.a.redeliver(pos.b);
         }
         lateDeliveries.clear();
      }
      committed = true;
      lateDeliveries = null;
   }

   public void store(final StorageManager storageManager, PagingManager pagingManager, final Transaction tx) throws Exception
   {
      storageManager.storePageTransaction(tx.getID(), this);
   }

   /* 
    * This is to be used after paging. We will update the PageTransactions until they get all the messages delivered. On that case we will delete the page TX
    * (non-Javadoc)
    * @see org.hornetq.core.paging.PageTransactionInfo#storeUpdate(org.hornetq.core.persistence.StorageManager, org.hornetq.core.transaction.Transaction, int)
    */
   public void storeUpdate(final StorageManager storageManager, final PagingManager pagingManager, final Transaction tx) throws Exception
   {
      internalUpdatePageManager(storageManager, pagingManager, tx, 1);
   }
   
   public void reloadUpdate(final StorageManager storageManager, final PagingManager pagingManager, final Transaction tx, final int increment) throws Exception
   {
      UpdatePageTXOperation updt = internalUpdatePageManager(storageManager, pagingManager, tx, increment);
      updt.setStored();
   }

   /**
    * @param storageManager
    * @param pagingManager
    * @param tx
    */
   protected UpdatePageTXOperation internalUpdatePageManager(final StorageManager storageManager,
                                            final PagingManager pagingManager,
                                            final Transaction tx,
                                            final int increment)
   {
      UpdatePageTXOperation pgtxUpdate = (UpdatePageTXOperation)tx.getProperty(TransactionPropertyIndexes.PAGE_TRANSACTION_UPDATE);
      
      if (pgtxUpdate == null)
      {
         pgtxUpdate = new UpdatePageTXOperation(storageManager, pagingManager);
         tx.putProperty(TransactionPropertyIndexes.PAGE_TRANSACTION_UPDATE, pgtxUpdate);
         tx.addOperation(pgtxUpdate);
      }
      
      tx.setContainsPersistent();
      
      pgtxUpdate.addUpdate(this, increment);
      
      return pgtxUpdate;
   }
   
   public void storeUpdate(final StorageManager storageManager, final PagingManager pagingManager) throws Exception
   {
      storageManager.updatePageTransaction(this, 1);
      storageManager.afterCompleteOperations(new IOAsyncTask()
      {
         public void onError(int errorCode, String errorMessage)
         {
         }
         
         public void done()
         {
            PageTransactionInfoImpl.this.onUpdate(1, storageManager, pagingManager);
         }

         public List<MessageReference> getRelatedMessageReferences()
         {
            return null;
         }
      });
   }
   
   

   public boolean isCommit()
   {
      return committed;
   }
   
   public void setCommitted(final boolean committed)
   {
      this.committed = committed;
   }

   public boolean isRollback()
   {
      return rolledback;
   }

   public synchronized void rollback()
   {
      rolledback = true;
      committed = false;

      if (lateDeliveries != null)
      {
         for (Pair<PageSubscription, PagePosition> pos : lateDeliveries)
         {
            pos.a.lateDeliveryRollback(pos.b);
         }
         lateDeliveries = null;
      }
   }

   public String toString()
   {
      return "PageTransactionInfoImpl(transactionID=" + transactionID +
             ",id=" +
             recordID +
             ",numberOfMessages=" +
             numberOfMessages +
             ")";
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.PageTransactionInfo#deliverAfterCommit(org.hornetq.core.paging.cursor.PageCursor, org.hornetq.core.paging.cursor.PagePosition)
    */
   public synchronized boolean deliverAfterCommit(PageSubscription cursor, PagePosition cursorPos)
   {
      if (committed && useRedelivery)
      {
         cursor.addPendingDelivery(cursorPos);
         cursor.redeliver(cursorPos);
         return true;
      }
      else
      if (committed)
      {
         return false;
      }
      else
      if (rolledback)
      {
         cursor.positionIgnored(cursorPos);
         return true;
      }
      else
      {
         useRedelivery = true;
         if (lateDeliveries == null)
         {
            lateDeliveries = new LinkedList<Pair<PageSubscription, PagePosition>>();
         }
         cursor.addPendingDelivery(cursorPos);
         lateDeliveries.add(new Pair<PageSubscription, PagePosition>(cursor, cursorPos));
         return true;
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   
   
   static class UpdatePageTXOperation extends TransactionOperationAbstract
   {
      private HashMap<PageTransactionInfo, AtomicInteger> countsToUpdate = new HashMap<PageTransactionInfo, AtomicInteger>();
      
      private boolean stored = false;
      
      private final StorageManager storageManager;
      
      private final PagingManager pagingManager;
      
      public UpdatePageTXOperation(final StorageManager storageManager, final PagingManager pagingManager)
      {
         this.storageManager = storageManager;
         this.pagingManager = pagingManager;
      }
      
      public void setStored()
      {
         stored = true;
      }
      
      public void addUpdate(final PageTransactionInfo info, final int increment)
      {
         AtomicInteger counter = countsToUpdate.get(info);
         
         if (counter == null)
         {
            counter = new AtomicInteger(0);
            countsToUpdate.put(info, counter);
         }
         
         counter.addAndGet(increment);
      }
      
      public void beforePrepare(Transaction tx) throws Exception
      {
         storeUpdates(tx);
      }
      
      public void beforeCommit(Transaction tx) throws Exception
      {
         storeUpdates(tx);
      }
      
      public void afterCommit(Transaction tx)
      {
         for (Map.Entry<PageTransactionInfo, AtomicInteger> entry : countsToUpdate.entrySet())
         {
            entry.getKey().onUpdate(entry.getValue().intValue(), storageManager, pagingManager);
         }
      }
      
      private void storeUpdates(Transaction tx) throws Exception
      {
         if (!stored)
         {
            stored = true;
            for (Map.Entry<PageTransactionInfo, AtomicInteger> entry : countsToUpdate.entrySet())
            {
               storageManager.updatePageTransaction(tx.getID(), entry.getKey(), entry.getValue().get());
            }
         }
      }
      

      
   }
}
