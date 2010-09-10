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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.PageTransactionInfo;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionOperation;
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

   private volatile CountDownLatch countDownCompleted;

   private volatile boolean committed;

   private volatile boolean rolledback;

   private AtomicInteger numberOfMessages = new AtomicInteger(0);

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PageTransactionInfoImpl(final long transactionID)
   {
      this.transactionID = transactionID;
      countDownCompleted = new CountDownLatch(1);
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

   public void update(final int update, final StorageManager storageManager)
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
      }
   }

   public void increment()
   {
      numberOfMessages.incrementAndGet();
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
      countDownCompleted = null;
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

   public void commit()
   {
      committed = true;
      /** 
       * this is to avoid a race condition where the transaction still being committed while another thread is depaging messages
       */
      countDownCompleted.countDown();
   }

   public boolean waitCompletion(final int timeoutMilliseconds) throws InterruptedException
   {
      if (countDownCompleted == null)
      {
         return true;
      }
      else
      {
         return countDownCompleted.await(timeoutMilliseconds, TimeUnit.MILLISECONDS);
      }
   }

   public void store(final StorageManager storageManager, final Transaction tx) throws Exception
   {
      storageManager.storePageTransaction(tx.getID(), this);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.PageTransactionInfo#storeUpdate(org.hornetq.core.persistence.StorageManager, org.hornetq.core.transaction.Transaction, int)
    */
   public void storeUpdate(final StorageManager storageManager, final Transaction tx, final int depages) throws Exception
   {
      storageManager.updatePageTransaction(tx.getID(), this, depages);
      
      final PageTransactionInfo pgToUpdate = this;
      
      tx.addOperation(new TransactionOperation()
      {
         public void beforeRollback(Transaction tx) throws Exception
         {
         }
         
         public void beforePrepare(Transaction tx) throws Exception
         {
         }
         
         public void beforeCommit(Transaction tx) throws Exception
         {
         }
         
         public void afterRollback(Transaction tx)
         {
         }
         
         public void afterPrepare(Transaction tx)
         {
         }
         
         public void afterCommit(Transaction tx)
         {
            pgToUpdate.update(depages, storageManager);
         }
      });
   }

   public boolean isCommit()
   {
      return committed;
   }

   public boolean isRollback()
   {
      return rolledback;
   }

   public void rollback()
   {
      rolledback = true;
      committed = false;
      countDownCompleted.countDown();
   }

   public void markIncomplete()
   {
      committed = false;
      rolledback = false;

      countDownCompleted = new CountDownLatch(1);
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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
