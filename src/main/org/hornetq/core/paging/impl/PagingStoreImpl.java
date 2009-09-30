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

import java.nio.ByteBuffer;
import java.text.DecimalFormat;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.paging.Page;
import org.hornetq.core.paging.PageTransactionInfo;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.PagingStoreFactory;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionPropertyIndexes;
import org.hornetq.core.transaction.impl.TransactionImpl;
import org.hornetq.utils.SimpleString;

/**
 * 
 * @see PagingStore
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class PagingStoreImpl implements TestSupportPageStore
{
   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(PagingStoreImpl.class);

   // Attributes ----------------------------------------------------

   private final StorageManager storageManager;

   private final PostOffice postOffice;

   private final DecimalFormat format = new DecimalFormat("000000000");

   private final AtomicInteger currentPageSize = new AtomicInteger(0);

   private final SimpleString storeName;

   // The FileFactory is created lazily as soon as the first write is attempted
   private volatile SequentialFileFactory fileFactory;

   private final PagingStoreFactory storeFactory;

   private final long maxSize;

   private final long pageSize;

   private final boolean dropMessagesWhenFull;

   private boolean printedDropMessagesWarning;

   private final PagingManager pagingManager;

   private final Executor executor;

   // Bytes consumed by the queue on the memory
   private final AtomicLong sizeInBytes = new AtomicLong();

   private final AtomicBoolean depaging = new AtomicBoolean(false);

   private volatile int numberOfPages;

   private volatile int firstPageId;

   private volatile int currentPageId;

   private volatile Page currentPage;

   private final ReentrantLock writeLock = new ReentrantLock();

   /** 
    * We need to perform checks on currentPage with minimal locking
    * */
   private final ReadWriteLock currentPageLock = new ReentrantReadWriteLock();

   private volatile boolean running = false;

   // Static --------------------------------------------------------

   private static final boolean isTrace = log.isTraceEnabled();

   // This is just a debug tool method.
   // During debugs you could make log.trace as log.info, and change the
   // variable isTrace above
   private static void trace(final String message)
   {
      log.trace(message);
   }

   // Constructors --------------------------------------------------

   public PagingStoreImpl(final PagingManager pagingManager,
                          final StorageManager storageManager,
                          final PostOffice postOffice,
                          final SequentialFileFactory fileFactory,
                          final PagingStoreFactory storeFactory,
                          final SimpleString storeName,
                          final AddressSettings addressSettings,
                          final Executor executor)
   {
      if (pagingManager == null)
      {
         throw new IllegalStateException("Paging Manager can't be null");
      }

      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.storeName = storeName;

      maxSize = addressSettings.getMaxSizeBytes();

      pageSize = addressSettings.getPageSizeBytes();

      dropMessagesWhenFull = addressSettings.isDropMessagesWhenFull();

      this.executor = executor;

      this.pagingManager = pagingManager;

      this.fileFactory = fileFactory;

      this.storeFactory = storeFactory;
   }

   // Public --------------------------------------------------------

   // PagingStore implementation ------------------------------------

   public long getAddressSize()
   {
      return sizeInBytes.get();
   }

   /** Maximum number of bytes allowed in memory */
   public long getMaxSizeBytes()
   {
      return maxSize;
   }

   public boolean isDropWhenMaxSize()
   {
      return dropMessagesWhenFull;
   }

   public long getPageSizeBytes()
   {
      return pageSize;
   }

   public boolean isPaging()
   {
      currentPageLock.readLock().lock();
      try
      {
         if (isDropWhenMaxSize())
         {
            return isDrop();
         }
         else
         {
            return currentPage != null;
         }
      }
      finally
      {
         currentPageLock.readLock().unlock();
      }
   }

   public int getNumberOfPages()
   {
      return numberOfPages;
   }

   public SimpleString getStoreName()
   {
      return storeName;
   }

   public void addSize(final long size) throws Exception
   {
      if (isDropWhenMaxSize())
      {
         addAddressSize(size);
         pagingManager.addSize(size);

         return;
      }
      else
      {
         pagingManager.addSize(size);

         final long addressSize = addAddressSize(size);

         if (size > 0)
         {
            if (maxSize > 0 && addressSize > maxSize)
            {
               if (startPaging())
               {
                  if (isTrace)
                  {
                     trace("Starting paging on " + getStoreName() + ", size = " + addressSize + ", maxSize=" + maxSize);
                  }
               }
            }
         }
         else
         {
            // When in Global mode, we use the default page size as the mark to start depage
            if (maxSize > 0 && currentPage != null && addressSize <= maxSize - pageSize && !depaging.get())
            {
               if (startDepaging())
               {
                  if (isTrace)
                  {
                     trace("Starting depaging Thread, size = " + addressSize);
                  }
               }
            }
         }

         return;
      }
   }

   // TODO all of this can be simplified
   public boolean page(final PagedMessage message, final boolean sync, final boolean duplicateDetection) throws Exception
   {
      if (!running)
      {
         throw new IllegalStateException("PagingStore(" + getStoreName() + ") not initialized");
      }

      if (dropMessagesWhenFull)
      {
         if (isDrop())
         {
            if (!printedDropMessagesWarning)
            {
               printedDropMessagesWarning = true;

               log.warn("Messages are being dropped on address " + getStoreName());
            }

            // Address is full, we just pretend we are paging, and drop the data
            return true;
         }
         else
         {
            return false;
         }
      }

      // We need to ensure a read lock, as depage could change the paging state
      currentPageLock.readLock().lock();

      try
      {
         // First check done concurrently, to avoid synchronization and increase throughput
         if (currentPage == null)
         {
            return false;
         }
      }
      finally
      {
         currentPageLock.readLock().unlock();
      }

      writeLock.lock();

      try
      {
         if (currentPage == null)
         {
            return false;
         }

         if (duplicateDetection)
         {
            // We set the duplicate detection header to prevent the message being depaged more than once in case of
            // failure during depage

            ServerMessage msg = message.getMessage(storageManager);

            byte[] bytes = new byte[8];

            ByteBuffer buff = ByteBuffer.wrap(bytes);

            buff.putLong(msg.getMessageID());

            msg.putBytesProperty(MessageImpl.HDR_DUPLICATE_DETECTION_ID, bytes);
         }

         int bytesToWrite = message.getEncodeSize() + PageImpl.SIZE_RECORD;

         if (currentPageSize.addAndGet(bytesToWrite) > pageSize && currentPage.getNumberOfMessages() > 0)
         {
            // Make sure nothing is currently validating or using currentPage
            currentPageLock.writeLock().lock();
            try
            {
               openNewPage();

               // openNewPage will set currentPageSize to zero, we need to set it again
               currentPageSize.addAndGet(bytesToWrite);
            }
            finally
            {
               currentPageLock.writeLock().unlock();
            }
         }

         currentPageLock.readLock().lock();

         try
         {
            if (currentPage != null)
            {
               currentPage.write(message);

               if (sync)
               {
                  currentPage.sync();
               }
               return true;
            }
            else
            {
               return false;
            }
         }
         finally
         {
            currentPageLock.readLock().unlock();
         }
      }
      finally
      {
         writeLock.unlock();
      }

   }

   public void sync() throws Exception
   {
      currentPageLock.readLock().lock();

      try
      {
         if (currentPage != null)
         {
            currentPage.sync();
         }
      }
      finally
      {
         currentPageLock.readLock().unlock();
      }
   }

   public boolean startDepaging()
   {
      if (!running)
      {
         return false;
      }

      currentPageLock.readLock().lock();
      try
      {
         if (currentPage == null)
         {
            return false;
         }
         else
         {
            // startDepaging and clearDepage needs to be atomic.
            // We can't use writeLock to this operation as writeLock would still be used by another thread, and still
            // being a valid usage
            synchronized (this)
            {
               if (!depaging.get())
               {
                  depaging.set(true);
                  Runnable depageAction = new DepageRunnable(executor);
                  executor.execute(depageAction);
                  return true;
               }
               else
               {
                  return false;
               }
            }
         }
      }
      finally
      {
         currentPageLock.readLock().unlock();
      }
   }

   // HornetQComponent implementation

   public synchronized boolean isStarted()
   {
      return running;
   }

   public synchronized void stop() throws Exception
   {
      if (running)
      {
         running = false;

         if (currentPage != null)
         {
            currentPage.close();
            currentPage = null;
         }
      }
   }

   public void start() throws Exception
   {
      writeLock.lock();

      try
      {

         if (running)
         {
            // don't throw an exception.
            // You could have two threads adding PagingStore to a
            // ConcurrentHashMap,
            // and having both threads calling init. One of the calls should just
            // need to be ignored
            return;
         }
         else
         {
            currentPageLock.writeLock().lock();

            try
            {
               running = true;
               firstPageId = Integer.MAX_VALUE;

               // There are no files yet on this Storage. We will just return it empty
               if (fileFactory != null)
               {

                  currentPageId = 0;
                  currentPage = null;

                  List<String> files = fileFactory.listFiles("page");

                  numberOfPages = files.size();

                  for (String fileName : files)
                  {
                     final int fileId = getPageIdFromFileName(fileName);

                     if (fileId > currentPageId)
                     {
                        currentPageId = fileId;
                     }

                     if (fileId < firstPageId)
                     {
                        firstPageId = fileId;
                     }
                  }

                  if (numberOfPages != 0)
                  {
                     startPaging();
                  }
               }
            }
            finally
            {
               currentPageLock.writeLock().unlock();
            }
         }

      }
      finally
      {
         writeLock.unlock();
      }
   }

   public boolean startPaging() throws Exception
   {
      if (!running)
      {
         return false;
      }

      // First check without any global locks.
      // (Faster)
      currentPageLock.readLock().lock();
      try
      {
         if (currentPage != null)
         {
            return false;
         }
      }
      finally
      {
         currentPageLock.readLock().unlock();
      }

      // if the first check failed, we do it again under a global currentPageLock
      // (writeLock) this time
      writeLock.lock();

      try
      {
         if (currentPage == null)
         {
            openNewPage();

            return true;
         }
         else
         {
            return false;
         }
      }
      finally
      {
         writeLock.unlock();
      }
   }

   /**
    * Depage one page-file, read it and send it to the pagingManager / postoffice
    * @return
    * @throws Exception
    */
   public boolean readPage() throws Exception
   {
      Page page = depage();

      if (page == null)
      {
         return false;
      }

      page.open();

      List<PagedMessage> messages = page.read();

      if (onDepage(page.getPageId(), storeName, messages))
      {
         page.delete();
         return true;
      }
      else
      {
         return false;
      }

   }

   public Page getCurrentPage()
   {
      return currentPage;
   }

   // TestSupportPageStore ------------------------------------------

   public void forceAnotherPage() throws Exception
   {
      openNewPage();
   }

   /** 
    *  It returns a Page out of the Page System without reading it. 
    *  The method calling this method will remove the page and will start reading it outside of any locks.
    *  This method could also replace the current file by a new file, and that process is done through acquiring a writeLock on currentPageLock
    *   
    *  Observation: This method is used internally as part of the regular depage process, but externally is used only on tests, 
    *               and that's why this method is part of the Testable Interface 
    * */
   public Page depage() throws Exception
   {
      writeLock.lock();

      currentPageLock.writeLock().lock(); // Make sure no checks are done on currentPage while we are depaging

      try
      {
         if (!running)
         {
            return null;
         }

         if (numberOfPages == 0)
         {
            return null;
         }
         else
         {
            numberOfPages--;

            final Page returnPage;

            // We are out of old pages, all that is left now is the current page.
            // On that case we need to replace it by a new empty page, and return the current page immediately
            if (currentPageId == firstPageId)
            {
               firstPageId = Integer.MAX_VALUE;

               if (currentPage != null)
               {
                  returnPage = currentPage;
                  returnPage.close();
                  currentPage = null;
               }
               else
               {
                  // sanity check... it shouldn't happen!
                  throw new IllegalStateException("CurrentPage is null");
               }

               // The current page is empty... which means we reached the end of the pages
               if (returnPage.getNumberOfMessages() == 0)
               {
                  returnPage.open();
                  returnPage.delete();

                  // This will trigger this Destination to exit the page mode,
                  // and this will make HornetQ start using the journal again
                  return null;
               }
               else
               {
                  // We need to create a new page, as we can't lock the address until we finish depaging.
                  openNewPage();
               }

               return returnPage;
            }
            else
            {
               returnPage = createPage(firstPageId++);
            }
            return returnPage;
         }
      }
      finally
      {
         currentPageLock.writeLock().unlock();
         writeLock.unlock();
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // In order to test failures, we need to be able to extend this class
   // and replace the Page for another Page that will fail before the file is removed
   // That's why createPage is not a private method
   protected Page createPage(final int page) throws Exception
   {
      String fileName = createFileName(page);

      if (fileFactory == null)
      {
         fileFactory = storeFactory.newFileFactory(getStoreName());
      }

      SequentialFile file = fileFactory.createSequentialFile(fileName, 1000);

      file.open();

      long size = file.size();

      if (fileFactory.isSupportsCallbacks() && size < pageSize)
      {
         file.fill((int)size, (int)(pageSize - size), (byte)0);
      }

      file.position(0);

      file.close();

      return new PageImpl(fileFactory, file, page);
   }

   // Private -------------------------------------------------------

   /**
    * This method will remove files from the page system and and route them, doing it transactionally
    *     
    * If persistent messages are also used, it will update eventual PageTransactions
    */

   private boolean onDepage(final int pageId, final SimpleString destination, final List<PagedMessage> pagedMessages) throws Exception
   {
      if (isTrace)
      {
         trace("Depaging....");
      }

      if (pagedMessages.size() == 0)
      {
         // nothing to be done on this case.
         return true;
      }

      // Depage has to be done atomically, in case of failure it should be
      // back to where it was

      Transaction depageTransaction = new TransactionImpl(storageManager);

      depageTransaction.putProperty(TransactionPropertyIndexes.IS_DEPAGE, Boolean.valueOf(true));

      HashSet<PageTransactionInfo> pageTransactionsToUpdate = new HashSet<PageTransactionInfo>();

      for (PagedMessage pagedMessage : pagedMessages)
      {
         ServerMessage message = null;

         message = pagedMessage.getMessage(storageManager);
         
         if (message.isLargeMessage())
         {
            LargeServerMessage largeMsg = (LargeServerMessage)message;
            if (!largeMsg.isFileExists())
            {
               log.warn("File for large message " + largeMsg.getMessageID() + " doesn't exist, so ignoring depage for this large message");
               continue;
            }
         }

         final long transactionIdDuringPaging = pagedMessage.getTransactionID();

         if (transactionIdDuringPaging >= 0)
         {
            final PageTransactionInfo pageTransactionInfo = pagingManager.getTransaction(transactionIdDuringPaging);

            if (pageTransactionInfo == null)
            {
               log.warn("Transaction " + pagedMessage.getTransactionID() +
                        " used during paging not found, ignoring message " +
                        message);

               continue;
            }

            // This is to avoid a race condition where messages are depaged
            // before the commit arrived

            while (running && !pageTransactionInfo.waitCompletion(500))
            {
               // This is just to give us a chance to interrupt the process..
               // if we start a shutdown in the middle of transactions, the commit/rollback may never come, delaying
               // the shutdown of the server
               if (isTrace)
               {
                  trace("Waiting pageTransaction to complete");
               }
            }

            if (!running)
            {
               break;
            }

            if (!pageTransactionInfo.isCommit())
            {
               if (isTrace)
               {
                  trace("Rollback was called after prepare, ignoring message " + message);
               }
               continue;
            }

            // Update information about transactions
            if (message.isDurable())
            {
               pageTransactionInfo.decrement();
               pageTransactionsToUpdate.add(pageTransactionInfo);
            }
         }

         postOffice.route(message, depageTransaction);
      }

      if (!running)
      {
         depageTransaction.rollback();
         return false;
      }

      for (PageTransactionInfo pageWithTransaction : pageTransactionsToUpdate)
      {
         // This will set the journal transaction to commit;
         depageTransaction.putProperty(TransactionPropertyIndexes.CONTAINS_PERSISTENT, true);

         if (pageWithTransaction.getNumberOfMessages() == 0)
         {
            // numberOfReads==numberOfWrites -> We delete the record
            storageManager.deletePageTransactional(depageTransaction.getID(), pageWithTransaction.getRecordID());
            pagingManager.removeTransaction(pageWithTransaction.getTransactionID());
         }
         else
         {
            storageManager.storePageTransaction(depageTransaction.getID(), pageWithTransaction);
         }
      }

      depageTransaction.commit();

      if (isTrace)
      {
         trace("Depage committed, running = " + running);
      }

      return true;
   }

   /**
    * @return
    */
   private boolean isAddressFull(final long nextPageSize)
   {
      return getMaxSizeBytes() > 0 && getAddressSize() + nextPageSize > getMaxSizeBytes();
   }

   private long addAddressSize(final long delta)
   {
      return sizeInBytes.addAndGet(delta);
   }

   /**
    * startDepaging and clearDepage needs to be atomic.
    * We can't use writeLock to this operation as writeLock would still be used by another thread, and still being a valid usage
    * @return true if the depage status was cleared
    */
   private synchronized boolean clearDepage()
   {
      final boolean addressFull = isAddressFull(getPageSizeBytes());

      if (isTrace)
      {
         trace("Clear Depage on Address = " + this.getStoreName() +
               " PagingManager size " +
               pagingManager.getTotalMemory() +
               " addressSize = " +
               this.getAddressSize() +
               " addressMax " +
               this.getMaxSizeBytes() +
               " isPaging = " +
               isPaging() +
               " addressFull = " +
               addressFull);
      }

      // It should stop the executor when the destination is full or when there is nothing else to be depaged
      if (addressFull || !isPaging())
      {
         depaging.set(false);
         return true;
      }
      else
      {
         return false;
      }
   }

   private void openNewPage() throws Exception
   {
      currentPageLock.writeLock().lock();

      try
      {
         numberOfPages++;

         currentPageId++;

         if (currentPageId < firstPageId)
         {
            firstPageId = currentPageId;
         }

         if (currentPage != null)
         {
            currentPage.close();
         }

         currentPage = createPage(currentPageId);

         currentPageSize.set(0);

         currentPage.open();
      }
      finally
      {
         currentPageLock.writeLock().unlock();
      }
   }

   /**
    * 
    * Note: Decimalformat is not thread safe, Use synchronization before calling this method
    * 
    * @param pageID
    * @return
    */
   private String createFileName(final int pageID)
   {
      return format.format(pageID) + ".page";
   }

   private static int getPageIdFromFileName(final String fileName)
   {
      return Integer.parseInt(fileName.substring(0, fileName.indexOf('.')));
   }

   // To be used on isDropMessagesWhenFull
   private boolean isDrop()
   {
      return getMaxSizeBytes() > 0 && getAddressSize() > getMaxSizeBytes();
   }

   // Inner classes -------------------------------------------------

   private class DepageRunnable implements Runnable
   {
      private final Executor followingExecutor;

      public DepageRunnable(final Executor followingExecutor)
      {
         this.followingExecutor = followingExecutor;
      }

      public void run()
      {
         try
         {
            if (running)
            {
               if (!isAddressFull(getPageSizeBytes()))
               {
                  readPage();
               }

               // Note: clearDepage is an atomic operation, it needs to be done even if readPage was not executed
               // however clearDepage shouldn't be executed if the page-store is being stopped, as stop will be holding
               // the lock and this would dead lock
               if (running && !clearDepage())
               {
                  followingExecutor.execute(this);
               }
            }
         }
         catch (Throwable e)
         {
            log.error(e, e);
         }
      }
   }
}
