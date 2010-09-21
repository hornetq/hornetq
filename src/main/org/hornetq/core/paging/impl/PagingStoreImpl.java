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

import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.Page;
import org.hornetq.core.paging.PageTransactionInfo;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.PagingStoreFactory;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.DuplicateIDCache;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionPropertyIndexes;
import org.hornetq.core.transaction.Transaction.State;
import org.hornetq.core.transaction.impl.TransactionImpl;

/**
 * 
 * @see PagingStore
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class PagingStoreImpl implements TestSupportPageStore
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(PagingStoreImpl.class);

   // Attributes ----------------------------------------------------

   private final SimpleString address;

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

   private final AddressFullMessagePolicy addressFullMessagePolicy;

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

   /** duplicate cache used at this address */
   private final DuplicateIDCache duplicateCache;

   /** 
    * We need to perform checks on currentPage with minimal locking
    * */
   private final ReadWriteLock currentPageLock = new ReentrantReadWriteLock();

   private volatile boolean running = false;

   protected final boolean syncNonTransactional;

   // Static --------------------------------------------------------

   private static final boolean isTrace = PagingStoreImpl.log.isTraceEnabled();

   // This is just a debug tool method.
   // During debugs you could make log.trace as log.info, and change the
   // variable isTrace above
   private static void trace(final String message)
   {
      PagingStoreImpl.log.trace(message);
   }

   // Constructors --------------------------------------------------

   public PagingStoreImpl(final SimpleString address,
                          final PagingManager pagingManager,
                          final StorageManager storageManager,
                          final PostOffice postOffice,
                          final SequentialFileFactory fileFactory,
                          final PagingStoreFactory storeFactory,
                          final SimpleString storeName,
                          final AddressSettings addressSettings,
                          final Executor executor,
                          final boolean syncNonTransactional)
   {
      if (pagingManager == null)
      {
         throw new IllegalStateException("Paging Manager can't be null");
      }

      this.address = address;

      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.storeName = storeName;

      maxSize = addressSettings.getMaxSizeBytes();

      pageSize = addressSettings.getPageSizeBytes();

      addressFullMessagePolicy = addressSettings.getAddressFullMessagePolicy();

      if (addressFullMessagePolicy == AddressFullMessagePolicy.PAGE && maxSize != -1 && pageSize >= maxSize)
      {
         throw new IllegalStateException("pageSize for address " + address +
                                         " >= maxSize. Normally pageSize should" +
                                         " be significantly smaller than maxSize, ms: " +
                                         maxSize +
                                         " ps " +
                                         pageSize);
      }

      this.executor = executor;

      this.pagingManager = pagingManager;

      this.fileFactory = fileFactory;

      this.storeFactory = storeFactory;

      this.syncNonTransactional = syncNonTransactional;

      // Post office could be null on the backup node
      if (postOffice == null)
      {
         this.duplicateCache = null;
      }
      else
      {
         this.duplicateCache = postOffice.getDuplicateIDCache(storeName);
      }

   }

   // Public --------------------------------------------------------

   // PagingStore implementation ------------------------------------

   public SimpleString getAddress()
   {
      return address;
   }

   public long getAddressSize()
   {
      return sizeInBytes.get();
   }

   public long getMaxSize()
   {
      return maxSize;
   }

   public AddressFullMessagePolicy getAddressFullMessagePolicy()
   {
      return addressFullMessagePolicy;
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
         if (addressFullMessagePolicy == AddressFullMessagePolicy.BLOCK)
         {
            return false;
         }
         else if (addressFullMessagePolicy == AddressFullMessagePolicy.DROP)
         {
            return isFull();
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

   public boolean page(final List<ServerMessage> message, final long transactionID) throws Exception
   {
      // The sync on transactions is done on commit only
      return page(message, transactionID, false);
   }

   public boolean page(final ServerMessage message) throws Exception
   {
      // If non Durable, there is no need to sync as there is no requirement for persistence for those messages in case
      // of crash
      return page(Arrays.asList(message), -1, syncNonTransactional && message.isDurable());
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

         final CountDownLatch latch = new CountDownLatch(1);

         executor.execute(new Runnable()
         {
            public void run()
            {
               latch.countDown();
            }
         });

         if (!latch.await(60, TimeUnit.SECONDS))
         {
            PagingStoreImpl.log.warn("Timed out on waiting PagingStore " + address + " to shutdown");
         }

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
                     final int fileId = PagingStoreImpl.getPageIdFromFileName(fileName);

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

   public boolean startPaging()
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
         // Already paging, nothing to be done
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
            try
            {
               openNewPage();
            }
            catch (Exception e)
            {
               // If not possible to starting page due to an IO error, we will just consider it non paging.
               // This shouldn't happen anyway
               PagingStoreImpl.log.warn("IO Error, impossible to start paging", e);
               return false;
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
         writeLock.unlock();
      }
   }

   public Page getCurrentPage()
   {
      return currentPage;
   }

   public Page createPage(final int page) throws Exception
   {
      String fileName = createFileName(page);

      if (fileFactory == null)
      {
         fileFactory = storeFactory.newFileFactory(getStoreName());
      }

      SequentialFile file = fileFactory.createSequentialFile(fileName, 1000);

      file.open();

      file.position(0);

      file.close();

      return new PageImpl(storeName, storageManager, fileFactory, file, page);
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

               if (currentPage == null)
               {
                  // sanity check... it shouldn't happen!
                  throw new IllegalStateException("CurrentPage is null");
               }

               returnPage = currentPage;
               returnPage.close();
               currentPage = null;

               // The current page is empty... which means we reached the end of the pages
               if (returnPage.getNumberOfMessages() == 0)
               {
                  returnPage.open();
                  returnPage.delete();

                  // This will trigger this address to exit the page mode,
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

   // Private -------------------------------------------------------

   /**
    * Depage one page-file, read it and send it to the pagingManager / postoffice
    * @return
    * @throws Exception
    */
   protected boolean readPage() throws Exception
   {
      Page page = depage();

      // It's important that only depage should happen while locked
      // or we would be holding a lock for a long time
      // The reading (IO part) should happen outside of any locks

      if (page == null)
      {
         return false;
      }

      page.open();

      List<PagedMessage> messages = null;

      try
      {
         messages = page.read();
      }
      finally
      {
         try
         {
            page.close();
         }
         catch (Throwable ignored)
         {
         }
      }

      if (onDepage(page.getPageId(), storeName, messages))
      {
         if (page.delete())
         {
            // DuplicateCache could be null during replication
            // however the deletes on the journal will happen through replicated journal
            if (duplicateCache != null)
            {
               duplicateCache.deleteFromCache(generateDuplicateID(page.getPageId()));
            }
         }

         return true;
      }
      else
      {
         return false;
      }

   }

   private Queue<OurRunnable> onMemoryFreedRunnables = new ConcurrentLinkedQueue<OurRunnable>();

   private class MemoryFreedRunnablesExecutor implements Runnable
   {
      public void run()
      {
         Runnable runnable;

         while ((runnable = onMemoryFreedRunnables.poll()) != null)
         {
            runnable.run();
         }
      }
   }

   private final Runnable memoryFreedRunnablesExecutor = new MemoryFreedRunnablesExecutor();

   class OurRunnable implements Runnable
   {
      boolean ran;

      final Runnable runnable;

      OurRunnable(final Runnable runnable)
      {
         this.runnable = runnable;
      }

      public synchronized void run()
      {
         if (!ran)
         {
            runnable.run();

            ran = true;
         }
      }
   }

   public void executeRunnableWhenMemoryAvailable(final Runnable runnable)
   {
      if (addressFullMessagePolicy == AddressFullMessagePolicy.BLOCK && maxSize != -1)
      {
         if (sizeInBytes.get() > maxSize)
         {
            OurRunnable ourRunnable = new OurRunnable(runnable);

            onMemoryFreedRunnables.add(ourRunnable);

            // We check again to avoid a race condition where the size can come down just after the element
            // has been added, but the check to execute was done before the element was added
            // NOTE! We do not fix this race by locking the whole thing, doing this check provides
            // MUCH better performance in a highly concurrent environment
            if (sizeInBytes.get() <= maxSize)
            {
               // run it now
               ourRunnable.run();
            }

            return;
         }
      }

      runnable.run();
   }

   public void addSize(final int size)
   {
      if (addressFullMessagePolicy == AddressFullMessagePolicy.BLOCK)
      {
         if (maxSize != -1)
         {
            long newSize = sizeInBytes.addAndGet(size);

            if (newSize <= maxSize)
            {
               if (!onMemoryFreedRunnables.isEmpty())
               {
                  executor.execute(memoryFreedRunnablesExecutor);
               }
            }
         }

         return;
      }
      else if (addressFullMessagePolicy == AddressFullMessagePolicy.PAGE)
      {
         final long addressSize = sizeInBytes.addAndGet(size);

         if (size > 0)
         {
            if (maxSize > 0 && addressSize > maxSize)
            {
               if (startPaging())
               {
                  if (PagingStoreImpl.isTrace)
                  {
                     PagingStoreImpl.trace("Starting paging on " + getStoreName() +
                                           ", size = " +
                                           addressSize +
                                           ", maxSize=" +
                                           maxSize);
                  }
               }
            }
         }
         else
         {
            if (maxSize > 0 && currentPage != null && addressSize <= maxSize - pageSize && !depaging.get())
            {
               if (startDepaging())
               {
                  if (PagingStoreImpl.isTrace)
                  {
                     PagingStoreImpl.trace("Starting depaging Thread, size = " + addressSize);
                  }
               }
            }
         }

         return;
      }
      else if (addressFullMessagePolicy == AddressFullMessagePolicy.DROP)
      {
         sizeInBytes.addAndGet(size);
      }

   }

   protected boolean page(final List<ServerMessage> messages, final long transactionID, final boolean sync) throws Exception
   {
      if (!running)
      {
         throw new IllegalStateException("PagingStore(" + getStoreName() + ") not initialized");
      }

      boolean full = isFull();

      if (addressFullMessagePolicy == AddressFullMessagePolicy.DROP)
      {
         if (full)
         {
            if (!printedDropMessagesWarning)
            {
               printedDropMessagesWarning = true;

               PagingStoreImpl.log.warn("Messages are being dropped on address " + getStoreName());
            }

            // Address is full, we just pretend we are paging, and drop the data
            return true;
         }
         else
         {
            return false;
         }
      }
      else if (addressFullMessagePolicy == AddressFullMessagePolicy.BLOCK)
      {
         return false;
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

         for (ServerMessage message : messages)
         {
            PagedMessage pagedMessage;

            if (!message.isDurable())
            {
               // The address should never be transient when paging (even for non-persistent messages when paging)
               // This will force everything to be persisted
               message.bodyChanged();
            }

            if (transactionID != -1)
            {
               pagedMessage = new PagedMessageImpl(message, transactionID);
            }
            else
            {
               pagedMessage = new PagedMessageImpl(message);
            }

            int bytesToWrite = pagedMessage.getEncodeSize() + PageImpl.SIZE_RECORD;

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
               currentPage.write(pagedMessage);
 
               if (sync)
               {
                  currentPage.sync();
               }
            }
            finally
            {
               currentPageLock.readLock().unlock();
            }
         }

         return true;
      }
      finally
      {
         writeLock.unlock();
      }

   }

   /**
    * This method will remove files from the page system and and route them, doing it transactionally
    *     
    * If persistent messages are also used, it will update eventual PageTransactions
    */

   private boolean onDepage(final int pageId, final SimpleString address, final List<PagedMessage> pagedMessages) throws Exception
   {
      if (PagingStoreImpl.isTrace)
      {
         PagingStoreImpl.trace("Depaging....");
      }

      if (pagedMessages.size() == 0)
      {
         // nothing to be done on this case.
         return true;
      }

      // Depage has to be done atomically, in case of failure it should be
      // back to where it was

      byte[] duplicateIdForPage = generateDuplicateID(pageId);

      Transaction depageTransaction = new TransactionImpl(storageManager);

      // DuplicateCache could be null during replication
      if (duplicateCache != null)
      {
         if (duplicateCache.contains(duplicateIdForPage))
         {
            log.warn("Page " + pageId +
                     " had been processed already but the file wasn't removed as a crash happened. Ignoring this page");
            return true;
         }

         duplicateCache.addToCache(duplicateIdForPage, depageTransaction);
      }

      depageTransaction.putProperty(TransactionPropertyIndexes.IS_DEPAGE, Boolean.valueOf(true));

      HashMap<PageTransactionInfo, AtomicInteger> pageTransactionsToUpdate = new HashMap<PageTransactionInfo, AtomicInteger>();

      for (PagedMessage pagedMessage : pagedMessages)
      {
         ServerMessage message = pagedMessage.getMessage(storageManager);

         if (message.isLargeMessage())
         {
            LargeServerMessage largeMsg = (LargeServerMessage)message;
            if (!largeMsg.isFileExists())
            {
               PagingStoreImpl.log.warn("File for large message " + largeMsg.getMessageID() +
                                        " doesn't exist, so ignoring depage for this large message");
               continue;
            }
         }

         final long transactionIdDuringPaging = pagedMessage.getTransactionID();

         PageTransactionInfo pageUserTransaction = null;
         AtomicInteger countPageTX = null;

         if (transactionIdDuringPaging >= 0)
         {
            pageUserTransaction = pagingManager.getTransaction(transactionIdDuringPaging);

            if (pageUserTransaction == null)
            {
               // This is not supposed to happen
               PagingStoreImpl.log.warn("Transaction " + pagedMessage.getTransactionID() +
                                        " used during paging not found");
               continue;
            }
            else
            {
               countPageTX = pageTransactionsToUpdate.get(pageUserTransaction);
               if (countPageTX == null)
               {
                  countPageTX = new AtomicInteger();
                  pageTransactionsToUpdate.put(pageUserTransaction, countPageTX);
               }

               // This is to avoid a race condition where messages are depaged
               // before the commit arrived

               while (running && !pageUserTransaction.waitCompletion(500))
               {
                  // This is just to give us a chance to interrupt the process..
                  // if we start a shutdown in the middle of transactions, the commit/rollback may never come, delaying
                  // the shutdown of the server
                  if (PagingStoreImpl.isTrace)
                  {
                     PagingStoreImpl.trace("Waiting pageTransaction to complete");
                  }
               }

               if (!running)
               {
                  break;
               }

               if (!pageUserTransaction.isCommit())
               {
                  if (PagingStoreImpl.isTrace)
                  {
                     PagingStoreImpl.trace("Rollback was called after prepare, ignoring message " + message);
                  }
                  continue;
               }
            }

         }

         postOffice.route(message, depageTransaction, false);

         // This means the page is duplicated. So we need to ignore this
         if (depageTransaction.getState() == State.ROLLBACK_ONLY)
         {
            break;
         }

         // Update information about transactions
         // This needs to be done after routing because of duplication detection
         if (pageUserTransaction != null && message.isDurable())
         {
            countPageTX.incrementAndGet();
         }
      }

      if (!running)
      {
         depageTransaction.rollback();
         return false;
      }

      for (Map.Entry<PageTransactionInfo, AtomicInteger> entry : pageTransactionsToUpdate.entrySet())
      {
         // This will set the journal transaction to commit;
         depageTransaction.setContainsPersistent();

         entry.getKey().storeUpdate(storageManager, this.pagingManager, depageTransaction, entry.getValue().intValue());
      }

      depageTransaction.commit();

      storageManager.waitOnOperations();

      if (PagingStoreImpl.isTrace)
      {
         PagingStoreImpl.trace("Depage committed, running = " + running);
      }

      return true;
   }

   /**
    * @param pageId
    * @return
    */
   private byte[] generateDuplicateID(final int pageId)
   {
      byte duplicateIdForPage[] = new SimpleString("page-" + pageId).getData();
      return duplicateIdForPage;
   }

   /**
    * @return
    */
   private boolean isAddressFull(final long nextPageSize)
   {
      return maxSize > 0 && getAddressSize() + nextPageSize > maxSize;
   }

   /**
    * startDepaging and clearDepage needs to be atomic.
    * We can't use writeLock to this operation as writeLock would still be used by another thread, and still being a valid usage
    * @return true if the depage status was cleared
    */
   private synchronized boolean clearDepage()
   {
      final boolean addressFull = isAddressFull(getPageSizeBytes());

      if (PagingStoreImpl.isTrace)
      {
         PagingStoreImpl.trace("Clear Depage on Address = " + getStoreName() +
                               " addressSize = " +
                               getAddressSize() +
                               " addressMax " +
                               maxSize +
                               " isPaging = " +
                               isPaging() +
                               " addressFull = " +
                               addressFull);
      }

      // It should stop the executor when the address is full or when there is nothing else to be depaged
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
   private boolean isFull()
   {
      return maxSize > 0 && getAddressSize() > maxSize;
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
            PagingStoreImpl.log.error(e, e);
         }
      }
   }
}
