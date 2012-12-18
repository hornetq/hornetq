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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
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
import org.hornetq.core.paging.cursor.LivePageCache;
import org.hornetq.core.paging.cursor.PageCursorProvider;
import org.hornetq.core.paging.cursor.impl.LivePageCacheImpl;
import org.hornetq.core.paging.cursor.impl.PageCursorProviderImpl;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.RouteContextList;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionOperation;
import org.hornetq.core.transaction.TransactionPropertyIndexes;
import org.hornetq.utils.Future;

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

   private final DecimalFormat format = new DecimalFormat("000000000");

   private final AtomicInteger currentPageSize = new AtomicInteger(0);

   private final SimpleString storeName;

   // The FileFactory is created lazily as soon as the first write is attempted
   private volatile SequentialFileFactory fileFactory;

   private final PagingStoreFactory storeFactory;

   // Used to schedule sync threads
   private final PageSyncTimer syncTimer;

   private long maxSize;

   private long pageSize;

   private AddressFullMessagePolicy addressFullMessagePolicy;

   private boolean printedDropMessagesWarning;

   private final PagingManager pagingManager;

   private final Executor executor;

   // Bytes consumed by the queue on the memory
   private final AtomicLong sizeInBytes = new AtomicLong();

   private volatile int numberOfPages;

   private volatile int firstPageId;

   private volatile int currentPageId;

   private volatile Page currentPage;

   private volatile boolean paging = false;

   private final PageCursorProvider cursorProvider;

   private final ReadWriteLock lock = new ReentrantReadWriteLock();

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
                          final ScheduledExecutorService scheduledExecutor,
                          final long syncTimeout,
                          final PagingManager pagingManager,
                          final StorageManager storageManager,
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

      this.storeName = storeName;

      applySetting(addressSettings);

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

      if (scheduledExecutor != null)
      {
         this.syncTimer = new PageSyncTimer(this, scheduledExecutor, syncTimeout);
      }
      else
      {
         this.syncTimer = null;
      }

      this.cursorProvider = new PageCursorProviderImpl(this, this.storageManager, executor, addressSettings.getPageCacheMaxSize());

   }

   /**
    * @param addressSettings
    */
   public void applySetting(final AddressSettings addressSettings)
   {
      maxSize = addressSettings.getMaxSizeBytes();

      pageSize = addressSettings.getPageSizeBytes();

      addressFullMessagePolicy = addressSettings.getAddressFullMessagePolicy();

      if (cursorProvider != null)
      {
         cursorProvider.setCacheMaxSize(addressSettings.getPageCacheMaxSize());
      }
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "PagingStoreImpl(" + this.address + ")";
   }

   // PagingStore implementation ------------------------------------

   public void lock()
   {
      lock.writeLock().lock();
   }

   public void unlock()
   {
      lock.writeLock().unlock();
   }

   public PageCursorProvider getCursorProvier()
   {
      return cursorProvider;
   }

   public long getFirstPage()
   {
      return firstPageId;
   }

   public long getTopPage()
   {
      return currentPageId;
   }

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

   public String getFolder()
   {
      SequentialFileFactory factoryUsed = this.fileFactory;
      if (factoryUsed != null)
      {
         return factoryUsed.getDirectory();
      }
      else
      {
         return null;
      }
   }

   public boolean isPaging()
   {
      lock.readLock().lock();

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
            return paging;
         }
      }
      finally
      {
         lock.readLock().unlock();
      }
   }

   public int getNumberOfPages()
   {
      return numberOfPages;
   }

   public int getCurrentWritingPage()
   {
      return currentPageId;
   }

   public SimpleString getStoreName()
   {
      return storeName;
   }

   public boolean page(final ServerMessage message, final RoutingContext ctx) throws Exception
   {
      return page(message, ctx, ctx.getContextListing(storeName));
   }

   public boolean page(final ServerMessage message, final RoutingContext ctx, RouteContextList listCtx) throws Exception
   {
      return page(message, ctx, listCtx, syncNonTransactional && ctx.getTransaction() == null);
   }

   public void sync() throws Exception
   {
      if (syncTimer != null)
      {
         syncTimer.addSync(storageManager.getContext());
      }
      else
      {
         ioSync();
      }

   }

   public void ioSync() throws Exception
   {
      lock.readLock().lock();

      try
      {
         if (currentPage != null)
         {
            currentPage.sync();
         }
      }
      finally
      {
         lock.readLock().unlock();
      }
   }

   public void processReload() throws Exception
   {
      cursorProvider.processReload();
   }

   public PagingManager getPagingManager()
   {
      return pagingManager;
   }

   // HornetQComponent implementation

   public boolean isStarted()
   {
      return running;
   }

   public synchronized void stop() throws Exception
   {
      if (running)
      {

         cursorProvider.stop();

         running = false;

         flushExecutors();

         if (currentPage != null)
         {
            currentPage.close();
            currentPage = null;
         }
      }
   }

   public void flushExecutors()
   {
      cursorProvider.flushExecutors();

      Future future = new Future();

      executor.execute(future);

      if (!future.await(60000))
      {
         PagingStoreImpl.log.warn("Timed out on waiting PagingStore " + address + " to shutdown");
      }
   }

   public void start() throws Exception
   {
      lock.writeLock().lock();

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
            running = true;
            firstPageId = Integer.MAX_VALUE;

            // There are no files yet on this Storage. We will just return it empty
            if (fileFactory != null)
            {

               currentPageId = 0;
               if (currentPage != null)
               {
                  currentPage.close();
               }
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
               
               if (currentPageId != 0)
               {
                  currentPage = createPage(currentPageId);
                  currentPage.open();

                  List<PagedMessage> messages = currentPage.read(storageManager);

                  LivePageCache pageCache = new LivePageCacheImpl(currentPage);

                  for (PagedMessage msg : messages)
                  {
                     pageCache.addLiveMessage(msg);
                     if (msg.getMessage().isLargeMessage())
                     {
                        // We have to do this since addLIveMessage will increment an extra one
                        ((LargeServerMessage)msg.getMessage()).decrementDelayDeletionCount();
                     }
                  }

                  currentPage.setLiveCache(pageCache);

                  currentPageSize.set(currentPage.getSize());

                  cursorProvider.addPageCache(pageCache);
               }

               // We will not mark it for paging if there's only a single empty file
               if (currentPage != null && !(numberOfPages == 1 && currentPage.getSize() == 0))
               {
                  startPaging();
               }
            }
         }

      }
      finally
      {
         lock.writeLock().unlock();
      }
   }

   public void stopPaging()
   {
      lock.writeLock().lock();
      try
      {
         paging = false;
         this.cursorProvider.onPageModeCleared();
      }
      finally
      {
         lock.writeLock().unlock();
      }
   }

   public boolean startPaging()
   {
      if (!running)
      {
         return false;
      }

      lock.readLock().lock();
      try
      {
         if (paging)
         {
            return false;
         }
      }
      finally
      {
         lock.readLock().unlock();
      }

      // if the first check failed, we do it again under a global currentPageLock
      // (writeLock) this time
      lock.writeLock().lock();

      try
      {
         if (paging)
         {
            return false;
         }

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
         }

         paging = true;

         return true;
      }
      finally
      {
         lock.writeLock().unlock();
      }
   }

   public Page getCurrentPage()
   {
      return currentPage;
   }

   public boolean checkPage(final int pageNumber)
   {
      String fileName = createFileName(pageNumber);
      SequentialFile file = fileFactory.createSequentialFile(fileName, 1);
      return file.exists();
   }

   public Page createPage(final int pageNumber) throws Exception
   {
      String fileName = createFileName(pageNumber);

      if (fileFactory == null)
      {
         fileFactory = storeFactory.newFileFactory(getStoreName());
      }

      SequentialFile file = fileFactory.createSequentialFile(fileName, 1000);

      Page page = new PageImpl(storeName, storageManager, fileFactory, file, pageNumber);

      // To create the file
      file.open();

      file.position(0);

      file.close();

      return page;
   }

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
      lock.writeLock().lock(); // Make sure no checks are done on currentPage while we are depaging
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
                  stopPaging();
                  returnPage.open();
                  returnPage.delete(null);

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
         lock.writeLock().unlock();
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

         return;
      }
      else if (addressFullMessagePolicy == AddressFullMessagePolicy.DROP)
      {
         sizeInBytes.addAndGet(size);
      }

   }

   protected boolean page(ServerMessage message, final RoutingContext ctx, RouteContextList listCtx, final boolean sync) throws Exception
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

            if (log.isDebugEnabled())
            {
               log.debug("Message " + message + " beig dropped for fullAddressPolicy==DROP");
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
      lock.readLock().lock();

      try
      {
         // First check done concurrently, to avoid synchronization and increase throughput
         if (!paging)
         {
            return false;
         }
      }
      finally
      {
         lock.readLock().unlock();
      }

      Transaction tx = ctx.getTransaction();

      lock.writeLock().lock();

      try
      {
         if (!paging)
         {
            return false;
         }

         if (!message.isDurable())
         {
            // The address should never be transient when paging (even for non-persistent messages when paging)
            // This will force everything to be persisted
            message.forceAddress(address);
         }


         PagedMessage pagedMessage = new PagedMessageImpl(message, routeQueues(tx, listCtx), tx == null ? -1 : tx.getID());

         if (message.isLargeMessage())
         {
            ((LargeServerMessage)message).setPaged();
         }

         int bytesToWrite = pagedMessage.getEncodeSize() + PageImpl.SIZE_RECORD;

         if (currentPageSize.addAndGet(bytesToWrite) > pageSize && currentPage.getNumberOfMessages() > 0)
         {
            // Make sure nothing is currently validating or using currentPage
            openNewPage();
            currentPageSize.addAndGet(bytesToWrite);
         }

         currentPage.write(pagedMessage);

         if (isTrace)
         {
            log.trace("Paging message " + pagedMessage + " on pageStore " + this.getStoreName() +
                      " pageId=" + currentPage.getPageId());
         }


         if (tx != null)
         {
            installPageTransaction(tx, listCtx);
            tx.setWaitBeforeCommit(true);
         }
         else
         if (sync && tx == null)
         {
            sync();
         }

         return true;
      }
      finally
      {
         lock.writeLock().unlock();
      }

   }

   private long[] routeQueues(Transaction tx, RouteContextList ctx) throws Exception
   {
      List<org.hornetq.core.server.Queue> durableQueues = ctx.getDurableQueues();
      List<org.hornetq.core.server.Queue> nonDurableQueues = ctx.getNonDurableQueues();
      long ids[] = new long[durableQueues.size() + nonDurableQueues.size()];
      int i = 0;

      for (org.hornetq.core.server.Queue q : durableQueues)
      {
         q.getPageSubscription().getCounter().increment(tx, 1);
         q.getPageSubscription().notEmpty();
         ids[i++] = q.getID();
      }

      for (org.hornetq.core.server.Queue q : nonDurableQueues)
      {
         q.getPageSubscription().getCounter().increment(tx, 1);
         q.getPageSubscription().notEmpty();
         ids[i++] = q.getID();
      }
      return ids;
   }

   private void installPageTransaction(final Transaction tx, final RouteContextList listCtx) throws Exception
   {
      FinishPageMessageOperation pgOper = (FinishPageMessageOperation)tx.getProperty(TransactionPropertyIndexes.PAGE_TRANSACTION);
      if (pgOper == null)
      {
         PageTransactionInfo pgTX = new PageTransactionInfoImpl(tx.getID());
         pagingManager.addTransaction(pgTX);
         pgOper = new FinishPageMessageOperation(pgTX, storageManager, pagingManager);
         tx.putProperty(TransactionPropertyIndexes.PAGE_TRANSACTION, pgOper);
         tx.addOperation(pgOper);
      }

      pgOper.addStore(this);
      pgOper.pageTransaction.increment(listCtx.getNumberOfDurableQueues(), listCtx.getNumberOfNonDurableQueues());

      return;
   }

   private static class FinishPageMessageOperation implements TransactionOperation
   {
      public final PageTransactionInfo pageTransaction;

      private final StorageManager storageManager;

      private final PagingManager pagingManager;

      private final Set<PagingStore> usedStores = new HashSet<PagingStore>();

      private boolean stored = false;

      public void addStore(PagingStore store)
      {
         this.usedStores.add(store);
      }

      public FinishPageMessageOperation(final PageTransactionInfo pageTransaction, final StorageManager storageManager, final PagingManager pagingManager)
      {
         this.pageTransaction = pageTransaction;
         this.storageManager = storageManager;
         this.pagingManager = pagingManager;
      }

      public void afterCommit(final Transaction tx)
      {
         // If part of the transaction goes to the queue, and part goes to paging, we can't let depage start for the
         // transaction until all the messages were added to the queue
         // or else we could deliver the messages out of order

         if (pageTransaction != null)
         {
            pageTransaction.commit();
         }
      }

      public void afterPrepare(final Transaction tx)
      {
      }

      public void afterRollback(final Transaction tx)
      {
         if (pageTransaction != null)
         {
            pageTransaction.rollback();
         }
      }

      public void beforeCommit(final Transaction tx) throws Exception
      {
         syncStore();
         storePageTX(tx);
      }

      /**
       * @throws Exception
       */
      private void syncStore() throws Exception
      {
         for (PagingStore store : usedStores)
         {
            store.sync();
         }
      }

      public void beforePrepare(final Transaction tx) throws Exception
      {
         syncStore();
         storePageTX(tx);
      }

      private void storePageTX(final Transaction tx) throws Exception
      {
         if (!stored)
         {
            tx.setContainsPersistent();
            pageTransaction.store(storageManager, pagingManager, tx);
            stored = true;
         }
      }

      public void beforeRollback(final Transaction tx) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.TransactionOperation#getRelatedMessageReferences()
       */
      public List<MessageReference> getRelatedMessageReferences()
      {
         return Collections.emptyList();
      }

   }

   private void openNewPage() throws Exception
   {
      lock.writeLock().lock();

      try
      {
         numberOfPages++;

         int tmpCurrentPageId = currentPageId + 1;

         if (currentPage != null)
         {
            currentPage.close();
         }

         currentPage = createPage(tmpCurrentPageId);

         LivePageCache pageCache = new LivePageCacheImpl(currentPage);

         currentPage.setLiveCache(pageCache);

         cursorProvider.addPageCache(pageCache);

         currentPageSize.set(0);

         currentPage.open();

         currentPageId = tmpCurrentPageId;

         if (currentPageId < firstPageId)
         {
            firstPageId = currentPageId;
         }
      }
      finally
      {
         lock.writeLock().unlock();
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
}
