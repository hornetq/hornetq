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

import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.paging.Page;
import org.jboss.messaging.core.paging.PageTransactionInfo;
import org.jboss.messaging.core.paging.PagedMessage;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.PagingStore;
import org.jboss.messaging.core.paging.PagingStoreFactory;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.TransactionPropertyIndexes;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;
import org.jboss.messaging.utils.Future;
import org.jboss.messaging.utils.SimpleString;

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
         pagingManager.addGlobalSize(size);

         return;
      }
      else
      {
         final long currentGlobalSize = pagingManager.addGlobalSize(size);

         final long maxGlobalSize = pagingManager.getMaxGlobalSize();

         final long addressSize = addAddressSize(size);

         if (size > 0)
         {
            if (maxGlobalSize > 0 && currentGlobalSize > maxGlobalSize)
            {
               pagingManager.setGlobalPageMode(true);

               if (startPaging())
               {
                  if (isTrace)
                  {
                     trace("Starting paging on " + getStoreName() + ", size = " + addressSize + ", maxSize=" + maxSize);
                  }
               }
            }
            else if (maxSize > 0 && addressSize > maxSize)
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
            // When in Global mode, we use the default page size as the minimal
            // watermark to start depage

            if (isTrace)
            {

               log.trace(" globalDepage = " + pagingManager.isGlobalPageMode() +
                         " currentGlobalSize = " +
                         currentGlobalSize +
                         " GlobalWatermark = " +
                         pagingManager.getGlobalDepageWatermarkBytes() +
                         " maxGlobalSize = " +
                         maxGlobalSize +
                         " maxGlobalSize - defaultPageSize = " +
                         (maxGlobalSize - pagingManager.getGlobalDepageWatermarkBytes()));
            }

            if (maxGlobalSize > 0 && pagingManager.isGlobalPageMode() &&
                currentGlobalSize < maxGlobalSize - pagingManager.getGlobalDepageWatermarkBytes())
            {
               pagingManager.startGlobalDepage();
            }
            else if (maxSize > 0 && addressSize < maxSize - pageSize)
            {
               if (startDepaging())
               {
                  log.info("Starting depaging Thread, size = " + addressSize);
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

               log.warn("Messages are being dropped on adress " + getStoreName());
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
      return startDepaging(executor);
   }

   public boolean startDepaging(final Executor executor)
   {
      if (!running)
      {
         return false;
      }

      if (pagingManager.isBackup())
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

   // MessagingComponent implementation

   public synchronized boolean isStarted()
   {
      return running;
   }

   public synchronized void stop() throws Exception
   {
      if (running)
      {
         running = false;

         org.jboss.messaging.utils.Future future = new Future();

         executor.execute(future);

         boolean ok = future.await(10000);

         if (!ok)
         {
            log.warn("Timed out waiting for depage executor on destination " + storeName + " to stop");
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
                  // and this will make JBM start using the journal again
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

         final long transactionIdDuringPaging = pagedMessage.getTransactionID();

         if (transactionIdDuringPaging >= 0)
         {
            final PageTransactionInfo pageTransactionInfo = pagingManager.getTransaction(transactionIdDuringPaging);

            // http://wiki.jboss.org/wiki/JBossMessaging2Paging
            // This is the Step D described on the "Transactions on Paging"
            // section
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
            // http://wiki.jboss.org/wiki/JBossMessaging2Paging
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
   private boolean isFull(final long nextPageSize)
   {
      return getMaxSizeBytes() > 0 && getAddressSize() + nextPageSize > getMaxSizeBytes();
   }

   /**
    * @param nextPageSize
    * @return
    */
   private boolean isGlobalFull(final long nextPageSize)
   {
      return pagingManager.getMaxGlobalSize() > 0 && pagingManager.getGlobalSize() + nextPageSize > pagingManager.getMaxGlobalSize();
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
      final boolean pageFull = isFull(getPageSizeBytes());
      final boolean globalFull = isGlobalFull(getPageSizeBytes());
      if (pageFull || globalFull || !isPaging())
      {
         depaging.set(false);
         if (!globalFull)
         {
            pagingManager.setGlobalPageMode(false);
         }
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
      return getMaxSizeBytes() > 0 && getAddressSize() > getMaxSizeBytes() ||
             pagingManager.getMaxGlobalSize() > 0 &&
             pagingManager.getGlobalSize() > pagingManager.getMaxGlobalSize();
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
               if (!isFull(getPageSizeBytes()) && !isGlobalFull(getPageSizeBytes()))
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
            e.printStackTrace();
            log.error(e, e);
         }
      }
   }
}
