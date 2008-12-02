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

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.paging.LastPageRecord;
import org.jboss.messaging.core.paging.Page;
import org.jboss.messaging.core.paging.PageTransactionInfo;
import org.jboss.messaging.core.paging.PagedMessage;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.PagingStore;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.util.SimpleString;

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

   private final AtomicInteger pageUsedSize = new AtomicInteger(0);

   private final SimpleString storeName;

   private final SequentialFileFactory fileFactory;

   private final long maxSize;

   private final long pageSize;

   private final boolean dropMessagesWhenFull;

   private boolean printedDropMessagesWarning;

   private final PagingManager pagingManager;

   private final ExecutorService executor;

   // Bytes consumed by the queue on the memory
   private final AtomicLong sizeInBytes = new AtomicLong();

   private volatile Runnable dequeueThread;

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

   private volatile LastPageRecord lastPageRecord;

   // Static --------------------------------------------------------

   // private static final boolean isTrace = log.isTraceEnabled();
   private static final boolean isTrace = true;

   // This is just a debug tool method.
   // During debugs you could make log.trace as log.info, and change the
   // variable isTrace above
   private static void trace(final String message)
   {
      // log.trace(message);
      log.info(message);
   }


   // Constructors --------------------------------------------------

   public PagingStoreImpl(final PagingManager pagingManager,
                          final StorageManager storageManager,
                          final PostOffice postOffice,
                          final SequentialFileFactory fileFactory,
                          final SimpleString storeName,
                          final QueueSettings queueSettings,
                          final ExecutorService executor)
   {
      if (pagingManager == null)
      {
         throw new IllegalStateException("Paging Manager can't be null");
      }
      
      this.storageManager = storageManager;
      
      this.postOffice = postOffice;
      
      this.fileFactory = fileFactory;
      
      this.storeName = storeName;
      
      maxSize = queueSettings.getMaxSizeBytes();
      
      if (queueSettings.getPageSizeBytes() != null)
      {
         this.pageSize = queueSettings.getPageSizeBytes();
      }     
      else
      {
         this.pageSize = pagingManager.getDefaultPageSize();
      }

      dropMessagesWhenFull = queueSettings.isDropMessagesWhenFull();
      
      this.executor = executor;
      
      this.pagingManager = pagingManager;
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
         return currentPage != null;
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

   /**
    * Depage one page-file, read it and send it to the pagingManager / postoffice
    * @return
    * @throws Exception
    */
   //FIXME - why is this public?
   public boolean readPage() throws Exception
   {
      Page page = depage();
      
      if (page == null)
      {
         if (lastPageRecord != null)
         {
            clearLastPageRecord(lastPageRecord);
         }
         
         lastPageRecord = null;
         
         return false;
      }
      
      page.open();
      
      PagedMessage messages[] = page.read();
      
      boolean addressNotFull = onDepage(page.getPageId(), storeName, messages);
      
      page.delete();

      return addressNotFull;
   }

   /** 
    *  It returns a Page out of the Page System without reading it. 
    *  The method calling this method will remove the page and will start reading it outside of any locks. 
    *  
    * */
   public Page depage() throws Exception
   {
      writeLock.lock();
      currentPageLock.writeLock().lock();  // Make sure no checks are done on currentPage while we are depaging

      try
      {
         if (numberOfPages == 0)
         {
            return null;
         }
         else
         {
            numberOfPages--;

            final Page returnPage;
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

               if (returnPage.getNumberOfMessages() == 0)
               {
                  returnPage.open();
                  returnPage.delete();
                  return null;
               }
               else
               {
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

   
   public long addSize(final long size) throws Exception
   {
      final long maxSize = getMaxSizeBytes();

      final long pageSize = getPageSizeBytes();

      if (isDropWhenMaxSize() && size > 0)
      {
         // if destination configured to drop messages && size is over the
         // limit, we return -1 which means drop the message
         if (getAddressSize() + size > maxSize || pagingManager.getMaxGlobalSize() > 0 && pagingManager.getGlobalSize() + size > pagingManager.getMaxGlobalSize())
         {
            if (!printedDropMessagesWarning)
            {
               printedDropMessagesWarning = true;
               
               log.warn("Messages are being dropped on adress " + getStoreName());
            }

            return -1l;
         }
         else
         {
            return addAddressSize(size);
         }
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
               log.trace("globalMode.get = " + pagingManager.isGlobalPageMode() +
                         " currentGlobalSize = " +
                         currentGlobalSize +
                         " defaultPageSize = " +
                         pagingManager.getDefaultPageSize() +
                         " maxGlobalSize = " +
                         maxGlobalSize +
                         "maxGlobalSize - defaultPageSize = " +
                         (maxGlobalSize - pagingManager.getDefaultPageSize()));
            }

            if (pagingManager.isGlobalPageMode() && currentGlobalSize < maxGlobalSize - pagingManager.getDefaultPageSize())
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

         return addressSize;
      }
   }

   
   public boolean page(final PagedMessage message) throws Exception
   {
      // Max-size is set, but reject is activated, what means.. never page on
      // this address
      if (dropMessagesWhenFull)
      {
         return false;
      }
      
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

         int bytesToWrite = fileFactory.calculateBlockSize(message.getEncodeSize() + PageImpl.SIZE_RECORD);

         if (pageUsedSize.addAndGet(bytesToWrite) > pageSize && currentPage.getNumberOfMessages() > 0)
         {
            // Make sure nothing is currently validating currentPaging
            currentPageLock.writeLock().lock();
            try
            {
               openNewPage();
               pageUsedSize.addAndGet(bytesToWrite);
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
      currentPageLock.readLock().lock();
      try
      {
         if (currentPage == null)
         {
            return false;
         }
         else
         {
            synchronized (this)
            {
               if (dequeueThread == null)
               {
                  dequeueThread = new DepageRunnable();
                  executor.execute(dequeueThread);
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

   public LastPageRecord getLastPageRecord()
   {
      return lastPageRecord;
   }

   public void setLastPageRecord(final LastPageRecord record)
   {
      lastPageRecord = record;
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
         writeLock.lock();
         currentPageLock.writeLock().lock();

         try
         {
            running = false;

            executor.shutdown();
            executor.awaitTermination(60, TimeUnit.SECONDS);

            if (currentPage != null)
            {
               currentPage.close();
            }
         }
         finally
         {
            writeLock.unlock();
            currentPageLock.writeLock().unlock();
         }
      }
   }

   public synchronized void start() throws Exception
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

      currentPageLock.writeLock().lock();

      firstPageId = Integer.MAX_VALUE;
      currentPageId = 0;
      currentPage = null;

      try
      {
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

         running = true;

         if (numberOfPages != 0)
         {
            startPaging();
         }
      }
      finally
      {
         currentPageLock.writeLock().unlock();
      }
   }

   public boolean startPaging() throws Exception
   {
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
   
   

   // TestSupportPageStore ------------------------------------------

   public void forceAnotherPage() throws Exception
   {
      openNewPage();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------
   
   
   /**
    * This method will remove files from the page system and and route them, doing it transactionally
    * 
    * A Transaction will be opened only if persistent messages are used.
    * 
    * If persistent messages are also used, it will update eventual PageTransactions
    */
   
   private boolean onDepage(final int pageId,
                           final SimpleString destination,
                           final PagedMessage[] data) throws Exception
   {
      trace("Depaging....");

      // Depage has to be done atomically, in case of failure it should be
      // back to where it was
      final long depageTransactionID = storageManager.generateUniqueID();

      LastPageRecord lastPage = getLastPageRecord();

      if (lastPage == null)
      {
         lastPage = new LastPageRecordImpl(pageId, destination);
         
         setLastPageRecord(lastPage);
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

      for (PagedMessage msg : data)
      {
         ServerMessage pagedMessage = null;

         pagedMessage = (ServerMessage)msg.getMessage(storageManager);

         final long transactionIdDuringPaging = msg.getTransactionID();
         
         if (transactionIdDuringPaging >= 0)
         {
            final PageTransactionInfo pageTransactionInfo = pagingManager.getTransaction(transactionIdDuringPaging); 

            // http://wiki.jboss.org/wiki/JBossMessaging2Paging
            // This is the Step D described on the "Transactions on Paging"
            // section
            if (pageTransactionInfo == null)
            {
               if (isTrace)
               {
                  trace("Transaction " + msg.getTransactionID() + " not found, ignoring message " + pagedMessage);
               }
               continue;
            }

            // This is to avoid a race condition where messages are depaged
            // before the commit arrived
            if (!pageTransactionInfo.waitCompletion())
            {
               trace("Rollback was called after prepare, ignoring message " + pagedMessage);
               continue;
            }

            // Update information about transactions
            if (pagedMessage.isDurable())
            {
               pageTransactionInfo.decrement();
               pageTransactionsToUpdate.add(pageTransactionInfo);
            }
         }

         refsToAdd.addAll(postOffice.route(pagedMessage));

         if (pagedMessage.getDurableRefCount() != 0)
         {
            storageManager.storeMessageTransactional(depageTransactionID, pagedMessage);
         }
      }

      for (PageTransactionInfo pageWithTransaction : pageTransactionsToUpdate)
      {
         if (pageWithTransaction.getNumberOfMessages() == 0)
         {
            // http://wiki.jboss.org/wiki/JBossMessaging2Paging
            // numberOfReads==numberOfWrites -> We delete the record
            storageManager.storeDeletePageTransaction(depageTransactionID, pageWithTransaction.getRecordID());
            pagingManager.removeTransaction(pageWithTransaction.getTransactionID());
         }
         else
         {
            storageManager.storePageTransaction(depageTransactionID, pageWithTransaction);
         }
      }

      storageManager.commit(depageTransactionID);

      trace("Depage committed");

      for (MessageReference ref : refsToAdd)
      {
         ref.getQueue().addLast(ref);
      }

      if (pagingManager.isGlobalPageMode())
      {
         // We use the Default Page Size when in global mode for the calculation of the Watermark
         return pagingManager.getGlobalSize() < pagingManager.getMaxGlobalSize() - pagingManager.getDefaultPageSize() && getMaxSizeBytes() <= 0 ||
                getAddressSize() < getMaxSizeBytes();
      }
      else
      {
         // If Max-size is not configured (-1) it will aways return true, as
         // this method was probably called by global-depage
         return getMaxSizeBytes() <= 0 || getAddressSize() < getMaxSizeBytes();
      }

   }


   private long addAddressSize(final long delta)
   {
      return sizeInBytes.addAndGet(delta);
   }

   private synchronized void clearDequeueThread()
   {
      dequeueThread = null;
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

         pageUsedSize.set(0);

         currentPage.open();
      }
      finally
      {
         currentPageLock.writeLock().unlock();
      }
   }

   public void clearLastPageRecord(final LastPageRecord lastRecord) throws Exception
   {
      trace("Clearing lastRecord information " + lastRecord.getLastId());
      
      storageManager.storeDelete(lastRecord.getRecordId());
   }

   private Page createPage(final int page) throws Exception
   {
      String fileName = createFileName(page);
      
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

   // Inner classes -------------------------------------------------

   private class DepageRunnable implements Runnable
   {
      public DepageRunnable()
      {
      }

      public void run()
      {
         try
         {
            boolean needMorePages = true;
            while (needMorePages && running)
            {
               needMorePages = readPage();
            }
         }
         catch (Exception e)
         {
            log.error(e, e);
         }
         finally
         {
            clearDequeueThread();
         }
      }
   }
}
