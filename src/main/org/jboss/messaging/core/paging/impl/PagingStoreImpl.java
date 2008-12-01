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
import org.jboss.messaging.core.paging.PagedMessage;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.PagingStore;
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

   // Constructors --------------------------------------------------

   public PagingStoreImpl(final PagingManager pagingManager,
                          final SequentialFileFactory fileFactory,
                          final SimpleString storeName,
                          final QueueSettings queueSettings,
                          final ExecutorService executor)
   {
      if (pagingManager == null)
      {
         throw new IllegalStateException("Paging Manager can't be null");
      }
      
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

   //TODO - this methods shouldn't be necessary if move functionality from
   //PagingManagerImpl to PagingStoreImpl
   public boolean isPrintedDropMessagesWarning()
   {
      return printedDropMessagesWarning;
   }

   public void setPrintedDropMessagesWarning(final boolean droppedMessages)
   {
      this.printedDropMessagesWarning = droppedMessages;
   }

   public long getAddressSize()
   {
      return sizeInBytes.get();
   }

   public long addAddressSize(final long delta)
   {
      return sizeInBytes.addAndGet(delta);
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
            pagingManager.clearLastPageRecord(lastPageRecord);
         }
         
         lastPageRecord = null;
         
         return false;
      }
      
      page.open();
      
      PagedMessage messages[] = page.read();
      
      boolean addressNotFull = pagingManager.onDepage(page.getPageId(), storeName, PagingStoreImpl.this, messages);
      
      page.delete();

      return addressNotFull;
   }

   /** 
    *  It returns a Page out of the Page System without reading it. 
    *  The method calling this method will remove the page and will start reading it outside of any locks. 
    *  
    * */
   //FIXME - why is this public?
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
