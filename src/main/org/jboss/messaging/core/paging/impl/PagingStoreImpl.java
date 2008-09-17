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
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.paging.LastPageRecord;
import org.jboss.messaging.core.paging.Page;
import org.jboss.messaging.core.paging.PageMessage;
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

   private final boolean dropMessagesOnSize;

   private boolean droppedMessages;

   private final PagingManager pagingManager;

   private final Executor executor;

   // Bytes consumed by the queue on the memory
   private final AtomicLong sizeInBytes = new AtomicLong();

   private volatile Runnable dequeueThread;

   private volatile int numberOfPages;

   private volatile int firstPageId = Integer.MAX_VALUE;

   private volatile int currentPageId;

   private volatile Page currentPage;

   // positioningGlobalLock protects opening/closing and messing up with
   // positions (currentPage and IDs)
   private final Semaphore positioningGlobalLock = new Semaphore(1);

   private final ReadWriteLock lock = new ReentrantReadWriteLock();

   private volatile boolean initialized = false;

   private volatile LastPageRecord lastPageRecord;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PagingStoreImpl(final PagingManager pagingManager,
                          final SequentialFileFactory fileFactory,
                          final SimpleString storeName,
                          final QueueSettings queueSettings,
                          final Executor executor)
   {
      this.fileFactory = fileFactory;
      this.storeName = storeName;
      maxSize = queueSettings.getMaxSizeBytes();
      pageSize = queueSettings.getPageSizeBytes();
      dropMessagesOnSize = queueSettings.isDropMessagesWhenFull();
      this.executor = executor;
      this.pagingManager = pagingManager;
   }

   // Public --------------------------------------------------------

   // PagingStore implementation ------------------------------------

   public boolean isDroppedMessage()
   {
      return droppedMessages;
   }

   public void setDroppedMessage(final boolean droppedMessages)
   {
      this.droppedMessages = droppedMessages;
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
      return dropMessagesOnSize;
   }

   public long getPageSizeBytes()
   {
      return pageSize;
   }

   public boolean isPaging()
   {
      lock.readLock().lock();
      try
      {
         return currentPage != null;
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

   public SimpleString getStoreName()
   {
      return storeName;
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
         if (lastPageRecord != null)
         {
            pagingManager.clearLastPageRecord(lastPageRecord);
         }
         lastPageRecord = null;
         return false;
      }
      page.open();
      PageMessage messages[] = page.read();
      boolean addressNotFull = pagingManager.onDepage(page.getPageId(), storeName, PagingStoreImpl.this, messages);
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
      validateInit();

      positioningGlobalLock.acquire(); // Can't change currentPage or any of ids
                                       // without a global lock
      lock.writeLock().lock(); // Wait pending writes to finish before
                                 // entering the block

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
         lock.writeLock().unlock();
         positioningGlobalLock.release();
      }

   }

   public boolean page(final PageMessage message) throws Exception
   {
      validateInit();

      // Max-size is set, but reject is activated, what means.. never page on
      // this address
      if (dropMessagesOnSize)
      {
         return false;
      }

      int bytesToWrite = fileFactory.calculateBlockSize(message.getEncodeSize() + PageImpl.SIZE_RECORD);

      // The only thing single-threaded done on paging is positioning and
      // check-files (verifying if we need to open a new page file)
      positioningGlobalLock.acquire();

      // After we have it locked we keep all the threads working until we need
      // to move to a new file (in which case we demand a writeLock, to wait for
      // the writes to finish)
      try
      {
         if (currentPage == null)
         {
            return false;
         }

         if (pageUsedSize.addAndGet(bytesToWrite) > pageSize && currentPage.getNumberOfMessages() > 0)
         {
            // Wait any pending write on the current page to finish before we
            // can open another page.
            lock.writeLock().lock();
            try
            {
               openNewPage();
               pageUsedSize.addAndGet(bytesToWrite);
            }
            finally
            {
               lock.writeLock().unlock();
            }
         }
         // we must get the readLock before we release the synchronizedBlockLock
         // or else we could end up with files records being added to the
         // currentPage even if the max size was already achieved.
         // (Condition tested by PagingStoreTestPage::testConcurrentPaging, The
         // test would eventually fail, 1 in 100)
         // This is because the checkSize and positioning has to be done
         // protected. We only allow writing the file in multi-thread.
         lock.readLock().lock();

      }
      finally
      {
         positioningGlobalLock.release();
      }

      // End of a synchronized block..

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
         lock.readLock().unlock();
      }
   }

   public void sync() throws Exception
   {
      validateInit();

      lock.readLock().lock();

      try
      {
         currentPage.sync();
      }
      finally
      {
         lock.readLock().unlock();
      }
   }

   public boolean startDepaging()
   {
      lock.readLock().lock();
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
         lock.readLock().unlock();
      }
   }

   public LastPageRecord getLastRecord()
   {
      return lastPageRecord;
   }

   public void setLastRecord(final LastPageRecord record)
   {
      lastPageRecord = record;
   }

   // MessagingComponent implementation

   public synchronized boolean isStarted()
   {
      return initialized;
   }

   public synchronized void stop() throws Exception
   {
      if (initialized)
      {
         lock.writeLock().lock();

         try
         {
            initialized = false;
            if (currentPage != null)
            {
               currentPage.close();
            }
         }
         finally
         {
            lock.writeLock().unlock();
         }
      }
   }

   public synchronized void start() throws Exception
   {

      if (initialized)
      {
         // don't throw an exception.
         // You could have two threads adding PagingStore to a
         // ConcurrentHashMap,
         // and having both threads calling init. One of the calls should just
         // need to be ignored
         return;
      }

      lock.writeLock().lock();

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

         initialized = true;

         if (numberOfPages != 0)
         {
            startPaging();
         }
      }
      finally
      {
         lock.writeLock().unlock();
      }
   }

   public boolean startPaging() throws Exception
   {
      validateInit();

      // First check without any global locks.
      // (Faster)
      lock.readLock().lock();
      try
      {
         if (currentPage != null)
         {
            return false;
         }
      }
      finally
      {
         lock.readLock().unlock();
      }

      // if the first check failed, we do it again under a global lock
      // (positioningGlobalLock) this time
      positioningGlobalLock.acquire();

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
         positioningGlobalLock.release();
      }
   }

   // TestSupportPageStore ------------------------------------------

   public void forceAnotherPage() throws Exception
   {
      validateInit();
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
      lock.writeLock().lock();

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
         lock.writeLock().unlock();
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

   private void validateInit()
   {
      if (!initialized)
      {
         throw new IllegalStateException("PagingStore " + storeName + " not initialized!");
      }
   }

   // Inner classes -------------------------------------------------

   class DepageRunnable implements Runnable
   {
      public DepageRunnable()
      {
      }

      public void run()
      {
         try
         {
            boolean needMorePages = false;
            do
            {
               needMorePages = readPage();
            }
            while (needMorePages);
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
