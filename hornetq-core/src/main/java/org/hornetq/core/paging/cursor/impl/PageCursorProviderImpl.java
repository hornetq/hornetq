/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.core.paging.cursor.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executor;

import org.hornetq.core.filter.Filter;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.cursor.PageCache;
import org.hornetq.core.paging.cursor.PageCursorProvider;
import org.hornetq.core.paging.cursor.PagePosition;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.paging.cursor.PagedReference;
import org.hornetq.core.paging.cursor.PagedReferenceImpl;
import org.hornetq.core.paging.impl.Page;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.server.HornetQLogger;
import org.hornetq.utils.FutureLatch;
import org.hornetq.utils.SoftValueHashMap;

/**
 * A PageProviderIMpl
 *
 * TODO: this may be moved entirely into PagingStore as there's an one-to-one relationship here
 *       However I want to keep this isolated as much as possible during development
 *
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 *
 */
public class PageCursorProviderImpl implements PageCursorProvider
{
   // Constants -----------------------------------------------------

   boolean isTrace = HornetQLogger.LOGGER.isTraceEnabled();

   // Attributes ----------------------------------------------------

   private final PagingStore pagingStore;

   private final StorageManager storageManager;

   // This is the same executor used at the PageStoreImpl. One Executor per pageStore
   private final Executor executor;

   private final SoftValueHashMap<Long, PageCache> softCache;

   private final ConcurrentMap<Long, PageSubscription> activeCursors = new ConcurrentHashMap<Long, PageSubscription>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PageCursorProviderImpl(final PagingStore pagingStore,
                                 final StorageManager storageManager,
                                 final Executor executor,
                                 final int maxCacheSize)
   {
      this.pagingStore = pagingStore;
      this.storageManager = storageManager;
      this.executor = executor;
      this.softCache = new SoftValueHashMap<Long, PageCache>(maxCacheSize);
   }

   // Public --------------------------------------------------------

   public PagingStore getAssociatedStore()
   {
      return pagingStore;
   }

   public synchronized PageSubscription createSubscription(long cursorID, Filter filter, boolean persistent)
   {
      if (HornetQLogger.LOGGER.isDebugEnabled())
      {
         HornetQLogger.LOGGER.debug(this.pagingStore.getAddress() + " creating subscription " + cursorID + " with filter " + filter, new Exception ("trace"));
      }

      if (activeCursors.containsKey(cursorID))
      {
         throw new IllegalStateException("Cursor " + cursorID + " had already been created");
      }

      PageSubscription activeCursor =
               new PageSubscriptionImpl(this, pagingStore, storageManager, executor, filter, cursorID, persistent);
      activeCursors.put(cursorID, activeCursor);
      return activeCursor;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCursorProvider#createCursor()
    */
   public synchronized PageSubscription getSubscription(long cursorID)
   {
      return activeCursors.get(cursorID);
   }

   public PagedMessage getMessage(final PagePosition pos) throws Exception
   {
      PageCache cache = getPageCache(pos);

      if (pos.getMessageNr() >= cache.getNumberOfMessages())
      {
         // sanity check, this should never happen unless there's a bug
         throw new IllegalStateException("Invalid messageNumber passed = " + pos + " on " + cache);
      }

      return cache.getMessage(pos.getMessageNr());
   }

   public PagedReference newReference(final PagePosition pos,
                                      final PagedMessage msg,
                                      final PageSubscription subscription)
   {
      return new PagedReferenceImpl(pos, msg, subscription);
   }

   /**
    * No need to synchronize this method since the private getPageCache will have a synchronized call
    */
   public PageCache getPageCache(PagePosition pos)
   {
      return getPageCache(pos.getPageNr());
   }

   public PageCache getPageCache(final long pageId)
   {
      try
      {
         boolean needToRead = false;
         PageCache cache = null;
         synchronized (softCache)
         {
            if (pageId > pagingStore.getCurrentWritingPage())
            {
               return null;
            }

            cache = softCache.get(pageId);
            if (cache == null)
            {
               if (!pagingStore.checkPageFileExists((int)pageId))
               {
                  return null;
               }

               cache = createPageCache(pageId);
               needToRead = true;
               // anyone reading from this cache will have to wait reading to finish first
               // we also want only one thread reading this cache
               cache.lock();
               if (isTrace)
               {
                  HornetQLogger.LOGGER.trace("adding " + pageId +  " into cursor = " + this.pagingStore.getAddress());
               }
               softCache.put(pageId, cache);
            }
         }

         // Reading is done outside of the synchronized block, however
         // the page stays locked until the entire reading is finished
         if (needToRead)
         {
            Page page = null;
            try
            {
               page = pagingStore.createPage((int)pageId);

               storageManager.beforePageRead();
               page.open();

               List<PagedMessage> pgdMessages = page.read(storageManager);
               cache.setMessages(pgdMessages.toArray(new PagedMessage[pgdMessages.size()]));
            }
            finally
            {
               try
               {
                  if (page != null)
                  {
                     page.close();
                  }
               }
               catch (Throwable ignored)
               {
               }
               storageManager.afterPageRead();
               cache.unlock();
            }
         }

         return cache;
      }
      catch (Exception e)
      {
         throw new RuntimeException("Couldn't complete paging due to an IO Exception on Paging - " + e.getMessage(), e);
      }
   }

   public void addPageCache(PageCache cache)
   {
      synchronized (softCache)
      {
         softCache.put(cache.getPageId(), cache);
      }
   }

   public int getCacheMaxSize()
   {
      return softCache.getMaxEelements();
   }

   public void setCacheMaxSize(final int size)
   {
      softCache.setMaxElements(size);
   }

   public int getCacheSize()
   {
      synchronized (softCache)
      {
         return softCache.size();
      }
   }

   public void processReload() throws Exception
   {
      for (PageSubscription cursor : this.activeCursors.values())
      {
         cursor.processReload();
      }

      cleanup();

   }

   public void stop()
   {
      for (PageSubscription cursor : activeCursors.values())
      {
         cursor.stop();
      }

      waitForFuture();
   }

   private void waitForFuture()
   {
      FutureLatch future = new FutureLatch();

      executor.execute(future);

      while (!future.await(10000))
      {
         HornetQLogger.LOGGER.timedOutStoppingPagingCursor(future, executor);
      }
   }

   public void flushExecutors()
   {
      for (PageSubscription cursor : activeCursors.values())
      {
         cursor.flushExecutors();
      }
      waitForFuture();
   }

   public void close(PageSubscription cursor)
   {
      activeCursors.remove(cursor.getId());

      scheduleCleanup();
   }

   @Override
   public void scheduleCleanup()
   {

      executor.execute(new Runnable()
      {
         public void run()
         {
            storageManager.setContext(storageManager.newSingleThreadContext());
            try
            {
               cleanup();
            }
            finally
            {
               storageManager.clearContext();
            }
         }

         @Override
         public String toString()
         {
            return "PageCursorProvider:scheduleCleanup()";
         }
      });
   }

   public void cleanup()
   {
      ArrayList<Page> depagedPages = new ArrayList<Page>();

      while (true)
      {
         if (pagingStore.lock(100))
         {
            break;
         }
         if (!pagingStore.isStarted())
            return;
      }

      synchronized (this)
      {
         try
         {
            if (!pagingStore.isStarted())
            {
               return;
            }

            if (pagingStore.getNumberOfPages() == 0)
            {
               return;
            }

            if (HornetQLogger.LOGGER.isDebugEnabled())
            {
               HornetQLogger.LOGGER.debug("Asserting cleanup for address " + this.pagingStore.getAddress());
            }

            ArrayList<PageSubscription> cursorList = cloneSubscriptions();

            long minPage = checkMinPage(cursorList);

            // if the current page is being written...
            // on that case we need to move to verify it in a different way
            if (minPage == pagingStore.getCurrentWritingPage() && pagingStore.getCurrentPage().getNumberOfMessages() > 0)
            {
               boolean complete = true;

               for (PageSubscription cursor : cursorList)
               {
                  if (!cursor.isComplete(minPage))
                  {
                     if (HornetQLogger.LOGGER.isDebugEnabled())
                     {
                        HornetQLogger.LOGGER.debug("Cursor " + cursor + " was considered incomplete at page " + minPage);
                     }

                     complete = false;
                     break;
                  }
                  else
                  {
                     if (HornetQLogger.LOGGER.isDebugEnabled())
                     {
                        HornetQLogger.LOGGER.debug("Cursor " + cursor + "was considered **complete** at page " + minPage);
                     }
                  }
               }

               if (!pagingStore.isStarted())
               {
                  return;
               }

               // All the pages on the cursor are complete.. so we will cleanup everything and store a bookmark
               if (complete)
               {

                  if (HornetQLogger.LOGGER.isDebugEnabled())
                  {
                     HornetQLogger.LOGGER.debug("Address " + pagingStore.getAddress() +
                        " is leaving page mode as all messages are consumed and acknowledged from the page store");
                  }

                  pagingStore.forceAnotherPage();

                  Page currentPage = pagingStore.getCurrentPage();

                  storeBookmark(cursorList, currentPage);

                  pagingStore.stopPaging();

                  // This has to be called after we stopped paging
                  for (PageSubscription cursor : cursorList)
                  {
                     cursor.scheduleCleanupCheck();
                  }

               }
            }

            for (long i = pagingStore.getFirstPage(); i < minPage; i++)
            {
               Page page = pagingStore.depage();
               if (page == null)
               {
                  break;
               }
               depagedPages.add(page);
            }

            if (pagingStore.getNumberOfPages() == 0 || pagingStore.getNumberOfPages() == 1 &&
               pagingStore.getCurrentPage().getNumberOfMessages() == 0)
            {
               pagingStore.stopPaging();
            }
            else
            {
               if (HornetQLogger.LOGGER.isTraceEnabled())
               {
                  HornetQLogger.LOGGER.trace("Couldn't cleanup page on address " + this.pagingStore.getAddress() +
                     " as numberOfPages == " +
                     pagingStore.getNumberOfPages() +
                     " and currentPage.numberOfMessages = " +
                     pagingStore.getCurrentPage().getNumberOfMessages());
               }
            }
         }
         catch (Exception ex)
         {
            HornetQLogger.LOGGER.problemCleaningPageAddress(ex, pagingStore.getAddress());
            return;
         }
         finally
         {
            pagingStore.unlock();
         }
      }

      try
      {
         for (Page depagedPage : depagedPages)
         {
            PageCache cache;
            PagedMessage[] pgdMessages;
            synchronized (softCache)
            {
               cache = softCache.get((long)depagedPage.getPageId());
            }

            if (isTrace)
            {
               HornetQLogger.LOGGER.trace("Removing page " + depagedPage.getPageId() + " from page-cache");
            }

            if (cache == null)
            {
               // The page is not on cache any more
               // We need to read the page-file before deleting it
               // to make sure we remove any large-messages pending
               storageManager.beforePageRead();

               List<PagedMessage> pgdMessagesList = null;
               try
               {
                  depagedPage.open();
                  pgdMessagesList = depagedPage.read(storageManager);
               }
               finally
               {
                  try
                  {
                     depagedPage.close();
                  }
                  catch (Exception e)
                  {
                  }

                  storageManager.afterPageRead();
               }
               depagedPage.close();
               pgdMessages = pgdMessagesList.toArray(new PagedMessage[pgdMessagesList.size()]);
            }
            else
            {
               pgdMessages = cache.getMessages();
            }

            depagedPage.delete(pgdMessages);
            onDeletePage(depagedPage);

            synchronized (softCache)
            {
               softCache.remove((long)depagedPage.getPageId());
            }
         }
      }
      catch (Exception ex)
      {
         HornetQLogger.LOGGER.problemCleaningPageAddress(ex, pagingStore.getAddress());
         return;
      }

   }

   /**
    * @return
    */
   private synchronized ArrayList<PageSubscription> cloneSubscriptions()
   {
      ArrayList<PageSubscription> cursorList = new ArrayList<PageSubscription>();
      cursorList.addAll(activeCursors.values());
      return cursorList;
   }

   protected void onDeletePage(Page deletedPage) throws Exception
   {
      List<PageSubscription> subscriptions = cloneSubscriptions();
      for (PageSubscription subs: subscriptions)
      {
         subs.onDeletePage(deletedPage);
      }
   }

   /**
    * @param cursorList
    * @param currentPage
    * @throws Exception
    */
   protected void storeBookmark(ArrayList<PageSubscription> cursorList, Page currentPage) throws Exception
   {
      try
      {
         // First step: Move every cursor to the next bookmarked page (that was just created)
         for (PageSubscription cursor : cursorList)
         {
            cursor.confirmPosition(new PagePositionImpl(currentPage.getPageId(), -1));
         }

         while (!storageManager.waitOnOperations(5000))
         {
            HornetQLogger.LOGGER.problemCompletingOperations(storageManager.getContext());
         }
      }
      finally
      {
         for (PageSubscription cursor : cursorList)
         {
            cursor.enableAutoCleanup();
         }
      }
   }

   public void printDebug()
   {
      System.out.println("Debug information for PageCursorProviderImpl:");
      for (PageCache cache : softCache.values())
      {
         System.out.println("Cache " + cache);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   /* Protected as we may let test cases to instrument the test */
   protected PageCacheImpl createPageCache(final long pageId) throws Exception
   {
      return new PageCacheImpl(pagingStore.createPage((int)pageId));
   }

   // Private -------------------------------------------------------

   /**
    * This method is synchronized because we want it to be atomic with the cursors being used
    */
   private long checkMinPage(List<PageSubscription> cursorList)
   {
      long minPage = Long.MAX_VALUE;

      for (PageSubscription cursor : cursorList)
      {
         long firstPage = cursor.getFirstPage();
         if (HornetQLogger.LOGGER.isDebugEnabled())
         {
            HornetQLogger.LOGGER.debug(this.pagingStore.getAddress() + " has a cursor " + cursor + " with first page=" + firstPage);
         }
         if (firstPage < minPage)
         {
            minPage = firstPage;
         }
      }

      if (HornetQLogger.LOGGER.isDebugEnabled())
      {
         HornetQLogger.LOGGER.debug(this.pagingStore.getAddress() + " has minPage=" + minPage);
      }

      return minPage;

   }

   // Inner classes -------------------------------------------------

}
