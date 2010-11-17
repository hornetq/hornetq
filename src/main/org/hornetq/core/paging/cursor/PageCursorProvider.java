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

package org.hornetq.core.paging.cursor;

import org.hornetq.core.filter.Filter;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingStore;

/**
 * The provider of Cursor for a given Address
 *
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 *
 */
public interface PageCursorProvider
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   PageCache getPageCache(PagePosition pos);
   
   PagedReference newReference(final PagePosition pos, final PagedMessage msg, PageSubscription sub);
   
   void addPageCache(PageCache cache);

   PagingStore getAssociatedStore();

   /**
    * 
    * @param queueId The cursorID should be the same as the queueId associated for persistance
    * @return
    */
   PageSubscription getSubscription(long queueId);
   
   PageSubscription createSubscription(long queueId, Filter filter, boolean durable);
   
   PagedMessage getMessage(PagePosition pos) throws Exception;

   void processReload() throws Exception;

   void stop();
   
   void flushExecutors();

   void scheduleCleanup();
   
   // Perform the cleanup at the caller's thread (for startup and recovery)
   void cleanup();

   /**
    * @param pageCursorImpl
    */
   void close(PageSubscription pageCursorImpl);
   
   // to be used on tests -------------------------------------------
   
   int getCacheSize();
   
   void printDebug();

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
