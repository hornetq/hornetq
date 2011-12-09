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

package org.hornetq.core.paging;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.paging.cursor.PageCursorProvider;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.RouteContextList;
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;

/**
 * 
 * <p>The implementation will take care of details such as PageSize.</p>
 * <p>The producers will write directly to PagingStore and that will decide what
 * Page file should be used based on configured size</p>
 * 
 * @see PagingManager

 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface PagingStore extends HornetQComponent
{
   SimpleString getAddress();

   int getNumberOfPages();
   
   // The current page in which the system is writing files
   int getCurrentWritingPage();

   SimpleString getStoreName();

   AddressFullMessagePolicy getAddressFullMessagePolicy();
   
   long getFirstPage();
   
   long getTopPage();

   long getPageSizeBytes();

   long getAddressSize();
   
   long getMaxSize();
   
   void applySetting(AddressSettings addressSettings);

   boolean isPaging();

   // It will schedule sync to the file storage
   void sync() throws Exception;
   
   // It will perform a real sync on the current IO file
   void ioSync() throws Exception;

   boolean page(ServerMessage message, RoutingContext ctx) throws Exception;

   boolean page(ServerMessage message, RoutingContext ctx, RouteContextList listCtx) throws Exception;

   Page createPage(final int page) throws Exception;
   
   boolean checkPage(final int page) throws Exception;
   
   PagingManager getPagingManager();
   
   PageCursorProvider getCursorProvier();
   
   void processReload() throws Exception;
   
   /** 
    * Remove the first page from the Writing Queue.
    * The file will still exist until Page.delete is called, 
    * So, case the system is reloaded the same Page will be loaded back if delete is not called.
    *
    * @throws Exception
    * 
    * Note: This should still be part of the interface, even though HornetQ only uses through the 
    */
   Page depage() throws Exception;


   void forceAnotherPage() throws Exception;

   Page getCurrentPage();


   /** @return true if paging was started, or false if paging was already started before this call */
   boolean startPaging() throws Exception;

   void stopPaging() throws Exception;

   void addSize(int size);
   
   void executeRunnableWhenMemoryAvailable(Runnable runnable);
   
   /** This method will hold and producer, but it wait operations to finish before locking (write lock) */
   void lock();
   
   /** 
    * 
    * Call this method using the same thread used by the last call of {@link PagingStore#lock()}
    * 
    */
    void unlock();

    /** This is used mostly by tests.
     *  We will wait any pending runnable to finish its execution */
    void flushExecutors();
}
