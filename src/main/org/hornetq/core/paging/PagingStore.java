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

import java.util.List;

import org.hornetq.api.core.SimpleString;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;

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

   SimpleString getStoreName();

   AddressFullMessagePolicy getAddressFullMessagePolicy();

   long getPageSizeBytes();

   long getAddressSize();
   
   long getMaxSize();

   boolean isPaging();

   void sync() throws Exception;

   boolean page(List<ServerMessage> messages, long transactionId) throws Exception;

   boolean page(ServerMessage message) throws Exception;

   Page createPage(final int page) throws Exception;

   /**
    * @return false if a thread was already started, or if not in page mode
    * @throws Exception 
    */
   boolean startDepaging();

   void addSize(int size);
   
   void executeRunnableWhenMemoryAvailable(Runnable runnable);
}
