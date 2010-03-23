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

import org.hornetq.core.paging.Page;
import org.hornetq.core.paging.PagingStore;

/**
 * All the methods required to TestCases on  PageStoreImpl
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface TestSupportPageStore extends PagingStore
{
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

   /** @return true if paging was started, or false if paging was already started before this call */
   boolean startPaging() throws Exception;

   Page getCurrentPage();
}
