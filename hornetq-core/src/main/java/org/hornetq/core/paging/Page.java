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

import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.paging.cursor.LivePageCache;
import org.hornetq.core.persistence.StorageManager;

/**
 *
 * @see PagingManager
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public interface Page
{
   int getPageId();

   void write(PagedMessage message) throws Exception;

   List<PagedMessage> read(StorageManager storage) throws Exception;

   void setLiveCache(LivePageCache pageCache);

   int getSize();

   int getNumberOfMessages();

   void sync() throws Exception;

   void open() throws Exception;

   void close() throws Exception;

   boolean delete(PagedMessage[] messages) throws Exception;

   SequentialFile getFile();
}
