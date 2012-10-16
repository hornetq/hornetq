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

import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.cursor.PageCache;
import org.hornetq.core.paging.impl.Page;

/**
 * The caching associated to a single page.
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
class PageCacheImpl implements PageCache
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final ReadWriteLock lock = new ReentrantReadWriteLock();

   private PagedMessage[] messages;

   private final Page page;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PageCacheImpl(final Page page)
   {
      this.page = page;
   }

   // Public --------------------------------------------------------

   @Override
   public Page getPage()
   {
      return page;
   }

   @Override
   public PagedMessage getMessage(final int messageNumber)
   {
      lock.readLock().lock();
      try
      {
         if (messageNumber < messages.length)
         {
            return messages[messageNumber];
         }
         else
         {
            return null;
         }
      }
      finally
      {
         lock.readLock().unlock();
      }
   }

   public long getPageId()
   {
      return page.getPageId();
   }

   public void lock()
   {
      lock.writeLock().lock();
   }

   public void unlock()
   {
      lock.writeLock().unlock();
   }

   public void setMessages(final PagedMessage[] messages)
   {
      this.messages = messages;
   }

   public int getNumberOfMessages()
   {
      lock.readLock().lock();
      try
      {
         return messages.length;
      }
      finally
      {
         lock.readLock().unlock();
      }
   }

   public void close()
   {
   }

   @Override
   public boolean isLive()
   {
      return false;
   }

   @Override
   public String toString()
   {
      return "PageCacheImpl::page=" + page.getPageId() + " numberOfMessages = " + messages.length;
   }

   @Override
   public PagedMessage[] getMessages()
   {
      return messages;
   }
}
