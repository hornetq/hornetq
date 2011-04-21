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

import java.util.LinkedList;
import java.util.List;

import org.hornetq.core.paging.Page;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.cursor.LivePageCache;
import org.hornetq.core.server.LargeServerMessage;

/**
 * This is the same as PageCache, however this is for the page that's being currently written.
 *
 * @author clebertsuconic
 *
 *
 */
public class LivePageCacheImpl implements LivePageCache
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------
   
   private final List<PagedMessage> messages = new LinkedList<PagedMessage>();
   
   private final Page page;
   
   private boolean isLive = true;
   
   public String toString()
   {
      return "LivePacheCacheImpl::page=" + page.getPageId() + " number of messages=" + messages.size() + " isLive = " + isLive;
   }

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   
   public LivePageCacheImpl(final Page page)
   {
      this.page = page;
   }

   // Public --------------------------------------------------------

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCache#getPage()
    */
   public Page getPage()
   {
      return page;
   }
   
   public long getPageId()
   {
      return page.getPageId();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCache#getNumberOfMessages()
    */
   public synchronized int getNumberOfMessages()
   {
      return messages.size();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCache#setMessages(org.hornetq.core.server.ServerMessage[])
    */
   public synchronized void setMessages(PagedMessage[] messages)
   {
      // This method shouldn't be called on liveCache, but we will provide the implementation for it anyway
      for (PagedMessage msg : messages)
      {
         addLiveMessage(msg);
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCache#getMessage(int)
    */
   public synchronized PagedMessage getMessage(int messageNumber)
   {
      if (messageNumber < messages.size())
      {
         return messages.get(messageNumber);
      }
      else
      {
         return null;
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCache#lock()
    */
   public void lock()
   {
      // nothing to be done on live cache
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCache#unlock()
    */
   public void unlock()
   {
      // nothing to be done on live cache
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCache#isLive()
    */
   public synchronized boolean isLive()
   {
      return isLive;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.LivePageCache#addLiveMessage(org.hornetq.core.server.ServerMessage)
    */
   public synchronized void addLiveMessage(PagedMessage message)
   {
      if (message.getMessage().isLargeMessage())
      {
         ((LargeServerMessage)message.getMessage()).incrementDelayDeletionCount();
      }
      this.messages.add(message);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.LivePageCache#close()
    */
   public synchronized void close()
   {
      this.isLive = false;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.paging.cursor.PageCache#getMessages()
    */
   public PagedMessage[] getMessages()
   {
      return messages.toArray(new PagedMessage[messages.size()]);
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
