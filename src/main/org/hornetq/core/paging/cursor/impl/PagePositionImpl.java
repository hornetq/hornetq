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

import java.util.concurrent.atomic.AtomicLong;

import org.hornetq.core.paging.cursor.PagePosition;

/**
 * A PagePosition
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class PagePositionImpl implements PagePosition
{
   
   // This is for tests validating memory leaks
   private static final AtomicLong counter = new AtomicLong(0);
   public static long getCount()
   {
      return counter.get();
   }
   
   public static void resetCount()
   {
      counter.set(0);
   }
   
   private long pageNr;

   /**
    * The index of the message on the page file.
    *
    * This can be used as -1 in cases where the message is irrelevant,
    * for instance when a cursor is storing the next message to be received
    * or when a page is marked as fully complete (as the ACKs are removed)
    */
   private int messageNr;

   /** ID used for storage */
   private long recordID = -1;

   /**
    * @param pageNr
    * @param messageNr
    */
   public PagePositionImpl(long pageNr, int messageNr)
   {
      //super();
      this();
      this.pageNr = pageNr;
      this.messageNr = messageNr;
   }

   /**
    * @param pageNr
    * @param messageNr
    */
   public PagePositionImpl()
   {
        super();
        counter.incrementAndGet();
   }

   /**
    * @return the recordID
    */
   public long getRecordID()
   {
      return recordID;
   }

   /**
    * @param recordID the recordID to set
    */
   public void setRecordID(long recordID)
   {
      this.recordID = recordID;
   }

   /**
    * @return the pageNr
    */
   public long getPageNr()
   {
      return pageNr;
   }

   /**
    * @return the messageNr
    */
   public int getMessageNr()
   {
      return messageNr;
   }

   /* (non-Javadoc)
    * @see java.lang.Comparable#compareTo(java.lang.Object)
    */
   public int compareTo(PagePosition o)
   {
      if (pageNr > o.getPageNr())
      {
         return 1;
      }
      else if (pageNr < o.getPageNr())
      {
         return -1;
      }
      else if (recordID > o.getRecordID())
      {
         return 1;
      }
      else if (recordID < o.getRecordID())
      {
         return -1;
      }
      else
      {
         return 0;
      }
   }

   public PagePosition nextMessage()
   {
      return new PagePositionImpl(this.pageNr, this.messageNr + 1);
   }

   public PagePosition nextPage()
   {
      return new PagePositionImpl(this.pageNr + 1, 0);
   }

   public boolean isNextSequenceOf(PagePosition pos)
   {
      return this.pageNr == pos.getPageNr() && this.getRecordID() - pos.getRecordID() == 1;
   }

   /* (non-Javadoc)
    * @see java.lang.Object#hashCode()
    */
   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = 1;
      result = prime * result + messageNr;
      result = prime * result + (int)(pageNr ^ (pageNr >>> 32));
      return result;
   }

   /* (non-Javadoc)
    * @see java.lang.Object#equals(java.lang.Object)
    */
   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      PagePositionImpl other = (PagePositionImpl)obj;
      if (messageNr != other.messageNr)
         return false;
      if (pageNr != other.pageNr)
         return false;
      return true;
   }

   @Override
   public String toString()
   {
      return "PagePositionImpl [pageNr=" + pageNr + ", messageNr=" + messageNr + ", recordID=" + recordID + "]";
   }

   protected void finalize()
   {
       counter.decrementAndGet();
   }

}
