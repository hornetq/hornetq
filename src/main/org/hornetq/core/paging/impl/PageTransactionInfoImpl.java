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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.paging.PageTransactionInfo;
import org.hornetq.utils.DataConstants;

/**
 *
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class PageTransactionInfoImpl implements PageTransactionInfo
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long transactionID;

   private volatile long recordID;

   private volatile CountDownLatch countDownCompleted;

   private volatile boolean committed;

   private volatile boolean rolledback;

   private final AtomicInteger numberOfMessages = new AtomicInteger(0);

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public PageTransactionInfoImpl(final long transactionID)
   {
      this.transactionID = transactionID;
      countDownCompleted = new CountDownLatch(1);
   }

   public PageTransactionInfoImpl()
   {
   }

   // Public --------------------------------------------------------

   public long getRecordID()
   {
      return recordID;
   }

   public void setRecordID(final long recordID)
   {
      this.recordID = recordID;
   }

   public long getTransactionID()
   {
      return transactionID;
   }

   public int increment()
   {
      return numberOfMessages.incrementAndGet();
   }

   public int decrement()
   {
      final int value = numberOfMessages.decrementAndGet();

      if (value < 0)
      {
         throw new IllegalStateException("Internal error Negative value on Paging transactions!");
      }

      return value;
   }

   public int getNumberOfMessages()
   {
      return numberOfMessages.get();
   }

   // EncodingSupport implementation

   public synchronized void decode(final HornetQBuffer buffer)
   {
      transactionID = buffer.readLong();
      numberOfMessages.set(buffer.readInt());
      countDownCompleted = null; // if it is being readed, probably it was
      // committed
      committed = true; // Unless it is a incomplete prepare, which is marked by
      // markIcomplete
   }

   public synchronized void encode(final HornetQBuffer buffer)
   {
      buffer.writeLong(transactionID);
      buffer.writeInt(numberOfMessages.get());
   }

   public synchronized int getEncodeSize()
   {
      return DataConstants.SIZE_LONG + DataConstants.SIZE_INT;
   }

   public void commit()
   {
      committed = true;
      /** 
       * this is to avoid a race condition where the transaction still being committed while another thread is depaging messages
       */
      countDownCompleted.countDown();
   }

   public boolean waitCompletion(final int timeoutMilliseconds) throws InterruptedException
   {
      if (countDownCompleted == null)
      {
         return true;
      }
      else
      {
         return countDownCompleted.await(timeoutMilliseconds, TimeUnit.MILLISECONDS);
      }
   }

   public boolean isCommit()
   {
      return committed;
   }

   public boolean isRollback()
   {
      return rolledback;
   }

   public void rollback()
   {
      rolledback = true;
      committed = false;
      countDownCompleted.countDown();
   }

   public void markIncomplete()
   {
      committed = false;
      rolledback = false;

      countDownCompleted = new CountDownLatch(1);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
