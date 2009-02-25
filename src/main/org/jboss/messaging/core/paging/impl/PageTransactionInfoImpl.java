/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.core.paging.impl;

import static org.jboss.messaging.utils.DataConstants.SIZE_INT;
import static org.jboss.messaging.utils.DataConstants.SIZE_LONG;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.paging.PageTransactionInfo;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;

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

   public synchronized void decode(final MessagingBuffer buffer)
   {
      transactionID = buffer.getLong();
      numberOfMessages.set(buffer.getInt());
      countDownCompleted = null; // if it is being readed, probably it was
      // committed
      committed = true; // Unless it is a incomplete prepare, which is marked by
      // markIcomplete
   }

   public synchronized void encode(final MessagingBuffer buffer)
   {
      buffer.putLong(transactionID);
      buffer.putInt(numberOfMessages.get());
   }

   public synchronized int getEncodeSize()
   {
      return SIZE_LONG + SIZE_INT;
   }

   public void commit()
   {
      committed = true;
      /** 
       * this is to avoid a race condition where the transaction still being committed while another thread is depaging messages
       */
      countDownCompleted.countDown();
   }
   
   
   public boolean waitCompletion(int timeoutMilliseconds) throws InterruptedException
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
