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

package org.hornetq.tests.unit.core.journal.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.journal.impl.TimedBuffer;
import org.hornetq.core.journal.impl.TimedBufferObserver;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A TimedBufferTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class TimedBufferTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final int ONE_SECOND = 1000000000; // in nanoseconds

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   IOAsyncTask dummyCallback = new IOAsyncTask()
   {

      public void done()
      {
      }

      public void onError(final int errorCode, final String errorMessage)
      {
      }
   };
   

   public void testFillBuffer()
   {
      final ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
      final AtomicInteger flushTimes = new AtomicInteger(0);
      class TestObserver implements TimedBufferObserver
      {
         public void flushBuffer(final ByteBuffer buffer, final boolean sync, final List<IOAsyncTask> callbacks)
         {
            buffers.add(buffer);
            flushTimes.incrementAndGet();
         }

         /* (non-Javadoc)
          * @see org.hornetq.utils.timedbuffer.TimedBufferObserver#newBuffer(int, int)
          */
         public ByteBuffer newBuffer(final int minSize, final int maxSize)
         {
            return ByteBuffer.allocate(maxSize);
         }

         public int getRemainingBytes()
         {
            return 1024 * 1024;
         }
      }

      TimedBuffer timedBuffer = new TimedBuffer(100, TimedBufferTest.ONE_SECOND, false);

      timedBuffer.start();

      try
      {

         timedBuffer.setObserver(new TestObserver());

         int x = 0;
         for (int i = 0; i < 10; i++)
         {
            byte[] bytes = new byte[10];
            for (int j = 0; j < 10; j++)
            {
               bytes[j] = UnitTestCase.getSamplebyte(x++);
            }

            HornetQBuffer buff = HornetQBuffers.wrappedBuffer(bytes);

            timedBuffer.checkSize(10);
            timedBuffer.addBytes(buff, false, dummyCallback);
         }

         timedBuffer.checkSize(1);

         Assert.assertEquals(1, flushTimes.get());

         ByteBuffer flushedBuffer = buffers.get(0);

         Assert.assertEquals(100, flushedBuffer.limit());

         Assert.assertEquals(100, flushedBuffer.capacity());

         flushedBuffer.rewind();

         for (int i = 0; i < 100; i++)
         {
            Assert.assertEquals(UnitTestCase.getSamplebyte(i), flushedBuffer.get());
         }
      }
      finally
      {
         timedBuffer.stop();
      }

   }

   public void testTimingAndFlush() throws Exception
   {
      final ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
      final AtomicInteger flushTimes = new AtomicInteger(0);
      class TestObserver implements TimedBufferObserver
      {
         public void flushBuffer(final ByteBuffer buffer, final boolean sync, final List<IOAsyncTask> callbacks)
         {
            buffers.add(buffer);
            flushTimes.incrementAndGet();
         }

         /* (non-Javadoc)
          * @see org.hornetq.utils.timedbuffer.TimedBufferObserver#newBuffer(int, int)
          */
         public ByteBuffer newBuffer(final int minSize, final int maxSize)
         {
            return ByteBuffer.allocate(maxSize);
         }

         public int getRemainingBytes()
         {
            return 1024 * 1024;
         }
      }

      TimedBuffer timedBuffer = new TimedBuffer(100, TimedBufferTest.ONE_SECOND / 10, false);

      timedBuffer.start();

      try
      {

         timedBuffer.setObserver(new TestObserver());

         int x = 0;

         byte[] bytes = new byte[10];
         for (int j = 0; j < 10; j++)
         {
            bytes[j] = UnitTestCase.getSamplebyte(x++);
         }

         HornetQBuffer buff = HornetQBuffers.wrappedBuffer(bytes);

         timedBuffer.checkSize(10);
         timedBuffer.addBytes(buff, false, dummyCallback);

         Thread.sleep(200);

         Assert.assertEquals(0, flushTimes.get());

         bytes = new byte[10];
         for (int j = 0; j < 10; j++)
         {
            bytes[j] = UnitTestCase.getSamplebyte(x++);
         }

         buff = HornetQBuffers.wrappedBuffer(bytes);

         timedBuffer.checkSize(10);
         timedBuffer.addBytes(buff, true, dummyCallback);

         Thread.sleep(500);

         Assert.assertEquals(1, flushTimes.get());

         ByteBuffer flushedBuffer = buffers.get(0);

         Assert.assertEquals(20, flushedBuffer.limit());

         Assert.assertEquals(20, flushedBuffer.capacity());

         flushedBuffer.rewind();

         for (int i = 0; i < 20; i++)
         {
            Assert.assertEquals(UnitTestCase.getSamplebyte(i), flushedBuffer.get());
         }
      }
      finally
      {
         timedBuffer.stop();
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
