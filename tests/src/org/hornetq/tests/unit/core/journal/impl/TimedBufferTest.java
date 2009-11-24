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

      public void waitCompletion() throws Exception
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

      TimedBuffer timedBuffer = new TimedBuffer(100, 3600 * 1000, false, false); // Any big timeout

      timedBuffer.setObserver(new TestObserver());

      int x = 0;
      for (int i = 0; i < 10; i++)
      {
         byte[] bytes = new byte[10];
         for (int j = 0; j < 10; j++)
         {
            bytes[j] = getSamplebyte(x++);
         }

         timedBuffer.checkSize(10);
         timedBuffer.addBytes(bytes, false, dummyCallback);
      }

      assertEquals(1, flushTimes.get());

      ByteBuffer flushedBuffer = buffers.get(0);

      assertEquals(100, flushedBuffer.limit());

      assertEquals(100, flushedBuffer.capacity());

      flushedBuffer.rewind();

      for (int i = 0; i < 100; i++)
      {
         assertEquals(getSamplebyte(i), flushedBuffer.get());
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
