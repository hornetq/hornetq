/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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


package org.jboss.messaging.tests.unit.util.timedbuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.messaging.core.asyncio.AIOCallback;
import org.jboss.messaging.core.asyncio.impl.TimedBuffer;
import org.jboss.messaging.core.asyncio.impl.TimedBufferObserver;
import org.jboss.messaging.tests.util.UnitTestCase;

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
   
   AIOCallback dummyCallback = new AIOCallback()
   {

      public void done()
      {
      }

      public void onError(int errorCode, String errorMessage)
      {
      }
   };

   
   public void testFillBuffer()
   {
      final ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();
      final AtomicInteger flushTimes = new AtomicInteger(0);
      class TestObserver implements TimedBufferObserver
      {
         //TODO: fix the test
         public void flushBuffer(ByteBuffer buffer, List<AIOCallback> callbacks)
         {
            buffers.add(buffer);
            flushTimes.incrementAndGet();
         }

         /* (non-Javadoc)
          * @see org.jboss.messaging.utils.timedbuffer.TimedBufferObserver#newBuffer(int, int)
          */
         public ByteBuffer newBuffer(int minSize, int maxSize)
         {
            return ByteBuffer.allocate(maxSize);
         }

         public int getRemainingBytes()
         {
            return 1024*1024;
         }
      }
      
      TimedBuffer timedBuffer = new TimedBuffer(new TestObserver(), 100, 3600 * 1000); // Any big timeout
      
      int x = 0;
      for (int i = 0 ; i < 10; i++)
      {
         ByteBuffer record = ByteBuffer.allocate(10);
         for (int j = 0 ; j < 10; j++)
         {
            record.put((byte)getSamplebyte(x++));
         }
         
         timedBuffer.checkSize(10);
         record.rewind();
         timedBuffer.addBytes(record, false, dummyCallback);
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
