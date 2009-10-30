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

package org.hornetq.tests.unit.core.asyncio;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestSuite;

import org.hornetq.core.asyncio.AIOCallback;
import org.hornetq.core.asyncio.BufferCallback;
import org.hornetq.core.asyncio.impl.AsynchronousFileImpl;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.utils.HornetQThreadFactory;

/**
 * 
 * you need to define -Djava.library.path=${project-root}/native/src/.libs when calling the JVM
 * If you are running this test in eclipse you should do:
 *   I - Run->Open Run Dialog
 *   II - Find the class on the list (you will find it if you already tried running this testcase before)  
 *   III - Add -Djava.library.path=<your project place>/native/src/.libs
 *  @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>.
 *   */
public class AsynchronousFileTest extends AIOTestBase
{
   
   public static TestSuite suite()
   {
      return createAIOTestSuite(AsynchronousFileTest.class);
   }

   private static final Logger log = Logger.getLogger(AsynchronousFileTest.class);

   private static CharsetEncoder UTF_8_ENCODER = Charset.forName("UTF-8").newEncoder();

   byte commonBuffer[] = null;
   
   ExecutorService executor;
   
   ExecutorService pollerExecutor;


   private static void debug(final String msg)
   {
      log.debug(msg);
   }

   
   
   protected void setUp() throws Exception
   {
      super.setUp();
      pollerExecutor = Executors.newCachedThreadPool(new HornetQThreadFactory("HornetQ-AIO-poller-pool" + System.identityHashCode(this), false));
      executor = Executors.newSingleThreadExecutor();
   }
   
   protected void tearDown() throws Exception
   {
      executor.shutdown();
      pollerExecutor.shutdown();
      super.tearDown();
   }
   
   /** 
    * Opening and closing a file immediately can lead to races on the native layer,
    * creating crash conditions.
    * */
   public void testOpenClose() throws Exception
   {
      final AsynchronousFileImpl controller = new AsynchronousFileImpl(executor, pollerExecutor);
      for (int i = 0; i < 1000; i++)
      {
         controller.open(FILE_NAME, 10000);
         controller.close();

      }
   }

   public void testFileNonExistent() throws Exception
   {
      final AsynchronousFileImpl controller = new AsynchronousFileImpl(executor, pollerExecutor);
      for (int i = 0; i < 1000; i++)
      {
         try
         {
            controller.open("/non-existent/IDontExist.error", 10000);
            fail("Exception expected! The test could create a file called /non-existent/IDontExist.error when it was supposed to fail.");
         }
         catch (Throwable ignored)
         {
         }
         try
         {
            controller.close();
            fail("Supposed to throw exception as the file wasn't opened");
         }
         catch (Throwable ignored)
         {
         }

      }
   }

   /**
    * This test is validating if the AIO layer can open two different
    * simultaneous files without loose any callbacks. This test made the native
    * layer to crash at some point during development
    */
   public void testTwoFiles() throws Exception
   {
      final AsynchronousFileImpl controller = new AsynchronousFileImpl(executor, pollerExecutor);
      final AsynchronousFileImpl controller2 = new AsynchronousFileImpl(executor, pollerExecutor);
      controller.open(FILE_NAME + ".1", 10000);
      controller2.open(FILE_NAME + ".2", 10000);

      int numberOfLines = 1000;
      int size = 1024;

      ByteBuffer buffer = null;
      try
      {
         CountDownLatch latchDone = new CountDownLatch(numberOfLines);
         CountDownLatch latchDone2 = new CountDownLatch(numberOfLines);

         buffer = AsynchronousFileImpl.newBuffer(size);
         encodeBufer(buffer);

         preAlloc(controller, numberOfLines * size);
         preAlloc(controller2, numberOfLines * size);

         ArrayList<CountDownCallback> list = new ArrayList<CountDownCallback>();
         ArrayList<CountDownCallback> list2 = new ArrayList<CountDownCallback>();

         for (int i = 0; i < numberOfLines; i++)
         {
            list.add(new CountDownCallback(latchDone));
            list2.add(new CountDownCallback(latchDone2));
         }

         long valueInitial = System.currentTimeMillis();

         int counter = 0;
         Iterator<CountDownCallback> iter2 = list2.iterator();

         for (CountDownCallback tmp : list)
         {
            CountDownCallback tmp2 = iter2.next();

            controller.write(counter * size, size, buffer, tmp);
            controller.write(counter * size, size, buffer, tmp2);
            ++counter;

         }

         latchDone.await();
         latchDone2.await();

         for (CountDownCallback callback : list)
         {
            assertEquals(1, callback.timesDoneCalled.get());
            assertTrue(callback.doneCalled);
            assertFalse(callback.errorCalled);
         }

         for (CountDownCallback callback : list2)
         {
            assertEquals(1, callback.timesDoneCalled.get());
            assertTrue(callback.doneCalled);
            assertFalse(callback.errorCalled);
         }

         controller.close();
      }
      finally
      {
         AsynchronousFileImpl.destroyBuffer(buffer);
         try
         {
            controller.close();
         }
         catch (Exception ignored)
         {
         }
         try
         {
            controller2.close();
         }
         catch (Exception ignored)
         {
         }
      }
   }

   public void testAddBeyongSimultaneousLimit() throws Exception
   {
      asyncData(3000, 1024, 10);
   }

   public void testAddAsyncData() throws Exception
   {
      asyncData(10000, 1024, 30000);
   }

   public void testInvalidReads() throws Exception
   {
      class LocalCallback implements AIOCallback
      {
         private final CountDownLatch latch = new CountDownLatch(1);

         volatile boolean error;

         public void done()
         {
            latch.countDown();
         }

         public void onError(final int errorCode, final String errorMessage)
         {
            error = true;
            latch.countDown();
         }
      }

      AsynchronousFileImpl controller = new AsynchronousFileImpl(executor, pollerExecutor);
      ByteBuffer buffer = null;
      try
      {

         final int SIZE = 512;

         controller.open(FILE_NAME, 10);
         controller.close();

         controller = new AsynchronousFileImpl(executor, pollerExecutor);

         controller.open(FILE_NAME, 10);

         controller.fill(0, 1, 512, (byte)'j');

         buffer = AsynchronousFileImpl.newBuffer(SIZE);

         buffer.clear();

         for (int i = 0; i < SIZE; i++)
         {
            buffer.put((byte)(i % 100));
         }

         LocalCallback callbackLocal = new LocalCallback();

         controller.write(0, 512, buffer, callbackLocal);

         callbackLocal.latch.await();

         ByteBuffer newBuffer = ByteBuffer.allocateDirect(50);

         callbackLocal = new LocalCallback();

         controller.read(0, 50, newBuffer, callbackLocal);

         callbackLocal.latch.await();

         // assertTrue(callbackLocal.error);

         callbackLocal = new LocalCallback();

         byte bytes[] = new byte[512];

         try
         {
            newBuffer = ByteBuffer.wrap(bytes);

            controller.read(0, 512, newBuffer, callbackLocal);

            fail("An exception was supposed to be thrown");
         }
         catch (HornetQException ignored)
         {
         }

         newBuffer = AsynchronousFileImpl.newBuffer(512);
         callbackLocal = new LocalCallback();
         controller.read(0, 512, newBuffer, callbackLocal);
         callbackLocal.latch.await();
         assertFalse(callbackLocal.error);

         newBuffer.rewind();

         byte[] bytesRead = new byte[SIZE];

         newBuffer.get(bytesRead);

         for (int i = 0; i < SIZE; i++)
         {
            assertEquals((byte)(i % 100), bytesRead[i]);
         }
      }
      finally
      {
         AsynchronousFileImpl.destroyBuffer(buffer);
         
         try
         {
            controller.close();
         }
         catch (HornetQException ignored)
         {
         }

      }

   }

   public void testBufferCallbackUniqueBuffers() throws Exception
   {
      boolean closed = false;
      final AsynchronousFileImpl controller = new AsynchronousFileImpl(executor, pollerExecutor);
      try
      {
         final int NUMBER_LINES = 1000;
         final int SIZE = 512;

         controller.open(FILE_NAME, 1000);

         controller.fill(0, 1, NUMBER_LINES * SIZE, (byte)'j');

         final ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();

         BufferCallback bufferCallback = new BufferCallback()
         {
            public void bufferDone(ByteBuffer buffer)
            {
               buffers.add(buffer);
            }
         };

         controller.setBufferCallback(bufferCallback);

         CountDownLatch latch = new CountDownLatch(NUMBER_LINES);
         CountDownCallback aio = new CountDownCallback(latch);
         for (int i = 0; i < NUMBER_LINES; i++)
         {
            ByteBuffer buffer = AsynchronousFileImpl.newBuffer(SIZE);
            buffer.rewind();
            for (int j = 0; j < SIZE; j++)
            {
               buffer.put((byte)(j % Byte.MAX_VALUE));
            }
            controller.write(i * SIZE, SIZE, buffer, aio);
         }

         // The buffer callback is only called after the complete callback was
         // called.
         // Because of that a race could happen on the assertions to
         // buffers.size what would invalidate the test
         // We close the file and that would guarantee the buffer callback was
         // called for all the elements
         controller.close();
         closed = true;

         assertEquals(NUMBER_LINES, buffers.size());

         // Make sure all the buffers are unique
         ByteBuffer lineOne = null;
         for (ByteBuffer bufferTmp : buffers)
         {
            if (lineOne == null)
            {
               lineOne = bufferTmp;
            }
            else
            {
               assertTrue(lineOne != bufferTmp);
            }
         }

         for (ByteBuffer bufferTmp : buffers)
         {
            AsynchronousFileImpl.destroyBuffer(bufferTmp);
         }

         buffers.clear();

      }
      finally
      {
         if (!closed)
         {
            controller.close();
         }
      }
   }

   public void testBufferCallbackAwaysSameBuffer() throws Exception
   {
      boolean closed = false;
      final AsynchronousFileImpl controller = new AsynchronousFileImpl(executor, pollerExecutor);
      ByteBuffer buffer = null;
      try
      {
         final int NUMBER_LINES = 1000;
         final int SIZE = 512;

         controller.open(FILE_NAME, 1000);

         controller.fill(0, 1, NUMBER_LINES * SIZE, (byte)'j');

         final ArrayList<ByteBuffer> buffers = new ArrayList<ByteBuffer>();

         BufferCallback bufferCallback = new BufferCallback()
         {
            public void bufferDone(ByteBuffer buffer)
            {
               buffers.add(buffer);
            }
         };

         controller.setBufferCallback(bufferCallback);

         CountDownLatch latch = new CountDownLatch(NUMBER_LINES);
         CountDownCallback aio = new CountDownCallback(latch);

         buffer = AsynchronousFileImpl.newBuffer(SIZE);
         buffer.rewind();
         for (int j = 0; j < SIZE; j++)
         {
            buffer.put((byte)(j % Byte.MAX_VALUE));
         }

         for (int i = 0; i < NUMBER_LINES; i++)
         {
            controller.write(i * SIZE, SIZE, buffer, aio);
         }

         // The buffer callback is only called after the complete callback was
         // called.
         // Because of that a race could happen on the assertions to
         // buffers.size what would invalidate the test
         // We close the file and that would guarantee the buffer callback was
         // called for all the elements
         controller.close();
         closed = true;

         assertEquals(NUMBER_LINES, buffers.size());

         // Make sure all the buffers are unique
         ByteBuffer lineOne = null;
         for (ByteBuffer bufferTmp : buffers)
         {
            if (lineOne == null)
            {
               lineOne = bufferTmp;
            }
            else
            {
               assertTrue(lineOne == bufferTmp);
            }
         }

         buffers.clear();

      }
      finally
      {
         AsynchronousFileImpl.destroyBuffer(buffer);
         if (!closed)
         {
            controller.close();
         }
      }
   }

   public void testRead() throws Exception
   {
      final AsynchronousFileImpl controller = new AsynchronousFileImpl(executor, pollerExecutor);
      controller.setBufferCallback(new BufferCallback()
      {

         public void bufferDone(ByteBuffer buffer)
         {
            AsynchronousFileImpl.destroyBuffer(buffer);
         }

      });

      ByteBuffer readBuffer = null;
      try
      {

         final int NUMBER_LINES = 1000;
         final int SIZE = 1024;

         controller.open(FILE_NAME, 1000);

         controller.fill(0, 1, NUMBER_LINES * SIZE, (byte)'j');

         {
            CountDownLatch latch = new CountDownLatch(NUMBER_LINES);
            CountDownCallback aio = new CountDownCallback(latch);

            for (int i = 0; i < NUMBER_LINES; i++)
            {
               if (i % 100 == 0)
               {
                  System.out.println("Wrote " + i + " lines");
               }
               ByteBuffer buffer = AsynchronousFileImpl.newBuffer(SIZE);
               for (int j = 0; j < SIZE; j++)
               {
                  buffer.put(getSamplebyte(j));
               }

               controller.write(i * SIZE, SIZE, buffer, aio);
            }

            latch.await();
            assertFalse(aio.errorCalled);
            assertEquals(NUMBER_LINES, aio.timesDoneCalled.get());
         }

         // If you call close you're supposed to wait events to finish before
         // closing it
         controller.close();
         controller.setBufferCallback(null);

         controller.open(FILE_NAME, 10);

         readBuffer = AsynchronousFileImpl.newBuffer(SIZE);

         for (int i = 0; i < NUMBER_LINES; i++)
         {
            if (i % 100 == 0)
            {
               System.out.println("Read " + i + " lines");
            }
            AsynchronousFileImpl.clearBuffer(readBuffer);

            CountDownLatch latch = new CountDownLatch(1);
            CountDownCallback aio = new CountDownCallback(latch);

            controller.read(i * SIZE, SIZE, readBuffer, aio);

            latch.await();
            assertFalse(aio.errorCalled);
            assertTrue(aio.doneCalled);

            byte bytesRead[] = new byte[SIZE];
            readBuffer.get(bytesRead);

            for (int count = 0; count < SIZE; count++)
            {
               assertEquals("byte position " + count + " differs on line " + i + " position = " + count,
                            getSamplebyte(count),
                            bytesRead[count]);
            }
         }
      }
      finally
      {
         if (readBuffer != null)
         {
            AsynchronousFileImpl.destroyBuffer(readBuffer);
         }
         try
         {
            controller.close();
         }
         catch (Throwable ignored)
         {
         }

      }

   }

   /** 
    *  This test will call file.close() when there are still callbacks being processed. 
    *  This could cause a crash or callbacks missing and this test is validating both situations.
    *  The file is also read after being written to validate its correctness */
   public void testConcurrentClose() throws Exception
   {
      final AsynchronousFileImpl controller = new AsynchronousFileImpl(executor, pollerExecutor);
      try
      {

         final int NUMBER_LINES = 1000;
         CountDownLatch readLatch = new CountDownLatch(NUMBER_LINES);
         final int SIZE = 1024;

         controller.open(FILE_NAME, 10000);

         controller.fill(0, 1, NUMBER_LINES * SIZE, (byte)'j');
         
         controller.setBufferCallback(new BufferCallback()
         {

            public void bufferDone(ByteBuffer buffer)
            {
               AsynchronousFileImpl.destroyBuffer(buffer);
            }
            
         });

         for (int i = 0; i < NUMBER_LINES; i++)
         {
            ByteBuffer buffer = AsynchronousFileImpl.newBuffer(SIZE);

            buffer.clear();
            addString("Str value " + i + "\n", buffer);
            for (int j = buffer.position(); j < buffer.capacity() - 1; j++)
            {
               buffer.put((byte)' ');
            }
            buffer.put((byte)'\n');

            CountDownCallback aio = new CountDownCallback(readLatch);
            controller.write(i * SIZE, SIZE, buffer, aio);
         }

         // If you call close you're supposed to wait events to finish before
         // closing it
         controller.close();
         
         controller.setBufferCallback(null);

         assertEquals(0, readLatch.getCount());
         readLatch.await();
         controller.open(FILE_NAME, 10);

         ByteBuffer newBuffer = AsynchronousFileImpl.newBuffer(SIZE);

         ByteBuffer buffer = AsynchronousFileImpl.newBuffer(SIZE);

         for (int i = 0; i < NUMBER_LINES; i++)
         {
            newBuffer.clear();
            addString("Str value " + i + "\n", newBuffer);
            for (int j = newBuffer.position(); j < newBuffer.capacity() - 1; j++)
            {
               newBuffer.put((byte)' ');
            }
            newBuffer.put((byte)'\n');

            CountDownLatch latch = new CountDownLatch(1);
            CountDownCallback aio = new CountDownCallback(latch);
            controller.read(i * SIZE, SIZE, buffer, aio);
            latch.await();
            assertFalse(aio.errorCalled);
            assertTrue(aio.doneCalled);

            byte bytesRead[] = new byte[SIZE];
            byte bytesCompare[] = new byte[SIZE];

            newBuffer.rewind();
            newBuffer.get(bytesCompare);
            buffer.rewind();
            buffer.get(bytesRead);

            for (int count = 0; count < SIZE; count++)
            {
               assertEquals("byte position " + count + " differs on line " + i, bytesCompare[count], bytesRead[count]);
            }

            assertTrue(buffer.equals(newBuffer));
         }
         
         AsynchronousFileImpl.destroyBuffer(newBuffer);
         AsynchronousFileImpl.destroyBuffer(buffer);

      }
      finally
      {
         try
         {
            controller.close();
         }
         catch (Throwable ignored)
         {
         }

      }
   }

   private void asyncData(final int numberOfLines, final int size, final int aioLimit) throws Exception
   {
      final AsynchronousFileImpl controller = new AsynchronousFileImpl(executor, pollerExecutor);
      controller.open(FILE_NAME, aioLimit);
      
      ByteBuffer buffer = null;
      
      try
      {
         CountDownLatch latchDone = new CountDownLatch(numberOfLines);

         buffer = AsynchronousFileImpl.newBuffer(size);
         encodeBufer(buffer);

         preAlloc(controller, numberOfLines * size);

         ArrayList<CountDownCallback> list = new ArrayList<CountDownCallback>();

         for (int i = 0; i < numberOfLines; i++)
         {
            list.add(new CountDownCallback(latchDone));
         }

         long valueInitial = System.currentTimeMillis();

         long lastTime = System.currentTimeMillis();
         int counter = 0;
         for (CountDownCallback tmp : list)
         {
            controller.write(counter * size, size, buffer, tmp);
            if (++counter % 20000 == 0)
            {
               debug(20000 * 1000 / (System.currentTimeMillis() - lastTime) + " rec/sec (Async)");
               lastTime = System.currentTimeMillis();
            }

         }

         latchDone.await();

         long timeTotal = System.currentTimeMillis() - valueInitial;
         debug("After completions time = " + timeTotal +
               " for " +
               numberOfLines +
               " registers " +
               " size each line = " +
               size +
               ", Records/Sec=" +
               numberOfLines *
               1000 /
               timeTotal +
               " (Assynchronous)");

         for (CountDownCallback tmp : list)
         {
            assertEquals(1, tmp.timesDoneCalled.get());
            assertTrue(tmp.doneCalled);
            assertFalse(tmp.errorCalled);
         }

         controller.close();
      }
      finally
      {
         AsynchronousFileImpl.destroyBuffer(buffer);
         try
         {
            controller.close();
         }
         catch (Exception ignored)
         {
         }
      }

   }

   public void testDirectSynchronous() throws Exception
   {
      ByteBuffer buffer = null;
      try
      {
         final int NUMBER_LINES = 3000;
         final int SIZE = 1024;

         final AsynchronousFileImpl controller = new AsynchronousFileImpl(executor, pollerExecutor);
         controller.open(FILE_NAME, 2000);

         buffer = AsynchronousFileImpl.newBuffer(SIZE);
         encodeBufer(buffer);

         preAlloc(controller, NUMBER_LINES * SIZE);

         long startTime = System.currentTimeMillis();

         for (int i = 0; i < NUMBER_LINES; i++)
         {
            CountDownLatch latchDone = new CountDownLatch(1);
            CountDownCallback aioBlock = new CountDownCallback(latchDone);
            controller.write(i * 512, 512, buffer, aioBlock);
            latchDone.await();
            assertTrue(aioBlock.doneCalled);
            assertFalse(aioBlock.errorCalled);
         }

         long timeTotal = System.currentTimeMillis() - startTime;
         debug("time = " + timeTotal +
               " for " +
               NUMBER_LINES +
               " registers " +
               " size each line = " +
               SIZE +
               " Records/Sec=" +
               NUMBER_LINES *
               1000 /
               timeTotal +
               " Synchronous");

         controller.close();
      }
      catch (Exception e)
      {
         throw e;
      }
      finally
      {
         AsynchronousFileImpl.destroyBuffer(buffer);
      }

   }

   public void testInvalidWrite() throws Exception
   {
      final AsynchronousFileImpl controller = new AsynchronousFileImpl(executor, pollerExecutor);
      controller.open(FILE_NAME, 2000);
      
      ByteBuffer buffer = null;
      
      try
      {
         final int SIZE = 512;

         buffer = AsynchronousFileImpl.newBuffer(SIZE);
         encodeBufer(buffer);

         preAlloc(controller, 10 * 512);

         CountDownLatch latchDone = new CountDownLatch(1);

         CountDownCallback aioBlock = new CountDownCallback(latchDone);
         controller.write(11, 512, buffer, aioBlock);

         latchDone.await();

         assertTrue(aioBlock.errorCalled);
         assertFalse(aioBlock.doneCalled);

      }
      catch (Exception e)
      {
         throw e;
      }
      finally
      {
         AsynchronousFileImpl.destroyBuffer(buffer);
         controller.close();
      }

   }

   public void testInvalidAlloc() throws Exception
   {
      try
      {
         @SuppressWarnings("unused")
         ByteBuffer buffer = AsynchronousFileImpl.newBuffer(300);
         fail("Exception expected");
      }
      catch (Exception ignored)
      {
      }

   }
   
   // This is in particular testing for http://bugs.sun.com/view_bug.do?bug_id=6791815
   public void testAllocations() throws Exception
   {
      final AtomicInteger errors = new AtomicInteger(0);
 
      Thread ts[] = new Thread[100];

      final CountDownLatch align = new CountDownLatch(ts.length);
      final CountDownLatch start = new CountDownLatch(1);

      for (int i = 0; i < ts.length; i++)
      {
         ts[i] = new Thread()
         {
            public void run()
            {
               try
               {
                  align.countDown();
                  start.await();
                  for (int i = 0; i < 1000; i++)
                  {
                     ByteBuffer buffer = AsynchronousFileImpl.newBuffer(512);
                     AsynchronousFileImpl.destroyBuffer(buffer);
                  }
               }
               catch (Throwable e)
               {
                  e.printStackTrace();
                  errors.incrementAndGet();
               }
            }
         };
         ts[i].start();
      }
      
      align.await();
      start.countDown();
      
      for (Thread t: ts)
      {
         t.join();
      }
      
      assertEquals(0, errors.get());
   }


   public void testSize() throws Exception
   {
      final AsynchronousFileImpl controller = new AsynchronousFileImpl(executor, pollerExecutor);

      final int NUMBER_LINES = 10;
      final int SIZE = 1024;

      controller.open(FILE_NAME, 1);

      controller.fill(0, 1, NUMBER_LINES * SIZE, (byte)'j');

      assertEquals(NUMBER_LINES * SIZE, controller.size());

      controller.close();

   }

   private void addString(final String str, final ByteBuffer buffer)
   {
      CharBuffer charBuffer = CharBuffer.wrap(str);
      UTF_8_ENCODER.encode(charBuffer, buffer, true);

   }

}
