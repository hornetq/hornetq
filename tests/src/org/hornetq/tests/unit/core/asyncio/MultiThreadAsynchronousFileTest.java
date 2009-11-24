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
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestSuite;

import org.hornetq.core.asyncio.AIOCallback;
import org.hornetq.core.asyncio.impl.AsynchronousFileImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.utils.HornetQThreadFactory;

/**
 * 
 * you need to define -Djava.library.path=${project-root}/native/src/.libs when calling the JVM
 * If you are running this test in eclipse you should do:
 *   I - Run->Open Run Dialog
 *   II - Find the class on the list (you will find it if you already tried running this testcase before)  
 *   III - Add -Djava.library.path=<your project place>/native/src/.libs
 *   
 *  @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>.
 *   */
public class MultiThreadAsynchronousFileTest extends AIOTestBase
{

   public static TestSuite suite()
   {
      return createAIOTestSuite(MultiThreadAsynchronousFileTest.class);
   }

   static Logger log = Logger.getLogger(MultiThreadAsynchronousFileTest.class);

   AtomicInteger position = new AtomicInteger(0);

   static final int SIZE = 1024;

   static final int NUMBER_OF_THREADS = 10;

   static final int NUMBER_OF_LINES = 1000;

   ExecutorService executor;

   ExecutorService pollerExecutor;

   private static void debug(final String msg)
   {
      log.debug(msg);
   }

   protected void setUp() throws Exception
   {
      super.setUp();
      pollerExecutor = Executors.newCachedThreadPool(new HornetQThreadFactory("HornetQ-AIO-poller-pool" + System.identityHashCode(this),
                                                                              false));
      executor = Executors.newSingleThreadExecutor();
   }

   protected void tearDown() throws Exception
   {
      executor.shutdown();
      pollerExecutor.shutdown();
      super.tearDown();
   }

   public void testMultipleASynchronousWrites() throws Throwable
   {
      executeTest(false);
   }

   public void testMultipleSynchronousWrites() throws Throwable
   {
      executeTest(true);
   }

   private void executeTest(final boolean sync) throws Throwable
   {
      debug(sync ? "Sync test:" : "Async test");
      AsynchronousFileImpl jlibAIO = new AsynchronousFileImpl(executor, pollerExecutor);
      jlibAIO.open(FILE_NAME, 21000);
      try
      {
         debug("Preallocating file");

         jlibAIO.fill(0l, NUMBER_OF_THREADS, SIZE * NUMBER_OF_LINES, (byte)0);
         debug("Done Preallocating file");

         CountDownLatch latchStart = new CountDownLatch(NUMBER_OF_THREADS + 1);

         ArrayList<ThreadProducer> list = new ArrayList<ThreadProducer>(NUMBER_OF_THREADS);
         for (int i = 0; i < NUMBER_OF_THREADS; i++)
         {
            ThreadProducer producer = new ThreadProducer("Thread " + i, latchStart, jlibAIO, sync);
            list.add(producer);
            producer.start();
         }

         latchStart.countDown();
         latchStart.await();

         long startTime = System.currentTimeMillis();

         for (ThreadProducer producer : list)
         {
            producer.join();
            if (producer.failed != null)
            {
               throw producer.failed;
            }
         }
         long endTime = System.currentTimeMillis();

         debug((sync ? "Sync result:" : "Async result:") + " Records/Second = " +
               NUMBER_OF_THREADS *
               NUMBER_OF_LINES *
               1000 /
               (endTime - startTime) +
               " total time = " +
               (endTime - startTime) +
               " total number of records = " +
               NUMBER_OF_THREADS *
               NUMBER_OF_LINES);
      }
      finally
      {
         jlibAIO.close();
      }

   }

   private int getNewPosition()
   {
      return position.addAndGet(1);
   }

   class ThreadProducer extends Thread
   {
      Throwable failed = null;

      CountDownLatch latchStart;

      boolean sync;

      AsynchronousFileImpl libaio;

      public ThreadProducer(final String name,
                            final CountDownLatch latchStart,
                            final AsynchronousFileImpl libaio,
                            final boolean sync)
      {
         super(name);
         this.latchStart = latchStart;
         this.libaio = libaio;
         this.sync = sync;
      }

      @Override
      public void run()
      {
         super.run();

         ByteBuffer buffer = null;

         synchronized (MultiThreadAsynchronousFileTest.class)
         {
            buffer = AsynchronousFileImpl.newBuffer(SIZE);
         }

         try
         {

            // I'm aways reusing the same buffer, as I don't want any noise from
            // malloc on the measurement
            // Encoding buffer
            addString("Thread name=" + Thread.currentThread().getName() + ";" + "\n", buffer);
            for (int local = buffer.position(); local < buffer.capacity() - 1; local++)
            {
               buffer.put((byte)' ');
            }
            buffer.put((byte)'\n');

            latchStart.countDown();
            latchStart.await();

            long startTime = System.currentTimeMillis();

            CountDownLatch latchFinishThread = null;

            if (!sync)
            {
               latchFinishThread = new CountDownLatch(NUMBER_OF_LINES);
            }

            LinkedList<CountDownCallback> list = new LinkedList<CountDownCallback>();

            for (int i = 0; i < NUMBER_OF_LINES; i++)
            {

               if (sync)
               {
                  latchFinishThread = new CountDownLatch(1);
               }
               CountDownCallback callback = new CountDownCallback(latchFinishThread, null, null, 0);
               if (!sync)
               {
                  list.add(callback);
               }
               addData(libaio, buffer, callback);
               if (sync)
               {
                  latchFinishThread.await();
                  assertTrue(callback.doneCalled);
                  assertFalse(callback.errorCalled != 0);
               }
            }
            if (!sync)
            {
               latchFinishThread.await();
            }

            for (CountDownCallback callback : list)
            {
               assertTrue(callback.doneCalled);
               assertFalse(callback.errorCalled != 0);
            }

            for (CountDownCallback callback : list)
            {
               assertTrue(callback.doneCalled);
               assertFalse(callback.errorCalled != 0);
            }

         }
         catch (Throwable e)
         {
            e.printStackTrace();
            failed = e;
         }
         finally
         {
            synchronized (MultiThreadAsynchronousFileTest.class)
            {
               AsynchronousFileImpl.destroyBuffer(buffer);
            }
         }

      }
   }

   private static void addString(final String str, final ByteBuffer buffer)
   {
      byte bytes[] = str.getBytes();
      buffer.put(bytes);
   }

   private void addData(final AsynchronousFileImpl aio, final ByteBuffer buffer, final AIOCallback callback) throws Exception
   {
      executor.execute(new WriteRunnable(aio, buffer, callback));
   }

   private class WriteRunnable implements Runnable
   {

      AsynchronousFileImpl aio;

      ByteBuffer buffer;

      AIOCallback callback;

      public WriteRunnable(final AsynchronousFileImpl aio, final ByteBuffer buffer, final AIOCallback callback)
      {
         this.aio = aio;
         this.buffer = buffer;
         this.callback = callback;
      }

      public void run()
      {
         try
         {
            aio.write(getNewPosition() * SIZE, SIZE, buffer, callback);

         }
         catch (Exception e)
         {
            callback.onError(-1, e.toString());
            e.printStackTrace();
         }
      }

   }

}
