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

package org.jboss.messaging.tests.unit.core.asyncio;

import org.jboss.messaging.core.asyncio.AIOCallback;
import org.jboss.messaging.core.asyncio.impl.AsynchronousFileImpl;
import org.jboss.messaging.core.logging.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

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

   static Logger log = Logger.getLogger(MultiThreadAsynchronousFileTest.class);

   AtomicInteger position = new AtomicInteger(0);

   static final int SIZE = 1024;

   static final int NUMBER_OF_THREADS = 10;

   static final int NUMBER_OF_LINES = 1000;

   // Executor exec

   Executor executor = Executors.newSingleThreadExecutor();

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      position.set(0);
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
      log.debug(sync ? "Sync test:" : "Async test");
      AsynchronousFileImpl jlibAIO = new AsynchronousFileImpl();
      jlibAIO.open(FILE_NAME, 21000);
      try
      {
         log.debug("Preallocating file");

         jlibAIO.fill(0l, NUMBER_OF_THREADS, SIZE * NUMBER_OF_LINES, (byte)0);
         log.debug("Done Preallocating file");

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

         log.debug((sync ? "Sync result:" : "Async result:") + " Records/Second = " +
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

         try
         {

            ByteBuffer buffer = libaio.newBuffer(SIZE);

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
               CountDownCallback callback = new CountDownCallback(latchFinishThread);
               if (!sync)
               {
                  list.add(callback);
               }
               addData(libaio, buffer, callback);
               if (sync)
               {
                  latchFinishThread.await();
                  assertTrue(callback.doneCalled);
                  assertFalse(callback.errorCalled);
               }
            }
            if (!sync)
            {
               latchFinishThread.await();
            }
            for (CountDownCallback callback : list)
            {
               assertTrue(callback.doneCalled);
               assertFalse(callback.errorCalled);
            }

            long endtime = System.currentTimeMillis();

            log.debug(Thread.currentThread().getName() + " Rec/Sec= " +
                      NUMBER_OF_LINES *
                      1000 /
                      (endtime - startTime) +
                      " total time = " +
                      (endtime - startTime) +
                      " number of lines=" +
                      NUMBER_OF_LINES);

            for (CountDownCallback callback : list)
            {
               assertTrue(callback.doneCalled);
               assertFalse(callback.errorCalled);
            }

         }
         catch (Throwable e)
         {
            e.printStackTrace();
            failed = e;
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
