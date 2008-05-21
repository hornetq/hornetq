/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.tests.integration.core.asyncio.impl;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.TestCase;

import org.jboss.messaging.core.asyncio.AIOCallback;
import org.jboss.messaging.core.asyncio.impl.AsynchronousFileImpl;
import org.jboss.messaging.core.logging.Logger;

/**
 * 
 * you need to define -Djava.library.path=${project-root}/native/src/.libs when calling the JVM
 * If you are running this test in eclipse you should do:
 *   I - Run->Open Run Dialog
 *   II - Find the class on the list (you will find it if you already tried running this testcase before)  
 *   III - Add -Djava.library.path=<your project place>/native/src/.libs
 *   */
public class MultiThreadWriteNativeTest extends TestCase
{

   static Logger log = Logger.getLogger(MultiThreadWriteNativeTest.class);
   
   static AtomicInteger position = new AtomicInteger(0);
   
   String FILE_NAME="/tmp/libaio.log";
   
   static final int SIZE = 1024;
   static final int NUMBER_OF_THREADS = 40;
   static final int NUMBER_OF_LINES = 5000;
   
//   Executor exec
   
   static Executor executor = Executors.newSingleThreadExecutor();
   
   static Semaphore semaphore = new Semaphore(1, false);

   
   static class ExecClass implements Runnable
   {
       
       AsynchronousFileImpl aio;
       ByteBuffer buffer;
       AIOCallback callback;
       
       
       public ExecClass(AsynchronousFileImpl aio, ByteBuffer buffer, AIOCallback callback)
       {
           this.aio = aio;
           this.buffer = buffer;
           this.callback = callback;
       }

       public void run()
       {
           try
           {
               aio.write(getNewPosition()*SIZE, SIZE, buffer, callback);
               
           }
           catch (Exception e)
           {
               callback.onError(-1, e.toString());
               e.printStackTrace();
           }
           finally
           {
               try { semaphore.release(); } catch (Exception ignored){}
           }
       }
       
   }

   
   
   private static void addData(AsynchronousFileImpl aio, ByteBuffer buffer, AIOCallback callback) throws Exception
   {
       //aio.write(getNewPosition()*SIZE, SIZE, buffer, callback);
       executor.execute(new ExecClass(aio, buffer, callback));
       
       //semaphore.acquire();
       //try
       //{
           //aio.write(getNewPosition()*SIZE, SIZE, buffer, callback);
       //}
       //finally
       //{
       //    semaphore.release();
       //}
       
       
       
   }
   
   
   
   
   protected void setUp() throws Exception
   {
       super.setUp();
       if (!AsynchronousFileImpl.isLoaded())
       {
          fail(String.format("libAIO is not loaded on %s %s %s", 
                System.getProperty("os.name"), 
                System.getProperty("os.arch"), 
                System.getProperty("os.version")));
       }

       File file = new File(FILE_NAME);
       file.delete();
       
       position.set(0);
   }
   
   protected void tearDown() throws Exception
   {
      super.tearDown();
      
      assertEquals(0, AsynchronousFileImpl.getTotalMaxIO());
   }
   
   public void testMultipleASynchronousWrites() throws Throwable
   {
       executeTest(false);
   }
   
   public void testMultipleSynchronousWrites() throws Throwable
   {
       executeTest(true);
   }
   
   private void executeTest(boolean sync) throws Throwable
   {
       log.info(sync?"Sync test:":"Async test");
       AsynchronousFileImpl jlibAIO = new AsynchronousFileImpl();
       jlibAIO.open(FILE_NAME, 21000);
       try
       {
          log.debug("Preallocating file");
         
          jlibAIO.fill(0l, NUMBER_OF_THREADS,  SIZE * NUMBER_OF_LINES, (byte)0);
          log.debug("Done Preallocating file");
          
          CountDownLatch latchStart = new CountDownLatch (NUMBER_OF_THREADS + 1);
          
          ArrayList<ThreadProducer> list = new ArrayList<ThreadProducer>(NUMBER_OF_THREADS);
          for(int i=0;i<NUMBER_OF_THREADS;i++)
          {
              ThreadProducer producer = new ThreadProducer("Thread " + i, latchStart, jlibAIO, sync);
              list.add(producer);
              producer.start();
          }
          
          latchStart.countDown();
          latchStart.await();
          
          
          long startTime = System.currentTimeMillis();
          
   
          
          for (ThreadProducer producer: list)
          {
              producer.join();
              if (producer.failed != null)
              {
                  throw producer.failed;
              }
          }
          long endTime = System.currentTimeMillis();
          
          log.debug((sync?"Sync result:":"Async result:") + " Records/Second = " + (NUMBER_OF_THREADS * NUMBER_OF_LINES * 1000 / (endTime - startTime)) + " total time = " + (endTime - startTime) + " total number of records = " + (NUMBER_OF_THREADS * NUMBER_OF_LINES));
       }
       finally
       {
          jlibAIO.close();
       }
       
   }
   
   

   
   private static int getNewPosition()
   {
       return position.addAndGet(1);
   }
   
   static class ThreadProducer extends Thread
   {

       Throwable failed = null;
       CountDownLatch latchStart;
       boolean sync;
       AsynchronousFileImpl libaio;

       public ThreadProducer(String name, CountDownLatch latchStart, AsynchronousFileImpl libaio, boolean sync)
       {
           super(name);
           this.latchStart = latchStart;
           this.libaio = libaio;
           this.sync = sync;
       }
       
       public void run()
       {
           super.run();
           
           
           try
           {
               
               ByteBuffer buffer = libaio.newBuffer(SIZE);

               // I'm aways reusing the same buffer, as I don't want any noise from malloc on the measurement
               // Encoding buffer
               addString ("Thread name=" + Thread.currentThread().getName() + ";" + "\n", buffer);
               for (int local = buffer.position(); local < buffer.capacity() - 1; local++)
               {
                   buffer.put((byte)' ');
               }
               buffer.put((byte)'\n');


               latchStart.countDown();
               latchStart.await();
               
               long startTime = System.currentTimeMillis();
               
               
               CountDownLatch latchFinishThread = null;
               
               if (!sync) latchFinishThread = new CountDownLatch(NUMBER_OF_LINES);

               LinkedList<LocalCallback> list = new LinkedList<LocalCallback>();
               
               for (int i=0;i<NUMBER_OF_LINES;i++)
               {
                
                   if (sync) latchFinishThread = new CountDownLatch(1);
                   LocalCallback callback = new LocalCallback(latchFinishThread, buffer, libaio);
                   if (!sync) list.add(callback);
                   addData(libaio, buffer,callback);
                   if (sync)
                   {
                       latchFinishThread.await();
                       assertTrue(callback.doneCalled);
                       assertFalse(callback.errorCalled);
                   }
               }
               if (!sync) latchFinishThread.await();
               for (LocalCallback callback: list)
               {
                   assertTrue (callback.doneCalled);
                   assertFalse (callback.errorCalled);
               }
               
               long endtime = System.currentTimeMillis();
               
               log.debug(Thread.currentThread().getName() + " Rec/Sec= " + (NUMBER_OF_LINES * 1000 / (endtime-startTime)) + " total time = " + (endtime-startTime) + " number of lines=" + NUMBER_OF_LINES);
               
               libaio.destroyBuffer(buffer);
               
               
               for (LocalCallback callback: list)
               {
                   assertTrue (callback.doneCalled);
                   assertFalse (callback.errorCalled);
               }
               
           }
           catch (Throwable e)
           {
               e.printStackTrace();
               failed = e;
           }
           
       }
   }
   
   private static void addString(String str, ByteBuffer buffer)
   {
       byte bytes[] = str.getBytes();
       //buffer.putInt(bytes.length);
       buffer.put(bytes);
       //CharBuffer charBuffer = CharBuffer.wrap(str);
       //UTF_8_ENCODER.encode(charBuffer, buffer, true);
       
   }
   
   static class LocalCallback implements AIOCallback
   {
       boolean doneCalled = false;
       boolean errorCalled = false;
       CountDownLatch latchDone;
       ByteBuffer releaseMe;
       AsynchronousFileImpl libaio;
       
       public LocalCallback(CountDownLatch latchDone, ByteBuffer releaseMe, AsynchronousFileImpl libaio)
       {
           this.latchDone = latchDone;
           this.releaseMe = releaseMe;
           this.libaio = libaio;
       }
       
       public void done()
       {
           doneCalled=true;
           latchDone.countDown();
           //libaio.destroyBuffer(releaseMe);
       }

       public void onError(int errorCode, String errorMessage)
       {
           errorCalled=true;
           latchDone.countDown();
           libaio.destroyBuffer(releaseMe);
       }
       
   }
}
