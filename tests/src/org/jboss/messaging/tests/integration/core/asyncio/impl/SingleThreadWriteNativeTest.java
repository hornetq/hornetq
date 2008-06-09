/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.tests.integration.core.asyncio.impl;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;

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
public class SingleThreadWriteNativeTest extends AIOTestBase
{
   
   private static final Logger log = Logger
         .getLogger(SingleThreadWriteNativeTest.class);
   
   private static CharsetEncoder UTF_8_ENCODER = Charset.forName("UTF-8")
         .newEncoder();
   
   byte commonBuffer[] = null;
   
   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
   }
   
   protected void tearDown() throws Exception
   {
      super.tearDown();
   }
   
   /** 
    * Opening and closing a file immediately can lead to races on the native layer,
    * creating crash conditions.
    * */
   public void testOpenClose() throws Exception
   {
      final AsynchronousFileImpl controller = new AsynchronousFileImpl();
      for (int i = 0; i < 1000; i++)
      {
         controller.open(FILE_NAME, 10000, 1200);
         controller.close();
         
      }
   }
   
   /**
    * This test is validating if the AIO layer can open two different
    * simultaneous files without loose any callbacks. This test made the native
    * layer to crash at some point during development
    */
   public void testTwoFiles() throws Exception
   {
      final AsynchronousFileImpl controller = new AsynchronousFileImpl();
      final AsynchronousFileImpl controller2 = new AsynchronousFileImpl();
      controller.open(FILE_NAME + ".1", 10000, 1200);
      controller2.open(FILE_NAME + ".2", 10000, 1200);
      
      int numberOfLines = 1000;
      int size = 1024;
      
      try
      {
         log.info("++testDirectDataNoPage");
         CountDownLatch latchDone = new CountDownLatch(numberOfLines);
         CountDownLatch latchDone2 = new CountDownLatch(numberOfLines);
         
         ByteBuffer block = controller.newBuffer(size);
         encodeBufer(block);
         
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
         
         long lastTime = System.currentTimeMillis();
         int counter = 0;
         Iterator<CountDownCallback> iter2 = list2.iterator();
         
         for (CountDownCallback tmp : list)
         {
            CountDownCallback tmp2 = iter2.next();
            
            controller.write(counter * size, size, block, tmp);
            controller.write(counter * size, size, block, tmp2);
            if (++counter % 5000 == 0)
            {
               log.info(5000 * 1000 / (System.currentTimeMillis() - lastTime)
                     + " rec/sec (Async)");
               lastTime = System.currentTimeMillis();
            }
            
         }
         
         long timeTotal = System.currentTimeMillis() - valueInitial;
         
         log.info("Asynchronous time = " + timeTotal + " for " + numberOfLines
               + " registers " + " size each line = " + size + " Records/Sec="
               + (numberOfLines * 1000 / timeTotal) + " (Assynchronous)");
         
         latchDone.await();
         latchDone2.await();
         
         timeTotal = System.currentTimeMillis() - valueInitial;
         log.info("After completions time = " + timeTotal + " for "
               + numberOfLines + " registers " + " size each line = " + size
               + " Records/Sec=" + (numberOfLines * 1000 / timeTotal)
               + " (Assynchronous)");
         
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
         
         controller.destroyBuffer(block);
         
         controller.close();
      } finally
      {
         try
         {
            controller.close();
         } catch (Exception ignored)
         {
         }
         try
         {
            controller2.close();
         } catch (Exception ignored)
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
         private CountDownLatch latch = new CountDownLatch(1);
         
         volatile boolean error;
         
         public void done()
         {
            latch.countDown();
         }
         
         public void onError(int errorCode, String errorMessage)
         {
            this.error = true;
            latch.countDown();
         }
      }
      
      AsynchronousFileImpl controller = new AsynchronousFileImpl();
      try
      {
         
         final int SIZE = 512;
         
         controller.open(FILE_NAME, 10, 1200);
         controller.close();
         
         controller = new AsynchronousFileImpl();
         
         controller.open(FILE_NAME, 10, 1200);
         
         controller.fill(0, 1, 512, (byte) 'j');
         
         ByteBuffer buffer = controller.newBuffer(SIZE);
         
         buffer.clear();
         
         for (int i = 0; i < SIZE; i++)
         {
            buffer.put((byte) (i % 100));
         }
         
         LocalCallback callbackLocal = new LocalCallback();
         
         controller.write(0, 512, buffer, callbackLocal);
         
         callbackLocal.latch.await();
         
         ByteBuffer newBuffer = ByteBuffer.allocateDirect(50);
         
         callbackLocal = new LocalCallback();
         
         controller.read(0, 50, newBuffer, callbackLocal);
         
         callbackLocal.latch.await();
         
         //assertTrue(callbackLocal.error);
         
         callbackLocal = new LocalCallback();
         
         byte bytes[] = new byte[512];
         
         try
         {
            newBuffer = ByteBuffer.wrap(bytes);
            
            controller.read(0, 512, newBuffer, callbackLocal);
            
            fail("An exception was supposed to be thrown");
         } catch (Exception ignored)
         {
         }
         
         //newBuffer = ByteBuffer.allocateDirect(512);
         newBuffer = controller.newBuffer(512);
         callbackLocal = new LocalCallback();
         controller.read(0, 512, newBuffer, callbackLocal);
         callbackLocal.latch.await();
         assertFalse(callbackLocal.error);
         
         newBuffer.rewind();
         
         byte[] bytesRead = new byte[SIZE];
         
         newBuffer.get(bytesRead);
         
         for (int i = 0; i < SIZE; i++)
         {
            assertEquals((byte) (i % 100), bytesRead[i]);
         }
         
         controller.destroyBuffer(buffer);
      } finally
      {
         try
         {
            controller.close();
         } catch (Throwable ignored)
         {
         }
         
      }
      
   }
   
   public void testRead() throws Exception
   {
      final AsynchronousFileImpl controller = new AsynchronousFileImpl();
      try
      {
         
         final int NUMBER_LINES = 1000;
         final int SIZE = 1024;
         
         controller.open(FILE_NAME, 10, 1200);
         
         log.info("Filling file");
         
         controller.fill(0, 1, NUMBER_LINES * SIZE, (byte) 'j');
         
         ByteBuffer buffer = controller.newBuffer(SIZE);
         
         log.info("Writing file");
         
         for (int i = 0; i < NUMBER_LINES; i++)
         {
            buffer.clear();
            addString("Str value " + i + "\n", buffer);
            for (int j = buffer.position(); j < buffer.capacity() - 1; j++)
            {
               buffer.put((byte) ' ');
            }
            buffer.put((byte) '\n');
            
            CountDownLatch latch = new CountDownLatch(1);
            CountDownCallback aio = new CountDownCallback(latch);
            controller.write(i * SIZE, SIZE, buffer, aio);
            latch.await();
            assertFalse(aio.errorCalled);
            assertTrue(aio.doneCalled);
         }
         
         // If you call close you're supposed to wait events to finish before closing it
         log.info("Closing file");
         controller.close();
         log.info("Reading file");
         controller.open(FILE_NAME, 10, 1200);
         
         ByteBuffer newBuffer = ByteBuffer.allocateDirect(SIZE);
         
         for (int i = 0; i < NUMBER_LINES; i++)
         {
            newBuffer.clear();
            addString("Str value " + i + "\n", newBuffer);
            for (int j = newBuffer.position(); j < newBuffer.capacity() - 1; j++)
            {
               newBuffer.put((byte) ' ');
            }
            newBuffer.put((byte) '\n');
            
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
               assertEquals("byte position " + count + " differs on line " + i,
                     bytesCompare[count], bytesRead[count]);
            }
            
            assertTrue(buffer.equals(newBuffer));
         }
         
         controller.destroyBuffer(buffer);
      } finally
      {
         try
         {
            controller.close();
         } catch (Throwable ignored)
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
      final AsynchronousFileImpl controller = new AsynchronousFileImpl();
      try
      {
         
         final int NUMBER_LINES = 1000;
         CountDownLatch readLatch = new CountDownLatch(NUMBER_LINES);
         final int SIZE = 1024;
         
         controller.open(FILE_NAME, 10000, 1200);
         
         log.info("Filling file");
         
         controller.fill(0, 1, NUMBER_LINES * SIZE, (byte) 'j');
         
         log.info("Writing file");
         
         for (int i = 0; i < NUMBER_LINES; i++)
         {
            ByteBuffer buffer = ByteBuffer.allocateDirect(SIZE);
            
            buffer.clear();
            addString("Str value " + i + "\n", buffer);
            for (int j = buffer.position(); j < buffer.capacity() - 1; j++)
            {
               buffer.put((byte) ' ');
            }
            buffer.put((byte) '\n');
            
            CountDownCallback aio = new CountDownCallback(readLatch);
            controller.write(i * SIZE, SIZE, buffer, aio);
         }
         
         long counter = readLatch.getCount();
         // If you call close you're supposed to wait events to finish before
         // closing it
         controller.close();
         log.info("Closed file with counter = " + counter);
         
         assertEquals(0, readLatch.getCount());
         readLatch.await();
         log.info("Reading file");
         controller.open(FILE_NAME, 10, 1200);
         
         ByteBuffer newBuffer = ByteBuffer.allocateDirect(SIZE);
         
         ByteBuffer buffer = ByteBuffer.allocateDirect(SIZE);
         
         for (int i = 0; i < NUMBER_LINES; i++)
         {
            newBuffer.clear();
            addString("Str value " + i + "\n", newBuffer);
            for (int j = newBuffer.position(); j < newBuffer.capacity() - 1; j++)
            {
               newBuffer.put((byte) ' ');
            }
            newBuffer.put((byte) '\n');
            
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
               assertEquals("byte position " + count + " differs on line " + i,
                     bytesCompare[count], bytesRead[count]);
            }
            
            assertTrue(buffer.equals(newBuffer));
         }
         
      } finally
      {
         try
         {
            controller.close();
         } catch (Throwable ignored)
         {
         }
         
      }
   }
   
   private void asyncData(int numberOfLines, int size, int aioLimit)
         throws Exception
   {
      final AsynchronousFileImpl controller = new AsynchronousFileImpl();
      controller.open(FILE_NAME, aioLimit, 1200);
      
      try
      {
         log.info("++testDirectDataNoPage");
         System.out.flush();
         CountDownLatch latchDone = new CountDownLatch(numberOfLines);
         
         ByteBuffer block = controller.newBuffer(size);
         encodeBufer(block);
         
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
            controller.write(counter * size, size, block, tmp);
            if (++counter % 20000 == 0)
            {
               log.info(20000 * 1000 / (System.currentTimeMillis() - lastTime)
                     + " rec/sec (Async)");
               lastTime = System.currentTimeMillis();
            }
            
         }
         
         System.out.print("waiting...");
         
         latchDone.await();
         
         log.info("done");
         
         long timeTotal = System.currentTimeMillis() - valueInitial;
         log.info("After completions time = " + timeTotal + " for "
               + numberOfLines + " registers " + " size each line = " + size
               + " Records/Sec=" + (numberOfLines * 1000 / timeTotal)
               + " (Assynchronous)");
         
         for (CountDownCallback tmp : list)
         {
            assertEquals(1, tmp.timesDoneCalled.get());
            assertTrue(tmp.doneCalled);
            assertFalse(tmp.errorCalled);
         }
         
         controller.destroyBuffer(block);
         
         controller.close();
      } finally
      {
         try
         {
            controller.close();
         } catch (Exception ignored)
         {
         }
      }
      
   }
   
   public void testDirectSynchronous() throws Exception
   {
      try
      {
         log.info("++testDirectDataNoPage");
         System.out.flush();
         final int NUMBER_LINES = 3000;
         final int SIZE = 1024;
         
         final AsynchronousFileImpl controller = new AsynchronousFileImpl();
         controller.open(FILE_NAME, 2000, 1200);
         
         ByteBuffer block = ByteBuffer.allocateDirect(SIZE);
         encodeBufer(block);
         
         preAlloc(controller, NUMBER_LINES * SIZE);
         
         long startTime = System.currentTimeMillis();
         
         for (int i = 0; i < NUMBER_LINES; i++)
         {
            CountDownLatch latchDone = new CountDownLatch(1);
            CountDownCallback aioBlock = new CountDownCallback(latchDone);
            controller.write(i * 512, 512, block, aioBlock);
            latchDone.await();
            assertTrue(aioBlock.doneCalled);
            assertFalse(aioBlock.errorCalled);
         }
         
         long timeTotal = System.currentTimeMillis() - startTime;
         log.info("time = " + timeTotal + " for " + NUMBER_LINES
               + " registers " + " size each line = " + SIZE + " Records/Sec="
               + (NUMBER_LINES * 1000 / timeTotal) + " Synchronous");
         
         controller.close();
      } catch (Exception e)
      {
         throw e;
      }
      
   }
 
//   disabled until http://jira.jboss.com/jira/browse/JBMESSAGING-1334 is done
//   public void testInvalidWrite() throws Exception
//   {
//      final AsynchronousFileImpl controller = new AsynchronousFileImpl();
//      controller.open(FILE_NAME, 2000, 120);
//      
//      try
//      {
//         
//         final int SIZE = 512;
//         
//         ByteBuffer block = controller.newBuffer(SIZE);
//         encodeBufer(block);
//         
//         preAlloc(controller, 1000 * 512);
//         
//         CountDownLatch latchDone = new CountDownLatch(1);
//         
//         CountDownCallback aioBlock = new CountDownCallback(latchDone);
//         controller.write(11, 512, block, aioBlock);
//         
//         latchDone.await();
//         
//         assertTrue(aioBlock.errorCalled);
//         assertFalse(aioBlock.doneCalled);
//         
//         controller.destroyBuffer(block);
//      } catch (Exception e)
//      {
//         throw e;
//      } finally
//      {
//         controller.close();
//      }
//      
//   }
   
   public void testInvalidAlloc() throws Exception
   {
      AsynchronousFileImpl controller = new AsynchronousFileImpl();
      try
      {
         ByteBuffer buffer = controller.newBuffer(300);
         fail("Exception expected");
      } catch (Exception ignored)
      {
      }
      
   }
   
   private void addString(String str, ByteBuffer buffer)
   {
      CharBuffer charBuffer = CharBuffer.wrap(str);
      UTF_8_ENCODER.encode(charBuffer, buffer, true);
      
   }
   
}
