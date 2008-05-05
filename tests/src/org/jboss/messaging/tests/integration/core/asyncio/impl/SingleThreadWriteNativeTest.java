/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.tests.integration.core.asyncio.impl;

import java.io.File;
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

import junit.framework.TestCase;

/**
 * 
 * you need to define -Djava.library.path=${project-root}/native/src/.libs when calling the JVM
 * If you are running this test in eclipse you should do:
 *   I - Run->Open Run Dialog
 *   II - Find the class on the list (you will find it if you already tried running this testcase before)  
 *   III - Add -Djava.library.path=<your project place>/native/src/.libs
 *   */
public class SingleThreadWriteNativeTest extends TestCase
{
   private static final Logger log = Logger.getLogger(SingleThreadWriteNativeTest.class);
   
   private static CharsetEncoder UTF_8_ENCODER = Charset.forName("UTF-8").newEncoder();
   
   
   byte commonBuffer[] = null; 
   
   String FILE_NAME="/tmp/libaio.log";
   
   
   @Override
   protected void setUp() throws Exception
   {
       super.setUp();
       LocalAIO.staticDone = 0;
       File file = new File(FILE_NAME);
       file.delete();
   }

   private void encodeBufer(ByteBuffer buffer)
   {
       buffer.clear();
       int size = buffer.limit();
       for (int i=0;i<size-1;i++)
       {
           buffer.put((byte)('a' + (i%20)));
       }
       
       buffer.put((byte)'\n');
       
   }

   public void testTwoFiles() throws Exception
   {
      final AsynchronousFileImpl controller = new AsynchronousFileImpl();
      final AsynchronousFileImpl controller2 = new AsynchronousFileImpl();
      controller.open(FILE_NAME + ".1", 10000);
      controller2.open(FILE_NAME + ".2", 10000);
      
      int numberOfLines = 100000;
      int size = 1024;
       
       try
       {
           System.out.println("++testDirectDataNoPage"); System.out.flush();
           CountDownLatch latchDone = new CountDownLatch(numberOfLines);
           CountDownLatch latchDone2 = new CountDownLatch(numberOfLines);
           
           ByteBuffer block = controller.newBuffer(size);
           encodeBufer(block);

           preAlloc(controller, numberOfLines * size);
           preAlloc(controller2, numberOfLines * size);

           ArrayList<LocalAIO> list = new ArrayList<LocalAIO>();
           ArrayList<LocalAIO> list2 = new ArrayList<LocalAIO>();
   
           for (int i=0; i<numberOfLines; i++)
           {
               list.add(new LocalAIO(latchDone));
               list2.add(new LocalAIO(latchDone2));
           }
           
          
           long valueInitial = System.currentTimeMillis();
   
           System.out.println("Adding data");
           
           long lastTime = System.currentTimeMillis();
           int counter = 0;
           Iterator<LocalAIO> iter2 = list2.iterator();
           
           for (LocalAIO tmp: list)
           {
               LocalAIO tmp2 = iter2.next();
               
               controller.write(counter * size, size, block, tmp);
               controller.write(counter * size, size, block, tmp2);
               if (++counter % 5000 == 0)
               {
                   System.out.println(5000*1000/(System.currentTimeMillis()-lastTime) + " rec/sec (Async)");
                   lastTime = System.currentTimeMillis();
               }
               
           }
           
           System.out.println("Data added " + (System.currentTimeMillis() - valueInitial));
           
           
           System.out.println("Finished append " + (System.currentTimeMillis() - valueInitial) + " received = " + LocalAIO.staticDone);
           System.out.println("Flush now");
           System.out.println("Received " + LocalAIO.staticDone);
           long timeTotal = System.currentTimeMillis() - valueInitial;

           System.out.println("Asynchronous time = " + timeTotal + " for " + numberOfLines + " registers " + " size each line = " + size  + " Records/Sec=" + (numberOfLines*1000/timeTotal) + " (Assynchronous)");

           latchDone.await();
           latchDone2.await();
   
           timeTotal = System.currentTimeMillis() - valueInitial;
           System.out.println("After completions time = " + timeTotal + " for " + numberOfLines + " registers " + " size each line = " + size  + " Records/Sec=" + (numberOfLines*1000/timeTotal) + " (Assynchronous)");
   
           for (LocalAIO tmp: list)
           {
               assertEquals(1, tmp.timesDoneCalled);
               assertTrue(tmp.doneCalled);
               assertFalse(tmp.errorCalled);
           }
           
           controller.destroyBuffer(block);
           
           controller.close();
       }
       finally
       {
           try {controller.close();} catch (Exception ignored){}
       }
       
       
   }
   
   public void testAnnoyingPoller() throws Exception
   {
      final AsynchronousFileImpl controller = new AsynchronousFileImpl();
      for (int i=0; i< 1000; i++)
      {
         controller.open(FILE_NAME, 10000);
         controller.close();
         
      }
   }
   

   public void testAddBeyongSimultaneousLimit() throws Exception
   {
       asyncData(150000,1024,100);
   }

   public void testAddAsyncData() throws Exception
   {
       asyncData(500000,1024,30000);
   }
   
   public void testValidateData() throws Exception
   {
      validateData(150000,1024,20000);
   }

   public void testInvalidReads() throws Exception
   {
      class LocalCallback implements AIOCallback
      {

         CountDownLatch latch = new CountDownLatch(1);
         boolean error;
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
          
          final int NUMBER_LINES = 1;
          final int SIZE = 512;
          
          controller.open(FILE_NAME, 10);
          controller.close();
          
          controller = new AsynchronousFileImpl();
          
          controller.open(FILE_NAME, 10);
          
          controller.fill(0,1, 512, (byte)'j');
          
          
          ByteBuffer buffer = controller.newBuffer(SIZE);
  
          
          buffer.clear();
          
          for (int i=0; i<SIZE; i++)
          {
              buffer.put((byte)(i%100));
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
          }
          catch (Exception ignored)
          {
          }
          
          //newBuffer = ByteBuffer.allocateDirect(512);
          newBuffer = controller.newBuffer(512);
          callbackLocal = new LocalCallback();
          controller.read(0, 512, newBuffer,callbackLocal);
          callbackLocal.latch.await();
          assertFalse(callbackLocal.error);
          
          newBuffer.rewind();
          
          byte[] bytesRead = new byte[SIZE];
          
          newBuffer.get(bytesRead);
          
          for (int i=0; i<SIZE;i++)
          {
             assertEquals((byte)(i%100), bytesRead[i]);
          }
          
          
          controller.destroyBuffer(buffer);
      }
      finally
      {
          try { controller.close(); } catch (Throwable ignored){}
          
      }
          
   }

   
   public void testRead() throws Exception
   {
       final AsynchronousFileImpl controller = new AsynchronousFileImpl();
       try
       {
           
           final int NUMBER_LINES = 1000;
           final int SIZE = 1024;
           
           controller.open(FILE_NAME, 10);

           log.info("Filling file");
           
           controller.fill(0,1, NUMBER_LINES * SIZE, (byte)'j');           
           
           ByteBuffer buffer = controller.newBuffer(SIZE);
   
           log.info("Writing file");

           for (int i=0; i<NUMBER_LINES; i++)
           {
               buffer.clear();
               addString ("Str value " + i + "\n", buffer);
               for (int j=buffer.position(); j<buffer.capacity()-1;j++)
               {
                   buffer.put((byte)' ');
               }
               buffer.put((byte)'\n');
               
               
               CountDownLatch latch = new CountDownLatch(1);
               LocalAIO aio = new LocalAIO(latch);
               controller.write(i * SIZE, SIZE, buffer, aio);
               latch.await();
               assertFalse(aio.errorCalled);
               assertTrue(aio.doneCalled);
           }
           

           // If you call close you're supposed to wait events to finish before closing it
           log.info("Closing file");
           controller.close();
           log.info("Reading file");
           controller.open(FILE_NAME, 10);
           
           ByteBuffer newBuffer = ByteBuffer.allocateDirect(SIZE);
           
           for (int i=0; i<NUMBER_LINES; i++)
           {
               newBuffer.clear();
               addString ("Str value " + i + "\n", newBuffer);
               for (int j=newBuffer.position(); j<newBuffer.capacity()-1;j++)
               {
                   newBuffer.put((byte)' ');
               }
               newBuffer.put((byte)'\n');
               
               
               CountDownLatch latch = new CountDownLatch(1);
               LocalAIO aio = new LocalAIO(latch);
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
               
               for (int count=0;count<SIZE;count++)
               {
                   assertEquals("byte position " + count + " differs on line " + i, bytesCompare[count], bytesRead[count]);
               }
               
               
               //byte[] byteCompare = new byte[SIZE];
               //byte[] byteRead = new byte[SIZE];

               assertTrue(buffer.equals(newBuffer));
           }
           
           controller.destroyBuffer(buffer);
       }
       finally
       {
           try { controller.close(); } catch (Throwable ignored){}
           
       }
           
   }
   
   
   
   public void testConcurrentClose() throws Exception
   {
      // The test might eventually pass if broken
      for (int i=0; i<10; i++)
         internalConcurrentClose();
   }
   
   public void internalConcurrentClose() throws Exception
   {
       final AsynchronousFileImpl controller = new AsynchronousFileImpl();
       try
       {
           
           final int NUMBER_LINES = 1000;
           CountDownLatch readLatch = new CountDownLatch (NUMBER_LINES);
           final int SIZE = 1024;
           
           controller.open(FILE_NAME, 10000);

           log.info("Filling file");
           
           controller.fill(0,1, NUMBER_LINES * SIZE, (byte)'j');           
           
           log.info("Writing file");

           for (int i=0; i<NUMBER_LINES; i++)
           {
               ByteBuffer buffer = ByteBuffer.allocateDirect(SIZE);
              
               buffer.clear();
               addString ("Str value " + i + "\n", buffer);
               for (int j=buffer.position(); j<buffer.capacity()-1;j++)
               {
                   buffer.put((byte)' ');
               }
               buffer.put((byte)'\n');
               
               
               LocalAIO aio = new LocalAIO(readLatch);
               controller.write(i * SIZE, SIZE, buffer, aio);
           }
           

           long counter = readLatch.getCount();
           // If you call close you're supposed to wait events to finish before closing it
           controller.close();
           log.info("Closed file with counter = " + counter);
           assertEquals(0, readLatch.getCount());
           readLatch.await();
           log.info("Reading file");
           controller.open(FILE_NAME, 10);
           
           ByteBuffer newBuffer = ByteBuffer.allocateDirect(SIZE);

           ByteBuffer buffer = ByteBuffer.allocateDirect(SIZE);
           
           for (int i=0; i<NUMBER_LINES; i++)
           {
               newBuffer.clear();
               addString ("Str value " + i + "\n", newBuffer);
               for (int j=newBuffer.position(); j<newBuffer.capacity()-1;j++)
               {
                   newBuffer.put((byte)' ');
               }
               newBuffer.put((byte)'\n');
               
               
               CountDownLatch latch = new CountDownLatch(1);
               LocalAIO aio = new LocalAIO(latch);
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
               
               for (int count=0;count<SIZE;count++)
               {
                   assertEquals("byte position " + count + " differs on line " + i, bytesCompare[count], bytesRead[count]);
               }
               
               
               //byte[] byteCompare = new byte[SIZE];
               //byte[] byteRead = new byte[SIZE];

               assertTrue(buffer.equals(newBuffer));
           }
           
       }
       finally
       {
           try { controller.close(); } catch (Throwable ignored){}
           
       }
           
   }
   
   /**
    * This method is not used unless you uncomment testValidateData
    * The purpose of this method is to verify if the information generated by one of the write methods is correct
    * @param numberOfLines
    * @param size
    * @param aioLimit
    * @throws Exception
    */
   private void validateData(int numberOfLines, int size, int aioLimit) throws Exception
   {
       final AsynchronousFileImpl controller = new AsynchronousFileImpl();
       controller.open(FILE_NAME, aioLimit);
       
       ByteBuffer compareBlock = ByteBuffer.allocateDirect(size);
       encodeBufer(compareBlock);
       
       ByteBuffer readBuffer = controller.newBuffer(size);
       
       
       boolean firstInvalid = false;
       for (int i=0;i<numberOfLines;i++)
       {
          if (i % 1000 == 0)
          {
             log.info("line = " + i);
          }
          CountDownLatch latch = new CountDownLatch(1);
          LocalAIO callback = new LocalAIO(latch);
          controller.read(i * size, size, readBuffer, callback);

          latch.await();
          
          if (!compareBuffers(compareBlock, readBuffer))
          {
             //log.info("Invalid line at " + i);
             firstInvalid=true;
          }
          else
          {
             if (firstInvalid)
             {
                for (int line=0;line<10;line++) log.info("*********************************************");
                log.warn("Valid line after an invalid line!!!");
             }
          }
          
          readBuffer.position(100);
          ByteBuffer buf1 = readBuffer.slice();
          
          //System.out.println("buf1=" + buf1);
          
          
          
       }
   }
   
   
   private boolean compareBuffers(ByteBuffer buffer1, ByteBuffer buffer2)
   {
      
      buffer1.rewind();
      buffer2.rewind();
      
      if (buffer1.limit() != buffer2.limit())
      {
         return false;
      }
      
      byte bytes1[] = new byte[buffer1.limit()];
      byte bytes2[] = new byte[buffer2.limit()];
      
      buffer1.get(bytes1);
      buffer2.get(bytes2);
      
      for (int i=0; i< bytes1.length; i++)
      {
         if (bytes1[i] != bytes2[i])
         {
            return false;
         }
      }
      
      return true;
   }
   
   private void asyncData(int numberOfLines, int size, int aioLimit) throws Exception
   {
       final AsynchronousFileImpl controller = new AsynchronousFileImpl();
       controller.open(FILE_NAME, aioLimit);
       
       try
       {
           System.out.println("++testDirectDataNoPage"); System.out.flush();
           CountDownLatch latchDone = new CountDownLatch(numberOfLines);
           
           ByteBuffer block = controller.newBuffer(size);
           encodeBufer(block);

           preAlloc(controller, numberOfLines * size);

           ArrayList<LocalAIO> list = new ArrayList<LocalAIO>();
   
           for (int i=0; i<numberOfLines; i++)
           {
               list.add(new LocalAIO(latchDone));
           }
           
          
           long valueInitial = System.currentTimeMillis();
   
           System.out.println("Adding data");
           
           long lastTime = System.currentTimeMillis();
           int counter = 0;
           for (LocalAIO tmp: list)
           {
               controller.write(counter * size, size, block, tmp);
               if (++counter % 20000 == 0)
               {
                   System.out.println(5000*1000/(System.currentTimeMillis()-lastTime) + " rec/sec (Async)");
                   lastTime = System.currentTimeMillis();
               }
               
           }
           
           System.out.println("Data added " + (System.currentTimeMillis() - valueInitial));
           
           
           System.out.println("Finished append " + (System.currentTimeMillis() - valueInitial) + " received = " + LocalAIO.staticDone);
           System.out.println("Flush now");
           System.out.println("Received " + LocalAIO.staticDone);
           long timeTotal = System.currentTimeMillis() - valueInitial;

           System.out.println("Asynchronous time = " + timeTotal + " for " + numberOfLines + " registers " + " size each line = " + size  + " Records/Sec=" + (numberOfLines*1000/timeTotal) + " (Assynchronous)");

           latchDone.await();
   
           timeTotal = System.currentTimeMillis() - valueInitial;
           System.out.println("After completions time = " + timeTotal + " for " + numberOfLines + " registers " + " size each line = " + size  + " Records/Sec=" + (numberOfLines*1000/timeTotal) + " (Assynchronous)");
   
           for (LocalAIO tmp: list)
           {
               assertEquals(1, tmp.timesDoneCalled);
               assertTrue(tmp.doneCalled);
               assertFalse(tmp.errorCalled);
           }
           
           controller.destroyBuffer(block);
           
           controller.close();
       }
       finally
       {
           try {controller.close();} catch (Exception ignored){}
       }
       
       
   }
   
   public void testDirectSynchronous() throws Exception
   {
       try
       {
           System.out.println("++testDirectDataNoPage"); System.out.flush();
           final int NUMBER_LINES = 10000; 
           final int SIZE = 1024;
           //final int SIZE = 512;
           
           final AsynchronousFileImpl controller = new AsynchronousFileImpl();
           controller.open(FILE_NAME, 2000);

           ByteBuffer block = controller.newBuffer(SIZE);
           encodeBufer(block);
           
           preAlloc(controller, NUMBER_LINES * SIZE);

           long valueInitial = System.currentTimeMillis();
   
           System.out.println("Adding data");
           
           long lastTime = System.currentTimeMillis();
           int counter = 0;
           
           for (int i=0; i<NUMBER_LINES; i++)
           {
               CountDownLatch latchDone = new CountDownLatch(1);
               LocalAIO aioBlock = new LocalAIO(latchDone);
               controller.write(i*512, 512, block, aioBlock);
               latchDone.await();
               assertTrue(aioBlock.doneCalled);
               assertFalse(aioBlock.errorCalled);
               if (++counter % 500 == 0)
               {
                   System.out.println(500*1000/(System.currentTimeMillis()-lastTime) + " rec/sec (Synchronous)");
                   lastTime = System.currentTimeMillis();
               }
           }

           System.out.println("Data added " + (System.currentTimeMillis() - valueInitial));
           
           
           System.out.println("Finished append " + (System.currentTimeMillis() - valueInitial) + " received = " + LocalAIO.staticDone);
           System.out.println("Flush now");
           System.out.println("Received " + LocalAIO.staticDone);
   
           long timeTotal = System.currentTimeMillis() - valueInitial;
           System.out.println("Flushed " + timeTotal);
           System.out.println("time = " + timeTotal + " for " + NUMBER_LINES + " registers " + " size each line = " + SIZE  + " Records/Sec=" + (NUMBER_LINES*1000/timeTotal) + " Synchronous");
   
           controller.destroyBuffer(block);
           controller.close();
       }
       catch (Exception e)
       {
           System.out.println("Received " + LocalAIO.staticDone + " before it failed");
           throw e;
       }
       
   }
   
   private void preAlloc(AsynchronousFileImpl controller, long size)
   {
       System.out.println("Pre allocating");  System.out.flush();
       long startPreAllocate = System.currentTimeMillis();
       controller.fill(0l, 1, size, (byte)0);
       long endPreAllocate = System.currentTimeMillis() - startPreAllocate;
       if (endPreAllocate != 0) System.out.println("PreAllocated the file in " + endPreAllocate + " seconds, What means " + (size/endPreAllocate) + " bytes per millisecond");
   }
   
   
   public void testInvalidWrite() throws Exception
   {
       final AsynchronousFileImpl controller = new AsynchronousFileImpl();
       controller.open(FILE_NAME, 2000);

       try
       {
           
           final int SIZE=512;

           ByteBuffer block = controller.newBuffer(SIZE);
           encodeBufer(block);
           
           preAlloc(controller, 1000 * 512);
           
           
           CountDownLatch latchDone = new CountDownLatch(1);
           
           LocalAIO aioBlock = new LocalAIO(latchDone);
           controller.write(11, 512, block, aioBlock);
           
           latchDone.await();
           
           assertTrue (aioBlock.errorCalled);
           assertFalse(aioBlock.doneCalled);
   
           controller.destroyBuffer(block);
       }
       catch (Exception e)
       {
           System.out.println("Received " + LocalAIO.staticDone + " before it failed");
           throw e;
       }
       finally
       {
           controller.close();
       }
       
   }
   
   public void testInvalidAlloc() throws Exception
   {
       AsynchronousFileImpl controller = new AsynchronousFileImpl();
       try
       {
           // You don't need to open the file to alloc it
           ByteBuffer buffer = controller.newBuffer(300);
           fail ("Exception expected");
       }
       catch (Exception ignored)
       {
       }
       
   }
   
   private static class LocalAIO implements AIOCallback
   {

       CountDownLatch latch;
       
       public LocalAIO(CountDownLatch latch)
       {
           this.latch = latch;
       }
       
       boolean doneCalled = false;
       boolean errorCalled = false;
       int timesDoneCalled = 0;
       static int staticDone = 0;
       public void decode(int length, ByteBuffer buffer)
       {
           // TODO Auto-generated method stub
           
       }

       public void done()
       {
           //System.out.println("Received Done"); System.out.flush();
           doneCalled = true;
           timesDoneCalled++;
           staticDone++;
           if (latch != null) 
           {
               latch.countDown();
           }
           
       }

       public void onError(int errorCode, String errorMessage)
       {
           errorCalled = true;
           if (latch != null)
           {
               // even thought an error happened, we need to inform the latch, or the test won't finish
               latch.countDown();
           }
           System.out.println("Received an Error - " + errorCode + " message=" + errorMessage);
           
       }

   }

   private void addString(String str, ByteBuffer buffer)
   {
       CharBuffer charBuffer = CharBuffer.wrap(str);
       UTF_8_ENCODER.encode(charBuffer, buffer, true);
       
   }
   


}
