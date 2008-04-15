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
import java.util.concurrent.CountDownLatch;

import org.jboss.messaging.core.asyncio.AIOCallback;
import org.jboss.messaging.core.asyncio.impl.JlibAIO;

import junit.framework.TestCase;

//you need to define java.library.path=${project-root}/native/src/.libs
public class SingleThreadWriteNativeTest extends TestCase
{
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
   
   public void testAddBeyongSimultaneousLimit() throws Exception
   {
       asyncData(150000,1024,100);
   }

   public void testAddAsyncData() throws Exception
   {
       asyncData(150000,1024,20000);
   }
   
   public void testRead() throws Exception
   {
       
       
       

       final JlibAIO controller = new JlibAIO();
       try
       {
           
           final int NUMBER_LINES = 300;
           final int SIZE = 1024;
           
           controller.open(FILE_NAME, 10);
           
           ByteBuffer buffer = controller.newBuffer(SIZE);
   
           
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
   
   private void asyncData(int numberOfLines, int size, int aioLimit) throws Exception
   {
       final JlibAIO controller = new JlibAIO();
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
           
           final JlibAIO controller = new JlibAIO();
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
   
   private void preAlloc(JlibAIO controller, long size)
   {
       System.out.println("Pre allocating");  System.out.flush();
       long startPreAllocate = System.currentTimeMillis();
       controller.preAllocate(1, size);
       long endPreAllocate = System.currentTimeMillis() - startPreAllocate;
       if (endPreAllocate != 0) System.out.println("PreAllocated the file in " + endPreAllocate + " seconds, What means " + (size/endPreAllocate) + " bytes per millisecond");
   }
   
   
   public void testInvalidWrite() throws Exception
   {
       final JlibAIO controller = new JlibAIO();
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
       JlibAIO controller = new JlibAIO();
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
