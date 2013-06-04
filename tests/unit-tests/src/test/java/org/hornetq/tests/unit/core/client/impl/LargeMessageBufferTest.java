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

package org.hornetq.tests.unit.core.client.impl;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.core.client.impl.ClientConsumerInternal;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.client.impl.LargeMessageControllerImpl;
import org.hornetq.core.protocol.core.impl.wireformat.SessionQueueQueryResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveContinuationMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveLargeMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveMessage;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.HornetQBufferInputStream;

/**
 * A LargeMessageBufferUnitTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class LargeMessageBufferTest extends UnitTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   static int tmpFileCounter = 0;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      tmpFileCounter++;

      File tmp = new File(getTestDir());
      tmp.mkdirs();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Test Simple getBytes
   @Test
   public void testGetBytes() throws Exception
   {
      LargeMessageControllerImpl buffer = create15BytesSample();

      for (int i = 1; i <= 15; i++)
      {
         try
         {
            Assert.assertEquals(i, buffer.readByte());
         }
         catch (Exception e)
         {
            throw new Exception("Exception at position " + i, e);
         }
      }

      try
      {
         buffer.readByte();
         Assert.fail("supposed to throw an exception");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   // Test for void getBytes(final int index, final byte[] dst)
   @Test
   public void testGetBytesIByteArray() throws Exception
   {
      LargeMessageControllerImpl buffer = create15BytesSample();

      byte[] bytes = new byte[15];
      buffer.getBytes(0, bytes);

      validateAgainstSample(bytes);

      try
      {
         buffer = create15BytesSample();

         bytes = new byte[16];
         buffer.getBytes(0, bytes);
         Assert.fail("supposed to throw an exception");
      }
      catch (java.lang.IndexOutOfBoundsException e)
      {
      }
   }

   // testing void getBytes(int index, ChannelBuffer dst, int dstIndex, int length)
   @Test
   public void testGetBytesILChannelBufferII() throws Exception
   {
      LargeMessageControllerImpl buffer = create15BytesSample();

      HornetQBuffer dstBuffer = HornetQBuffers.fixedBuffer(20);

      dstBuffer.setIndex(0, 5);

      buffer.getBytes(0, dstBuffer);

      byte[] compareBytes = new byte[15];
      dstBuffer.getBytes(5, compareBytes);

      validateAgainstSample(compareBytes);
   }

   // testing void getBytes(int index, ChannelBuffer dst, int dstIndex, int length)
   @Test
   public void testReadIntegers() throws Exception
   {
      LargeMessageControllerImpl buffer = createBufferWithIntegers(3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);

      for (int i = 1; i <= 15; i++)
      {
         Assert.assertEquals(i, buffer.readInt());
      }

      try
      {
         buffer.readByte();
         Assert.fail("supposed to throw an exception");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   @Test
   public void testReadIntegersOverStream() throws Exception
   {
      LargeMessageControllerImpl buffer = createBufferWithIntegers(3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
      HornetQBufferInputStream is = new HornetQBufferInputStream(buffer);
      DataInputStream dataInput = new DataInputStream(is);

      for (int i = 1; i <= 15; i++)
      {
         Assert.assertEquals(i, dataInput.readInt());
      }

      assertEquals(-1, dataInput.read());
      dataInput.close();
   }

   // testing void getBytes(int index, ChannelBuffer dst, int dstIndex, int length)
   @Test
   public void testReadLongs() throws Exception
   {
      LargeMessageControllerImpl buffer = createBufferWithLongs(3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);

      for (int i = 1; i <= 15; i++)
      {
         Assert.assertEquals(i, buffer.readLong());
      }

      try
      {
         buffer.readByte();
         Assert.fail("supposed to throw an exception");
      }
      catch (IndexOutOfBoundsException e)
      {
      }
   }

   @Test
   public void testReadLongsOverStream() throws Exception
   {
      LargeMessageControllerImpl buffer = createBufferWithLongs(3, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15);
      HornetQBufferInputStream is = new HornetQBufferInputStream(buffer);
      DataInputStream dataInput = new DataInputStream(is);

      for (int i = 1; i <= 15; i++)
      {
         Assert.assertEquals(i, dataInput.readLong());
      }

      assertEquals(-1, dataInput.read());
      dataInput.close();
   }

   @Test
   public void testReadData() throws Exception
   {
      HornetQBuffer dynamic = HornetQBuffers.dynamicBuffer(1);

      String str1 = RandomUtil.randomString();
      String str2 = RandomUtil.randomString();
      double d1 = RandomUtil.randomDouble();
      float f1 = RandomUtil.randomFloat();

      dynamic.writeUTF(str1);
      dynamic.writeString(str2);
      dynamic.writeDouble(d1);
      dynamic.writeFloat(f1);

      LargeMessageControllerImpl readBuffer = splitBuffer(3, dynamic.toByteBuffer().array());

      Assert.assertEquals(str1, readBuffer.readUTF());
      Assert.assertEquals(str2, readBuffer.readString());
      Assert.assertEquals(d1, readBuffer.readDouble(), 0.000001);
      Assert.assertEquals(f1, readBuffer.readFloat(), 0.000001);
   }

   private File getTestFile()
   {
      return new File(getTestDir(), "temp." + tmpFileCounter + ".file");
   }

   @Test
   public void testReadDataOverCached() throws Exception
   {
      clearDataRecreateServerDirs();

      HornetQBuffer dynamic = HornetQBuffers.dynamicBuffer(1);

      String str1 = RandomUtil.randomString();
      String str2 = RandomUtil.randomString();
      double d1 = RandomUtil.randomDouble();
      float f1 = RandomUtil.randomFloat();

      dynamic.writeUTF(str1);
      dynamic.writeString(str2);
      dynamic.writeDouble(d1);
      dynamic.writeFloat(f1);

      LargeMessageControllerImpl readBuffer = splitBuffer(3, dynamic.toByteBuffer().array(), getTestFile());

      Assert.assertEquals(str1, readBuffer.readUTF());
      Assert.assertEquals(str2, readBuffer.readString());
      Assert.assertEquals(d1, readBuffer.readDouble(), 0.00000001);
      Assert.assertEquals(f1, readBuffer.readFloat(), 0.000001);

      readBuffer.readerIndex(0);

      Assert.assertEquals(str1, readBuffer.readUTF());
      Assert.assertEquals(str2, readBuffer.readString());
      Assert.assertEquals(d1, readBuffer.readDouble(), 0.00000001);
      Assert.assertEquals(f1, readBuffer.readFloat(), 0.000001);

      readBuffer.close();
   }

   @Test
   public void testReadPartialData() throws Exception
   {

      final LargeMessageControllerImpl buffer = new LargeMessageControllerImpl(new FakeConsumerInternal(), 10, 10);

      buffer.addPacket(new FakePacket(-1, new byte[] { 0, 1, 2, 3, 4 }, true, true));

      byte bytes[] = new byte[30];
      buffer.readBytes(bytes, 0, 5);

      for (byte i = 0; i < 5; i++)
      {
         Assert.assertEquals(i, bytes[i]);
      }

      final CountDownLatch latchGo = new CountDownLatch(1);

      final AtomicInteger errorCount = new AtomicInteger(0);

      Thread t = new Thread()
      {
         @Override
         public void run()
         {

            try
            {
               latchGo.countDown();
               buffer.readBytes(new byte[5]);
            }
            catch (IndexOutOfBoundsException ignored)
            {
            }
            catch (IllegalAccessError ignored)
            {

            }
            catch (Throwable e)
            {
               e.printStackTrace();
               errorCount.incrementAndGet();
            }
         }
      };

      t.start();

      UnitTestCase.waitForLatch(latchGo);

      buffer.cancel();

      t.join();

      Assert.assertEquals(0, errorCount.get());

   }

   @Test
   public void testInterruptData() throws Exception
   {
      LargeMessageControllerImpl readBuffer = splitBuffer(3, new byte[] { 0, 1, 2, 3, 4 });

      byte bytes[] = new byte[30];
      readBuffer.readBytes(bytes, 0, 5);

      for (byte i = 0; i < 5; i++)
      {
         Assert.assertEquals(i, bytes[i]);
      }
   }

   @Test
   public void testSplitBufferOnFile() throws Exception
   {
      LargeMessageControllerImpl outBuffer = new LargeMessageControllerImpl(new FakeConsumerInternal(),
                                                                    1024 * 1024,
                                                                    1,
                                                                    getTestFile(),
                                                                    1024);
      try
      {

         long count = 0;
         for (int i = 0; i < 10; i++)
         {
            byte buffer[] = new byte[10240];
            for (int j = 0; j < 10240; j++)
            {
               buffer[j] = getSamplebyte(count++);
            }
            outBuffer.addPacket(new FakePacket(1, buffer, true, false));
         }

         outBuffer.readerIndex(0);

         for (int i = 0; i < 10240 * 10; i++)
         {
            assertEquals("position " + i, getSamplebyte(i), outBuffer.readByte());
         }

         outBuffer.readerIndex(0);

         for (int i = 0; i < 10240 * 10; i++)
         {
            assertEquals("position " + i, getSamplebyte(i), outBuffer.readByte());
         }
      }
      finally
      {
         outBuffer.close();
      }

   }

   @Test
   public void testStreamData() throws Exception
   {
      final LargeMessageControllerImpl outBuffer = new LargeMessageControllerImpl(new FakeConsumerInternal(),
                                                                          1024 * 11 + 123,
                                                                          1000);

      final PipedOutputStream output = new PipedOutputStream();
      final PipedInputStream input = new PipedInputStream(output);

      final AtomicInteger errors = new AtomicInteger(0);

      // Done reading 3 elements
      final CountDownLatch done1 = new CountDownLatch(1);
      // Done with the thread
      final CountDownLatch done2 = new CountDownLatch(1);

      final AtomicInteger count = new AtomicInteger(0);
      final AtomicInteger totalBytes = new AtomicInteger(0);

      Thread treader = new Thread("treader")
      {
         @Override
         public void run()
         {
            try
            {

               byte line[] = new byte[1024];
               int dataRead = 0;
               while (dataRead >= 0)
               {
                  dataRead = input.read(line);
                  if (dataRead > 0)
                  {
                     totalBytes.addAndGet(dataRead);
                     if (count.incrementAndGet() == 3)
                     {
                        done1.countDown();
                     }
                  }
               }
            }
            catch (Exception e)
            {
               e.printStackTrace();
               errors.incrementAndGet();
            }
            finally
            {
               done1.countDown();
               done2.countDown();
            }
         }
      };

      treader.setDaemon(true);
      treader.start();

      for (int i = 0; i < 3; i++)
      {
         outBuffer.addPacket(new FakePacket(-1, new byte[1024], true, false));
      }

      outBuffer.setOutputStream(output);

      final CountDownLatch waiting = new CountDownLatch(1);

      Thread twaiter = new Thread("twaiter")
      {
         @Override
         public void run()
         {
            try
            {
               outBuffer.waitCompletion(0);
               waiting.countDown();
            }
            catch (Exception e)
            {
               e.printStackTrace();
               errors.incrementAndGet();
            }
         }
      };

      twaiter.setDaemon(true);
      twaiter.start();

      Assert.assertTrue(done1.await(10, TimeUnit.SECONDS));

      Assert.assertEquals(3, count.get());
      Assert.assertEquals(1024 * 3, totalBytes.get());

      for (int i = 0; i < 8; i++)
      {
         outBuffer.addPacket(new FakePacket(-1, new byte[1024], true, false));
      }

      Assert.assertEquals(1, waiting.getCount());

      outBuffer.addPacket(new FakePacket(-1, new byte[123], false, false));

      Assert.assertTrue(done2.await(10, TimeUnit.SECONDS));

      Assert.assertTrue(waiting.await(10, TimeUnit.SECONDS));

      Assert.assertEquals(12, count.get());
      Assert.assertEquals(1024 * 11 + 123, totalBytes.get());

      treader.join();

      twaiter.join();

      Assert.assertEquals(0, errors.get());
      input.close();
   }

   @Test
   public void testStreamDataWaitCompletionOnCompleteBuffer() throws Exception
   {
      final LargeMessageControllerImpl outBuffer = create15BytesSample();

      outBuffer.saveBuffer(new OutputStream()
      {
         @Override
         public void write(final int b) throws IOException
         {
            // nohting to be done
         }
      });
   }

   @Test
   public void testStreamDataWaitCompletionOnInCompleteBuffer() throws Exception
   {
      final LargeMessageControllerImpl outBuffer = new LargeMessageControllerImpl(new FakeConsumerInternal(), 5, 1000);


      class FakeOutputStream extends OutputStream
      {
         @Override
         public void write(int b) throws IOException
         {
         }
      }

      outBuffer.setOutputStream(new FakeOutputStream());

      long time = System.currentTimeMillis();
      try
      {
         outBuffer.waitCompletion(0);
         fail("supposed to throw an exception");
      }
      catch (HornetQException e)
      {
      }

      assertTrue("It was supposed to wait at least 1 second", System.currentTimeMillis() - time > 1000);
   }


   @Test
   public void testStreamDataWaitCompletionOnSlowComingBuffer() throws Exception
   {
      final LargeMessageControllerImpl outBuffer = new LargeMessageControllerImpl(new FakeConsumerInternal(), 5, 1000);


      class FakeOutputStream extends OutputStream
      {
         @Override
         public void write(int b) throws IOException
         {
         }
      }

      outBuffer.setOutputStream(new FakeOutputStream());

      Thread sender = new Thread()
      {
         @Override
         public void run()
         {
            try
            {
               Thread.sleep(200);
               outBuffer.addPacket(new FakePacket(-1, new byte[] { 0 }, true, false));
               Thread.sleep(1000);
               outBuffer.addPacket(new FakePacket(-1, new byte[] { 0 }, true, false));
               Thread.sleep(1000);
               outBuffer.addPacket(new FakePacket(-1, new byte[] { 0 }, false, false));
            }
            catch (Exception e)
            {
            }
         }
      };

      sender.start();
      outBuffer.waitCompletion(0);
      sender.join();
   }

   @Test
   public void testErrorOnSetStreaming() throws Exception
   {
      long start = System.currentTimeMillis();
      final LargeMessageControllerImpl outBuffer = new LargeMessageControllerImpl(new FakeConsumerInternal(), 5, 30000);

      outBuffer.addPacket(new FakePacket(-1, new byte[] { 0, 1, 2, 3, 4 }, true, false));

      final CountDownLatch latchBytesWritten1 = new CountDownLatch(5);
      final CountDownLatch latchBytesWritten2 = new CountDownLatch(10);

      outBuffer.setOutputStream(new OutputStream()
      {
         @Override
         public void write(final int b) throws IOException
         {
            latchBytesWritten1.countDown();
            latchBytesWritten2.countDown();
         }
      });

      UnitTestCase.waitForLatch(latchBytesWritten1);

      try
      {
         outBuffer.readByte();
         Assert.fail("supposed to throw an exception");
      }
      catch (IllegalAccessError ignored)
      {
      }

      Assert.assertTrue("It waited too much", System.currentTimeMillis() - start < 30000);

   }

   @Test
   public void testReadBytesOnStreaming() throws Exception
   {
      byte[] byteArray = new byte[1024];
      for (int i = 0; i < byteArray.length; i++)
      {
         byteArray[i] = getSamplebyte(i);
      }

      HornetQBuffer splitbuffer = splitBuffer(3, byteArray);

      HornetQBufferInputStream is = new HornetQBufferInputStream(splitbuffer);

      for (int i = 0; i < 100; i++)
      {
         assertEquals(getSamplebyte(i), (byte)is.read());
      }

      for (int i = 100; i < byteArray.length; i += 10)
      {
         byte readBytes[] = new byte[10];

         int size = is.read(readBytes);

         for (int j = 0; j < size; j++)
         {
            assertEquals(getSamplebyte(i + j), readBytes[j]);
         }
      }
      is.close();
   }

   /**
    * @return
    */
   private LargeMessageControllerImpl create15BytesSample() throws Exception
   {
      return splitBuffer(5, new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 });
   }

   private LargeMessageControllerImpl createBufferWithIntegers(final int splitFactor, final int... values) throws Exception
   {
      ByteArrayOutputStream byteOut = new ByteArrayOutputStream(values.length * 4);
      DataOutputStream dataOut = new DataOutputStream(byteOut);

      for (int value : values)
      {
         dataOut.writeInt(value);
      }

      return splitBuffer(splitFactor, byteOut.toByteArray());
   }

   private LargeMessageControllerImpl createBufferWithLongs(final int splitFactor, final long... values) throws Exception
   {
      ByteArrayOutputStream byteOut = new ByteArrayOutputStream(values.length * 8);
      DataOutputStream dataOut = new DataOutputStream(byteOut);

      for (long value : values)
      {
         dataOut.writeLong(value);
      }

      return splitBuffer(splitFactor, byteOut.toByteArray());
   }

   private LargeMessageControllerImpl splitBuffer(final int splitFactor, final byte[] bytes) throws Exception
   {
      return splitBuffer(splitFactor, bytes, null);
   }

   private LargeMessageControllerImpl splitBuffer(final int splitFactor, final byte[] bytes, final File file) throws Exception
   {
      LargeMessageControllerImpl outBuffer = new LargeMessageControllerImpl(new FakeConsumerInternal(), bytes.length, 5000, file);

      ByteArrayInputStream input = new ByteArrayInputStream(bytes);

      while (true)
      {
         byte[] splitElement = new byte[splitFactor];
         int size = input.read(splitElement);
         if (size <= 0)
         {
            break;
         }

         SessionReceiveContinuationMessage packet = null;

         if (size < splitFactor)
         {
            byte[] newSplit = new byte[size];
            System.arraycopy(splitElement, 0, newSplit, 0, size);

            packet = new FakePacket(1, newSplit, input.available() > 0, false);
         }
         else
         {
            packet = new FakePacket(1, splitElement, input.available() > 0, false);
         }

         outBuffer.addPacket(packet);
      }

      return outBuffer;

   }

   private class FakePacket extends SessionReceiveContinuationMessage
   {
      public FakePacket(final long consumerID,
                        final byte[] body,
                        final boolean continues,
                        final boolean requiresResponse)
      {
         super(consumerID, body, continues, requiresResponse);
         size = 1;
      }
   }

   /**
       * @param bytes
       */
   private void validateAgainstSample(final byte[] bytes)
   {
      for (int i = 1; i <= 15; i++)
      {
         Assert.assertEquals(i, bytes[i - 1]);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   static class FakeConsumerInternal implements ClientConsumerInternal
   {

      public void close() throws HornetQException
      {
      }

      public Exception getLastException()
      {
         return null;
      }

      public MessageHandler getMessageHandler() throws HornetQException
      {
         return null;
      }

      public boolean isClosed()
      {
         return false;
      }

      public ClientMessage receive() throws HornetQException
      {
         return null;
      }

      public ClientMessage receive(final long timeout) throws HornetQException
      {
         return null;
      }

      public ClientMessage receiveImmediate() throws HornetQException
      {
         return null;
      }

      public void setMessageHandler(final MessageHandler handler) throws HornetQException
      {

      }

      public void acknowledge(final ClientMessage message) throws HornetQException
      {
      }

      @Override
      public void individualAcknowledge(ClientMessage message) throws HornetQException
      {
      }

      public void cleanUp() throws HornetQException
      {
      }

      public void clear(boolean waitForOnMessage) throws HornetQException
      {
      }

      public void clearAtFailover()
      {
      }

      public void flowControl(final int messageBytes, final boolean discountSlowConsumer) throws HornetQException
      {
      }

      public void flushAcks() throws HornetQException
      {
      }

      public int getBufferSize()
      {

         return 0;
      }

      public int getClientWindowSize()
      {

         return 0;
      }

      public SimpleString getFilterString()
      {

         return null;
      }

      public long getID()
      {

         return 0;
      }

      public SimpleString getQueueName()
      {

         return null;
      }

      public void handleLargeMessage(final SessionReceiveLargeMessage largeMessageHeader) throws Exception
      {


      }

      public void handleLargeMessageContinuation(final SessionReceiveContinuationMessage continuation) throws Exception
      {


      }

      public void handleMessage(final SessionReceiveMessage message) throws Exception
      {


      }

      public boolean isBrowseOnly()
      {

         return false;
      }

      public void start()
      {


      }

      public void stop() throws HornetQException
      {


      }

      public void stop(boolean waitForOnMessage) throws HornetQException
      {
      }

      public SessionQueueQueryResponseMessage getQueueInfo()
      {
         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.client.impl.ClientConsumerInternal#getNonXAsession()
       */
      public ClientSessionInternal getSession()
      {

         return null;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.client.impl.ClientConsumerInternal#interruptHandlers()
       */
      public void interruptHandlers() throws HornetQException
      {
      }
   }

}
