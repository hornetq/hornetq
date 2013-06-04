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

package org.hornetq.tests.integration.largemessage;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.junit.Assert;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.*;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.DataConstants;
import org.hornetq.utils.DeflaterReader;

/**
 * A LargeMessageTestBase
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * Created Oct 29, 2008 11:43:52 AM
 *
 *
 */
public abstract class LargeMessageTestBase extends ServiceTestBase
{

   // Constants -----------------------------------------------------
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   protected final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void testChunks(final boolean isXA,
                             final boolean restartOnXA,
                             final boolean rollbackFirstSend,
                             final boolean useStreamOnConsume,
                             final boolean realFiles,
                             final boolean preAck,
                             final boolean sendingBlocking,
                             final boolean testBrowser,
                             final boolean useMessageConsumer,
                             final int numberOfMessages,
                             final long numberOfBytes,
                             final int waitOnConsumer,
                             final long delayDelivery) throws Exception
   {
      testChunks(isXA,
                 restartOnXA,
                 rollbackFirstSend,
                 useStreamOnConsume,
                 realFiles,
                 preAck,
                 sendingBlocking,
                 testBrowser,
                 useMessageConsumer,
                 numberOfMessages,
                 numberOfBytes,
                 waitOnConsumer,
                 delayDelivery,
                 -1,
                 10 * 1024);
   }

   protected void testChunks(final boolean isXA,
                             final boolean restartOnXA,
                             final boolean rollbackFirstSend,
                             final boolean useStreamOnConsume,
                             final boolean realFiles,
                             final boolean preAck,
                             final boolean sendingBlocking,
                             final boolean testBrowser,
                             final boolean useMessageConsumer,
                             final int numberOfMessages,
                             final long numberOfBytes,
                             final int waitOnConsumer,
                             final long delayDelivery,
                             final int producerWindow,
                             final int minSize) throws Exception
   {
      clearDataRecreateServerDirs();

      HornetQServer server = createServer(realFiles);
      server.start();

      ServerLocator locator = createInVMNonHALocator();
      try
      {

         if (sendingBlocking)
         {
            locator.setBlockOnNonDurableSend(true);
            locator.setBlockOnDurableSend(true);
            locator.setBlockOnAcknowledge(true);
         }

         if (producerWindow > 0)
         {
            locator.setConfirmationWindowSize(producerWindow);
         }

         locator.setMinLargeMessageSize(minSize);

         ClientSessionFactory sf = locator.createSessionFactory();

         ClientSession session;

         Xid xid = null;
         session = sf.createSession(null, null, isXA, false, false, preAck, 0);

         if (isXA)
         {
            xid = newXID();
            session.start(xid, XAResource.TMNOFLAGS);
         }

         session.createQueue(ADDRESS, ADDRESS, null, true);

         ClientProducer producer = session.createProducer(ADDRESS);

         if (rollbackFirstSend)
         {
            sendMessages(numberOfMessages, numberOfBytes, delayDelivery, session, producer);

            if (isXA)
            {
               session.end(xid, XAResource.TMSUCCESS);
               session.prepare(xid);

               session.close();

               if (realFiles && restartOnXA)
               {
                  server.stop();
                  server.start();
                  sf = locator.createSessionFactory();
               }

               session = sf.createSession(null, null, isXA, false, false, preAck, 0);

               Xid[] xids = session.recover(XAResource.TMSTARTRSCAN);
               Assert.assertEquals(1, xids.length);
               Assert.assertEquals(xid, xids[0]);

               session.rollback(xid);
               producer = session.createProducer(ADDRESS);
               xid = newXID();
               session.start(xid, XAResource.TMNOFLAGS);
            }
            else
            {
               session.rollback();
            }

            validateNoFilesOnLargeDir();
         }

         sendMessages(numberOfMessages, numberOfBytes, delayDelivery, session, producer);

         if (isXA)
         {
            session.end(xid, XAResource.TMSUCCESS);
            session.prepare(xid);

            session.close();

            if (realFiles && restartOnXA)
            {
               server.stop();
               server.start();
               //we need to recreate sf's
               sf = locator.createSessionFactory();
            }

            session = sf.createSession(null, null, isXA, false, false, preAck, 0);

            Xid[] xids = session.recover(XAResource.TMSTARTRSCAN);
            Assert.assertEquals(1, xids.length);
            Assert.assertEquals(xid, xids[0]);

            producer = session.createProducer(ADDRESS);

            session.commit(xid, false);
            xid = newXID();
            session.start(xid, XAResource.TMNOFLAGS);
         }
         else
         {
            session.commit();
         }

         session.close();

         if (realFiles)
         {
            server.stop();

            server = createServer(realFiles);
            server.start();

            sf = locator.createSessionFactory();
         }

         session = sf.createSession(null, null, isXA, false, false, preAck, 0);

         if (isXA)
         {
            xid = newXID();
            session.start(xid, XAResource.TMNOFLAGS);
         }

         ClientConsumer consumer = null;

         for (int iteration = testBrowser ? 0 : 1; iteration < 2; iteration++)
         {
            session.stop();

            // first time with a browser
            consumer = session.createConsumer(ADDRESS, null, iteration == 0);

            if (useMessageConsumer)
            {
               final CountDownLatch latchDone = new CountDownLatch(numberOfMessages);
               final AtomicInteger errors = new AtomicInteger(0);

               MessageHandler handler = new MessageHandler()
               {
                  int msgCounter;

                  public void onMessage(final ClientMessage message)
                  {
                     try
                     {
                        if (delayDelivery > 0)
                        {
                           long originalTime = (Long)message.getObjectProperty(new SimpleString("original-time"));
                           Assert.assertTrue(System.currentTimeMillis() - originalTime + "<" + delayDelivery,
                                             System.currentTimeMillis() - originalTime >= delayDelivery);
                        }

                        if (!preAck)
                        {
                           message.acknowledge();
                        }

                        Assert.assertNotNull(message);

                        if (delayDelivery <= 0)
                        {
                           // right now there is no guarantee of ordered delivered on multiple scheduledMessages with
                           // the same
                           // scheduled delivery time
                           Assert.assertEquals(msgCounter,
                                               ((Integer)message.getObjectProperty(new SimpleString("counter-message"))).intValue());
                        }

                        if (useStreamOnConsume)
                        {
                           final AtomicLong bytesRead = new AtomicLong(0);
                           message.saveToOutputStream(new OutputStream()
                           {

                              @Override
                              public void write(final byte b[]) throws IOException
                              {
                                 if (b[0] == UnitTestCase.getSamplebyte(bytesRead.get()))
                                 {
                                    bytesRead.addAndGet(b.length);
                                    LargeMessageTestBase.log.debug("Read position " + bytesRead.get() + " on consumer");
                                 }
                                 else
                                 {
                                    LargeMessageTestBase.log.warn("Received invalid packet at position " + bytesRead.get());
                                 }
                              }

                              @Override
                              public void write(final int b) throws IOException
                              {
                                 if (b == UnitTestCase.getSamplebyte(bytesRead.get()))
                                 {
                                    bytesRead.incrementAndGet();
                                 }
                                 else
                                 {
                                    LargeMessageTestBase.log.warn("byte not as expected!");
                                 }
                              }
                           });

                           Assert.assertEquals(numberOfBytes, bytesRead.get());
                        }
                        else
                        {

                           HornetQBuffer buffer = message.getBodyBuffer();
                           buffer.resetReaderIndex();
                           for (long b = 0; b < numberOfBytes; b++)
                           {
                              if (b % (1024l * 1024l) == 0)
                              {
                                 LargeMessageTestBase.log.debug("Read " + b + " bytes");
                              }

                              Assert.assertEquals(UnitTestCase.getSamplebyte(b), buffer.readByte());
                           }

                           try
                           {
                              buffer.readByte();
                              Assert.fail("Supposed to throw an exception");
                           }
                           catch (Exception e)
                           {
                           }
                        }
                     }
                     catch (Throwable e)
                     {
                        e.printStackTrace();
                        LargeMessageTestBase.log.warn("Got an error", e);
                        errors.incrementAndGet();
                     }
                     finally
                     {
                        latchDone.countDown();
                        msgCounter++;
                     }
                  }
               };

               session.start();

               consumer.setMessageHandler(handler);

               Assert.assertTrue(latchDone.await(waitOnConsumer, TimeUnit.MILLISECONDS));
               Assert.assertEquals(0, errors.get());
            }
            else
            {

               session.start();

               for (int i = 0; i < numberOfMessages; i++)
               {
                  System.currentTimeMillis();

                  ClientMessage message = consumer.receive(waitOnConsumer + delayDelivery);

                  Assert.assertNotNull(message);

                  System.currentTimeMillis();

                  if (delayDelivery > 0)
                  {
                     long originalTime = (Long)message.getObjectProperty(new SimpleString("original-time"));
                     Assert.assertTrue(System.currentTimeMillis() - originalTime + "<" + delayDelivery,
                                       System.currentTimeMillis() - originalTime >= delayDelivery);
                  }

                  if (!preAck)
                  {
                     message.acknowledge();
                  }

                  Assert.assertNotNull(message);

                  if (delayDelivery <= 0)
                  {
                     // right now there is no guarantee of ordered delivered on multiple scheduledMessages with the same
                     // scheduled delivery time
                     Assert.assertEquals(i,
                                         ((Integer)message.getObjectProperty(new SimpleString("counter-message"))).intValue());
                  }

                  if (useStreamOnConsume)
                  {
                     final AtomicLong bytesRead = new AtomicLong(0);
                     message.saveToOutputStream(new OutputStream()
                     {

                        @Override
                        public void write(final byte b[]) throws IOException
                        {
                           if (b[0] == UnitTestCase.getSamplebyte(bytesRead.get()))
                           {
                              bytesRead.addAndGet(b.length);
                           }
                           else
                           {
                              LargeMessageTestBase.log.warn("Received invalid packet at position " + bytesRead.get());
                           }

                        }

                        @Override
                        public void write(final int b) throws IOException
                        {
                           if (bytesRead.get() % (1024l * 1024l) == 0)
                           {
                              LargeMessageTestBase.log.debug("Read " + bytesRead.get() + " bytes");
                           }
                           if (b == (byte)'a')
                           {
                              bytesRead.incrementAndGet();
                           }
                           else
                           {
                              LargeMessageTestBase.log.warn("byte not as expected!");
                           }
                        }
                     });

                     Assert.assertEquals(numberOfBytes, bytesRead.get());
                  }
                  else
                  {
                     HornetQBuffer buffer = message.getBodyBuffer();
                     buffer.resetReaderIndex();

                     for (long b = 0; b < numberOfBytes; b++)
                     {
                        if (b % (1024l * 1024l) == 0l)
                        {
                           LargeMessageTestBase.log.debug("Read " + b + " bytes");
                        }
                        Assert.assertEquals(UnitTestCase.getSamplebyte(b), buffer.readByte());
                     }
                  }

               }

            }
            consumer.close();

            if (iteration == 0)
            {
               if (isXA)
               {
                  session.end(xid, XAResource.TMSUCCESS);
                  session.rollback(xid);
                  xid = newXID();
                  session.start(xid, XAResource.TMNOFLAGS);
               }
               else
               {
                  session.rollback();
               }
            }
            else
            {
               if (isXA)
               {
                  session.end(xid, XAResource.TMSUCCESS);
                  session.commit(xid, true);
                  xid = newXID();
                  session.start(xid, XAResource.TMNOFLAGS);
               }
               else
               {
                  session.commit();
               }
            }
         }

         session.close();

         Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(ADDRESS).getBindable()).getDeliveringCount());
         Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(ADDRESS).getBindable()).getMessageCount());

         validateNoFilesOnLargeDir();

      }
      finally
      {
         locator.close();
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   /**
    * @param useFile
    * @param numberOfMessages
    * @param numberOfIntegers
    * @param delayDelivery
    * @param testTime
    * @param session
    * @param producer
    * @throws FileNotFoundException
    * @throws IOException
    * @throws HornetQException
    */
   private void sendMessages(final int numberOfMessages,
                             final long numberOfBytes,
                             final long delayDelivery,
                             final ClientSession session,
                             final ClientProducer producer) throws Exception
   {
      LargeMessageTestBase.log.debug("NumberOfBytes = " + numberOfBytes);
      for (int i = 0; i < numberOfMessages; i++)
      {
         ClientMessage message = session.createMessage(true);

         // If the test is using more than 1M, we will only use the Streaming, as it require too much memory from the
         // test
         if (numberOfBytes > 1024 * 1024 || i % 2 == 0)
         {
            LargeMessageTestBase.log.debug("Sending message (stream)" + i);
            message.setBodyInputStream(UnitTestCase.createFakeLargeStream(numberOfBytes));
         }
         else
         {
            LargeMessageTestBase.log.debug("Sending message (array)" + i);
            byte[] bytes = new byte[(int)numberOfBytes];
            for (int j = 0; j < bytes.length; j++)
            {
               bytes[j] = UnitTestCase.getSamplebyte(j);
            }
            message.getBodyBuffer().writeBytes(bytes);
         }
         message.putIntProperty(new SimpleString("counter-message"), i);
         if (delayDelivery > 0)
         {
            long time = System.currentTimeMillis();
            message.putLongProperty(new SimpleString("original-time"), time);
            message.putLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME, time + delayDelivery);

            producer.send(message);
         }
         else
         {
            producer.send(message);
         }
      }
   }

   protected HornetQBuffer createLargeBuffer(final int numberOfIntegers)
   {
      HornetQBuffer body = HornetQBuffers.fixedBuffer(DataConstants.SIZE_INT * numberOfIntegers);

      for (int i = 0; i < numberOfIntegers; i++)
      {
         body.writeInt(i);
      }

      return body;

   }

   protected ClientMessage createLargeClientMessage(final ClientSession session, final int numberOfBytes) throws Exception
   {
      return createLargeClientMessage(session, numberOfBytes, true);
   }

   protected ClientMessage createLargeClientMessage (final ClientSession session, final byte[] buffer, final boolean durable) throws Exception
   {
      ClientMessage msgs = session.createMessage(durable);
      msgs.getBodyBuffer().writeBytes(buffer);
      return msgs;
   }

   protected ClientMessage createLargeClientMessage(final ClientSession session,
                                                    final long numberOfBytes,
                                                    final boolean persistent) throws Exception
   {

      ClientMessage clientMessage = session.createMessage(persistent);

      clientMessage.setBodyInputStream(UnitTestCase.createFakeLargeStream(numberOfBytes));

      return clientMessage;
   }

   /**
    * @param session
    * @param queueToRead
    * @param numberOfIntegers
    * @throws HornetQException
    * @throws FileNotFoundException
    * @throws IOException
    */
   protected void readMessage(final ClientSession session, final SimpleString queueToRead, final int numberOfBytes) throws HornetQException,
                                                                                                                   FileNotFoundException,
                                                                                                                   IOException
   {
      session.start();

      ClientConsumer consumer = session.createConsumer(queueToRead);

      ClientMessage clientMessage = consumer.receive(5000);

      Assert.assertNotNull(clientMessage);

      clientMessage.acknowledge();

      session.commit();

      consumer.close();
   }

   protected OutputStream createFakeOutputStream() throws Exception
   {

      return new OutputStream()
      {
         private boolean closed = false;

         private int count;

         @Override
         public void close() throws IOException
         {
            super.close();
            closed = true;
         }

         @Override
         public void write(final int b) throws IOException
         {
            if (count++ % 1024 * 1024 == 0)
            {
               LargeMessageTestBase.log.debug("OutputStream received " + count + " bytes");
            }
            if (closed)
            {
               throw new IOException("Stream was closed");
            }
         }

      };

   }

   //depending on the value of regular argument, it can produce a text stream
   //whose size is above minLargeMessageSize but whose compressed size is either
   //below minLargeMessageSize (regular = true) or above it (regular = false)
   public static void adjustLargeCompression(boolean regular, TestLargeMessageInputStream stream, int step) throws IOException
   {
      int absoluteStep = Math.abs(step);
      while (true)
      {
         DeflaterReader compressor = new DeflaterReader(stream, new AtomicLong());
         try
         {
            byte[] buffer = new byte[1048 * 50];

            int totalCompressed = 0;
            int n = compressor.read(buffer);
            while (n != -1)
            {
               totalCompressed += n;
               n = compressor.read(buffer);
            }

            // check compressed size
            if (regular && (totalCompressed < stream.getMinLarge()))
            {
               // ok it can be sent as regular
               stream.resetAdjust(0);
               break;
            }
            else if ((!regular) && (totalCompressed > stream.getMinLarge()))
            {
               // now it cannot be sent as regular
               stream.resetAdjust(0);
               break;
            }
            else
            {
               stream.resetAdjust(regular ? -absoluteStep : absoluteStep);
            }
         }
         finally
         {
            compressor.close();
         }
      }
   }
   
   public static class TestLargeMessageInputStream extends InputStream
   {
      private final int minLarge;
      private int size;
      private int pos;
      private boolean random;

      public TestLargeMessageInputStream(int minLarge)
      {
         this(minLarge, false);
      }

      public TestLargeMessageInputStream(int minLarge, boolean random)
      {
         pos = 0;
         this.minLarge = minLarge;
         this.size = minLarge + 1024;
         this.random = random;
      }

      public int getChar(int index)
      {
         if (random)
         {
            Random r = new Random();
            return 'A' + r.nextInt(26);
         }
         else
         {
            return 'A' + index % 26;
         }
      }

      public void setSize(int size)
      {
         this.size = size;
      }

      public TestLargeMessageInputStream(TestLargeMessageInputStream other)
      {
         this.minLarge = other.minLarge;
         this.size = other.size;
         this.pos = other.pos;
      }

      public int getSize()
      {
         return size;
      }

      public int getMinLarge()
      {
         return this.minLarge;
      }

      @Override
      public int read() throws IOException
      {
         if (pos == size) return -1;
         pos++;
         
         return getChar(pos - 1);
      }
      
      public void resetAdjust(int step)
      {
         size += step;
         if (size <= minLarge)
         {
            throw new IllegalStateException("Couldn't adjust anymore, size smaller than minLarge " + minLarge);
         }
         pos = 0;
      }
      
      public TestLargeMessageInputStream clone()
      {
         return new TestLargeMessageInputStream(this);
      }

      public char[] toArray() throws IOException {
         char[] result = new char[size];
         for (int i = 0; i < result.length; i++)
         {
            result[i] = (char)read();
         }
         return result;
      }
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
