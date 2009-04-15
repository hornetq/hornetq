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

package org.jboss.messaging.tests.integration.chunkmessage;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;

import org.jboss.messaging.core.buffers.ChannelBuffers;
import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.DataConstants;
import org.jboss.messaging.utils.SimpleString;

/**
 * A ChunkTestBase
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Oct 29, 2008 11:43:52 AM
 *
 *
 */
public class ChunkTestBase extends ServiceTestBase
{

   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(ChunkTestBase.class);

   protected final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   protected MessagingServer server;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void testChunks(final boolean isXA,
                             final boolean realFiles,
                             final boolean preAck,
                             final boolean sendingBlocking,
                             final boolean testBrowser,
                             final boolean useMessageConsumer,
                             final int numberOfMessages,
                             final int numberOfBytes,
                             final int waitOnConsumer,
                             final long delayDelivery) throws Exception
   {
      testChunks(isXA,
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
                             final boolean realFiles,
                             final boolean preAck,
                             final boolean sendingBlocking,
                             final boolean testBrowser,
                             final boolean useMessageConsumer,
                             final int numberOfMessages,
                             final int numberOfBytes,
                             final int waitOnConsumer,
                             final long delayDelivery,
                             final int producerWindow,
                             final int minSize) throws Exception
   {
      clearData();

      server = createServer(realFiles);
      server.start();

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         if (sendingBlocking)
         {
            sf.setBlockOnNonPersistentSend(true);
            sf.setBlockOnPersistentSend(true);
            sf.setBlockOnAcknowledge(true);
         }

         if (producerWindow > 0)
         {
            sf.setSendWindowSize(producerWindow);
         }

         sf.setMinLargeMessageSize(minSize);

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

         sendMessages(numberOfMessages, numberOfBytes, delayDelivery, session, producer);

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

         validateNoFilesOnLargeDir();

         sendMessages(numberOfMessages, numberOfBytes, delayDelivery, session, producer);

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

         session.close();

         if (realFiles)
         {
            server.stop();

            server = createServer(realFiles);
            server.start();

            sf = createInVMFactory();
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

            System.out.println("Iteration: " + iteration);

            session.stop();

            // first time with a browser
            consumer = session.createConsumer(ADDRESS, null, iteration == 0);

            if (useMessageConsumer)
            {
               final CountDownLatch latchDone = new CountDownLatch(numberOfMessages);
               final AtomicInteger errrors = new AtomicInteger(0);

               MessageHandler handler = new MessageHandler()
               {
                  int msgCounter;

                  public void onMessage(final ClientMessage message)
                  {

                     try
                     {
                        latchDone.countDown();

                        System.out.println("Message on consumer: " + msgCounter);

                        if (delayDelivery > 0)
                        {
                           long originalTime = (Long)message.getProperty(new SimpleString("original-time"));
                           assertTrue(System.currentTimeMillis() - originalTime + "<" + delayDelivery,
                                      System.currentTimeMillis() - originalTime >= delayDelivery);
                        }

                        if (!preAck)
                        {
                           message.acknowledge();
                        }

                        assertNotNull(message);

                        if (delayDelivery <= 0)
                        {
                           // right now there is no guarantee of ordered delivered on multiple scheduledMessages with
                           // the same
                           // scheduled delivery time
                           assertEquals(msgCounter,
                                        ((Integer)message.getProperty(new SimpleString("counter-message"))).intValue());
                        }

                        MessagingBuffer buffer = message.getBody();
                        buffer.resetReaderIndex();
                        assertEquals(numberOfBytes, buffer.writerIndex());
                        for (int b = 0; b < numberOfBytes; b++)
                        {
                           assertEquals((byte)'a', buffer.readByte());
                        }
                     }
                     catch (Throwable e)
                     {
                        e.printStackTrace();
                        errrors.incrementAndGet();
                     }
                     finally
                     {
                        msgCounter++;
                     }
                  }
               };

               session.start();
               
               Thread.sleep(1000);

               consumer.setMessageHandler(handler);

               assertTrue(latchDone.await(20, TimeUnit.SECONDS));
               assertEquals(0, errrors.get());

            }
            else
            {

               session.start();

               for (int i = 0; i < numberOfMessages; i++)
               {
                  System.currentTimeMillis();

                  ClientMessage message = consumer.receive(waitOnConsumer + delayDelivery);

                  assertNotNull(message);

                  System.out.println("Message: " + i);

                  System.currentTimeMillis();

                  if (delayDelivery > 0)
                  {
                     long originalTime = (Long)message.getProperty(new SimpleString("original-time"));
                     assertTrue(System.currentTimeMillis() - originalTime + "<" + delayDelivery,
                                System.currentTimeMillis() - originalTime >= delayDelivery);
                  }

                  if (!preAck)
                  {
                     message.acknowledge();
                  }

                  assertNotNull(message);

                  if (delayDelivery <= 0)
                  {
                     // right now there is no guarantee of ordered delivered on multiple scheduledMessages with the same
                     // scheduled delivery time
                     assertEquals(i, ((Integer)message.getProperty(new SimpleString("counter-message"))).intValue());
                  }

                  MessagingBuffer buffer = message.getBody();
                  buffer.resetReaderIndex();
                  assertEquals(numberOfBytes, buffer.writerIndex());
                  for (int b = 0; b < numberOfBytes; b++)
                  {
                     assertEquals((byte)'a', buffer.readByte());
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

         long globalSize = server.getPostOffice().getPagingManager().getGlobalSize();
         assertEquals(0l, globalSize);
         assertEquals(0, ((Queue)server.getPostOffice().getBinding(ADDRESS).getBindable()).getDeliveringCount());
         assertEquals(0, ((Queue)server.getPostOffice().getBinding(ADDRESS).getBindable()).getMessageCount());

         validateNoFilesOnLargeDir();

      }
      finally
      {
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
    * @throws MessagingException
    */
   private void sendMessages(final int numberOfMessages,
                             final int numberOfBytes,
                             final long delayDelivery,
                             final ClientSession session,
                             final ClientProducer producer) throws Exception
   {
      for (int i = 0; i < numberOfMessages; i++)
      {
         ClientMessage message = session.createClientMessage(true);
         
         // If the test is using more than 1M, we will only use the Streaming, as it require too much memory from the test
         if (numberOfBytes > 1024 * 1024 || i % 2 == 0)
         {
            System.out.println("Sending message (stream)" + i);
            message.setBodyInputStream(createFakeLargeStream(numberOfBytes, (byte)'a'));
         }
         else
         {
            System.out.println("Sending message (array)" + i);
            byte[] bytes = new byte[numberOfBytes];
            for (int j = 0; j < bytes.length; j++)
            {
               bytes[j] = 'a';
            }
            message.getBody().writeBytes(bytes);
         }
         message.putIntProperty(new SimpleString("counter-message"), i);
         if (delayDelivery > 0)
         {
            long time = System.currentTimeMillis();
            message.putLongProperty(new SimpleString("original-time"), time);
            message.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, time + delayDelivery);

            producer.send(message);
         }
         else
         {
            producer.send(message);
         }
      }
   }

   protected MessagingBuffer createLargeBuffer(final int numberOfIntegers)
   {
      MessagingBuffer body = ChannelBuffers.buffer(DataConstants.SIZE_INT * numberOfIntegers);

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

   protected ClientMessage createLargeClientMessage(final ClientSession session,
                                                    final int numberOfBytes,
                                                    final boolean persistent) throws Exception
   {

      ClientMessage clientMessage = session.createClientMessage(persistent);

      clientMessage.setBodyInputStream(createFakeLargeStream(numberOfBytes, (byte)'a'));

      return clientMessage;
   }

   /**
    * @param session
    * @param queueToRead
    * @param numberOfIntegers
    * @throws MessagingException
    * @throws FileNotFoundException
    * @throws IOException
    */
   protected void readMessage(final ClientSession session, final SimpleString queueToRead, final int numberOfBytes) throws MessagingException,
                                                                                                                   FileNotFoundException,
                                                                                                                   IOException
   {
      session.start();

      ClientConsumer consumer = session.createConsumer(queueToRead);

      ClientMessage clientMessage = consumer.receive(5000);

      assertNotNull(clientMessage);

      clientMessage.acknowledge();

      session.commit();

      consumer.close();
   }

   /**
    * Deleting a file on LargeDire is an asynchronous process. Wee need to keep looking for a while if the file hasn't been deleted yet
    */
   protected void validateNoFilesOnLargeDir() throws Exception
   {
      File largeMessagesFileDir = new File(getLargeMessagesDir());

      // Deleting the file is async... we keep looking for a period of the time until the file is really gone
      for (int i = 0; i < 100; i++)
      {
         if (largeMessagesFileDir.listFiles().length > 0)
         {
            Thread.sleep(10);
         }
         else
         {
            break;
         }
      }

      assertEquals(0, largeMessagesFileDir.listFiles().length);
   }

   protected InputStream createFakeLargeStream(final int size, final byte byteUsed) throws Exception
   {
      return new InputStream()
      {
         private int count;

         private boolean closed = false;

         @Override
         public void close() throws IOException
         {
            super.close();
            closed = true;
         }

         @Override
         public int read() throws IOException
         {
            if (closed)
            {
               throw new IOException("Stream was closed");
            }
            if (count++ < size)
            {
               return byteUsed;
            }
            else
            {
               return -1;
            }
         }
      };

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
               System.out.println("OutputStream received " + count + " bytes");
            }
            if (closed)
            {
               throw new IOException("Stream was closed");
            }
         }

      };

   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
