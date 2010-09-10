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

package org.hornetq.tests.integration.client;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;
import junit.framework.AssertionFailedError;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.DivertConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.impl.TestSupportPageStore;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A PagingTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * 
 * Created Dec 5, 2008 8:25:58 PM
 *
 *
 */
public class PagingTest extends ServiceTestBase
{

   public PagingTest(final String name)
   {
      super(name);
   }

   public PagingTest()
   {
      super();
   }

   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(PagingTest.class);

   private static final int RECEIVE_TIMEOUT = 30000;

   private static final int PAGE_MAX = 100 * 1024;

   private static final int PAGE_SIZE = 10 * 1024;

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   static final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testSendReceivePagingPersistent() throws Exception
   {
      internaltestSendReceivePaging(true);
   }

   public void testSendReceivePagingNonPersistent() throws Exception
   {
      internaltestSendReceivePaging(false);
   }

   public void testWithDiverts() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      DivertConfiguration divert1 = new DivertConfiguration("dv1",
                                                            "nm1",
                                                            PagingTest.ADDRESS.toString(),
                                                            PagingTest.ADDRESS.toString() + "-1",
                                                            true,
                                                            null,
                                                            null);

      DivertConfiguration divert2 = new DivertConfiguration("dv2",
                                                            "nm2",
                                                            PagingTest.ADDRESS.toString(),
                                                            PagingTest.ADDRESS.toString() + "-2",
                                                            true,
                                                            null,
                                                            null);

      ArrayList<DivertConfiguration> divertList = new ArrayList<DivertConfiguration>();
      divertList.add(divert1);
      divertList.add(divert2);

      config.setDivertConfigurations(divertList);

      server.start();

      final int numberOfIntegers = 256;

      final int numberOfMessages = 30000;

      final byte[] body = new byte[numberOfIntegers * 4];

      ByteBuffer bb = ByteBuffer.wrap(body);

      for (int j = 1; j <= numberOfIntegers; j++)
      {
         bb.putInt(j);
      }

      try
      {
         {
            ClientSessionFactory sf = createInVMFactory();

            sf.setBlockOnNonDurableSend(true);
            sf.setBlockOnDurableSend(true);
            sf.setBlockOnAcknowledge(true);

            ClientSession session = sf.createSession(false, false, false);

            session.createQueue(PagingTest.ADDRESS + "-1", PagingTest.ADDRESS + "-1", null, true);

            session.createQueue(PagingTest.ADDRESS + "-2", PagingTest.ADDRESS + "-2", null, true);

            ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

            ClientMessage message = null;

            for (int i = 0; i < numberOfMessages; i++)
            {
               message = session.createMessage(true);

               HornetQBuffer bodyLocal = message.getBodyBuffer();

               bodyLocal.writeBytes(body);

               message.putIntProperty(new SimpleString("id"), i);

               producer.send(message);
            }

            session.commit();

            session.close();

            server.stop();
         }

         server = createServer(true,
                               config,
                               PagingTest.PAGE_SIZE,
                               PagingTest.PAGE_MAX,
                               new HashMap<String, AddressSettings>());
         server.start();

         final ClientSessionFactory sf2 = createInVMFactory();

         final AtomicInteger errors = new AtomicInteger(0);

         Thread threads[] = new Thread[2];

         for (int start = 1; start <= 2; start++)
         {

            final String addressToSubscribe = PagingTest.ADDRESS + "-" + start;

            threads[start - 1] = new Thread()
            {
               public void run()
               {
                  try
                  {
                     ClientSession session = sf2.createSession(null, null, false, true, true, false, 0);

                     ClientConsumer consumer = session.createConsumer(addressToSubscribe);

                     session.start();

                     for (int i = 0; i < numberOfMessages; i++)
                     {
                        ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

                        Assert.assertNotNull(message2);

                        Assert.assertEquals(i, message2.getIntProperty("id").intValue());

                        message2.acknowledge();

                        Assert.assertNotNull(message2);

                        session.commit();

                        try
                        {
                           assertBodiesEqual(body, message2.getBodyBuffer());
                        }
                        catch (AssertionFailedError e)
                        {
                           PagingTest.log.info("Expected buffer:" + UnitTestCase.dumbBytesHex(body, 40));
                           PagingTest.log.info("Arriving buffer:" + UnitTestCase.dumbBytesHex(message2.getBodyBuffer()
                                                                                                      .toByteBuffer()
                                                                                                      .array(), 40));
                           throw e;
                        }
                     }

                     consumer.close();
                  }
                  catch (Throwable e)
                  {
                     e.printStackTrace();
                     errors.incrementAndGet();
                  }

               }
            };
         }

         for (int i = 0; i < 2; i++)
         {
            threads[i].start();
         }

         for (int i = 0; i < 2; i++)
         {
            threads[i].join();
         }
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

   private void internaltestSendReceivePaging(final boolean persistentMessages) throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfIntegers = 256;

      final int numberOfMessages = 10000;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonDurableSend(true);
         sf.setBlockOnDurableSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[numberOfIntegers * 4];

         ByteBuffer bb = ByteBuffer.wrap(body);

         for (int j = 1; j <= numberOfIntegers; j++)
         {
            bb.putInt(j);
         }

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(persistentMessages);

            HornetQBuffer bodyLocal = message.getBodyBuffer();

            bodyLocal.writeBytes(body);

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
         }

         session.close();

         if (persistentMessages)
         {
            server.stop();

            server = createServer(true,
                                  config,
                                  PagingTest.PAGE_SIZE,
                                  PagingTest.PAGE_MAX,
                                  new HashMap<String, AddressSettings>());
            server.start();
         }

         sf = createInVMFactory();

         session = sf.createSession(null, null, false, true, true, false, 0);

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();

         for (int i = 0; i < numberOfMessages; i++)
         {
            ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message2);

            Assert.assertEquals(i, message2.getIntProperty("id").intValue());

            message2.acknowledge();

            Assert.assertNotNull(message2);

            session.commit();

            try
            {
               assertBodiesEqual(body, message2.getBodyBuffer());
            }
            catch (AssertionFailedError e)
            {
               PagingTest.log.info("Expected buffer:" + UnitTestCase.dumbBytesHex(body, 40));
               PagingTest.log.info("Arriving buffer:" + UnitTestCase.dumbBytesHex(message2.getBodyBuffer()
                                                                                          .toByteBuffer()
                                                                                          .array(), 40));
               throw e;
            }
         }

         consumer.close();

         session.close();
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

   private void assertBodiesEqual(final byte[] body, final HornetQBuffer buffer)
   {
      byte[] other = new byte[body.length];

      buffer.readBytes(other);

      UnitTestCase.assertEqualsByteArrays(body, other);
   }

   /**
    * - Make a destination in page mode
    * - Add stuff to a transaction
    * - Consume the entire destination (not in page mode any more)
    * - Add stuff to a transaction again
    * - Check order
    * 
    */
   public void testDepageDuringTransaction() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024; // 1k

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonDurableSend(true);
         sf.setBlockOnDurableSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         byte[] body = new byte[messageSize];
         // HornetQBuffer bodyLocal = HornetQChannelBuffers.buffer(DataConstants.SIZE_INT * numberOfIntegers);

         ClientMessage message = null;

         int numberOfMessages = 0;
         while (true)
         {
            message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(body);

            // Stop sending message as soon as we start paging
            if (server.getPostOffice().getPagingManager().getPageStore(PagingTest.ADDRESS).isPaging())
            {
               break;
            }
            numberOfMessages++;

            producer.send(message);
         }

         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PagingTest.ADDRESS).isPaging());

         session.start();

         ClientSession sessionTransacted = sf.createSession(null, null, false, false, false, false, 0);

         ClientProducer producerTransacted = sessionTransacted.createProducer(PagingTest.ADDRESS);

         for (int i = 0; i < 10; i++)
         {
            message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(body);
            message.putIntProperty(new SimpleString("id"), i);

            // Consume messages to force an eventual out of order delivery
            if (i == 5)
            {
               ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);
               for (int j = 0; j < numberOfMessages; j++)
               {
                  ClientMessage msg = consumer.receive(PagingTest.RECEIVE_TIMEOUT);
                  msg.acknowledge();
                  Assert.assertNotNull(msg);
               }

               Assert.assertNull(consumer.receiveImmediate());
               consumer.close();
            }

            Integer messageID = (Integer)message.getObjectProperty(new SimpleString("id"));
            Assert.assertNotNull(messageID);
            Assert.assertEquals(messageID.intValue(), i);

            producerTransacted.send(message);
         }

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         Assert.assertNull(consumer.receiveImmediate());

         sessionTransacted.commit();

         sessionTransacted.close();

         for (int i = 0; i < 10; i++)
         {
            message = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message);

            Integer messageID = (Integer)message.getObjectProperty(new SimpleString("id"));

            Assert.assertNotNull(messageID);
            Assert.assertEquals("message received out of order", messageID.intValue(), i);

            message.acknowledge();
         }

         Assert.assertNull(consumer.receiveImmediate());

         consumer.close();

         session.close();
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
    * - Make a destination in page mode
    * - Add stuff to a transaction
    * - Consume the entire destination (not in page mode any more)
    * - Add stuff to a transaction again
    * - Check order
    * 
    *  Test under discussion at : http://community.jboss.org/thread/154061?tstart=0
    * 
    */
   public void testDepageDuringTransaction2() throws Exception
   {
      boolean IS_DURABLE_MESSAGE = true;
      clearData();

      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024; // 1k

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonDurableSend(true);
         sf.setBlockOnDurableSend(true);
         sf.setBlockOnAcknowledge(true);

         byte[] body = new byte[messageSize];

         ClientSession sessionTransacted = sf.createSession(null, null, false, false, false, false, 0);
         ClientProducer producerTransacted = sessionTransacted.createProducer(PagingTest.ADDRESS);

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);
         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientMessage firstMessage = sessionTransacted.createMessage(IS_DURABLE_MESSAGE);
         firstMessage.getBodyBuffer().writeBytes(body);
         firstMessage.putIntProperty(new SimpleString("id"), 0);

         producerTransacted.send(firstMessage);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         int numberOfMessages = 0;
         while (true)
         {
            message = session.createMessage(IS_DURABLE_MESSAGE);
            message.getBodyBuffer().writeBytes(body);

            // Stop sending message as soon as we start paging
            if (server.getPostOffice().getPagingManager().getPageStore(PagingTest.ADDRESS).isPaging())
            {
               break;
            }
            numberOfMessages++;

            producer.send(message);
         }

         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PagingTest.ADDRESS).isPaging());

         session.start();

         for (int i = 1; i < 10; i++)
         {
            message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(body);
            message.putIntProperty(new SimpleString("id"), i);

            // Consume messages to force an eventual out of order delivery
            if (i == 5)
            {
               ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);
               for (int j = 0; j < numberOfMessages; j++)
               {
                  ClientMessage msg = consumer.receive(PagingTest.RECEIVE_TIMEOUT);
                  msg.acknowledge();
                  Assert.assertNotNull(msg);
               }

               Assert.assertNull(consumer.receiveImmediate());
               consumer.close();
            }

            Integer messageID = (Integer)message.getObjectProperty(new SimpleString("id"));
            Assert.assertNotNull(messageID);
            Assert.assertEquals(messageID.intValue(), i);

            producerTransacted.send(message);
         }

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         Assert.assertNull(consumer.receiveImmediate());

         sessionTransacted.commit();

         sessionTransacted.close();

         for (int i = 0; i < 10; i++)
         {
            message = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message);

            Integer messageID = (Integer)message.getObjectProperty(new SimpleString("id"));

            // System.out.println(messageID);
            Assert.assertNotNull(messageID);
            Assert.assertEquals("message received out of order", i, messageID.intValue());

            message.acknowledge();
         }

         Assert.assertNull(consumer.receiveImmediate());

         consumer.close();

         session.close();
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

   public void testDepageDuringTransaction3() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int messageSize = 1024; // 1k

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonDurableSend(true);
         sf.setBlockOnDurableSend(true);
         sf.setBlockOnAcknowledge(true);

         byte[] body = new byte[messageSize];

         ClientSession sessionTransacted = sf.createSession(null, null, false, false, false, false, 0);
         ClientProducer producerTransacted = sessionTransacted.createProducer(PagingTest.ADDRESS);

         ClientSession sessionNonTX = sf.createSession(true, true, 0);
         sessionNonTX.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producerNonTransacted = sessionNonTX.createProducer(PagingTest.ADDRESS);

         sessionNonTX.start();

         for (int i = 0; i < 50; i++)
         {
            System.out.println("Sending " + i);
            ClientMessage message = sessionNonTX.createMessage(true);
            message.getBodyBuffer().writeBytes(body);
            message.putIntProperty(new SimpleString("id"), i);

            producerTransacted.send(message);
            
            if (i % 2 == 0)
            {
               System.out.println("Sending 20 msgs to make it page");
               for (int j = 0 ; j < 20; j++)
               {
                  ClientMessage msgSend = sessionNonTX.createMessage(true);
                  msgSend.getBodyBuffer().writeBytes(new byte[10 * 1024]);
                  producerNonTransacted.send(msgSend);
               }
               assertTrue(server.getPostOffice().getPagingManager().getPageStore(PagingTest.ADDRESS).isPaging());
            }
            else
            {
               System.out.println("Consuming 20 msgs to make it page");
               ClientConsumer consumer = sessionNonTX.createConsumer(PagingTest.ADDRESS);
               for (int j = 0 ; j < 20; j++)
               {
                  ClientMessage msgReceived = consumer.receive(10000);
                  assertNotNull(msgReceived);
                  msgReceived.acknowledge();
               }
               consumer.close();
            }
         }
         
         ClientConsumer consumerNonTX = sessionNonTX.createConsumer(PagingTest.ADDRESS);
         while (true)
         {
            ClientMessage msgReceived = consumerNonTX.receive(1000);
            if (msgReceived == null)
            {
               break;
            }
            msgReceived.acknowledge();
         }
         consumerNonTX.close();
         

         ClientConsumer consumer = sessionNonTX.createConsumer(PagingTest.ADDRESS);

         Assert.assertNull(consumer.receiveImmediate());

         sessionTransacted.commit();

         sessionTransacted.close();

         for (int i = 0; i < 50; i++)
         {
            ClientMessage message = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message);

            Integer messageID = (Integer)message.getObjectProperty(new SimpleString("id"));

            // System.out.println(messageID);
            Assert.assertNotNull(messageID);
            Assert.assertEquals("message received out of order", i, messageID.intValue());
            
            System.out.println("MessageID = " + messageID);

            message.acknowledge();
         }

         Assert.assertNull(consumer.receiveImmediate());

         consumer.close();

         sessionNonTX.close();
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

   public void testPageOnSchedulingNoRestart() throws Exception
   {
      internalTestPageOnScheduling(false);
   }

   public void testPageOnSchedulingRestart() throws Exception
   {
      internalTestPageOnScheduling(true);
   }

   public void internalTestPageOnScheduling(final boolean restart) throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfMessages = 10000;

      final int numberOfBytes = 1024;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonDurableSend(true);
         sf.setBlockOnDurableSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[numberOfBytes];

         for (int j = 0; j < numberOfBytes; j++)
         {
            body[j] = UnitTestCase.getSamplebyte(j);
         }

         long scheduledTime = System.currentTimeMillis() + 5000;

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(true);

            message.getBodyBuffer().writeBytes(body);
            message.putIntProperty(new SimpleString("id"), i);

            TestSupportPageStore store = (TestSupportPageStore)server.getPostOffice()
                                                                     .getPagingManager()
                                                                     .getPageStore(PagingTest.ADDRESS);

            // Worse scenario possible... only schedule what's on pages
            if (store.getCurrentPage() != null)
            {
               message.putLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME, scheduledTime);
            }

            producer.send(message);
         }

         if (restart)
         {
            session.close();

            server.stop();

            server = createServer(true,
                                  config,
                                  PagingTest.PAGE_SIZE,
                                  PagingTest.PAGE_MAX,
                                  new HashMap<String, AddressSettings>());
            server.start();

            sf = createInVMFactory();

            session = sf.createSession(null, null, false, true, true, false, 0);
         }

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();

         for (int i = 0; i < numberOfMessages; i++)
         {

            ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message2);

            message2.acknowledge();

            Assert.assertNotNull(message2);

            Long scheduled = (Long)message2.getObjectProperty(Message.HDR_SCHEDULED_DELIVERY_TIME);
            if (scheduled != null)
            {
               Assert.assertTrue("Scheduling didn't work", System.currentTimeMillis() >= scheduledTime);
            }

            try
            {
               assertBodiesEqual(body, message2.getBodyBuffer());
            }
            catch (AssertionFailedError e)
            {
               PagingTest.log.info("Expected buffer:" + UnitTestCase.dumbBytesHex(body, 40));
               PagingTest.log.info("Arriving buffer:" + UnitTestCase.dumbBytesHex(message2.getBodyBuffer()
                                                                                          .toByteBuffer()
                                                                                          .array(), 40));
               throw e;
            }
         }

         consumer.close();

         session.close();
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

   public void testRollbackOnSend() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfIntegers = 256;

      final int numberOfMessages = 10;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonDurableSend(true);
         sf.setBlockOnDurableSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, false, true, false, 0);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(true);

            HornetQBuffer bodyLocal = message.getBodyBuffer();

            for (int j = 1; j <= numberOfIntegers; j++)
            {
               bodyLocal.writeInt(j);
            }

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
         }

         session.rollback();

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();

         Assert.assertNull(consumer.receiveImmediate());

         session.close();
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

   public void testCommitOnSend() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      final int numberOfIntegers = 10;

      final int numberOfMessages = 500;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonDurableSend(true);
         sf.setBlockOnDurableSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, false, false, false, 0);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         for (int i = 0; i < numberOfMessages; i++)
         {
            message = session.createMessage(true);

            HornetQBuffer bodyLocal = message.getBodyBuffer();

            for (int j = 1; j <= numberOfIntegers; j++)
            {
               bodyLocal.writeInt(j);
            }

            message.putIntProperty(new SimpleString("id"), i);

            producer.send(message);
         }

         session.commit();

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();
         for (int i = 0; i < numberOfMessages; i++)
         {
            System.out.println("Received " + i);
            ClientMessage msg = consumer.receive(5000);
            Assert.assertNotNull(msg);
            msg.acknowledge();
            session.commit();
         }

         session.close();
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

   public void testPageMultipleDestinations() throws Exception
   {
      internalTestPageMultipleDestinations(false);
   }

   public void testPageMultipleDestinationsTransacted() throws Exception
   {
      internalTestPageMultipleDestinations(true);
   }

   public void testDropMessages() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HashMap<String, AddressSettings> settings = new HashMap<String, AddressSettings>();

      AddressSettings set = new AddressSettings();
      set.setAddressFullMessagePolicy(AddressFullMessagePolicy.DROP);

      settings.put(PagingTest.ADDRESS.toString(), set);

      HornetQServer server = createServer(true, config, 1024, 10 * 1024, settings);

      server.start();

      final int numberOfMessages = 1000;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonDurableSend(true);
         sf.setBlockOnDurableSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         for (int i = 0; i < numberOfMessages; i++)
         {
            byte[] body = new byte[1024];

            message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(body);

            producer.send(message);
         }

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();

         for (int i = 0; i < 6; i++)
         {
            ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message2);

            message2.acknowledge();
         }

         Assert.assertNull(consumer.receiveImmediate());

         Assert.assertEquals(0, server.getPostOffice()
                                      .getPagingManager()
                                      .getPageStore(PagingTest.ADDRESS)
                                      .getAddressSize());

         for (int i = 0; i < numberOfMessages; i++)
         {
            byte[] body = new byte[1024];

            message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(body);

            producer.send(message);
         }

         for (int i = 0; i < 6; i++)
         {
            ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message2);

            message2.acknowledge();
         }

         Assert.assertNull(consumer.receiveImmediate());

         session.close();

         session = sf.createSession(false, true, true);

         producer = session.createProducer(PagingTest.ADDRESS);

         for (int i = 0; i < numberOfMessages; i++)
         {
            byte[] body = new byte[1024];

            message = session.createMessage(true);
            message.getBodyBuffer().writeBytes(body);

            producer.send(message);
         }

         session.commit();

         consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();

         for (int i = 0; i < 6; i++)
         {
            ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

            Assert.assertNotNull(message2);

            message2.acknowledge();
         }

         session.commit();

         Assert.assertNull(consumer.receiveImmediate());

         session.close();

         Assert.assertEquals(0, server.getPostOffice()
                                      .getPagingManager()
                                      .getPageStore(PagingTest.ADDRESS)
                                      .getAddressSize());

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

   public void testDropMessagesExpiring() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig();

      HashMap<String, AddressSettings> settings = new HashMap<String, AddressSettings>();

      AddressSettings set = new AddressSettings();
      set.setAddressFullMessagePolicy(AddressFullMessagePolicy.DROP);

      settings.put(PagingTest.ADDRESS.toString(), set);

      HornetQServer server = createServer(true, config, 1024, 1024 * 1024, settings);

      server.start();

      final int numberOfMessages = 30000;

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setAckBatchSize(0);

         ClientSession session = sf.createSession();

         session.createQueue(PagingTest.ADDRESS, PagingTest.ADDRESS, null, true);

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         class MyHandler implements MessageHandler
         {
            int count;

            public void onMessage(ClientMessage message)
            {
               try
               {
                  Thread.sleep(1);
               }
               catch (Exception e)
               {

               }

               count++;

               if (count % 1000 == 0)
               {
                  log.info("received " + count);
               }

               try
               {
                  message.acknowledge();
               }
               catch (Exception e)
               {
                  e.printStackTrace();
               }
            }
         }

         ClientConsumer consumer = session.createConsumer(PagingTest.ADDRESS);

         session.start();

         consumer.setMessageHandler(new MyHandler());

         for (int i = 0; i < numberOfMessages; i++)
         {
            byte[] body = new byte[1024];

            message = session.createMessage(false);
            message.getBodyBuffer().writeBytes(body);

            message.setExpiration(System.currentTimeMillis() + 100);

            producer.send(message);
         }

         session.close();
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

   private void internalTestPageMultipleDestinations(final boolean transacted) throws Exception
   {
      Configuration config = createDefaultConfig();

      final int NUMBER_OF_BINDINGS = 100;

      int NUMBER_OF_MESSAGES = 2;

      HornetQServer server = createServer(true,
                                          config,
                                          PagingTest.PAGE_SIZE,
                                          PagingTest.PAGE_MAX,
                                          new HashMap<String, AddressSettings>());

      server.start();

      try
      {
         ClientSessionFactory sf = createInVMFactory();

         sf.setBlockOnNonDurableSend(true);
         sf.setBlockOnDurableSend(true);
         sf.setBlockOnAcknowledge(true);

         ClientSession session = sf.createSession(null, null, false, !transacted, true, false, 0);

         for (int i = 0; i < NUMBER_OF_BINDINGS; i++)
         {
            session.createQueue(PagingTest.ADDRESS, new SimpleString("someQueue" + i), null, true);
         }

         ClientProducer producer = session.createProducer(PagingTest.ADDRESS);

         ClientMessage message = null;

         byte[] body = new byte[1024];

         message = session.createMessage(true);
         message.getBodyBuffer().writeBytes(body);

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            producer.send(message);

            if (transacted)
            {
               session.commit();
            }
         }

         session.close();

         server.stop();

         server = createServer(true,
                               config,
                               PagingTest.PAGE_SIZE,
                               PagingTest.PAGE_MAX,
                               new HashMap<String, AddressSettings>());
         server.start();

         sf = createInVMFactory();

         session = sf.createSession(null, null, false, true, true, false, 0);

         session.start();

         for (int msg = 0; msg < NUMBER_OF_MESSAGES; msg++)
         {

            for (int i = 0; i < NUMBER_OF_BINDINGS; i++)
            {
               ClientConsumer consumer = session.createConsumer(new SimpleString("someQueue" + i));

               ClientMessage message2 = consumer.receive(PagingTest.RECEIVE_TIMEOUT);

               Assert.assertNotNull(message2);

               message2.acknowledge();

               Assert.assertNotNull(message2);

               consumer.close();

            }
         }

         session.close();

         for (int i = 0; i < NUMBER_OF_BINDINGS; i++)
         {
            Queue queue = (Queue)server.getPostOffice().getBinding(new SimpleString("someQueue" + i)).getBindable();

            Assert.assertEquals("Queue someQueue" + i + " was supposed to be empty", 0, queue.getMessageCount());
            Assert.assertEquals("Queue someQueue" + i + " was supposed to be empty", 0, queue.getDeliveringCount());
         }

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

   public void testPagingOneDestinationOnly() throws Exception
   {
      SimpleString PAGED_ADDRESS = new SimpleString("paged");
      SimpleString NON_PAGED_ADDRESS = new SimpleString("non-paged");

      Configuration configuration = createDefaultConfig();

      Map<String, AddressSettings> addresses = new HashMap<String, AddressSettings>();

      addresses.put("#", new AddressSettings());

      AddressSettings pagedDestination = new AddressSettings();
      pagedDestination.setPageSizeBytes(1024);
      pagedDestination.setMaxSizeBytes(10 * 1024);

      addresses.put(PAGED_ADDRESS.toString(), pagedDestination);

      HornetQServer server = createServer(true, configuration, -1, -1, addresses);

      try
      {
         server.start();

         ClientSessionFactory sf = createInVMFactory();

         ClientSession session = sf.createSession(false, true, false);

         session.createQueue(PAGED_ADDRESS, PAGED_ADDRESS, true);

         session.createQueue(NON_PAGED_ADDRESS, NON_PAGED_ADDRESS, true);

         ClientProducer producerPaged = session.createProducer(PAGED_ADDRESS);
         ClientProducer producerNonPaged = session.createProducer(NON_PAGED_ADDRESS);

         int NUMBER_OF_MESSAGES = 100;

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = session.createMessage(true);
            msg.getBodyBuffer().writeBytes(new byte[512]);

            producerPaged.send(msg);
            producerNonPaged.send(msg);
         }

         session.close();

         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS).isPaging());
         Assert.assertFalse(server.getPostOffice().getPagingManager().getPageStore(NON_PAGED_ADDRESS).isPaging());

         session = sf.createSession(false, true, false);

         session.start();

         ClientConsumer consumerNonPaged = session.createConsumer(NON_PAGED_ADDRESS);
         ClientConsumer consumerPaged = session.createConsumer(PAGED_ADDRESS);

         ClientMessage ackList[] = new ClientMessage[NUMBER_OF_MESSAGES];

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = consumerNonPaged.receive(5000);
            Assert.assertNotNull(msg);
            ackList[i] = msg;
         }

         Assert.assertNull(consumerNonPaged.receiveImmediate());

         consumerNonPaged.close();

         for (ClientMessage ack : ackList)
         {
            ack.acknowledge();
         }

         session.commit();

         ackList = null;

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = consumerPaged.receive(5000);
            Assert.assertNotNull(msg);
            msg.acknowledge();
            session.commit();
         }

         Assert.assertNull(consumerPaged.receiveImmediate());

         session.close();

      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testPagingDifferentSizes() throws Exception
   {
      SimpleString PAGED_ADDRESS_A = new SimpleString("paged-a");
      SimpleString PAGED_ADDRESS_B = new SimpleString("paged-b");

      Configuration configuration = createDefaultConfig();

      Map<String, AddressSettings> addresses = new HashMap<String, AddressSettings>();

      addresses.put("#", new AddressSettings());

      AddressSettings pagedDestinationA = new AddressSettings();
      pagedDestinationA.setPageSizeBytes(1024);
      pagedDestinationA.setMaxSizeBytes(10 * 1024);

      int NUMBER_MESSAGES_BEFORE_PAGING = 11;

      addresses.put(PAGED_ADDRESS_A.toString(), pagedDestinationA);

      AddressSettings pagedDestinationB = new AddressSettings();
      pagedDestinationB.setPageSizeBytes(2024);
      pagedDestinationB.setMaxSizeBytes(25 * 1024);

      addresses.put(PAGED_ADDRESS_B.toString(), pagedDestinationB);

      HornetQServer server = createServer(true, configuration, -1, -1, addresses);

      try
      {
         server.start();

         ClientSessionFactory sf = createInVMFactory();

         ClientSession session = sf.createSession(false, true, false);

         session.createQueue(PAGED_ADDRESS_A, PAGED_ADDRESS_A, true);

         session.createQueue(PAGED_ADDRESS_B, PAGED_ADDRESS_B, true);

         ClientProducer producerA = session.createProducer(PAGED_ADDRESS_A);
         ClientProducer producerB = session.createProducer(PAGED_ADDRESS_B);

         int NUMBER_OF_MESSAGES = 100;

         for (int i = 0; i < NUMBER_MESSAGES_BEFORE_PAGING; i++)
         {
            ClientMessage msg = session.createMessage(true);
            msg.getBodyBuffer().writeBytes(new byte[512]);

            producerA.send(msg);
            producerB.send(msg);
         }

         session.commit(); // commit was called to clean the buffer only (making sure everything is on the server side)

         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_A).isPaging());
         Assert.assertFalse(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_B).isPaging());

         for (int i = 0; i < NUMBER_MESSAGES_BEFORE_PAGING; i++)
         {
            ClientMessage msg = session.createMessage(true);
            msg.getBodyBuffer().writeBytes(new byte[512]);

            producerA.send(msg);
            producerB.send(msg);
         }

         session.commit(); // commit was called to clean the buffer only (making sure everything is on the server side)

         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_A).isPaging());
         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_B).isPaging());

         for (int i = NUMBER_MESSAGES_BEFORE_PAGING * 2; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = session.createMessage(true);
            msg.getBodyBuffer().writeBytes(new byte[512]);

            producerA.send(msg);
            producerB.send(msg);
         }

         session.close();

         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_A).isPaging());
         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_B).isPaging());

         session = sf.createSession(null, null, false, true, true, false, 0);

         session.start();

         ClientConsumer consumerA = session.createConsumer(PAGED_ADDRESS_A);

         ClientConsumer consumerB = session.createConsumer(PAGED_ADDRESS_B);

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = consumerA.receive(5000);
            Assert.assertNotNull("Couldn't receive a message on consumerA, iteration = " + i, msg);
            msg.acknowledge();
         }

         Assert.assertNull(consumerA.receiveImmediate());

         consumerA.close();

         Assert.assertTrue(server.getPostOffice().getPagingManager().getPageStore(PAGED_ADDRESS_B).isPaging());

         for (int i = 0; i < NUMBER_OF_MESSAGES; i++)
         {
            ClientMessage msg = consumerB.receive(5000);
            Assert.assertNotNull(msg);
            msg.acknowledge();
            session.commit();
         }

         Assert.assertNull(consumerB.receiveImmediate());

         consumerB.close();

         session.close();

      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected Configuration createDefaultConfig()
   {
      Configuration config = super.createDefaultConfig();
      config.setJournalSyncNonTransactional(false);
      return config;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
