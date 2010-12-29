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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import junit.framework.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientProducerCreditManagerImpl;
import org.hornetq.core.client.impl.ClientProducerCredits;
import org.hornetq.core.client.impl.ClientProducerInternal;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;

/**
 * 
 * A ProducerFlowControlTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> fox
 *
 */
public class ProducerFlowControlTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(ProducerFlowControlTest.class);

   private ServerLocator locator;

   protected boolean isNetty()
   {
      return false;
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      locator = createFactory(isNetty());
   }

   @Override
   protected void tearDown() throws Exception
   {
      locator.close();

      super.tearDown();
   }

   // TODO need to test crashing a producer with unused credits returns them to the pool

   public void testFlowControlSingleConsumer() throws Exception
   {
      testFlowControl(1000, 500, 10 * 1024, 1024, 1024, 1024, 1, 1, 0, false);
   }

   public void testFlowControlAnon() throws Exception
   {
      testFlowControl(1000, 500, 10 * 1024, 1024, 1024, 1024, 1, 1, 0, true);
   }

   public void testFlowControlSingleConsumerLargeMaxSize() throws Exception
   {
      testFlowControl(1000, 500, 1024 * 1024, 1024, 1024, 1024, 1, 1, 0, false);
   }

   public void testFlowControlMultipleConsumers() throws Exception
   {
      testFlowControl(1000, 500, -1, 1024, 1024, 1024, 5, 1, 0, false);
   }

   public void testFlowControlZeroConsumerWindowSize() throws Exception
   {
      testFlowControl(1000, 500, 10 * 1024, 1024, 0, 1024, 1, 1, 0, false);
   }

   public void testFlowControlZeroAckBatchSize() throws Exception
   {
      testFlowControl(1000, 500, 10 * 1024, 1024, 1024, 0, 1, 1, 0, false);
   }

   public void testFlowControlSingleConsumerSlowConsumer() throws Exception
   {
      testFlowControl(100, 500, 1024, 512, 512, 512, 1, 1, 10, false);
   }

   public void testFlowControlSmallMessages() throws Exception
   {
      testFlowControl(1000, 0, 10 * 1024, 1024, 1024, 1024, 1, 1, 0, false);
   }

   public void testFlowControlLargerMessagesSmallWindowSize() throws Exception
   {
      testFlowControl(1000, 10 * 1024, 10 * 1024, 1024, 1024, 1024, 1, 1, 0, false);
   }

   public void testFlowControlMultipleProducers() throws Exception
   {
      testFlowControl(1000, 500, 1024 * 1024, 1024, 1024, 1024, 1, 5, 0, false);
   }

   public void testFlowControlMultipleProducersAndConsumers() throws Exception
   {
      testFlowControl(500, 500, 100 * 1024, 1024, 1024, 1024, 1, 3, 3, false);
   }

   public void testFlowControlMultipleProducersAnon() throws Exception
   {
      testFlowControl(1000, 500, 1024 * 1024, 1024, 1024, 1024, 1, 5, 0, true);
   }

   public void testFlowControlLargeMessages2() throws Exception
   {
      testFlowControl(1000, 10000, -1, 1024, 0, 0, 1, 1, 0, false, 1000, true);
   }

   public void testFlowControlLargeMessages3() throws Exception
   {
      testFlowControl(1000, 10000, 100 * 1024, 1024, 1024, 0, 1, 1, 0, false, 1000, true);
   }

   public void testFlowControlLargeMessages4() throws Exception
   {
      testFlowControl(1000, 10000, 100 * 1024, 1024, 1024, 1024, 1, 1, 0, false, 1000, true);
   }

   public void testFlowControlLargeMessages5() throws Exception
   {
      testFlowControl(1000, 10000, 100 * 1024, 1024, -1, 1024, 1, 1, 0, false, 1000, true);
   }

   public void testFlowControlLargeMessages6() throws Exception
   {
      testFlowControl(1000, 10000, 100 * 1024, 1024, 1024, 1024, 1, 1, 0, true, 1000, true);
   }

   public void testFlowControlLargeMessages7() throws Exception
   {
      testFlowControl(1000, 10000, 100 * 1024, 1024, 1024, 1024, 2, 2, 0, true, 1000, true);
   }

   private void testFlowControl(final int numMessages,
                                final int messageSize,
                                final int maxSize,
                                final int producerWindowSize,
                                final int consumerWindowSize,
                                final int ackBatchSize,
                                final int numConsumers,
                                final int numProducers,
                                final long consumerDelay,
                                final boolean anon) throws Exception
   {
      testFlowControl(numMessages,
                      messageSize,
                      maxSize,
                      producerWindowSize,
                      consumerWindowSize,
                      ackBatchSize,
                      numConsumers,
                      numProducers,
                      consumerDelay,
                      anon,
                      -1,
                      false);
   }

   private void testFlowControl(final int numMessages,
                                final int messageSize,
                                final int maxSize,
                                final int producerWindowSize,
                                final int consumerWindowSize,
                                final int ackBatchSize,
                                final int numConsumers,
                                final int numProducers,
                                final long consumerDelay,
                                final boolean anon,
                                final int minLargeMessageSize,
                                final boolean realFiles) throws Exception
   {
      final SimpleString address = new SimpleString("testaddress");

      HornetQServer server = createServer(realFiles, isNetty());

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxSizeBytes(maxSize);
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);

      HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
      repos.addMatch(address.toString(), addressSettings);

      server.start();

      locator.setProducerWindowSize(producerWindowSize);
      locator.setConsumerWindowSize(consumerWindowSize);
      locator.setAckBatchSize(ackBatchSize);

      if (minLargeMessageSize != -1)
      {
         locator.setMinLargeMessageSize(minLargeMessageSize);
      }

      ClientSessionFactory sf = locator.createSessionFactory();
      ClientSession session = sf.createSession(false, true, true, true);

      session.start();

      final String queueName = "testqueue";

      for (int i = 0; i < numConsumers; i++)
      {
         session.createQueue(address, new SimpleString(queueName + i), null, false);
      }

      final byte[] bytes = RandomUtil.randomBytes(messageSize);

      class MyHandler implements MessageHandler
      {
         int count = 0;

         final CountDownLatch latch = new CountDownLatch(1);

         volatile Exception exception;

         public void onMessage(final ClientMessage message)
         {
            try
            {
               byte[] bytesRead = new byte[messageSize];

               message.getBodyBuffer().readBytes(bytesRead);

               UnitTestCase.assertEqualsByteArrays(bytes, bytesRead);

               message.acknowledge();

               if (++count == numMessages * numProducers)
               {
                  latch.countDown();
               }

               if (consumerDelay > 0)
               {
                  Thread.sleep(consumerDelay);
               }

            }
            catch (Exception e)
            {
               ProducerFlowControlTest.log.error("Failed to handle message", e);

               exception = e;

               latch.countDown();
            }
         }
      }

      MyHandler[] handlers = new MyHandler[numConsumers];

      for (int i = 0; i < numConsumers; i++)
      {
         handlers[i] = new MyHandler();

         ClientConsumer consumer = session.createConsumer(new SimpleString(queueName + i));

         consumer.setMessageHandler(handlers[i]);
      }

      ClientProducer[] producers = new ClientProducer[numProducers];

      for (int i = 0; i < numProducers; i++)
      {
         if (anon)
         {
            producers[i] = session.createProducer();
         }
         else
         {
            producers[i] = session.createProducer(address);
         }
      }

      long start = System.currentTimeMillis();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         message.getBodyBuffer().writeBytes(bytes);

         for (int j = 0; j < numProducers; j++)
         {
            if (anon)
            {
               producers[j].send(address, message);
            }
            else
            {
               producers[j].send(message);
            }

         }
      }

      for (int i = 0; i < numConsumers; i++)
      {
         assertTrue(handlers[i].latch.await(5, TimeUnit.MINUTES));

         Assert.assertNull(handlers[i].exception);
      }

      long end = System.currentTimeMillis();

      double rate = 1000 * (double)numMessages / (end - start);

      ProducerFlowControlTest.log.info("rate is " + rate + " msgs / sec");

      session.close();

      sf.close();

      server.stop();
   }

   public void testClosingSessionUnblocksBlockedProducer() throws Exception
   {
      final SimpleString address = new SimpleString("testaddress");

      HornetQServer server = createServer(false, isNetty());

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxSizeBytes(1024);
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);

      HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
      repos.addMatch(address.toString(), addressSettings);

      server.start();

      locator.setProducerWindowSize(1024);
      locator.setConsumerWindowSize(1024);
      locator.setAckBatchSize(1024);

      ClientSessionFactory sf = locator.createSessionFactory();
      final ClientSession session = sf.createSession(false, true, true, true);

      final SimpleString queueName = new SimpleString("testqueue");

      session.createQueue(address, queueName, null, false);

      ClientProducer producer = session.createProducer(address);

      byte[] bytes = new byte[2000];

      ClientMessage message = session.createMessage(false);

      message.getBodyBuffer().writeBytes(bytes);

      final AtomicBoolean closed = new AtomicBoolean(false);

      Thread t = new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               Thread.sleep(500);

               closed.set(true);

               session.close();
            }
            catch (Exception e)
            {
            }
         }
      });

      t.start();

      // This will block
      producer.send(message);

      Assert.assertTrue(closed.get());

      t.join();

      server.stop();
   }

   public void testFlowControlMessageNotRouted() throws Exception
   {
      final SimpleString address = new SimpleString("testaddress");

      HornetQServer server = createServer(false, isNetty());

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxSizeBytes(1024);
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);

      HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
      repos.addMatch(address.toString(), addressSettings);

      server.start();

      locator.setProducerWindowSize(1024);
      locator.setConsumerWindowSize(1024);
      locator.setAckBatchSize(1024);

      ClientSessionFactory sf = locator.createSessionFactory();

      final ClientSession session = sf.createSession(false, true, true, true);

      ClientProducer producer = session.createProducer(address);

      byte[] bytes = new byte[100];

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         message.getBodyBuffer().writeBytes(bytes);

         producer.send(message);
      }

      session.close();

      server.stop();
   }

   // Not technically a flow control test, but what the hell
   public void testMultipleConsumers() throws Exception
   {
      HornetQServer server = createServer(false, isNetty());

      server.start();

      ClientSessionFactory sf = locator.createSessionFactory();

      final ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue("address", "queue1", null, false);
      session.createQueue("address", "queue2", null, false);
      session.createQueue("address", "queue3", null, false);
      session.createQueue("address", "queue4", null, false);
      session.createQueue("address", "queue5", null, false);

      ClientConsumer consumer1 = session.createConsumer("queue1");
      ClientConsumer consumer2 = session.createConsumer("queue2");
      ClientConsumer consumer3 = session.createConsumer("queue3");
      ClientConsumer consumer4 = session.createConsumer("queue4");
      ClientConsumer consumer5 = session.createConsumer("queue5");

      ClientProducer producer = session.createProducer("address");

      byte[] bytes = new byte[2000];

      ClientMessage message = session.createMessage(false);

      message.getBodyBuffer().writeBytes(bytes);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         producer.send(message);
      }

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage msg = consumer1.receive(1000);

         Assert.assertNotNull(msg);

         msg = consumer2.receive(5000);

         Assert.assertNotNull(msg);

         msg = consumer3.receive(5000);

         Assert.assertNotNull(msg);

         msg = consumer4.receive(5000);

         Assert.assertNotNull(msg);

         msg = consumer5.receive(5000);

         Assert.assertNotNull(msg);
      }

      session.close();

      server.stop();
   }

   public void testProducerCreditsCaching1() throws Exception
   {
      HornetQServer server = createServer(false, isNetty());

      server.start();

      ClientSessionFactory sf = locator.createSessionFactory();

      final ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue("address", "queue1", null, false);

      ClientProducerCredits credits = null;

      for (int i = 0; i < ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE * 2; i++)
      {
         ClientProducer prod = session.createProducer("address");

         ClientProducerCredits newCredits = ((ClientProducerInternal)prod).getProducerCredits();

         if (credits != null)
         {
            assertTrue(newCredits == credits);
         }

         credits = newCredits;

         assertEquals(1, ((ClientSessionInternal)session).getProducerCreditManager().creditsMapSize());
         assertEquals(0, ((ClientSessionInternal)session).getProducerCreditManager().unReferencedCreditsSize());
      }

      session.close();

      server.stop();
   }

   public void testProducerCreditsCaching2() throws Exception
   {
      HornetQServer server = createServer(false, isNetty());

      server.start();

      ClientSessionFactory sf = locator.createSessionFactory();

      final ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue("address", "queue1", null, false);

      ClientProducerCredits credits = null;

      for (int i = 0; i < ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE * 2; i++)
      {
         ClientProducer prod = session.createProducer("address");

         ClientProducerCredits newCredits = ((ClientProducerInternal)prod).getProducerCredits();

         if (credits != null)
         {
            assertTrue(newCredits == credits);
         }

         credits = newCredits;

         prod.close();

         assertEquals(1, ((ClientSessionInternal)session).getProducerCreditManager().creditsMapSize());
         assertEquals(1, ((ClientSessionInternal)session).getProducerCreditManager().unReferencedCreditsSize());
      }

      session.close();

      server.stop();
   }

   public void testProducerCreditsCaching3() throws Exception
   {
      HornetQServer server = createServer(false, isNetty());

      server.start();

      ClientSessionFactory sf = locator.createSessionFactory();

      final ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue("address", "queue1", null, false);

      ClientProducerCredits credits = null;

      for (int i = 0; i < ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE; i++)
      {
         ClientProducer prod = session.createProducer("address" + i);

         ClientProducerCredits newCredits = ((ClientProducerInternal)prod).getProducerCredits();

         if (credits != null)
         {
            assertFalse(newCredits == credits);
         }

         credits = newCredits;

         assertEquals(i + 1, ((ClientSessionInternal)session).getProducerCreditManager().creditsMapSize());
         assertEquals(0, ((ClientSessionInternal)session).getProducerCreditManager().unReferencedCreditsSize());
      }

      session.close();

      server.stop();
   }

   public void testProducerCreditsCaching4() throws Exception
   {
      HornetQServer server = createServer(false, isNetty());

      server.start();

      ClientSessionFactory sf = locator.createSessionFactory();

      final ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue("address", "queue1", null, false);

      ClientProducerCredits credits = null;

      for (int i = 0; i < ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE; i++)
      {
         ClientProducer prod = session.createProducer("address" + i);

         ClientProducerCredits newCredits = ((ClientProducerInternal)prod).getProducerCredits();

         if (credits != null)
         {
            assertFalse(newCredits == credits);
         }

         credits = newCredits;

         prod.close();

         assertEquals(i + 1, ((ClientSessionInternal)session).getProducerCreditManager().creditsMapSize());
         assertEquals(i + 1, ((ClientSessionInternal)session).getProducerCreditManager().unReferencedCreditsSize());
      }

      session.close();

      server.stop();
   }

   public void testProducerCreditsCaching5() throws Exception
   {
      HornetQServer server = createServer(false, isNetty());

      server.start();

      ClientSessionFactory sf = locator.createSessionFactory();

      final ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue("address", "queue1", null, false);

      ClientProducerCredits credits = null;

      List<ClientProducerCredits> creditsList = new ArrayList<ClientProducerCredits>();

      for (int i = 0; i < ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE; i++)
      {
         ClientProducer prod = session.createProducer("address" + i);

         ClientProducerCredits newCredits = ((ClientProducerInternal)prod).getProducerCredits();

         if (credits != null)
         {
            assertFalse(newCredits == credits);
         }

         credits = newCredits;

         assertEquals(i + 1, ((ClientSessionInternal)session).getProducerCreditManager().creditsMapSize());
         assertEquals(0, ((ClientSessionInternal)session).getProducerCreditManager().unReferencedCreditsSize());

         creditsList.add(credits);
      }

      Iterator<ClientProducerCredits> iter = creditsList.iterator();

      for (int i = 0; i < ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE; i++)
      {
         ClientProducer prod = session.createProducer("address" + i);

         ClientProducerCredits newCredits = ((ClientProducerInternal)prod).getProducerCredits();

         assertTrue(newCredits == iter.next());

         assertEquals(ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE,
                      ((ClientSessionInternal)session).getProducerCreditManager().creditsMapSize());
         assertEquals(0, ((ClientSessionInternal)session).getProducerCreditManager().unReferencedCreditsSize());
      }

      for (int i = 0; i < 10; i++)
      {
         ClientProducer prod = session.createProducer("address" + (i + ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE));

         assertEquals(ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE + i + 1,
                      ((ClientSessionInternal)session).getProducerCreditManager().creditsMapSize());
         assertEquals(0, ((ClientSessionInternal)session).getProducerCreditManager().unReferencedCreditsSize());
      }

      session.close();

      server.stop();
   }

   public void testProducerCreditsCaching6() throws Exception
   {
      HornetQServer server = createServer(false, isNetty());

      server.start();

      ClientSessionFactory sf = locator.createSessionFactory();

      final ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue("address", "queue1", null, false);

      for (int i = 0; i < ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE; i++)
      {
         ClientProducer prod = session.createProducer((String)null);

         prod.send("address", session.createMessage(false));

         assertEquals(1, ((ClientSessionInternal)session).getProducerCreditManager().creditsMapSize());
         assertEquals(1, ((ClientSessionInternal)session).getProducerCreditManager().unReferencedCreditsSize());
      }

      session.close();

      server.stop();
   }

   public void testProducerCreditsCaching7() throws Exception
   {
      HornetQServer server = createServer(false, isNetty());

      server.start();

      ClientSessionFactory sf = locator.createSessionFactory();

      final ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue("address", "queue1", null, false);

      for (int i = 0; i < ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE; i++)
      {
         ClientProducer prod = session.createProducer((String)null);

         prod.send("address" + i, session.createMessage(false));

         assertEquals(i + 1, ((ClientSessionInternal)session).getProducerCreditManager().creditsMapSize());
         assertEquals(i + 1, ((ClientSessionInternal)session).getProducerCreditManager().unReferencedCreditsSize());
      }

      for (int i = 0; i < 10; i++)
      {
         ClientProducer prod = session.createProducer((String)null);

         prod.send("address" + i, session.createMessage(false));

         assertEquals(ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE,
                      ((ClientSessionInternal)session).getProducerCreditManager().creditsMapSize());
         assertEquals(ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE,
                      ((ClientSessionInternal)session).getProducerCreditManager().unReferencedCreditsSize());
      }

      for (int i = 0; i < 10; i++)
      {
         ClientProducer prod = session.createProducer((String)null);

         prod.send("address2-" + i, session.createMessage(false));

         assertEquals(ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE,
                      ((ClientSessionInternal)session).getProducerCreditManager().creditsMapSize());
         assertEquals(ClientProducerCreditManagerImpl.MAX_UNREFERENCED_CREDITS_CACHE_SIZE,
                      ((ClientSessionInternal)session).getProducerCreditManager().unReferencedCreditsSize());
      }

      session.close();

      server.stop();
   }

   public void testProducerCreditsRefCounting() throws Exception
   {
      HornetQServer server = createServer(false, isNetty());

      server.start();

      ClientSessionFactory sf = locator.createSessionFactory();

      final ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue("address", "queue1", null, false);

      ClientProducer prod1 = session.createProducer("address");
      assertEquals(1, ((ClientSessionInternal)session).getProducerCreditManager().creditsMapSize());
      assertEquals(0, ((ClientSessionInternal)session).getProducerCreditManager().unReferencedCreditsSize());

      ClientProducer prod2 = session.createProducer("address");
      assertEquals(1, ((ClientSessionInternal)session).getProducerCreditManager().creditsMapSize());
      assertEquals(0, ((ClientSessionInternal)session).getProducerCreditManager().unReferencedCreditsSize());

      ClientProducer prod3 = session.createProducer("address");
      assertEquals(1, ((ClientSessionInternal)session).getProducerCreditManager().creditsMapSize());
      assertEquals(0, ((ClientSessionInternal)session).getProducerCreditManager().unReferencedCreditsSize());

      prod1.close();

      assertEquals(1, ((ClientSessionInternal)session).getProducerCreditManager().creditsMapSize());
      assertEquals(0, ((ClientSessionInternal)session).getProducerCreditManager().unReferencedCreditsSize());

      prod2.close();

      assertEquals(1, ((ClientSessionInternal)session).getProducerCreditManager().creditsMapSize());
      assertEquals(0, ((ClientSessionInternal)session).getProducerCreditManager().unReferencedCreditsSize());

      prod3.close();

      assertEquals(1, ((ClientSessionInternal)session).getProducerCreditManager().creditsMapSize());
      assertEquals(1, ((ClientSessionInternal)session).getProducerCreditManager().unReferencedCreditsSize());

      session.close();

      server.stop();
   }

}
