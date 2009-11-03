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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.MessageHandler;
import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.impl.TestSupportPageStore;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.impl.ServerProducerCreditManager;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;

/**
 * 
 * A ProducerFlowControlTest
 *
 * @author tim fox
 *
 */
public class ProducerFlowControlTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(ProducerFlowControlTest.class);

   // TODO need to test crashing a producer with unused credits returns them to the pool

   public void testFlowControlSingleConsumer() throws Exception
   {
      testFlowControl(false, 1000, 500, 10 * 1024, 1024, 1024, 1024, 1, 1, 0, false);
   }

   public void testFlowControlAnon() throws Exception
   {
      testFlowControl(false, 1000, 500, 10 * 1024, 1024, 1024, 1024, 1, 1, 0, true);
   }

   public void testFlowControlSingleConsumerLargeMaxSize() throws Exception
   {
      testFlowControl(false, 1000, 500, 1024 * 1024, 1024, 1024, 1024, 1, 1, 0, false);
   }

   public void testFlowControlMultipleConsumers() throws Exception
   {
      testFlowControl(false, 1000, 500, 10 * 1024, 1024, 1024, 1024, 5, 1, 0, false);
   }

   public void testFlowControlZeroConsumerWindowSize() throws Exception
   {
      testFlowControl(false, 1000, 500, 10 * 1024, 1024, 0, 1024, 1, 1, 0, false);
   }

   public void testFlowControlZeroAckBatchSize() throws Exception
   {
      testFlowControl(false, 1000, 500, 10 * 1024, 1024, 1024, 0, 1, 1, 0, false);
   }

   public void testFlowControlSingleConsumerSlowConsumer() throws Exception
   {
      testFlowControl(false, 100, 500, 1024, 512, 512, 512, 1, 1, 10, false);
   }

   public void testFlowControlSmallMessages() throws Exception
   {
      testFlowControl(false, 1000, 0, 10 * 1024, 1024, 1024, 1024, 1, 1, 0, false);
   }

   public void testFlowControlLargerMessagesSmallWindowSize() throws Exception
   {
      testFlowControl(false, 1000, 10 * 1024, 10 * 1024, 1024, 1024, 1024, 1, 1, 0, false);
   }

   public void testFlowControlMultipleProducers() throws Exception
   {
      testFlowControl(false, 1000, 500, 1024 * 1024, 1024, 1024, 1024, 1, 5, 0, false);
   }

   public void testFlowControlMultipleProducersAndConsumers() throws Exception
   {
      testFlowControl(false, 500, 500, 100 * 1024, 1024, 1024, 1024, 1, 3, 3, false);
   }

   public void testFlowControlMultipleProducersAnon() throws Exception
   {
      testFlowControl(false, 1000, 500, 1024 * 1024, 1024, 1024, 1024, 1, 5, 0, true);
   }

   public void testFlowControlSingleConsumerNetty() throws Exception
   {
      testFlowControl(true, 1000, 500, 10 * 1024, 1024, 1024, 1024, 1, 1, 0, false);
   }

   public void testFlowControlSingleConsumerLargeMaxSizeNetty() throws Exception
   {
      testFlowControl(true, 1000, 500, 1024 * 1024, 1024, 1024, 1024, 1, 1, 0, false);
   }

   public void testFlowControlMultipleConsumersNetty() throws Exception
   {
      testFlowControl(true, 1000, 500, 10 * 1024, 1024, 1024, 1024, 5, 1, 0, false);
   }

   public void testFlowControlZeroConsumerWindowSizeNetty() throws Exception
   {
      testFlowControl(true, 1000, 500, 10 * 1024, 1024, 0, 1024, 1, 1, 0, false);
   }

   public void testFlowControlZeroAckBatchSizeNetty() throws Exception
   {
      testFlowControl(true, 1000, 500, 10 * 1024, 1024, 1024, 0, 1, 1, 0, false);
   }

   public void testFlowControlSingleConsumerSlowConsumerNetty() throws Exception
   {
      testFlowControl(true, 100, 500, 1024, 512, 512, 512, 1, 1, 10, false);
   }

   public void testFlowControlSmallMessagesNetty() throws Exception
   {
      testFlowControl(true, 1000, 0, 10 * 1024, 1024, 1024, 1024, 1, 1, 0, false);
   }

   public void testFlowControlLargerMessagesSmallWindowSizeNetty() throws Exception
   {
      testFlowControl(true, 1000, 10 * 1024, 10 * 1024, 1024, 1024, 1024, 1, 1, 0, false);
   }

   public void testFlowControlMultipleProducersNetty() throws Exception
   {
      testFlowControl(true, 1000, 500, 1024 * 1024, 1024, 1024, 1024, 1, 5, 0, false);
   }

   public void testFlowControlMultipleProducersAndConsumersNetty() throws Exception
   {
      testFlowControl(false, 500, 500, 100 * 1024, 1024, 1024, 1024, 1, 3, 3, false);
   }

   public void testFlowControlMultipleProducersAnonNetty() throws Exception
   {
      testFlowControl(true, 1000, 500, 1024 * 1024, 1024, 1024, 1024, 1, 5, 0, true);
   }

   private void testFlowControl(final boolean netty,
                                final int numMessages,
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
      testFlowControl(netty,
                      numMessages,
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
   
   public void testFlowControlLargeMessages() throws Exception
   {
      testFlowControl(true, 1000, 10000, 100 * 1024, 1024, 1024, 0, 1, 1, 0, false, 1000, true);
   }
   
   public void testFlowControlLargeMessages2() throws Exception
   {
      testFlowControl(true, 1000, 10000, -1, 1024, 0, 0, 1, 1, 0, false, 1000, true);
   }

   private void testFlowControl(final boolean netty,
                                final int numMessages,
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

      Configuration config = super.createDefaultConfig(netty);

      HornetQServer server = createServer(realFiles, config);

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxSizeBytes(maxSize);
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);

      HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
      repos.addMatch(address.toString(), addressSettings);

      server.start();

      ClientSessionFactory sf;

      if (netty)
      {
         sf = createNettyFactory();
      }
      else
      {
         sf = createInVMFactory();
      }

      sf.setProducerWindowSize(producerWindowSize);
      sf.setConsumerWindowSize(consumerWindowSize);
      sf.setAckBatchSize(ackBatchSize);

      if (minLargeMessageSize != -1)
      {
         sf.setMinLargeMessageSize(minLargeMessageSize);
      }

      ClientSession session = sf.createSession(false, true, true, true);

      final String queueName = "testqueue";

      for (int i = 0; i < numConsumers; i++)
      {
         session.createQueue(address, new SimpleString(queueName + i), null, false);
      }

      
      class MyHandler implements MessageHandler
      {
         int count = 0;

         final CountDownLatch latch = new CountDownLatch(1);

         volatile Exception exception;

         public void onMessage(ClientMessage message)
         {
            try
            {
               log.info("got message " + count);
               
               int availBytes = message.getBody().readableBytes();
               
               assertEquals(messageSize, availBytes);
               
               byte[] bytes = new byte[availBytes];
               
               message.getBody().readBytes(bytes);
               
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
               log.error("Failed to handle message", e);

               this.exception = e;
            }
         }
      }

      MyHandler[] handlers = new MyHandler[numConsumers];

      for (int i = 0; i < numConsumers; i++)
      {
         handlers[i] = new MyHandler();

         log.info("created consumer");
         
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

      byte[] bytes = new byte[messageSize];

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = new ClientMessageImpl(false, bytes);

         for (int j = 0; j < numProducers; j++)
         {
            if (anon)
            {
               producers[j].send(address, message);
            }
            else
            {
               producers[j].send(message);
               
               //log.info("sent message " + i);
            }

         }
      }
      
      log.info("sent messages");
      
      session.start();

      for (int i = 0; i < numConsumers; i++)
      {
         handlers[i].latch.await();

         assertNull(handlers[i].exception);
      }

      long end = System.currentTimeMillis();

      double rate = 1000 * (double)numMessages / (end - start);

      log.info("rate is " + rate + " msgs / sec");

      session.close();

      TestSupportPageStore store = (TestSupportPageStore)server.getPostOffice()
                                                               .getPagingManager()
                                                               .getPageStore(address);

      assertFalse(store.isExceededAvailableCredits());

      server.stop();
   }

   public void testUnusedCreditsAreReturnedOnSessionCloseWithWaitingEntries() throws Exception
   {
      final SimpleString address = new SimpleString("testaddress");

      Configuration config = super.createDefaultConfig(false);

      HornetQServer server = createServer(false, config);

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxSizeBytes(1024);
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);

      HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
      repos.addMatch(address.toString(), addressSettings);

      server.start();

      ClientSessionFactory sf = createInVMFactory();

      sf.setProducerWindowSize(1024);
      sf.setConsumerWindowSize(1024);
      sf.setAckBatchSize(1024);

      final ClientSession session = sf.createSession(false, true, true, true);

      final SimpleString queueName = new SimpleString("testqueue");

      session.createQueue(address, queueName, null, false);

      ClientProducer producer = session.createProducer(address);

      ClientSession session2 = sf.createSession(false, true, true, true);

      ClientProducer producer2 = session2.createProducer(address);

      ServerProducerCreditManager mgr = server.getPostOffice()
                                              .getPagingManager()
                                              .getPageStore(address)
                                              .getProducerCreditManager();

      long start = System.currentTimeMillis();

      int waiting;
      do
      {
         waiting = mgr.waitingEntries();

         Thread.sleep(10);
      }
      while (waiting != 1 || (System.currentTimeMillis() - start) > 3000);

      assertEquals(1, waiting);

      byte[] bytes = new byte[0];

      ClientMessage message = new ClientMessageImpl(false, bytes);

      producer.send(message);

      class SessionCloser implements Runnable
      {
         public void run()
         {
            try
            {
               Thread.sleep(2000);

               closed = true;

               session.close();
            }
            catch (Exception e)
            {
            }
         }

         volatile boolean closed;
      }

      SessionCloser closer = new SessionCloser();

      Thread t = new Thread(closer);

      t.start();

      ClientMessage message2 = new ClientMessageImpl(false, bytes);

      producer2.send(message2);

      // Make sure it blocked until the first producer was closed
      assertTrue(closer.closed);

      t.join();

      session2.close();

      TestSupportPageStore store = (TestSupportPageStore)server.getPostOffice()
                                                               .getPagingManager()
                                                               .getPageStore(address);

      assertFalse(store.isExceededAvailableCredits());

      server.stop();
   }

   public void testUnusedCreditsAreReturnedOnSessionCloseNoWaitingEntries() throws Exception
   {
      final SimpleString address = new SimpleString("testaddress");

      Configuration config = super.createDefaultConfig(false);

      HornetQServer server = createServer(false, config);

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxSizeBytes(1024);
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);

      HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
      repos.addMatch(address.toString(), addressSettings);

      server.start();

      ClientSessionFactory sf = createInVMFactory();

      sf.setProducerWindowSize(1024);
      sf.setConsumerWindowSize(1024);
      sf.setAckBatchSize(1024);

      final ClientSession session = sf.createSession(false, true, true, true);

      final SimpleString queueName = new SimpleString("testqueue");

      session.createQueue(address, queueName, null, false);

      ClientProducer producer = session.createProducer(address);

      byte[] bytes = new byte[0];

      ClientMessage message = new ClientMessageImpl(false, bytes);

      producer.send(message);

      session.close();

      ClientSession session2 = sf.createSession(false, true, true, true);

      ClientProducer producer2 = session2.createProducer(address);

      ServerProducerCreditManager mgr = server.getPostOffice()
                                              .getPagingManager()
                                              .getPageStore(address)
                                              .getProducerCreditManager();

      long start = System.currentTimeMillis();

      int waiting;
      do
      {
         waiting = mgr.waitingEntries();

         Thread.sleep(10);
      }
      while (waiting != 1 || (System.currentTimeMillis() - start) > 3000);

      assertEquals(1, waiting);

      message = new ClientMessageImpl(false, bytes);

      producer2.send(message);

      session2.close();

      TestSupportPageStore store = (TestSupportPageStore)server.getPostOffice()
                                                               .getPagingManager()
                                                               .getPageStore(address);

      assertFalse(store.isExceededAvailableCredits());

      server.stop();
   }

   public void testUnusedCreditsAreReturnedWaitingEntriesForClosedSessions() throws Exception
   {
      final SimpleString address = new SimpleString("testaddress");

      Configuration config = super.createDefaultConfig(false);

      HornetQServer server = createServer(false, config);

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxSizeBytes(1024);
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);

      HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
      repos.addMatch(address.toString(), addressSettings);

      server.start();

      ClientSessionFactory sf = createInVMFactory();

      sf.setProducerWindowSize(1024);
      sf.setConsumerWindowSize(1024);
      sf.setAckBatchSize(1024);

      final ClientSession session = sf.createSession(false, true, true, true);

      final SimpleString queueName = new SimpleString("testqueue");

      session.createQueue(address, queueName, null, false);

      ClientProducer producer = session.createProducer(address);

      byte[] bytes = new byte[0];

      ClientMessage message = new ClientMessageImpl(false, bytes);

      producer.send(message);

      ClientSession session2 = sf.createSession(false, true, true, true);

      ClientProducer producer2 = session2.createProducer(address);

      ServerProducerCreditManager mgr = server.getPostOffice()
                                              .getPagingManager()
                                              .getPageStore(address)
                                              .getProducerCreditManager();

      long start = System.currentTimeMillis();

      int waiting;
      do
      {
         waiting = mgr.waitingEntries();

         Thread.sleep(10);
      }
      while (waiting != 1 || (System.currentTimeMillis() - start) > 3000);

      assertEquals(1, waiting);

      ClientSession session3 = sf.createSession(false, true, true, true);

      ClientProducer producer3 = session3.createProducer(address);

      start = System.currentTimeMillis();

      do
      {
         waiting = mgr.waitingEntries();

         Thread.sleep(10);
      }
      while (waiting != 2 || (System.currentTimeMillis() - start) > 3000);

      assertEquals(2, waiting);

      session2.close();

      session.close();

      message = new ClientMessageImpl(false, bytes);

      producer3.send(message);

      session3.close();

      TestSupportPageStore store = (TestSupportPageStore)server.getPostOffice()
                                                               .getPagingManager()
                                                               .getPageStore(address);

      assertFalse(store.isExceededAvailableCredits());

      server.stop();
   }

   public void testClosingSessionUnblocksBlockedProducer() throws Exception
   {
      final SimpleString address = new SimpleString("testaddress");

      Configuration config = super.createDefaultConfig(false);

      HornetQServer server = createServer(false, config);

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxSizeBytes(1024);
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);

      HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
      repos.addMatch(address.toString(), addressSettings);

      server.start();

      ClientSessionFactory sf = createInVMFactory();

      sf.setProducerWindowSize(1024);
      sf.setConsumerWindowSize(1024);
      sf.setAckBatchSize(1024);

      final ClientSession session = sf.createSession(false, true, true, true);

      final SimpleString queueName = new SimpleString("testqueue");

      session.createQueue(address, queueName, null, false);

      ClientProducer producer = session.createProducer(address);

      byte[] bytes = new byte[2000];

      ClientMessage message = new ClientMessageImpl(false, bytes);

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

      assertTrue(closed.get());

      t.join();

      TestSupportPageStore store = (TestSupportPageStore)server.getPostOffice()
                                                               .getPagingManager()
                                                               .getPageStore(address);

      assertFalse(store.isExceededAvailableCredits());

      server.stop();
   }

   public void testFlowControlMessageNotRouted() throws Exception
   {
      final SimpleString address = new SimpleString("testaddress");

      Configuration config = super.createDefaultConfig(false);

      HornetQServer server = createServer(false, config);

      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setMaxSizeBytes(1024);
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.BLOCK);

      HierarchicalRepository<AddressSettings> repos = server.getAddressSettingsRepository();
      repos.addMatch(address.toString(), addressSettings);

      server.start();

      ClientSessionFactory sf = createInVMFactory();

      sf.setProducerWindowSize(1024);
      sf.setConsumerWindowSize(1024);
      sf.setAckBatchSize(1024);

      final ClientSession session = sf.createSession(false, true, true, true);

      ClientProducer producer = session.createProducer(address);

      byte[] bytes = new byte[100];

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = new ClientMessageImpl(false, bytes);

         producer.send(message);
      }

      session.close();

      TestSupportPageStore store = (TestSupportPageStore)server.getPostOffice()
                                                               .getPagingManager()
                                                               .getPageStore(address);

      assertFalse(store.isExceededAvailableCredits());

      server.stop();
   }

}
