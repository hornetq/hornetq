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
package org.hornetq.tests.integration.server;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.server.Queue;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ExpiryRunnerTest extends UnitTestCase
{
   private HornetQServer server;

   private ClientSession clientSession;

   private final SimpleString qName = new SimpleString("ExpiryRunnerTestQ");

   private final SimpleString qName2 = new SimpleString("ExpiryRunnerTestQ2");

   private SimpleString expiryQueue;

   private SimpleString expiryAddress;
   private ServerLocator locator;

   @Test
   public void testBasicExpire() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(qName);
      int numMessages = 100;
      long expiration = System.currentTimeMillis();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage m = createTextMessage(clientSession, "m" + i);
         m.setExpiration(expiration);
         producer.send(m);
      }
      Thread.sleep(1600);
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(qName).getBindable()).getMessageCount());
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(qName).getBindable()).getDeliveringCount());
   }

   @Test
   public void testExpireFromMultipleQueues() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(qName);
      clientSession.createQueue(qName2, qName2, null, false);
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setExpiryAddress(expiryAddress);
      server.getAddressSettingsRepository().addMatch(qName2.toString(), addressSettings);
      ClientProducer producer2 = clientSession.createProducer(qName2);
      int numMessages = 100;
      long expiration = System.currentTimeMillis();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage m = createTextMessage(clientSession, "m" + i);
         m.setExpiration(expiration);
         producer.send(m);
         m = createTextMessage(clientSession, "m" + i);
         m.setExpiration(expiration);
         producer2.send(m);
      }
      Thread.sleep(1600);
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(qName).getBindable()).getMessageCount());
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(qName).getBindable()).getDeliveringCount());
   }

   @Test
   public void testExpireHalf() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(qName);
      int numMessages = 100;
      long expiration = System.currentTimeMillis();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage m = createTextMessage(clientSession, "m" + i);
         if (i % 2 == 0)
         {
            m.setExpiration(expiration);
         }
         producer.send(m);
      }
      Thread.sleep(1600);
      Assert.assertEquals(numMessages / 2,
                          ((Queue)server.getPostOffice().getBinding(qName).getBindable()).getMessageCount());
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(qName).getBindable()).getDeliveringCount());
   }

   @Test
   public void testExpireConsumeHalf() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(qName);
      int numMessages = 100;
      long expiration = System.currentTimeMillis() + 1000;
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage m = createTextMessage(clientSession, "m" + i);
         m.setExpiration(expiration);
         producer.send(m);
      }
      ClientConsumer consumer = clientSession.createConsumer(qName);
      clientSession.start();
      for (int i = 0; i < numMessages / 2; i++)
      {
         ClientMessage cm = consumer.receive(500);
         Assert.assertNotNull("message not received " + i, cm);
         cm.acknowledge();
         Assert.assertEquals("m" + i, cm.getBodyBuffer().readString());
      }
      consumer.close();
      Thread.sleep(2100);
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(qName).getBindable()).getMessageCount());
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(qName).getBindable()).getDeliveringCount());
   }

   @Test
   public void testExpireToExpiryQueue() throws Exception
   {
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setExpiryAddress(expiryAddress);
      server.getAddressSettingsRepository().addMatch(qName2.toString(), addressSettings);
      clientSession.deleteQueue(qName);
      clientSession.createQueue(qName, qName, null, false);
      clientSession.createQueue(qName, qName2, null, false);
      ClientProducer producer = clientSession.createProducer(qName);
      int numMessages = 100;
      long expiration = System.currentTimeMillis();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage m = createTextMessage(clientSession, "m" + i);
         m.setExpiration(expiration);
         producer.send(m);
      }
      Thread.sleep(1600);
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(qName).getBindable()).getMessageCount());
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(qName).getBindable()).getDeliveringCount());

      ClientConsumer consumer = clientSession.createConsumer(expiryQueue);
      clientSession.start();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage cm = consumer.receive(500);
         Assert.assertNotNull(cm);
         // assertEquals("m" + i, cm.getBody().getString());
      }
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage cm = consumer.receive(500);
         Assert.assertNotNull(cm);
         // assertEquals("m" + i, cm.getBody().getString());
      }
      consumer.close();
   }

   @Test
   public void testExpireWhilstConsumingMessagesStillInOrder() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(qName);
      ClientConsumer consumer = clientSession.createConsumer(qName);
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageHandler dummyMessageHandler = new DummyMessageHandler(consumer, latch);
      clientSession.start();
      Thread thr = new Thread(dummyMessageHandler);
      thr.start();
      long expiration = System.currentTimeMillis() + 1000;
      int numMessages = 0;
      long sendMessagesUntil = System.currentTimeMillis() + 2000;
      do
      {
         ClientMessage m = createTextMessage(clientSession, "m" + numMessages++);
         m.setExpiration(expiration);
         producer.send(m);
         Thread.sleep(100);
      }
      while (System.currentTimeMillis() < sendMessagesUntil);
      Assert.assertTrue(latch.await(10000, TimeUnit.MILLISECONDS));
      consumer.close();

      consumer = clientSession.createConsumer(expiryQueue);
      do
      {
         ClientMessage cm = consumer.receive(2000);
         if (cm == null)
         {
            break;
         }
         String text = cm.getBodyBuffer().readString();
         cm.acknowledge();
         Assert.assertFalse(dummyMessageHandler.payloads.contains(text));
         dummyMessageHandler.payloads.add(text);
      }
      while (true);

      for (int i = 0; i < numMessages; i++)
      {
         if (dummyMessageHandler.payloads.isEmpty())
         {
            break;
         }
         Assert.assertTrue("m" + i, dummyMessageHandler.payloads.remove("m" + i));
      }
      consumer.close();
      thr.join();
   }

//
//   public static void main(final String[] args) throws Exception
//   {
//      for (int i = 0; i < 1000; i++)
//      {
//         TestSuite suite = new TestSuite();
//         ExpiryRunnerTest expiryRunnerTest = new ExpiryRunnerTest();
//         expiryRunnerTest.setName("testExpireWhilstConsuming");
//         suite.addTest(expiryRunnerTest);
//
//         TestResult result = TestRunner.run(suite);
//         if (result.errorCount() > 0 || result.failureCount() > 0)
//         {
//            System.exit(1);
//         }
//      }
//   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      ConfigurationImpl configuration = createBasicConfig();
      configuration.setSecurityEnabled(false);
      configuration.setMessageExpiryScanPeriod(1000);
      TransportConfiguration transportConfig = new TransportConfiguration(UnitTestCase.INVM_ACCEPTOR_FACTORY);
      configuration.getAcceptorConfigurations().add(transportConfig);
      server = HornetQServers.newHornetQServer(configuration, false);
      // start the server
      server.start();
      // then we create a client as normal
      locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(UnitTestCase.INVM_CONNECTOR_FACTORY));
      locator.setBlockOnAcknowledge(true);
      ClientSessionFactory sessionFactory = createSessionFactory(locator);

      clientSession = sessionFactory.createSession(false, true, true);
      clientSession.createQueue(qName, qName, null, false);
      expiryAddress = new SimpleString("EA");
      expiryQueue = new SimpleString("expiryQ");
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setExpiryAddress(expiryAddress);
      server.getAddressSettingsRepository().addMatch(qName.toString(), addressSettings);
      server.getAddressSettingsRepository().addMatch(qName2.toString(), addressSettings);
      clientSession.createQueue(expiryAddress, expiryQueue, null, false);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      if (clientSession != null)
      {
         try
         {
            clientSession.close();
         }
         catch (HornetQException e1)
         {
            //
         }
      }
      locator.close();
      if (server != null && server.isStarted())
      {
         try
         {
            server.stop();
         }
         catch (Exception e1)
         {
            //
         }
      }
      server = null;
      clientSession = null;

      super.tearDown();
   }

   private static class DummyMessageHandler implements Runnable
   {
      List<String> payloads = new ArrayList<String>();

      private final ClientConsumer consumer;

      private final CountDownLatch latch;

      public DummyMessageHandler(final ClientConsumer consumer, final CountDownLatch latch)
      {
         this.consumer = consumer;
         this.latch = latch;
      }

      public void run()
      {
         while (true)
         {
            try
            {
               ClientMessage message = consumer.receive(5000);
               if (message == null)
               {
                  break;
               }
               message.acknowledge();
               payloads.add(message.getBodyBuffer().readString());

               Thread.sleep(110);
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
         latch.countDown();

      }
   }
}
