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
package org.jboss.messaging.tests.integration.queue;

import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.util.SimpleString;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.ArrayList;

import junit.framework.TestCase;
import junit.framework.TestResult;
import junit.framework.TestSuite;
import junit.textui.TestRunner;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ExpiryRunnerTest extends UnitTestCase
{
   private MessagingService messagingService;

   private ClientSession clientSession;

   private SimpleString qName = new SimpleString("ExpiryRunnerTestQ");

   private SimpleString qName2 = new SimpleString("ExpiryRunnerTestQ2");

   private SimpleString expiryQueue;

   private SimpleString expiryAddress;

   public void testBasicExpire() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(qName);
      int numMessages = 100;
      long expiration = System.currentTimeMillis();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage m = createTextMessage("m" + i, clientSession);
         m.setExpiration(expiration);
         producer.send(m);
      }
      Thread.sleep(1600);
      assertEquals(0, messagingService.getServer().getPostOffice().getBinding(qName).getQueue().getMessageCount());
      assertEquals(0, messagingService.getServer().getPostOffice().getBinding(qName).getQueue().getDeliveringCount());

      ClientConsumer consumer = clientSession.createConsumer(expiryQueue);
      clientSession.start();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage cm = consumer.receive(500);
         assertNotNull(cm);
         assertEquals("m" + i, cm.getBody().getString());
      }
      consumer.close();
   }

   public void testExpireHalf() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(qName);
      int numMessages = 100;
      long expiration = System.currentTimeMillis();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage m = createTextMessage("m" + i, clientSession);
         if (i % 2 == 0)
         {
            m.setExpiration(expiration);
         }
         producer.send(m);
      }
      Thread.sleep(1600);
      assertEquals(50, messagingService.getServer().getPostOffice().getBinding(qName).getQueue().getMessageCount());
      assertEquals(0, messagingService.getServer().getPostOffice().getBinding(qName).getQueue().getDeliveringCount());

      ClientConsumer consumer = clientSession.createConsumer(expiryQueue);
      clientSession.start();
      for (int i = 0; i < numMessages; i += 2)
      {
         ClientMessage cm = consumer.receive(500);
         assertNotNull(cm);
         assertEquals("m" + i, cm.getBody().getString());
      }
      consumer.close();
   }

   public void testExpireConsumeHalf() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(qName);
      int numMessages = 100;
      long expiration = System.currentTimeMillis() + 1000;
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage m = createTextMessage("m" + i, clientSession);
         m.setExpiration(expiration);
         producer.send(m);
      }
      ClientConsumer consumer = clientSession.createConsumer(qName);
      clientSession.start();
      for (int i = 0; i < numMessages / 2; i++)
      {
         ClientMessage cm = consumer.receive(500);
         assertNotNull("message not received " + i, cm);
         cm.acknowledge();
         assertEquals("m" + i, cm.getBody().getString());
      }
      consumer.close();
      Thread.sleep(2100);
      assertEquals(0, messagingService.getServer().getPostOffice().getBinding(qName).getQueue().getMessageCount());
      assertEquals(0, messagingService.getServer().getPostOffice().getBinding(qName).getQueue().getDeliveringCount());

      consumer = clientSession.createConsumer(expiryQueue);
      clientSession.start();
      for (int i = 50; i < numMessages; i++)
      {
         ClientMessage cm = consumer.receive(500);
         assertNotNull(cm);
         assertEquals("m" + i, cm.getBody().getString());
      }
      consumer.close();
   }

   public void testExpireFromMultipleQueues() throws Exception
   {
      clientSession.createQueue(qName, qName2, null, false, false, true);
      QueueSettings queueSettings = new QueueSettings();
      queueSettings.setExpiryAddress(expiryAddress);
      messagingService.getServer().getQueueSettingsRepository().addMatch(qName2.toString(), queueSettings);
      ClientProducer producer = clientSession.createProducer(qName);
      int numMessages = 100;
      long expiration = System.currentTimeMillis();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage m = createTextMessage("m" + i, clientSession);
         m.setExpiration(expiration);
         producer.send(m);
      }
      Thread.sleep(1600);
      assertEquals(0, messagingService.getServer().getPostOffice().getBinding(qName).getQueue().getMessageCount());
      assertEquals(0, messagingService.getServer().getPostOffice().getBinding(qName).getQueue().getDeliveringCount());

      ClientConsumer consumer = clientSession.createConsumer(expiryQueue);
      clientSession.start();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage cm = consumer.receive(500);
         assertNotNull(cm);
         assertEquals("m" + i, cm.getBody().getString());
      }
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage cm = consumer.receive(500);
         assertNotNull(cm);
         assertEquals("m" + i, cm.getBody().getString());
      }
      consumer.close();
   }

   public void testExpireWhilstConsuming() throws Exception
   {
      ClientProducer producer = clientSession.createProducer(qName);
      ClientConsumer consumer = clientSession.createConsumer(qName);
      CountDownLatch latch = new CountDownLatch(1);
      DummyMessageHandler dummyMessageHandler = new DummyMessageHandler(consumer, latch);
      clientSession.start();
      new Thread(dummyMessageHandler).start();
      long expiration = System.currentTimeMillis() + 1000;
      int numMessages = 0;
      long sendMessagesUntil = System.currentTimeMillis() + 2000;
      do
      {
         ClientMessage m = createTextMessage("m" + (numMessages++), clientSession);
         m.setExpiration(expiration);
         producer.send(m);
         Thread.sleep(100);
      }
      while (System.currentTimeMillis() < sendMessagesUntil);
      assertTrue(latch.await(10000, TimeUnit.MILLISECONDS));
      consumer.close();

      consumer = clientSession.createConsumer(expiryQueue);
      do
      {
         ClientMessage cm = consumer.receive(2000);
         if(cm == null)
         {
            break;
         }
         String text = cm.getBody().getString();
         cm.acknowledge();
         assertFalse(dummyMessageHandler.payloads.contains(text));
         dummyMessageHandler.payloads.add(text);
      } while(true);

      for(int i = 0; i < numMessages; i++)
      {
         assertTrue(dummyMessageHandler.payloads.remove("m" + i));
      }
      assertTrue(dummyMessageHandler.payloads.isEmpty());
      consumer.close();
   }

   public static void main(String[] args) throws Exception
   {
      for (int i = 0; i < 1000; i++)
      {
         TestSuite suite = new TestSuite();
         ExpiryRunnerTest expiryRunnerTest = new ExpiryRunnerTest();
         expiryRunnerTest.setName("testExpireWhilstConsuming");
         suite.addTest(expiryRunnerTest);

         TestResult result = TestRunner.run(suite);
         if(result.errorCount() > 0 || result.failureCount() > 0)
         {
            System.exit(1);
         }
      }
   }

   @Override
   protected void setUp() throws Exception
   {
      ConfigurationImpl configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      configuration.setMessageExpiryScanPeriod(1000);
      TransportConfiguration transportConfig = new TransportConfiguration(INVM_ACCEPTOR_FACTORY);
      configuration.getAcceptorConfigurations().add(transportConfig);
      messagingService = MessagingServiceImpl.newNullStorageMessagingServer(configuration);
      // start the server
      messagingService.start();
      // then we create a client as normal
      ClientSessionFactory sessionFactory = new ClientSessionFactoryImpl(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      sessionFactory.setBlockOnAcknowledge(true);
      clientSession = sessionFactory.createSession(false, true, true);
      clientSession.createQueue(qName, qName, null, false, false, true);
      expiryAddress = new SimpleString("EA");
      expiryQueue = new SimpleString("expiryQ");
      QueueSettings queueSettings = new QueueSettings();
      queueSettings.setExpiryAddress(expiryAddress);
      messagingService.getServer().getQueueSettingsRepository().addMatch(qName.toString(), queueSettings);
      messagingService.getServer().getQueueSettingsRepository().addMatch(qName2.toString(), queueSettings);
      clientSession.createQueue(expiryAddress, expiryQueue, null, false, false, true);
   }

   @Override
   protected void tearDown() throws Exception
   {
      if (clientSession != null)
      {
         try
         {
            clientSession.close();
         }
         catch (MessagingException e1)
         {
            //
         }
      }
      if (messagingService != null && messagingService.isStarted())
      {
         try
         {
            messagingService.stop();
         }
         catch (Exception e1)
         {
            //
         }
      }
      messagingService = null;
      clientSession = null;
   }

   private static class DummyMessageHandler implements Runnable
   {
      List<String> payloads = new ArrayList<String>();

      private final ClientConsumer consumer;

      private final CountDownLatch latch;

      public DummyMessageHandler(ClientConsumer consumer, CountDownLatch latch)
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
               payloads.add(message.getBody().getString());

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
