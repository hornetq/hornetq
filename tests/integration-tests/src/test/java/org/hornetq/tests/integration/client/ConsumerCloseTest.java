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
import java.util.concurrent.TimeUnit;

import org.hornetq.core.client.impl.ClientConsumerImpl;
import org.hornetq.core.settings.impl.AddressSettings;
import org.junit.Before;

import org.junit.Test;

import org.junit.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;

/**
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ConsumerCloseTest extends ServiceTestBase
{

   private ClientSessionFactory sf;
   private HornetQServer server;

   private ClientSession session;

   private SimpleString queue;

   private SimpleString address;
   private ServerLocator locator;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testCanNotUseAClosedConsumer() throws Exception
   {
      final ClientConsumer consumer = session.createConsumer(queue);

      consumer.close();

      Assert.assertTrue(consumer.isClosed());

      UnitTestCase.expectHornetQException(HornetQExceptionType.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            consumer.receive();
         }
      });

      UnitTestCase.expectHornetQException(HornetQExceptionType.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            consumer.receiveImmediate();
         }
      });

      UnitTestCase.expectHornetQException(HornetQExceptionType.OBJECT_CLOSED, new HornetQAction()
      {
         public void run() throws HornetQException
         {
            consumer.setMessageHandler(new MessageHandler()
            {
               public void onMessage(final ClientMessage message)
               {
               }
            });
         }
      });
   }

   // https://jira.jboss.org/jira/browse/JBMESSAGING-1526
   @Test
   public void testCloseWithManyMessagesInBufferAndSlowConsumer() throws Exception
   {
      ClientConsumer consumer = session.createConsumer(queue);

      ClientProducer producer = session.createProducer(address);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         producer.send(message);
      }

      final CountDownLatch received = new CountDownLatch(1);
      final CountDownLatch waitingToProceed = new CountDownLatch(1);
      class MyHandler implements MessageHandler
      {
         public void onMessage(final ClientMessage message)
         {
            try
            {
               received.countDown();
               waitingToProceed.await();
            }
            catch (Exception e)
            {
            }
         }
      }

      consumer.setMessageHandler(new MyHandler());

      session.start();

      assertTrue(received.await(5, TimeUnit.SECONDS));

      long timeout = System.currentTimeMillis() + 1000;

      // Instead of waiting a long time (like 1 second) we just make sure the buffer is full on the client
      while (((ClientConsumerImpl)consumer).getBufferSize() < 2 && System.currentTimeMillis() > timeout)
      {
         Thread.sleep(10);
      }

      waitingToProceed.countDown();



      // Close shouldn't wait for all messages to be processed before closing
      long start = System.currentTimeMillis();
      consumer.close();
      long end = System.currentTimeMillis();

      Assert.assertTrue(end - start <= 1500);

   }

   @Test
   public void testCloseWithScheduledRedelivery() throws Exception
   {


      AddressSettings settings = new AddressSettings();
      settings.setRedeliveryDelay(50000);
      server.getAddressSettingsRepository().addMatch("#", settings);

      ClientConsumer consumer = session.createConsumer(queue);

      ClientProducer producer = session.createProducer(address);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         producer.send(message);
      }

      session.start();

      ClientMessage msg = consumer.receive(5000);
      msg.acknowledge();

      long timeout = System.currentTimeMillis() + 1000;

      while (((ClientConsumerImpl)consumer).getBufferSize() < 2 && System.currentTimeMillis() > timeout)
      {
         Thread.sleep(10);
      }

      consumer.close();


      consumer = session.createConsumer(queue);

      // We received one, so we must receive the others now
      for (int i = 0 ; i < numMessages - 1; i++)
      {
         msg = consumer.receive(1000);
         assertNotNull("Expected message at i=" + i, msg);
         msg.acknowledge();
      }

      assertNull(consumer.receiveImmediate());

      // Close shouldn't wait for all messages to be processed before closing
      long start = System.currentTimeMillis();
      consumer.close();
      long end = System.currentTimeMillis();

      Assert.assertTrue(end - start <= 1500);

   }

   @Test
   public void testCloseWithScheduledRedeliveryWithTX() throws Exception
   {


      AddressSettings settings = new AddressSettings();
      settings.setRedeliveryDelay(1000);
      server.getAddressSettingsRepository().addMatch("#", settings);

      ClientProducer producer = session.createProducer(address);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);
         message.putIntProperty("count", i);
         producer.send(message);
      }

      session.close();

      session = addClientSession(sf.createSession(false, false));

      ClientConsumer consumer = session.createConsumer(queue);

      session.start();

      ClientMessage msg = consumer.receive(500);
      msg.acknowledge();

      long timeout = System.currentTimeMillis() + 1000;

      while (((ClientConsumerImpl)consumer).getBufferSize() < 2 && System.currentTimeMillis() > timeout)
      {
         Thread.sleep(10);
      }

      consumer.close();

      session.rollback();


      consumer = session.createConsumer(queue);

      // We received one, so we must receive the others now
      for (int i = 0 ; i < numMessages - 1; i++)
      {
         msg = consumer.receive(1000);
         assertNotNull("Expected message at i=" + i, msg);
         msg.acknowledge();
      }

      assertNull(consumer.receiveImmediate());

      // The first message received after redeliveryDelay
      msg = consumer.receive(5000);
      assertNotNull(msg);
      assertEquals(0, msg.getIntProperty("count").intValue());
      msg.acknowledge();
      session.commit();

      assertNull(consumer.receiveImmediate());


      // Close shouldn't wait for all messages to be processed before closing
      long start = System.currentTimeMillis();
      consumer.close();
      long end = System.currentTimeMillis();

      Assert.assertTrue(end - start <= 1500);

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration config = createDefaultConfig();
      config.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName()));
      config.setSecurityEnabled(false);

      server = addServer(HornetQServers.newHornetQServer(config, false));
      server.start();

      address = RandomUtil.randomSimpleString();
      queue = RandomUtil.randomSimpleString();

      locator =
               addServerLocator(HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(
                                                                                                      ServiceTestBase.INVM_CONNECTOR_FACTORY)));

      sf = createSessionFactory(locator);

      session = addClientSession(sf.createSession(false, true, true));
      session.createQueue(address, queue, false);
   }

}
