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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Dec 1, 2009
 */
public class MessageGroupingConnectionFactoryTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(MessageGroupingTest.class);

   private HornetQServer server;

   private ClientSession clientSession;

   private final SimpleString qName = new SimpleString("MessageGroupingTestQueue");

   public void testBasicGroupingUsingConnection() throws Exception
   {
      doTestBasicGroupingUsingConnectionFactory();
   }

   public void testBasicGroupingMultipleProducers() throws Exception
   {
      doTestBasicGroupingMultipleProducers();
   }

   private void doTestBasicGroupingUsingConnectionFactory() throws Exception
   {
      ClientProducer clientProducer = clientSession.createProducer(qName);
      ClientConsumer consumer = clientSession.createConsumer(qName);
      ClientConsumer consumer2 = clientSession.createConsumer(qName);
      clientSession.start();
      
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, clientSession);
         clientProducer.send(message);
      }
      CountDownLatch latch = new CountDownLatch(numMessages);
      DummyMessageHandler dummyMessageHandler = new DummyMessageHandler(latch, true);
      consumer.setMessageHandler(dummyMessageHandler);
      DummyMessageHandler dummyMessageHandler2 = new DummyMessageHandler(latch, true);
      consumer2.setMessageHandler(dummyMessageHandler2);
      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
      Assert.assertEquals(100, dummyMessageHandler.list.size());
      Assert.assertEquals(0, dummyMessageHandler2.list.size());
      consumer.close();
      consumer2.close();
   }

   private void doTestBasicGroupingMultipleProducers() throws Exception
   {
      ClientProducer clientProducer = clientSession.createProducer(qName);
      ClientProducer clientProducer2 = clientSession.createProducer(qName);
      ClientProducer clientProducer3 = clientSession.createProducer(qName);
      ClientConsumer consumer = clientSession.createConsumer(qName);
      ClientConsumer consumer2 = clientSession.createConsumer(qName);
      clientSession.start();
      
      int numMessages = 100;
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, clientSession);
         clientProducer.send(message);
         clientProducer2.send(message);
         clientProducer3.send(message);
      }
      CountDownLatch latch = new CountDownLatch(numMessages * 3);
      DummyMessageHandler dummyMessageHandler = new DummyMessageHandler(latch, true);
      consumer.setMessageHandler(dummyMessageHandler);
      DummyMessageHandler dummyMessageHandler2 = new DummyMessageHandler(latch, true);
      consumer2.setMessageHandler(dummyMessageHandler2);
      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
      Assert.assertEquals(300, dummyMessageHandler.list.size());
      Assert.assertEquals(0, dummyMessageHandler2.list.size());
      consumer.close();
      consumer2.close();
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
         catch (HornetQException e1)
         {
            //
         }
      }
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

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      ConfigurationImpl configuration = new ConfigurationImpl();
      configuration.setSecurityEnabled(false);
      TransportConfiguration transportConfig = new TransportConfiguration(UnitTestCase.INVM_ACCEPTOR_FACTORY);
      configuration.getAcceptorConfigurations().add(transportConfig);
      server = HornetQServers.newHornetQServer(configuration, false);
      // start the server
      server.start();

      // then we create a client as normal

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY));

      locator.setGroupID("grp1");
      ClientSessionFactory sessionFactory = locator.createSessionFactory();
      clientSession = sessionFactory.createSession(false, true, true);
      clientSession.createQueue(qName, qName, null, false);
   }

   private static class DummyMessageHandler implements MessageHandler
   {
      ArrayList<ClientMessage> list = new ArrayList<ClientMessage>();

      private CountDownLatch latch;

      private final boolean acknowledge;

      public DummyMessageHandler(final CountDownLatch latch, final boolean acknowledge)
      {
         this.latch = latch;
         this.acknowledge = acknowledge;
      }

      public void onMessage(final ClientMessage message)
      {
         list.add(message);
         if (acknowledge)
         {
            try
            {
               message.acknowledge();
            }
            catch (HornetQException e)
            {
               // ignore
            }
         }
         latch.countDown();
      }

      public void reset(final CountDownLatch latch)
      {
         list.clear();
         this.latch = latch;
      }
   }
}
