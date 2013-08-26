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
import org.junit.Before;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class ReceiveImmediateTest extends ServiceTestBase
{
   private HornetQServer server;

   private final SimpleString QUEUE = new SimpleString("ReceiveImmediateTest.queue");

   private final SimpleString ADDRESS = new SimpleString("ReceiveImmediateTest.address");

   private ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration config = createDefaultConfig(false);
      server = createServer(false, config);
      server.start();
      locator = createInVMNonHALocator();
   }

   private ClientSessionFactory sf;

   @Test
   public void testConsumerReceiveImmediateWithNoMessages() throws Exception
   {
      doConsumerReceiveImmediateWithNoMessages(false);
   }

   @Test
   public void testConsumerReceiveImmediate() throws Exception
   {
      doConsumerReceiveImmediate(false);
   }

   @Test
   public void testBrowserReceiveImmediateWithNoMessages() throws Exception
   {
      doConsumerReceiveImmediateWithNoMessages(true);
   }

   @Test
   public void testBrowserReceiveImmediate() throws Exception
   {
      doConsumerReceiveImmediate(true);
   }

   @Test
   public void testConsumerReceiveImmediateWithSessionStop() throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnAcknowledge(true);
      locator.setAckBatchSize(0);
      sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, QUEUE, null, false);

      ClientConsumer consumer = session.createConsumer(QUEUE, null, false);
      session.start();

      session.stop();
      Assert.assertNull(consumer.receiveImmediate());

      session.start();
      long start = System.currentTimeMillis();
      ClientMessage msg = consumer.receive(2000);
      long end = System.currentTimeMillis();
      Assert.assertNull(msg);
      // we waited for at least 2000ms
      Assert.assertTrue("waited only " + (end - start), end - start >= 2000);

      consumer.close();

      session.close();
   }

   // https://jira.jboss.org/browse/HORNETQ-450
   @Test
   public void testReceivedImmediateFollowedByReceive() throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, QUEUE, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      ClientMessage message = session.createMessage(false);

      producer.send(message);

      ClientConsumer consumer = session.createConsumer(QUEUE, null, false);

      session.start();

      ClientMessage received = consumer.receiveImmediate();

      assertNotNull(received);

      received.acknowledge();

      received = consumer.receive(1);

      assertNull(received);

      session.close();
   }

   // https://jira.jboss.org/browse/HORNETQ-450
   @Test
   public void testReceivedImmediateFollowedByAsyncConsume() throws Exception
   {

      locator.setBlockOnNonDurableSend(true);
      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, QUEUE, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      ClientMessage message = session.createMessage(false);

      producer.send(message);

      ClientConsumer consumer = session.createConsumer(QUEUE, null, false);

      session.start();

      ClientMessage received = consumer.receiveImmediate();

      assertNotNull(received);

      received.acknowledge();

      final AtomicBoolean receivedAsync = new AtomicBoolean(false);

      consumer.setMessageHandler(new MessageHandler()
      {
         public void onMessage(ClientMessage message)
         {
            receivedAsync.set(true);
         }
      });

      Thread.sleep(1000);

      assertFalse(receivedAsync.get());

      session.close();
   }

   private void doConsumerReceiveImmediateWithNoMessages(final boolean browser) throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnAcknowledge(true);
      locator.setAckBatchSize(0);
      sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, false);

      session.createQueue(ADDRESS, QUEUE, null, false);

      ClientConsumer consumer = session.createConsumer(QUEUE, null, browser);
      session.start();

      ClientMessage message = consumer.receiveImmediate();
      Assert.assertNull(message);

      session.close();
   }

   private void doConsumerReceiveImmediate(final boolean browser) throws Exception
   {

      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnAcknowledge(true);
      locator.setAckBatchSize(0);
      sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, QUEUE, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage(session, "m" + i);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE, null, browser);
      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receiveImmediate();
         Assert.assertNotNull("did not receive message " + i, message2);
         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
         if (!browser)
         {
            message2.acknowledge();
         }
      }

      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());

      Assert.assertNull(consumer.receiveImmediate());

      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      int messagesOnServer = browser ? numMessages : 0;
      Assert.assertEquals(messagesOnServer,
                          ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getMessageCount());

      consumer.close();

      session.close();
   }

}
