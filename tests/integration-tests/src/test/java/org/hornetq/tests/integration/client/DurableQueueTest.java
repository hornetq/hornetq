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

import org.junit.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A TemporaryQueueTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class DurableQueueTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server;

   private ClientSession session;

   private ClientSessionFactory sf;

   private ServerLocator locator;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testConsumeFromDurableQueue() throws Exception
   {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, true);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(false));

      session.start();
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = consumer.receive(500);
      Assert.assertNotNull(message);
      message.acknowledge();

      consumer.close();
      session.deleteQueue(queue);
   }

   @Test
   public void testConsumeFromDurableQueueAfterServerRestart() throws Exception
   {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, true);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(true));

      session.close();

      server.stop();
      server.start();

      sf = createSessionFactory(locator);
      session = sf.createSession(false, true, true);

      session.start();
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = consumer.receive(500);
      Assert.assertNotNull(message);
      message.acknowledge();

      consumer.close();
      session.deleteQueue(queue);
   }

   @Test
   public void testProduceAndConsumeFromDurableQueueAfterServerRestart() throws Exception
   {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();

      session.createQueue(address, queue, true);

      session.close();

      server.stop();
      server.start();

      sf = createSessionFactory(locator);

      session = sf.createSession(false, true, true);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(true));

      session.start();
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = consumer.receive(500);
      Assert.assertNotNull(message);
      message.acknowledge();

      consumer.close();
      session.deleteQueue(queue);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      server = createServer(true);

      server.start();

      locator = createInVMNonHALocator();

      sf = createSessionFactory(locator);

      session = addClientSession(sf.createSession(false, true, true));
   }
}
