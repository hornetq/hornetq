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

import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.impl.QueueImpl;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * 
 * A ConsumerFilterTest
 *
 * @author Tim Fox
 *
 *
 */
public class ConsumerFilterTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(ConsumerFilterTest.class);

   private HornetQServer server;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      server = createServer(false);

      server.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      server.stop();

      server = null;

      super.tearDown();
   }

   public void testNonMatchingMessagesFollowedByMatchingMessages() throws Exception
   {
      ClientSessionFactory sf = createInVMFactory();

      ClientSession session = sf.createSession();

      session.start();

      session.createQueue("foo", "foo");

      ClientProducer producer = session.createProducer("foo");

      ClientConsumer consumer = session.createConsumer("foo", "animal='giraffe'");

      ClientMessage message = session.createMessage(false);

      message.putStringProperty("animal", "hippo");

      producer.send(message);

      assertNull(consumer.receive(500));

      message = session.createMessage(false);

      message.putStringProperty("animal", "giraffe");

      log.info("sending second msg");

      producer.send(message);

      ClientMessage received = consumer.receive(500);

      assertNotNull(received);

      assertEquals("giraffe", received.getStringProperty("animal"));

      assertNull(consumer.receive(500));

      session.close();
   }

   public void testNonMatchingMessagesFollowedByMatchingMessagesMany() throws Exception
   {
      ClientSessionFactory sf = createInVMFactory();

      ClientSession session = sf.createSession();

      session.start();

      session.createQueue("foo", "foo");

      ClientProducer producer = session.createProducer("foo");

      ClientConsumer consumer = session.createConsumer("foo", "animal='giraffe'");

      for (int i = 0; i < QueueImpl.MAX_DELIVERIES_IN_LOOP * 2; i++)
      {

         ClientMessage message = session.createMessage(false);

         message.putStringProperty("animal", "hippo");

         producer.send(message);
      }

      assertNull(consumer.receive(500));

      for (int i = 0; i < QueueImpl.MAX_DELIVERIES_IN_LOOP * 2; i++)
      {
         ClientMessage message = session.createMessage(false);

         message.putStringProperty("animal", "giraffe");

         producer.send(message);
      }

      for (int i = 0; i < QueueImpl.MAX_DELIVERIES_IN_LOOP * 2; i++)
      {
         ClientMessage received = consumer.receive(500);

         assertNotNull(received);

         assertEquals("giraffe", received.getStringProperty("animal"));
      }

      assertNull(consumer.receive(500));

      session.close();
   }
}
