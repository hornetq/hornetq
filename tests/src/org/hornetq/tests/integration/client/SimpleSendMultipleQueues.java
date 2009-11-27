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

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * 
 * A SimpleSendMultipleQueues
 *
 * @author Tim Fox
 *
 *
 */
public class SimpleSendMultipleQueues extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(SimpleSendMultipleQueues.class);

   public static final String address = "testaddress";

   public static final String queueName = "testqueue";

   private HornetQServer server;

   private ClientSession session;

   private ClientProducer producer;

   private ClientConsumer consumer1;

   private ClientConsumer consumer2;

   private ClientConsumer consumer3;

   public void test() throws Exception
   {
      for (int i = 0; i < 1000; i++)
      {
         ClientMessage message = session.createClientMessage(false);

         final String body = RandomUtil.randomString();

         message.getBodyBuffer().writeString(body);
         
         producer.send(message);

         ClientMessage received1 = consumer1.receive(1000);
         assertNotNull(received1);
         assertEquals(body, received1.getBodyBuffer().readString());
         
         ClientMessage received2 = consumer2.receive(1000);
         assertNotNull(received2);
         assertEquals(body, received2.getBodyBuffer().readString());
         
         ClientMessage received3 = consumer3.receive(1000);
         assertNotNull(received3);
         assertEquals(body, received3.getBodyBuffer().readString());
      }
   }

   protected void setUp() throws Exception
   {
      super.setUp();

      server = createServer(false, true);

      server.start();

      ClientSessionFactory cf = createFactory();

      session = cf.createSession();

      session.createQueue(address, "queue1");
      session.createQueue(address, "queue2");
      session.createQueue(address, "queue3");

      producer = session.createProducer(address);

      consumer1 = session.createConsumer("queue1");

      consumer2 = session.createConsumer("queue2");

      consumer3 = session.createConsumer("queue3");

      session.start();
   }

   protected ClientSessionFactory createFactory()
   {
      return this.createNettyFactory();
   }

   protected void tearDown() throws Exception
   {
      if (session != null)
      {
         consumer1.close();

         consumer2.close();

         consumer3.close();

         session.deleteQueue("queue1");
         session.deleteQueue("queue2");
         session.deleteQueue("queue3");

         session.close();
      }

      if (server.isStarted())
      {
         server.stop();
      }

      super.tearDown();
   }

   private ClientMessage sendAndReceive(final ClientMessage message) throws Exception
   {
      producer.send(message);

      ClientMessage received = consumer1.receive(10000);

      return received;
   }

}
