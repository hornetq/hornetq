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

import junit.framework.Assert;

import org.hornetq.api.core.client.*;
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

   private ServerLocator locator;

   public void test() throws Exception
   {
      for (int i = 0; i < 1000; i++)
      {
         ClientMessage message = session.createMessage(false);

         final String body = RandomUtil.randomString();

         message.getBodyBuffer().writeString(body);

       //  log.info("sending message");
         producer.send(message);
        // log.info("sent message");

         ClientMessage received1 = consumer1.receive(1000);
         Assert.assertNotNull(received1);
         Assert.assertEquals(body, received1.getBodyBuffer().readString());

         ClientMessage received2 = consumer2.receive(1000);
         Assert.assertNotNull(received2);
         Assert.assertEquals(body, received2.getBodyBuffer().readString());

         ClientMessage received3 = consumer3.receive(1000);
         Assert.assertNotNull(received3);
         Assert.assertEquals(body, received3.getBodyBuffer().readString());
      }
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      server = createServer(false, true);

      server.start();

      locator = createNettyNonHALocator();

      ClientSessionFactory cf = locator.createSessionFactory();

      session = cf.createSession();

      session.createQueue(SimpleSendMultipleQueues.address, "queue1");
      session.createQueue(SimpleSendMultipleQueues.address, "queue2");
      session.createQueue(SimpleSendMultipleQueues.address, "queue3");

      producer = session.createProducer(SimpleSendMultipleQueues.address);

      consumer1 = session.createConsumer("queue1");

      consumer2 = session.createConsumer("queue2");

      consumer3 = session.createConsumer("queue3");

      session.start();
   }

   @Override
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

         locator.close();
      }

      if (server.isStarted())
      {
         server.stop();
      }

      super.tearDown();
   }

}
