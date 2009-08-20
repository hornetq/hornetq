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

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.MessageHandler;
import org.hornetq.core.exception.MessagingException;
import org.hornetq.core.server.MessagingServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class AutogroupIdTest extends ServiceTestBase
{
   public final SimpleString addressA = new SimpleString("addressA");

   public final SimpleString queueA = new SimpleString("queueA");

   public final SimpleString queueB = new SimpleString("queueB");

   public final SimpleString queueC = new SimpleString("queueC");

   private final SimpleString groupTestQ = new SimpleString("testGroupQueue");

   /* auto group id tests*/

   /*
  * tests when the autogroupid is set only 1 consumer (out of 2) gets all the messages from a single producer
  * */

   public void testGroupIdAutomaticallySet() throws Exception
   {
      MessagingServer server = createServer(false);
      try
      {
         server.start();

         ClientSessionFactory sf = createInVMFactory();
         sf.setAutoGroup(true);
         ClientSession session = sf.createSession(false, true, true);

         session.createQueue(groupTestQ, groupTestQ, null, false);

         ClientProducer producer = session.createProducer(groupTestQ);

         final CountDownLatch latch = new CountDownLatch(100);

         MyMessageHandler myMessageHandler = new MyMessageHandler(latch);
         MyMessageHandler myMessageHandler2 = new MyMessageHandler(latch);

         ClientConsumer consumer = session.createConsumer(groupTestQ);
         consumer.setMessageHandler(myMessageHandler);
         ClientConsumer consumer2 = session.createConsumer(groupTestQ);
         consumer2.setMessageHandler(myMessageHandler2);

         session.start();

         final int numMessages = 100;

         for (int i = 0; i < numMessages; i++)
         {
            producer.send(session.createClientMessage(false));
         }
         latch.await();

         session.close();

         assertEquals(myMessageHandler.messagesReceived, 100);
         assertEquals(myMessageHandler2.messagesReceived, 0);
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }

   }

   /*
  * tests when the autogroupid is set only 2 consumers (out of 3) gets all the messages from 2 producers
  * */
   public void testGroupIdAutomaticallySetMultipleProducers() throws Exception
   {
      MessagingServer server = createServer(false);
      try
      {
         server.start();

         ClientSessionFactory sf = createInVMFactory();
         sf.setAutoGroup(true);
         ClientSession session = sf.createSession(false, true, true);

         session.createQueue(groupTestQ, groupTestQ, null, false);

         ClientProducer producer = session.createProducer(groupTestQ);
         ClientProducer producer2 = session.createProducer(groupTestQ);

         final CountDownLatch latch = new CountDownLatch(200);

         MyMessageHandler myMessageHandler = new MyMessageHandler(latch);
         MyMessageHandler myMessageHandler2 = new MyMessageHandler(latch);
         MyMessageHandler myMessageHandler3 = new MyMessageHandler(latch);

         ClientConsumer consumer = session.createConsumer(groupTestQ);
         consumer.setMessageHandler(myMessageHandler);
         ClientConsumer consumer2 = session.createConsumer(groupTestQ);
         consumer2.setMessageHandler(myMessageHandler2);
         ClientConsumer consumer3 = session.createConsumer(groupTestQ);
         consumer3.setMessageHandler(myMessageHandler3);

         session.start();

         final int numMessages = 100;

         for (int i = 0; i < numMessages; i++)
         {
            producer.send(session.createClientMessage(false));
         }
         for (int i = 0; i < numMessages; i++)
         {
            producer2.send(session.createClientMessage(false));
         }
         latch.await();

         session.close();

         assertEquals(myMessageHandler.messagesReceived, 100);
         assertEquals(myMessageHandler2.messagesReceived, 100);
         assertEquals(myMessageHandler3.messagesReceived, 0);
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }

   }

   /*
  * tests that even tho we have an grouping round robin distributor we don't pin the consumer as autogroup is false
  * */
   public void testGroupIdAutomaticallyNotSet() throws Exception
   {
      MessagingServer server = createServer(false);
      try
      {
         server.start();

         ClientSessionFactory sf = createInVMFactory();

         ClientSession session = sf.createSession(false, true, true);

         session.createQueue(groupTestQ, groupTestQ, null, false);

         ClientProducer producer = session.createProducer(groupTestQ);

         final CountDownLatch latch = new CountDownLatch(100);

         MyMessageHandler myMessageHandler = new MyMessageHandler(latch);
         MyMessageHandler myMessageHandler2 = new MyMessageHandler(latch);

         ClientConsumer consumer = session.createConsumer(groupTestQ);
         consumer.setMessageHandler(myMessageHandler);
         ClientConsumer consumer2 = session.createConsumer(groupTestQ);
         consumer2.setMessageHandler(myMessageHandler2);

         session.start();

         final int numMessages = 100;

         for (int i = 0; i < numMessages; i++)
         {
            producer.send(session.createClientMessage(false));
         }
         latch.await();

         session.close();

         assertEquals(myMessageHandler.messagesReceived, 50);
         assertEquals(myMessageHandler2.messagesReceived, 50);
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }

   }

   private static class MyMessageHandler implements MessageHandler
   {
      volatile int messagesReceived = 0;

      private final CountDownLatch latch;

      public MyMessageHandler(CountDownLatch latch)
      {
         this.latch = latch;
      }

      public void onMessage(ClientMessage message)
      {
         messagesReceived++;
         try
         {
            message.acknowledge();
         }
         catch (MessagingException e)
         {
            e.printStackTrace();
         }
         latch.countDown();
      }
   }
}
