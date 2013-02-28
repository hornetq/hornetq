/*
 * Copyright 2013 Red Hat, Inc.
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

import java.util.concurrent.Semaphore;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.wireformat.SessionReceiveMessage;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * This test will simulate a consumer hanging on the delivery packet due to unbehaved clients
 * and it will make sure we can still perform certain operations on the queue such as produce
 * and verify the counters
 */
public class HangConsumerTest extends ServiceTestBase
{

   private HornetQServer server;

   private final SimpleString QUEUE = new SimpleString("ConsumerTestQueue");


   private Queue queue;

   private ServerLocator locator;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      server = addServer(createServer(false));
      server.start();
      queue = server.createQueue(QUEUE, QUEUE, null, true, false);
      locator = addServerLocator(createInVMNonHALocator());
   }

   public void testHangOnDelivery() throws Exception
   {
      HangInterceptor hangInt = new HangInterceptor();
      try
      {
         server.getRemotingService().addOutgoingInterceptor(hangInt);

         ClientSessionFactory factory = addSessionFactory(locator.createSessionFactory());
         ClientSession session = addClientSession(factory.createSession(false, false, false));

         ClientProducer producer = session.createProducer(QUEUE);

         ClientConsumer consumer = session.createConsumer(QUEUE);

         producer.send(session.createMessage(true));
         session.commit();

         hangInt.close();

         session.start();

         producer.send(session.createMessage(true));
         session.commit();

         ClientConsumer consumer2 = session.createConsumer(QUEUE);

         // These two operations should finish without the test hanging
         queue.getMessagesAdded();
         queue.getMessageCount();

         hangInt.open();

         // a rollback to make sure everything will be reset on the deliveries
         // and that both consumers will receive each a message
         // this is to guarantee the server will have both consumers regsitered
         session.rollback();

         // a flush to guarantee any pending task is finished on flushing out delivery and pending msgs
         queue.flushExecutor();
         assertEquals(2, queue.getMessageCount());
         assertEquals(2, queue.getMessagesAdded());

         ClientMessage msg = consumer.receive(5000);
         assertNotNull(msg);
         msg.acknowledge();

         msg = consumer2.receive(5000);
         assertNotNull(msg);
         msg.acknowledge();

         session.commit();
      }
      finally
      {
         hangInt.open();
      }

   }

   // An exception during delivery shouldn't make the message disappear
   public void testExceptionWhileDelivering() throws Exception
   {
      HangInterceptor hangInt = new HangInterceptor();
      try
      {
         server.getRemotingService().addOutgoingInterceptor(hangInt);

         ClientSessionFactory factory = addSessionFactory(locator.createSessionFactory());
         ClientSession session = addClientSession(factory.createSession(false, false, false));

         ClientProducer producer = session.createProducer(QUEUE);

         ClientConsumer consumer = session.createConsumer(QUEUE);

         producer.send(session.createMessage(true));
         session.commit();

         hangInt.close();

         session.start();

         // These two operations should finish without the test hanging
         queue.getMessagesAdded();
         queue.getMessageCount();
         hangInt.pendingException = new HornetQException("fake error");

         hangInt.open();

         session.close();

         session = factory.createSession(false, false);
         session.start();

         consumer = session.createConsumer(QUEUE);

         ClientMessage msg = consumer.receive(5000);
         assertNotNull(msg);
         msg.acknowledge();

         session.commit();
      }
      finally
      {
         hangInt.open();
      }

   }

   class HangInterceptor implements Interceptor
   {
      Semaphore semaphore = new Semaphore(1);

      volatile HornetQException pendingException = null;

      public void close() throws Exception
      {
         semaphore.acquire();
      }

      public void open() throws Exception
      {
         semaphore.release();
      }

      public boolean intercept(Packet packet, RemotingConnection connection) throws HornetQException
      {
         if (packet instanceof SessionReceiveMessage)
         {
            System.out.println("Receiving message");
            try
            {
               semaphore.acquire();
               semaphore.release();
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }

         if (pendingException != null)
         {
            HornetQException exToThrow = pendingException;
            pendingException = null;
            throw exToThrow;
         }
         return true;
      }

   }
}
