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
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.*;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class DeliveryOrderTest extends ServiceTestBase
{
   public final SimpleString addressA = new SimpleString("addressA");

   public final SimpleString queueA = new SimpleString("queueA");

   public final SimpleString queueB = new SimpleString("queueB");

   public final SimpleString queueC = new SimpleString("queueC");

   private ServerLocator locator;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      locator = createInVMNonHALocator();
   }

   @Override
   protected void tearDown() throws Exception
   {
      locator.close();
      locator = null;
      super.tearDown();
   }

   public void testSendDeliveryOrderOnCommit() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = locator.createSessionFactory();
         ClientSession sendSession = cf.createSession(false, false, true);
         ClientProducer cp = sendSession.createProducer(addressA);
         int numMessages = 1000;
         sendSession.createQueue(addressA, queueA, false);
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage cm = sendSession.createMessage(false);
            cm.getBodyBuffer().writeInt(i);
            cp.send(cm);
            if (i % 10 == 0)
            {
               sendSession.commit();
            }
            sendSession.commit();
         }
         ClientConsumer c = sendSession.createConsumer(queueA);
         sendSession.start();
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage cm = c.receive(5000);
            Assert.assertNotNull(cm);
            Assert.assertEquals(i, cm.getBodyBuffer().readInt());
         }
         sendSession.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testReceiveDeliveryOrderOnRollback() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ServerLocator locator = createInVMNonHALocator();
         ClientSessionFactory cf = locator.createSessionFactory();
         ClientSession sendSession = cf.createSession(false, true, false);
         ClientProducer cp = sendSession.createProducer(addressA);
         int numMessages = 1000;
         sendSession.createQueue(addressA, queueA, false);
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage cm = sendSession.createMessage(false);
            cm.getBodyBuffer().writeInt(i);
            cp.send(cm);
         }
         ClientConsumer c = sendSession.createConsumer(queueA);
         sendSession.start();
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage cm = c.receive(5000);
            Assert.assertNotNull(cm);
            cm.acknowledge();
            Assert.assertEquals(i, cm.getBodyBuffer().readInt());
         }
         sendSession.rollback();
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage cm = c.receive(5000);
            Assert.assertNotNull(cm);
            cm.acknowledge();
            Assert.assertEquals(i, cm.getBodyBuffer().readInt());
         }
         sendSession.close();
      }
      finally
      {
         locator.close();
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testMultipleConsumersMessageOrder() throws Exception
   {
      HornetQServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = locator.createSessionFactory();
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientSession recSession = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, false);
         int numReceivers = 100;
         AtomicInteger count = new AtomicInteger(0);
         int numMessage = 10000;
         ClientConsumer[] clientConsumers = new ClientConsumer[numReceivers];
         Receiver[] receivers = new Receiver[numReceivers];
         CountDownLatch latch = new CountDownLatch(numMessage);
         for (int i = 0; i < numReceivers; i++)
         {
            clientConsumers[i] = recSession.createConsumer(queueA);
            receivers[i] = new Receiver(latch);
            clientConsumers[i].setMessageHandler(receivers[i]);
         }
         recSession.start();
         ClientProducer clientProducer = sendSession.createProducer(addressA);
         for (int i = 0; i < numMessage; i++)
         {
            ClientMessage cm = sendSession.createMessage(false);
            cm.getBodyBuffer().writeInt(count.getAndIncrement());
            clientProducer.send(cm);
         }
         Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));
         for (Receiver receiver : receivers)
         {
            Assert.assertFalse("" + receiver.lastMessage, receiver.failed);
         }
         sendSession.close();
         recSession.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   class Receiver implements MessageHandler
   {
      final CountDownLatch latch;

      int lastMessage = -1;

      boolean failed = false;

      public Receiver(final CountDownLatch latch)
      {
         this.latch = latch;
      }

      public void onMessage(final ClientMessage message)
      {
         int i = message.getBodyBuffer().readInt();
         try
         {
            message.acknowledge();
         }
         catch (HornetQException e)
         {
            e.printStackTrace();
         }
         if (i <= lastMessage)
         {
            failed = true;
         }
         lastMessage = i;
         latch.countDown();
      }

   }

}
