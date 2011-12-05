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

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class AcknowledgeTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(AcknowledgeTest.class);

   public final SimpleString addressA = new SimpleString("addressA");

   public final SimpleString queueA = new SimpleString("queueA");

   public final SimpleString queueB = new SimpleString("queueB");

   public final SimpleString queueC = new SimpleString("queueC");

   public void testReceiveAckLastMessageOnly() throws Exception
   {
      HornetQServer server = createServer(false);
      server.start();
         ServerLocator locator = createInVMNonHALocator();
         locator.setAckBatchSize(0);
         locator.setBlockOnAcknowledge(true);
         ClientSessionFactory cf = createSessionFactory(locator);
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientSession session = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, false);
         ClientProducer cp = sendSession.createProducer(addressA);
         ClientConsumer cc = session.createConsumer(queueA);
         int numMessages = 100;
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(sendSession.createMessage(false));
         }
         session.start();
         ClientMessage cm = null;
         for (int i = 0; i < numMessages; i++)
         {
            cm = cc.receive(5000);
            Assert.assertNotNull(cm);
         }
         cm.acknowledge();
         Queue q = (Queue)server.getPostOffice().getBinding(queueA).getBindable();

         Assert.assertEquals(0, q.getDeliveringCount());
         session.close();
         sendSession.close();
   }

   public void testAsyncConsumerNoAck() throws Exception
   {
      HornetQServer server = createServer(false);

         server.start();
         ServerLocator locator = createInVMNonHALocator();
         ClientSessionFactory cf = createSessionFactory(locator);
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientSession session = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, false);
         ClientProducer cp = sendSession.createProducer(addressA);
         ClientConsumer cc = session.createConsumer(queueA);
         int numMessages = 3;
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(sendSession.createMessage(false));
         }

         Thread.sleep(500);
         log.info("woke up");

         final CountDownLatch latch = new CountDownLatch(numMessages);
         session.start();
         cc.setMessageHandler(new MessageHandler()
         {
            int c = 0;
            public void onMessage(final ClientMessage message)
            {
               log.info("Got message " + c++);
               latch.countDown();
            }
         });
         Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
         Queue q = (Queue)server.getPostOffice().getBinding(queueA).getBindable();
         Assert.assertEquals(numMessages, q.getDeliveringCount());
         sendSession.close();
         session.close();
         }

   public void testAsyncConsumerAck() throws Exception
   {
      HornetQServer server = createServer(false);
      server.start();
         ServerLocator locator = createInVMNonHALocator();
         locator.setBlockOnAcknowledge(true);
         locator.setAckBatchSize(0);
         ClientSessionFactory cf = createSessionFactory(locator);
         ClientSession sendSession = cf.createSession(false, true, true);
         final ClientSession session = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, false);
         ClientProducer cp = sendSession.createProducer(addressA);
         ClientConsumer cc = session.createConsumer(queueA);
         int numMessages = 100;
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(sendSession.createMessage(false));
         }
         final CountDownLatch latch = new CountDownLatch(numMessages);
         session.start();
         cc.setMessageHandler(new MessageHandler()
         {
            public void onMessage(final ClientMessage message)
            {
               try
               {
                  message.acknowledge();
               }
               catch (HornetQException e)
               {
                  try
                  {
                     session.close();
                  }
                  catch (HornetQException e1)
                  {
                     e1.printStackTrace();
                  }
               }
               latch.countDown();
            }
         });
         Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
         Queue q = (Queue)server.getPostOffice().getBinding(queueA).getBindable();
         Assert.assertEquals(0, q.getDeliveringCount());
         sendSession.close();
         session.close();
   }

   public void testAsyncConsumerAckLastMessageOnly() throws Exception
   {
      HornetQServer server = createServer(false);
      server.start();
         ServerLocator locator = createInVMNonHALocator();
         locator.setBlockOnAcknowledge(true);
         locator.setAckBatchSize(0);
         ClientSessionFactory cf = createSessionFactory(locator);
         ClientSession sendSession = cf.createSession(false, true, true);
         final ClientSession session = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, false);
         ClientProducer cp = sendSession.createProducer(addressA);
         ClientConsumer cc = session.createConsumer(queueA);
         int numMessages = 100;
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(sendSession.createMessage(false));
         }
         final CountDownLatch latch = new CountDownLatch(numMessages);
         session.start();
         cc.setMessageHandler(new MessageHandler()
         {
            public void onMessage(final ClientMessage message)
            {
               if (latch.getCount() == 1)
               {
                  try
                  {
                     message.acknowledge();
                  }
                  catch (HornetQException e)
                  {
                     try
                     {
                        session.close();
                     }
                     catch (HornetQException e1)
                     {
                        e1.printStackTrace();
                     }
                  }
               }
               latch.countDown();
            }
         });
         Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
         Queue q = (Queue)server.getPostOffice().getBinding(queueA).getBindable();
         Assert.assertEquals(0, q.getDeliveringCount());
         sendSession.close();
         session.close();
   }

}
