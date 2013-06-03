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
import org.hornetq.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class RoutingTest extends ServiceTestBase
{
   public final SimpleString addressA = new SimpleString("addressA");
   public final SimpleString queueA = new SimpleString("queueA");
   public final SimpleString queueB = new SimpleString("queueB");
   public final SimpleString queueC = new SimpleString("queueC");

   private ServerLocator locator;
   private HornetQServer server;
   private ClientSessionFactory cf;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      locator = createInVMNonHALocator();
      server = createServer(false);

      server.start();
      cf = createSessionFactory(locator);
   }

   @Test
   public void testRouteToMultipleQueues() throws Exception
   {
         ClientSession sendSession = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, false);
         sendSession.createQueue(addressA, queueB, false);
         sendSession.createQueue(addressA, queueC, false);
         int numMessages = 300;
         ClientProducer p = sendSession.createProducer(addressA);
         for (int i = 0; i < numMessages; i++)
         {
            p.send(sendSession.createMessage(false));
         }
         ClientSession session = cf.createSession(false, true, true);
         ClientConsumer c1 = session.createConsumer(queueA);
         ClientConsumer c2 = session.createConsumer(queueB);
         ClientConsumer c3 = session.createConsumer(queueC);
         session.start();
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage m = c1.receive(5000);
            Assert.assertNotNull(m);
            m.acknowledge();
            c2.receive(5000);
            Assert.assertNotNull(m);
            m.acknowledge();
            c3.receive(5000);
            Assert.assertNotNull(m);
            m.acknowledge();
         }
         Assert.assertNull(c1.receiveImmediate());
         Assert.assertNull(c2.receiveImmediate());
         Assert.assertNull(c3.receiveImmediate());
         sendSession.close();
         session.close();
   }

   @Test
   public void testRouteToSingleNonDurableQueue() throws Exception
   {
         ClientSession sendSession = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, false);
         int numMessages = 300;
         ClientProducer p = sendSession.createProducer(addressA);
         for (int i = 0; i < numMessages; i++)
         {
            p.send(sendSession.createMessage(false));
         }
         ClientSession session = cf.createSession(false, true, true);
         ClientConsumer c1 = session.createConsumer(queueA);
         session.start();
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage m = c1.receive(5000);
            Assert.assertNotNull(m);
            m.acknowledge();
         }
         Assert.assertNull(c1.receiveImmediate());
         sendSession.close();
         session.close();
   }

   @Test
   public void testRouteToSingleDurableQueue() throws Exception
   {
      ClientSession sendSession = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, true);
         int numMessages = 300;
         ClientProducer p = sendSession.createProducer(addressA);
         for (int i = 0; i < numMessages; i++)
         {
            p.send(sendSession.createMessage(false));
         }
         ClientSession session = cf.createSession(false, true, true);
         ClientConsumer c1 = session.createConsumer(queueA);
         session.start();
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage m = c1.receive(5000);
            Assert.assertNotNull(m);
            m.acknowledge();
         }
         Assert.assertNull(c1.receiveImmediate());
         sendSession.close();
         session.close();
   }

   @Test
   public void testRouteToSingleQueueWithFilter() throws Exception
   {
      ClientSession sendSession = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, new SimpleString("foo = 'bar'"), false);
         int numMessages = 300;
         ClientProducer p = sendSession.createProducer(addressA);
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage clientMessage = sendSession.createMessage(false);
            clientMessage.putStringProperty(new SimpleString("foo"), new SimpleString("bar"));
            p.send(clientMessage);
         }
         ClientSession session = cf.createSession(false, true, true);
         ClientConsumer c1 = session.createConsumer(queueA);
         session.start();
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage m = c1.receive(5000);
            Assert.assertNotNull(m);
            m.acknowledge();
         }
         Assert.assertNull(c1.receiveImmediate());
         sendSession.close();
         session.close();
   }

   @Test
   public void testRouteToMultipleQueueWithFilters() throws Exception
   {
      ClientSession sendSession = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, new SimpleString("foo = 'bar'"), false);
         sendSession.createQueue(addressA, queueB, new SimpleString("x = 1"), false);
         sendSession.createQueue(addressA, queueC, new SimpleString("b = false"), false);
         int numMessages = 300;
         ClientProducer p = sendSession.createProducer(addressA);
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage clientMessage = sendSession.createMessage(false);
            if (i % 3 == 0)
            {
               clientMessage.putStringProperty(new SimpleString("foo"), new SimpleString("bar"));
            }
            else if (i % 3 == 1)
            {
               clientMessage.putIntProperty(new SimpleString("x"), 1);
            }
            else
            {
               clientMessage.putBooleanProperty(new SimpleString("b"), false);
            }
            p.send(clientMessage);
         }
         ClientSession session = cf.createSession(false, true, true);
         ClientConsumer c1 = session.createConsumer(queueA);
         ClientConsumer c2 = session.createConsumer(queueB);
         ClientConsumer c3 = session.createConsumer(queueC);
         session.start();
         for (int i = 0; i < numMessages / 3; i++)
         {
            ClientMessage m = c1.receive(5000);
            Assert.assertNotNull(m);
            m.acknowledge();
            m = c2.receive(5000);
            Assert.assertNotNull(m);
            m.acknowledge();
            m = c3.receive(5000);
            Assert.assertNotNull(m);
            m.acknowledge();
         }
         Assert.assertNull(c1.receiveImmediate());
         Assert.assertNull(c2.receiveImmediate());
         Assert.assertNull(c3.receiveImmediate());
         sendSession.close();
         session.close();
   }

   @Test
   public void testRouteToSingleTemporaryQueue() throws Exception
   {
      ClientSession sendSession = cf.createSession(false, true, true);
         sendSession.createTemporaryQueue(addressA, queueA);
         int numMessages = 300;
         ClientProducer p = sendSession.createProducer(addressA);
         for (int i = 0; i < numMessages; i++)
         {
            p.send(sendSession.createMessage(false));
         }
         ClientSession session = cf.createSession(false, true, true);
         ClientConsumer c1 = session.createConsumer(queueA);
         session.start();
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage m = c1.receive(5000);
            Assert.assertNotNull(m);
            m.acknowledge();
         }
         Assert.assertNull(c1.receiveImmediate());
         sendSession.close();
         session.close();
   }
}
