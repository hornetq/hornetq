/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */
package org.jboss.messaging.tests.integration.client;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ClientRoutingTest extends ServiceTestBase
{
   public final SimpleString addressA = new SimpleString("addressA");

   public final SimpleString queueA = new SimpleString("queueA");

   public final SimpleString queueB = new SimpleString("queueB");

   public final SimpleString queueC = new SimpleString("queueC");


   public void testRouteToMultipleQueues() throws Exception
   {
      MessagingServer server = createServer(false);

      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession sendSession = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, false);
         sendSession.createQueue(addressA, queueB, false);
         sendSession.createQueue(addressA, queueC, false);
         int numMessages = 300;
         ClientProducer p = sendSession.createProducer(addressA);
         for (int i = 0; i < numMessages; i++)
         {
            p.send(sendSession.createClientMessage(false));
         }
         ClientSession session = cf.createSession(false, true, true);
         ClientConsumer c1 = session.createConsumer(queueA);
         ClientConsumer c2 = session.createConsumer(queueB);
         ClientConsumer c3 = session.createConsumer(queueC);
         session.start();
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage m = c1.receive(5000);
            assertNotNull(m);
            m.acknowledge();
            c2.receive(5000);
            assertNotNull(m);
            m.acknowledge();
            c3.receive(5000);
            assertNotNull(m);
            m.acknowledge();
         }
         assertNull(c1.receiveImmediate());
         assertNull(c2.receiveImmediate());
         assertNull(c3.receiveImmediate());
         sendSession.close();
         session.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testRouteToSingleNonDurableQueue() throws Exception
   {
      MessagingServer server = createServer(false);

      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession sendSession = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, false);
         int numMessages = 300;
         ClientProducer p = sendSession.createProducer(addressA);
         for (int i = 0; i < numMessages; i++)
         {
            p.send(sendSession.createClientMessage(false));
         }
         ClientSession session = cf.createSession(false, true, true);
         ClientConsumer c1 = session.createConsumer(queueA);
         session.start();
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage m = c1.receive(5000);
            assertNotNull(m);
            m.acknowledge();
         }
         assertNull(c1.receiveImmediate());
         sendSession.close();
         session.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testRouteToSingleDurableQueue() throws Exception
   {
      MessagingServer server = createServer(false);

      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession sendSession = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, true);
         int numMessages = 300;
         ClientProducer p = sendSession.createProducer(addressA);
         for (int i = 0; i < numMessages; i++)
         {
            p.send(sendSession.createClientMessage(false));
         }
         ClientSession session = cf.createSession(false, true, true);
         ClientConsumer c1 = session.createConsumer(queueA);
         session.start();
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage m = c1.receive(5000);
            assertNotNull(m);
            m.acknowledge();
         }
         assertNull(c1.receiveImmediate());
         sendSession.close();
         session.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testRouteToSingleQueueWithFilter() throws Exception
   {
      MessagingServer server = createServer(false);

      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession sendSession = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, new SimpleString("foo = 'bar'"), false);
         int numMessages = 300;
         ClientProducer p = sendSession.createProducer(addressA);
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage clientMessage = sendSession.createClientMessage(false);
            clientMessage.putStringProperty(new SimpleString("foo"), new SimpleString("bar"));
            p.send(clientMessage);
         }
         ClientSession session = cf.createSession(false, true, true);
         ClientConsumer c1 = session.createConsumer(queueA);
         session.start();
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage m = c1.receive(5000);
            assertNotNull(m);
            m.acknowledge();
         }
         assertNull(c1.receiveImmediate());
         sendSession.close();
         session.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testRouteToMultipleQueueWithFilters() throws Exception
   {
      MessagingServer server = createServer(false);

      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession sendSession = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, new SimpleString("foo = 'bar'"), false);
         sendSession.createQueue(addressA, queueB, new SimpleString("x = 1"), false);
         sendSession.createQueue(addressA, queueC, new SimpleString("b = false"), false);
         int numMessages = 300;
         ClientProducer p = sendSession.createProducer(addressA);
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage clientMessage = sendSession.createClientMessage(false);
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
            assertNotNull(m);
            m.acknowledge();
            m = c2.receive(5000);
            assertNotNull(m);
            m.acknowledge();
            m = c3.receive(5000);
            assertNotNull(m);
            m.acknowledge();
         }
         assertNull(c1.receiveImmediate());
         assertNull(c2.receiveImmediate());
         assertNull(c3.receiveImmediate());
         sendSession.close();
         session.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testRouteToSingleTemporaryQueue() throws Exception
   {
      MessagingServer server = createServer(false);

      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession sendSession = cf.createSession(false, true, true);
         sendSession.createTemporaryQueue(addressA, queueA);
         int numMessages = 300;
         ClientProducer p = sendSession.createProducer(addressA);
         for (int i = 0; i < numMessages; i++)
         {
            p.send(sendSession.createClientMessage(false));
         }
         ClientSession session = cf.createSession(false, true, true);
         ClientConsumer c1 = session.createConsumer(queueA);
         session.start();
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage m = c1.receive(5000);
            assertNotNull(m);
            m.acknowledge();
         }
         assertNull(c1.receiveImmediate());
         sendSession.close();
         session.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

}
