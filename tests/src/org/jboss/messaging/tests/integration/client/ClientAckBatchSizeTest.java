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
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ClientAckBatchSizeTest extends ServiceTestBase
{
   public final SimpleString addressA = new SimpleString("addressA");

   public final SimpleString queueA = new SimpleString("queueA");

   public final SimpleString queueB = new SimpleString("queueB");

   public final SimpleString queueC = new SimpleString("queueC");

   /*ackbatchSize tests*/

   /*
   * tests that wed don't acknowledge until the correct ackBatchSize is reached
   * */

   public void testAckBatchSize() throws Exception
   {
      MessagingServer server = createServer(false);

      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientMessage message = sendSession.createClientMessage(false);
         // we need to set the destination so we can calculate the encodesize correctly
         message.setDestination(addressA);
         int encodeSize = message.getEncodeSize();
         int numMessages = 100;
         cf.setAckBatchSize(numMessages * encodeSize);
         cf.setBlockOnAcknowledge(true);
         ClientSession session = cf.createSession(false, true, true);
         session.createQueue(addressA, queueA, false);
         ClientProducer cp = sendSession.createProducer(addressA);
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(sendSession.createClientMessage(false));
         }

         ClientConsumer consumer = session.createConsumer(queueA);
         session.start();
         for (int i = 0; i < numMessages - 1; i++)
         {
            ClientMessage m = consumer.receive(5000);
            m.acknowledge();
         }

         ClientMessage m = consumer.receive(5000);
         Queue q = (Queue) server.getPostOffice().getBinding(queueA).getBindable();
         assertEquals(numMessages, q.getDeliveringCount());
         m.acknowledge();
         assertEquals(0, q.getDeliveringCount());
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

   /*
   * tests that when the ackBatchSize is 0 we ack every message directly
   * */
   public void testAckBatchSizeZero() throws Exception
   {
      MessagingServer server = createServer(false);

      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientMessage message = sendSession.createClientMessage(false);
         message.setDestination(addressA);
         int numMessages = 100;
         cf.setAckBatchSize(0);
         cf.setBlockOnAcknowledge(true);
         ClientSession session = cf.createSession(false, true, true);
         session.createQueue(addressA, queueA, false);
         ClientProducer cp = sendSession.createProducer(addressA);
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(sendSession.createClientMessage(false));
         }

         ClientConsumer consumer = session.createConsumer(queueA);
         session.start();
         Queue q = (Queue) server.getPostOffice().getBinding(queueA).getBindable();
         ClientMessage[] messages = new ClientMessage[numMessages];
         for (int i = 0; i < numMessages; i++)
         {
            messages[i] = consumer.receive(5000);
            assertNotNull(messages[i]);
         }
         for (int i = 0; i < numMessages; i++)
         {
            messages[i].acknowledge();
            assertEquals(numMessages - i - 1, q.getDeliveringCount());
         }
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
