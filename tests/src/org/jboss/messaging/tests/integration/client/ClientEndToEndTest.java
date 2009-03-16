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
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.impl.GroupingRoundRobinDistributor;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ClientEndToEndTest extends ServiceTestBase
{
   public final SimpleString addressA = new SimpleString("addressA");

   public final SimpleString queueA = new SimpleString("queueA");

   public final SimpleString queueB = new SimpleString("queueB");

   public final SimpleString queueC = new SimpleString("queueC");

   private final SimpleString groupTestQ = new SimpleString("testGroupQueue");

   /*ackbatchSize tests*/

   /*
   * tests that wed don't acknowledge until the correct ackBatchSize is reached
   * */

   public void testAckBatchSize() throws Exception
   {
      MessagingService messagingService = createService(false);

      try
      {
         messagingService.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientMessage message = sendSession.createClientMessage(false);
         //we need to set the destination so we can calculate the encodesize correctly
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
         Queue q = (Queue) messagingService.getServer().getPostOffice().getBinding(queueA).getBindable();
         assertEquals(numMessages, q.getDeliveringCount());
         m.acknowledge();
         assertEquals(0, q.getDeliveringCount());
         sendSession.close();
         session.close();
      }
      finally
      {
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   /*
   * tests that when the ackBatchSize is 0 we ack every message directly
   * */
   public void testAckBatchSizeZero() throws Exception
   {
      MessagingService messagingService = createService(false);

      try
      {
         messagingService.start();
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
         Queue q = (Queue) messagingService.getServer().getPostOffice().getBinding(queueA).getBindable();
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
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   /* auto group id tests*/

   /*
   * tests when the autogroupid is set only 1 consumer (out of 2) gets all the messages from a single producer
   * */

   public void testGroupIdAutomaticallySet() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         AddressSettings qs = new AddressSettings();
         qs.setDistributionPolicyClass(GroupingRoundRobinDistributor.class.getName());
         messagingService.getServer().getAddressSettingsRepository().addMatch(groupTestQ.toString(), qs);
         messagingService.start();

         ClientSessionFactory sf = createInVMFactory();
         sf.setAutoGroup(true);
         ClientSession session = sf.createSession(false, true, true);

         session.createQueue(groupTestQ, groupTestQ, null, false, false);

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
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }

   }

   /*
   * tests when the autogroupid is set only 2 consumers (out of 3) gets all the messages from 2 producers
   * */
   public void testGroupIdAutomaticallySetMultipleProducers() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         AddressSettings qs = new AddressSettings();
         qs.setDistributionPolicyClass(GroupingRoundRobinDistributor.class.getName());
         messagingService.getServer().getAddressSettingsRepository().addMatch(groupTestQ.toString(), qs);
         messagingService.start();

         ClientSessionFactory sf = createInVMFactory();
         sf.setAutoGroup(true);
         ClientSession session = sf.createSession(false, true, true);

         session.createQueue(groupTestQ, groupTestQ, null, false, false);

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
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }

   }

   /*
   * tests that even tho we have an grouping round robin distributor we don't pin the consumer as autogroup is false
   * */
   public void testGroupIdAutomaticallyNotSet() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         AddressSettings qs = new AddressSettings();
         qs.setDistributionPolicyClass(GroupingRoundRobinDistributor.class.getName());

         messagingService.getServer().getAddressSettingsRepository().addMatch(groupTestQ.toString(), qs);
         messagingService.start();

         ClientSessionFactory sf = createInVMFactory();

         ClientSession session = sf.createSession(false, true, true);

         session.createQueue(groupTestQ, groupTestQ, null, false, false);

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
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }

   }

   /*
   * tests send window size. we do this by having 2 receivers on the q. since we roundrobin the consumer for delivery we
   * know if consumer 1 has received n messages then consumer 2 must have also have received n messages or at least up
   * to its window size
   * */
   public void testSendWindowSize() throws Exception
   {
      MessagingService messagingService = createService(false);
      ClientSessionFactory cf = createInVMFactory();
      try
      {
         messagingService.start();
         cf.setBlockOnNonPersistentSend(true);
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientSession receiveSession = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, false);
         ClientConsumer receivingConsumer = receiveSession.createConsumer(queueA);
         ClientMessage cm = sendSession.createClientMessage(false);
         cm.setDestination(addressA);
         int encodeSize = cm.getEncodeSize();
         int numMessage = 100;
         cf.setConsumerWindowSize(numMessage * encodeSize);
         ClientSession session = cf.createSession(false, true, true);
         ClientProducer cp = sendSession.createProducer(addressA);
         ClientConsumer cc = session.createConsumer(queueA);
         session.start();
         receiveSession.start();
         for (int i = 0; i < numMessage * 4; i++)
         {
            cp.send(sendSession.createClientMessage(false));
         }

         for (int i = 0; i < numMessage * 2; i++)
         {
            ClientMessage m = receivingConsumer.receive(5000);
            assertNotNull(m);
            m.acknowledge();
         }
         receiveSession.close();
         Queue q = (Queue) messagingService.getServer().getPostOffice().getBinding(queueA).getBindable();
         assertEquals(numMessage, q.getDeliveringCount());

         session.close();
         sendSession.close();
      }
      finally
      {
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testRouteToMultipleQueues() throws Exception
   {
      MessagingService messagingService = createService(false);

      try
      {
         messagingService.start();
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
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testRouteToSingleNonDurableQueue() throws Exception
   {
      MessagingService messagingService = createService(false);

      try
      {
         messagingService.start();
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
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testRouteToSingleDurableQueue() throws Exception
   {
      MessagingService messagingService = createService(false);

      try
      {
         messagingService.start();
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
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testRouteToSingleQueueWithFilter() throws Exception
   {
      MessagingService messagingService = createService(false);

      try
      {
         messagingService.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession sendSession = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, new SimpleString("foo = 'bar'"), false, false);
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
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testRouteToMultipleQueueWithFilters() throws Exception
   {
      MessagingService messagingService = createService(false);

      try
      {
         messagingService.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession sendSession = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, new SimpleString("foo = 'bar'"), false, false);
         sendSession.createQueue(addressA, queueB, new SimpleString("x = 1"), false, false);
         sendSession.createQueue(addressA, queueC, new SimpleString("b = false"), false, false);
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
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testRouteToSingleTemporaryQueue() throws Exception
   {
      MessagingService messagingService = createService(false);

      try
      {
         messagingService.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession sendSession = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, false, true);
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
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testSendWithCommit() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession session = cf.createSession(false, false, false);
         session.createQueue(addressA, queueA, false);
         ClientProducer cp = session.createProducer(addressA);
         int numMessages = 100;
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(session.createClientMessage(false));
         }
         Queue q = (Queue) messagingService.getServer().getPostOffice().getBinding(queueA).getBindable();
         assertEquals(q.getMessageCount(), 0);
         session.commit();
         assertEquals(q.getMessageCount(), numMessages);
         //now send some more
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(session.createClientMessage(false));
         }
         assertEquals(q.getMessageCount(), numMessages);
         session.commit();
         assertEquals(q.getMessageCount(), numMessages * 2);
         session.close();
      }
      finally
      {
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testSendWithRollback() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession session = cf.createSession(false, false, false);
         session.createQueue(addressA, queueA, false);
         ClientProducer cp = session.createProducer(addressA);
         int numMessages = 100;
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(session.createClientMessage(false));
         }
         Queue q = (Queue) messagingService.getServer().getPostOffice().getBinding(queueA).getBindable();
         assertEquals(q.getMessageCount(), 0);
         session.rollback();
         assertEquals(q.getMessageCount(), 0);
         //now send some more
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(session.createClientMessage(false));
         }
         assertEquals(q.getMessageCount(), 0);
         session.commit();
         assertEquals(q.getMessageCount(), numMessages);
         session.close();
      }
      finally
      {
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testReceiveWithCommit() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientSession session = cf.createSession(false, false, false);
         sendSession.createQueue(addressA, queueA, false);
         ClientProducer cp = sendSession.createProducer(addressA);
         ClientConsumer cc = session.createConsumer(queueA);
         int numMessages = 100;
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(sendSession.createClientMessage(false));
         }
         session.start();
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage cm = cc.receive(5000);
            assertNotNull(cm);
            cm.acknowledge();
         }
         Queue q = (Queue) messagingService.getServer().getPostOffice().getBinding(queueA).getBindable();
         assertEquals(numMessages, q.getDeliveringCount());
         session.commit();
         assertEquals(0, q.getDeliveringCount());
         session.close();
      }
      finally
      {
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testReceiveWithRollback() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientSession session = cf.createSession(false, false, false);
         sendSession.createQueue(addressA, queueA, false);
         ClientProducer cp = sendSession.createProducer(addressA);
         ClientConsumer cc = session.createConsumer(queueA);
         int numMessages = 100;
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(sendSession.createClientMessage(false));
         }
         session.start();
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage cm = cc.receive(5000);
            assertNotNull(cm);
            cm.acknowledge();
         }
         Queue q = (Queue) messagingService.getServer().getPostOffice().getBinding(queueA).getBindable();
         assertEquals(numMessages, q.getDeliveringCount());
         session.rollback();
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage cm = cc.receive(5000);
            assertNotNull(cm);
            cm.acknowledge();
         }
         assertEquals(numMessages, q.getDeliveringCount());
         session.close();
         sendSession.close();
      }
      finally
      {
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testReceiveAckLastMessageOnly() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setAckBatchSize(0);
         cf.setBlockOnAcknowledge(true);
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientSession session = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, false);
         ClientProducer cp = sendSession.createProducer(addressA);
         ClientConsumer cc = session.createConsumer(queueA);
         int numMessages = 100;
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(sendSession.createClientMessage(false));
         }
         session.start();
         ClientMessage cm = null;
         for (int i = 0; i < numMessages; i++)
         {
            cm = cc.receive(5000);
            assertNotNull(cm);
         }
         cm.acknowledge();
         Queue q = (Queue) messagingService.getServer().getPostOffice().getBinding(queueA).getBindable();

         assertEquals(0, q.getDeliveringCount());
         session.close();
         sendSession.close();
      }
      finally
      {
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testAsyncConsumerNoAck() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientSession session = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, false);
         ClientProducer cp = sendSession.createProducer(addressA);
         ClientConsumer cc = session.createConsumer(queueA);
         int numMessages = 100;
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(sendSession.createClientMessage(false));
         }
         final CountDownLatch latch = new CountDownLatch(numMessages);
         session.start();
         cc.setMessageHandler(new MessageHandler()
         {
            public void onMessage(ClientMessage message)
            {
               latch.countDown();
            }
         });
         assertTrue(latch.await(5, TimeUnit.SECONDS));
         Queue q = (Queue) messagingService.getServer().getPostOffice().getBinding(queueA).getBindable();
         assertEquals(numMessages, q.getDeliveringCount());
         sendSession.close();
         session.close();
      }
      finally
      {
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testAsyncConsumerAck() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setBlockOnAcknowledge(true);
         cf.setAckBatchSize(0);
         ClientSession sendSession = cf.createSession(false, true, true);
         final ClientSession session = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, false);
         ClientProducer cp = sendSession.createProducer(addressA);
         ClientConsumer cc = session.createConsumer(queueA);
         int numMessages = 100;
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(sendSession.createClientMessage(false));
         }
         final CountDownLatch latch = new CountDownLatch(numMessages);
         session.start();
         cc.setMessageHandler(new MessageHandler()
         {
            public void onMessage(ClientMessage message)
            {
               try
               {
                  message.acknowledge();
               }
               catch (MessagingException e)
               {
                  try
                  {
                     session.close();
                  }
                  catch (MessagingException e1)
                  {
                     e1.printStackTrace();
                  }
               }
               latch.countDown();
            }
         });
         assertTrue(latch.await(5, TimeUnit.SECONDS));
         Queue q = (Queue) messagingService.getServer().getPostOffice().getBinding(queueA).getBindable();
         assertEquals(0, q.getDeliveringCount());
         sendSession.close();
         session.close();
      }
      finally
      {
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testAsyncConsumerAckLastMessageOnly() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setBlockOnAcknowledge(true);
         cf.setAckBatchSize(0);
         ClientSession sendSession = cf.createSession(false, true, true);
         final ClientSession session = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, false);
         ClientProducer cp = sendSession.createProducer(addressA);
         ClientConsumer cc = session.createConsumer(queueA);
         int numMessages = 100;
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(sendSession.createClientMessage(false));
         }
         final CountDownLatch latch = new CountDownLatch(numMessages);
         session.start();
         cc.setMessageHandler(new MessageHandler()
         {
            public void onMessage(ClientMessage message)
            {
               if (latch.getCount() == 1)
               {
                  try
                  {
                     message.acknowledge();
                  }
                  catch (MessagingException e)
                  {
                     try
                     {
                        session.close();
                     }
                     catch (MessagingException e1)
                     {
                        e1.printStackTrace();
                     }
                  }
               }
               latch.countDown();
            }
         });
         assertTrue(latch.await(5, TimeUnit.SECONDS));
         Queue q = (Queue) messagingService.getServer().getPostOffice().getBinding(queueA).getBindable();
         assertEquals(0, q.getDeliveringCount());
         sendSession.close();
         session.close();
      }
      finally
      {
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testAsyncConsumerCommit() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setBlockOnAcknowledge(true);
         cf.setAckBatchSize(0);
         ClientSession sendSession = cf.createSession(false, true, true);
         final ClientSession session = cf.createSession(false, true, false);
         sendSession.createQueue(addressA, queueA, false);
         ClientProducer cp = sendSession.createProducer(addressA);
         ClientConsumer cc = session.createConsumer(queueA);
         int numMessages = 100;
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(sendSession.createClientMessage(false));
         }
         final CountDownLatch latch = new CountDownLatch(numMessages);
         session.start();
         cc.setMessageHandler(new MessageHandler()
         {
            public void onMessage(ClientMessage message)
            {
               try
               {
                  message.acknowledge();
               }
               catch (MessagingException e)
               {
                  try
                  {
                     session.close();
                  }
                  catch (MessagingException e1)
                  {
                     e1.printStackTrace();
                  }
               }
               latch.countDown();
            }
         });
         assertTrue(latch.await(5, TimeUnit.SECONDS));
         Queue q = (Queue) messagingService.getServer().getPostOffice().getBinding(queueA).getBindable();
         assertEquals(numMessages, q.getDeliveringCount());
         assertEquals(numMessages, q.getMessageCount());
         session.commit();
         assertEquals(0, q.getDeliveringCount());
         assertEquals(0, q.getMessageCount());
         sendSession.close();
         session.close();
      }
      finally
      {
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testAsyncConsumerRollback() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         ClientSessionFactory cf = createInVMFactory();
         cf.setBlockOnAcknowledge(true);
         cf.setAckBatchSize(0);
         ClientSession sendSession = cf.createSession(false, true, true);
         final ClientSession session = cf.createSession(false, true, false);
         sendSession.createQueue(addressA, queueA, false);
         ClientProducer cp = sendSession.createProducer(addressA);
         ClientConsumer cc = session.createConsumer(queueA);
         int numMessages = 100;
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(sendSession.createClientMessage(false));
         }
         CountDownLatch latch = new CountDownLatch(numMessages);
         session.start();
         cc.setMessageHandler(new ackHandler(session, latch));
         assertTrue(latch.await(5, TimeUnit.SECONDS));
         Queue q = (Queue) messagingService.getServer().getPostOffice().getBinding(queueA).getBindable();
         assertEquals(numMessages, q.getDeliveringCount());
         assertEquals(numMessages, q.getMessageCount());
         session.rollback();
         assertEquals(0, q.getDeliveringCount());
         assertEquals(numMessages, q.getMessageCount());
         latch = new CountDownLatch(numMessages);
         cc.setMessageHandler(new ackHandler(session, latch));
         assertTrue(latch.await(5, TimeUnit.SECONDS));
         sendSession.close();
         session.close();
      }
      finally
      {
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testSendDeliveryOrderOnCommit() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession sendSession = cf.createSession(false, false, true);
         ClientProducer cp = sendSession.createProducer(addressA);
         int numMessages = 1000;
         sendSession.createQueue(addressA, queueA, false);
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage cm = sendSession.createClientMessage(false);
            cm.getBody().writeInt(i);
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
            assertNotNull(cm);
            assertEquals(i, cm.getBody().readInt());
         }
      }
      finally
      {
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testReceiveDeliveryOrderOnRollback() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession sendSession = cf.createSession(false, true, false);
         ClientProducer cp = sendSession.createProducer(addressA);
         int numMessages = 1000;
         sendSession.createQueue(addressA, queueA, false);
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage cm = sendSession.createClientMessage(false);
            cm.getBody().writeInt(i);
            cp.send(cm);
         }
         ClientConsumer c = sendSession.createConsumer(queueA);
         sendSession.start();
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage cm = c.receive(5000);
            assertNotNull(cm);
            cm.acknowledge();
            assertEquals(i, cm.getBody().readInt());
         }
         sendSession.rollback();
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage cm = c.receive(5000);
            assertNotNull(cm);
            cm.acknowledge();
            assertEquals(i, cm.getBody().readInt());
         }
         sendSession.close();
      }
      finally
      {
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }

   public void testMultipleConsumersMessageOrder() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientSession recSession = cf.createSession(false, true, true);
         sendSession.createQueue(addressA, queueA, false);
         int numReceivers = 100;
         AtomicInteger count = new AtomicInteger(0);
         int numMessage = 10000;
         ClientConsumer[] clientConsumers = new ClientConsumer[numReceivers];
         Receiver[] receivers = new Receiver[numReceivers];
         CountDownLatch latch = new CountDownLatch(numMessage);
         for(int i = 0; i < numReceivers; i++)
         {
            clientConsumers[i] = recSession.createConsumer(queueA);
            receivers[i] = new Receiver(latch);
            clientConsumers[i].setMessageHandler(receivers[i]);
         }
         recSession.start();
         ClientProducer clientProducer = sendSession.createProducer(addressA);
         for(int i = 0; i < numMessage; i++)
         {
            ClientMessage cm = sendSession.createClientMessage(false);
            cm.getBody().writeInt(count.getAndIncrement());
            clientProducer.send(cm);   
         }
         assertTrue(latch.await(10, TimeUnit.SECONDS));
         for (Receiver receiver : receivers)
         {
            assertFalse("" + receiver.lastMessage, receiver.failed);
         }
         sendSession.close();
         recSession.close();
      }
      finally
      {
         if (messagingService.isStarted())
         {
            messagingService.stop();
         }
      }
   }


   class Receiver implements MessageHandler
   {
      final CountDownLatch latch;
      int lastMessage = -1;
      boolean failed = false;
      public Receiver(CountDownLatch latch)
      {
         this.latch = latch;
      }

      public void onMessage(ClientMessage message)
      {
         int i = message.getBody().readInt();
         try
         {
            message.acknowledge();
         }
         catch (MessagingException e)
         {
            e.printStackTrace();
         }
         if( i <= lastMessage)
         {
            failed = true;
         }
         lastMessage = i;
         latch.countDown();
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
   private static class ackHandler implements MessageHandler
   {
      private final ClientSession session;

      private final CountDownLatch latch;

      public ackHandler(ClientSession session, CountDownLatch latch)
      {
         this.session = session;
         this.latch = latch;
      }

      public void onMessage(ClientMessage message)
      {
         try
         {
            message.acknowledge();
         }
         catch (MessagingException e)
         {
            try
            {
               session.close();
            }
            catch (MessagingException e1)
            {
               e1.printStackTrace();
            }
         }
         latch.countDown();
      }
   }
}
