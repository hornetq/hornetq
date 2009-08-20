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
import org.hornetq.core.client.MessageHandler;
import org.hornetq.core.exception.MessagingException;
import org.hornetq.core.server.MessagingServer;
import org.hornetq.core.server.Queue;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class CommitRollbackTest extends ServiceTestBase
{
   public final SimpleString addressA = new SimpleString("addressA");

   public final SimpleString addressB = new SimpleString("addressB");

   public final SimpleString queueA = new SimpleString("queueA");

   public final SimpleString queueB = new SimpleString("queueB");

   public final SimpleString queueC = new SimpleString("queueC");


   public void testReceiveWithCommit() throws Exception
   {
      MessagingServer server = createServer(false);
      try
      {
         server.start();
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
         Queue q = (Queue) server.getPostOffice().getBinding(queueA).getBindable();
         assertEquals(numMessages, q.getDeliveringCount());
         session.commit();
         assertEquals(0, q.getDeliveringCount());
         session.close();
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

   public void testReceiveWithRollback() throws Exception
   {
      MessagingServer server = createServer(false);
      try
      {
         server.start();
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
         Queue q = (Queue) server.getPostOffice().getBinding(queueA).getBindable();
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
         cf.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testReceiveWithRollbackMultipleConsumersDifferentQueues() throws Exception
   {
      MessagingServer server = createServer(false);
      try
      {
         server.start();
         ClientSessionFactory cf = createInVMFactory();
         ClientSession sendSession = cf.createSession(false, true, true);
         ClientSession session = cf.createSession(false, false, false);
         sendSession.createQueue(addressA, queueA, false);
         sendSession.createQueue(addressB, queueB, false);
         ClientProducer cp = sendSession.createProducer(addressA);
         ClientProducer cp2 = sendSession.createProducer(addressB);
         ClientConsumer cc = session.createConsumer(queueA);
         ClientConsumer cc2 = session.createConsumer(queueB);
         int numMessages = 100;
         for (int i = 0; i < numMessages; i++)
         {
            cp.send(sendSession.createClientMessage(false));
            cp2.send(sendSession.createClientMessage(false));
         }
         session.start();
         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage cm = cc.receive(5000);
            assertNotNull(cm);
            cm.acknowledge();
            cm = cc2.receive(5000);
            assertNotNull(cm);
            cm.acknowledge();
         }
         Queue q = (Queue) server.getPostOffice().getBinding(queueA).getBindable();
         Queue q2 = (Queue) server.getPostOffice().getBinding(queueB).getBindable();
         assertEquals(numMessages, q.getDeliveringCount());
         cc.close();
         cc2.close();
         session.rollback();
         assertEquals(0, q2.getDeliveringCount());
         assertEquals(numMessages, q.getMessageCount());
         assertEquals(0, q2.getDeliveringCount());
         assertEquals(numMessages, q.getMessageCount());
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

   public void testAsyncConsumerCommit() throws Exception
   {
      MessagingServer server = createServer(false);
      try
      {
         server.start();
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
         Queue q = (Queue)server.getPostOffice().getBinding(queueA).getBindable();
         assertEquals(numMessages, q.getDeliveringCount());
         assertEquals(numMessages, q.getMessageCount());
         session.commit();
         assertEquals(0, q.getDeliveringCount());
         assertEquals(0, q.getMessageCount());
         sendSession.close();
         session.close();
         cf.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
      }
   }

   public void testAsyncConsumerRollback() throws Exception
   {
      MessagingServer server = createServer(false);
      try
      {
         server.start();
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
         Queue q = (Queue) server.getPostOffice().getBinding(queueA).getBindable();
         assertEquals(numMessages, q.getDeliveringCount());
         assertEquals(numMessages, q.getMessageCount());
         session.stop();
         session.rollback();
         assertEquals(0, q.getDeliveringCount());
         assertEquals(numMessages, q.getMessageCount());
         latch = new CountDownLatch(numMessages);
         cc.setMessageHandler(new ackHandler(session, latch));
         session.start();
         assertTrue(latch.await(5, TimeUnit.SECONDS));
         sendSession.close();
         session.close();
         cf.close();
      }
      finally
      {
         if (server.isStarted())
         {
            server.stop();
         }
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
