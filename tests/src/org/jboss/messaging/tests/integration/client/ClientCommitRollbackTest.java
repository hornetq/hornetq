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
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ClientCommitRollbackTest extends ServiceTestBase
{
   public final SimpleString addressA = new SimpleString("addressA");

   public final SimpleString addressB = new SimpleString("addressB");

   public final SimpleString queueA = new SimpleString("queueA");

   public final SimpleString queueB = new SimpleString("queueB");

   public final SimpleString queueC = new SimpleString("queueC");


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

   /*public void testReceiveWithRollbackMultipleConsumersDifferentQueues() throws Exception
   {
      MessagingService messagingService = createService(false);
      try
      {
         messagingService.start();
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
         Queue q = (Queue) messagingService.getServer().getPostOffice().getBinding(queueA).getBindable();
         Queue q2 = (Queue) messagingService.getServer().getPostOffice().getBinding(queueB).getBindable();
         assertEquals(numMessages, q.getDeliveringCount());
         session.rollback();
         assertEquals(numMessages, q2.getDeliveringCount());
         assertEquals(numMessages, q.getMessageCount());
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
   }*/

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
         Queue q = (Queue)messagingService.getServer().getPostOffice().getBinding(queueA).getBindable();
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
      }
      finally
      {
         if (messagingService.isStarted())
         {
            messagingService.stop();
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
