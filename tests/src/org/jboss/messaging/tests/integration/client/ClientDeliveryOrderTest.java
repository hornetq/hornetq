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
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ClientDeliveryOrderTest extends ServiceTestBase
{
   public final SimpleString addressA = new SimpleString("addressA");

      public final SimpleString queueA = new SimpleString("queueA");

      public final SimpleString queueB = new SimpleString("queueB");

      public final SimpleString queueC = new SimpleString("queueC");



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
            if (i <= lastMessage)
            {
               failed = true;
            }
            lastMessage = i;
            latch.countDown();
         }

      }
     
}
