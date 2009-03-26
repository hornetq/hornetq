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
import org.jboss.messaging.core.client.impl.ClientConsumerImpl;
import org.jboss.messaging.core.client.impl.ClientConsumerInternal;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ClientConsumerWindowSizeTest extends ServiceTestBase
{
   public final SimpleString addressA = new SimpleString("addressA");

   public final SimpleString queueA = new SimpleString("queueA");

   public final SimpleString queueB = new SimpleString("queueB");

   public final SimpleString queueC = new SimpleString("queueC");

   private final SimpleString groupTestQ = new SimpleString("testGroupQueue");

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
         Queue q = (Queue)messagingService.getServer().getPostOffice().getBinding(queueA).getBindable();
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

   public void testSlowConsumer() throws Exception
   {
      MessagingService service = createService(false);

      ClientSession sessionNotUsed = null;
      ClientSession session = null;

      try
      {
         final int numberOfMessages = 100;

         service.start();

         ClientSessionFactory sf = createInVMFactory();
         sf.setConsumerWindowSize(1);

         session = sf.createSession(false, true, true);

         SimpleString ADDRESS = new SimpleString("some-queue");

         session.createQueue(ADDRESS, ADDRESS, true);

         sessionNotUsed = sf.createSession(false, true, true);
         sessionNotUsed.start();

         session.start();

         ClientConsumer consNeverUsed = sessionNotUsed.createConsumer(ADDRESS);

         ClientConsumer cons1 = session.createConsumer(ADDRESS);

         ClientProducer prod = session.createProducer(ADDRESS);

         for (int i = 0; i < numberOfMessages; i++)
         {
            prod.send(createTextMessage(session, "Msg" + i));
         }

         session.commit();

         for (int i = 0; i < numberOfMessages -1; i++)
         {
            ClientMessage msg = cons1.receive(1000);
            assertNotNull("expected message at i = " + i, msg);
            msg.acknowledge();
         }
         
         ClientMessage msg = consNeverUsed.receive(500);
         assertNotNull(msg);
         msg.acknowledge();
         sessionNotUsed.commit();

         session.commit();

         assertEquals(0, getMessageCount(service, ADDRESS.toString()));

      }
      finally
      {
         try
         {
            if (session != null)
               session.close();
            if (sessionNotUsed != null)
               sessionNotUsed.close();
         }
         catch (Exception ignored)
         {
         }

         if (service.isStarted())
         {
            service.stop();
         }
      }
   }

   public void testDeleteme() throws Exception
   {
      MessagingService service = createService(false);

      ClientSession sessionNotUsed = null;
      ClientSession session = null;

      try
      {
         final int numberOfMessages = 100;

         service.start();

         ClientSessionFactory sf = createInVMFactory();
         sf.setConsumerWindowSize(1);

         session = sf.createSession(false, true, true);

         SimpleString ADDRESS = new SimpleString("some-queue");

         session.createQueue(ADDRESS, ADDRESS, true);

         sessionNotUsed = sf.createSession(false, true, true);
         sessionNotUsed.start();

         session.start();

         ClientConsumerInternal consNeverUsed = (ClientConsumerInternal)sessionNotUsed.createConsumer(ADDRESS);
         // ClientConsumer consNeverUsed = sessionNotUsed.createConsumer(ADDRESS, null, -1, -1, false);

         ClientConsumerInternal cons1 = (ClientConsumerInternal)session.createConsumer(ADDRESS);

         ClientProducer prod = session.createProducer(ADDRESS);
         
         for (int i = 0; i < numberOfMessages; i++)
         {
            prod.send(createTextMessage(session, "Msg" + i));
         }
         
         session.commit();
         
         Thread.sleep(1000);
         
         for (int i = 0; i < numberOfMessages; i++)
         {
            System.out.println("cons1:" + cons1.getBufferSize());
            System.out.println("consNeverUsed:" + consNeverUsed.getBufferSize());

            ClientMessage msg = cons1.receive(1000);
            assertNotNull("expected message at i = " + i, msg);
            msg.acknowledge();
         }

         session.commit();

         // assertEquals(0, getMessageCount(service, ADDRESS.toString()));

      }
      finally
      {
         try
         {
            if (session != null)
               session.close();
            if (sessionNotUsed != null)
               sessionNotUsed.close();
         }
         catch (Exception ignored)
         {
         }

         if (service.isStarted())
         {
            service.stop();
         }
      }
   }
}
