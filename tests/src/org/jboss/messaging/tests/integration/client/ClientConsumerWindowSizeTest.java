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

         for (int i = 0; i < numberOfMessages - 1; i++)
         {
            ClientMessage msg = cons1.receive(1000);
            assertNotNull("expected message at i = " + i, msg);
            msg.acknowledge();
         }

         ClientMessage msg = consNeverUsed.receive(500);
         assertNotNull(msg);
         msg.acknowledge();
         
         session.close();
         session = null;
         
         sessionNotUsed.close();
         sessionNotUsed = null;

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

   // A better slow consumer test
   
   //Commented out until behaviour is fixed
//   public void testSlowConsumer2() throws Exception
//   {
//      MessagingService service = createService(false);
//
//      ClientSession session1 = null;
//      ClientSession session2 = null;
//
//      try
//      {
//         final int numberOfMessages = 100;
//
//         service.start();
//
//         ClientSessionFactory sf = createInVMFactory();
//
//         sf.setConsumerWindowSize(1);
//
//         session1 = sf.createSession(false, true, true);
//
//         session2 = sf.createSession(false, true, true);
//         
//         session1.start();
//         
//         session2.start();
//
//         SimpleString ADDRESS = new SimpleString("some-queue");
//
//         session1.createQueue(ADDRESS, ADDRESS, true);
//
//         ClientConsumer cons1 = session1.createConsumer(ADDRESS);
//         
//         //Note we make sure we send the messages *before* cons2 is created
//         
//         ClientProducer prod = session1.createProducer(ADDRESS);
//
//         for (int i = 0; i < numberOfMessages; i++)
//         {
//            prod.send(createTextMessage(session1, "Msg" + i));
//         }
//
//         ClientConsumer cons2 = session2.createConsumer(ADDRESS);
//         
//         for (int i = 0; i < numberOfMessages; i += 2)
//         {
//            ClientMessage msg = cons1.receive(1000);
//            assertNotNull("expected message at i = " + i, msg);
//
//            //assertEquals("Msg" + i, msg.getBody().readString());
//
//            msg.acknowledge();
//         }
//
//         for (int i = 1; i < numberOfMessages; i += 2)
//         {
//            ClientMessage msg = cons2.receive(1000);
//
//            assertNotNull("expected message at i = " + i, msg);
//
//            assertEquals("Msg" + i, msg.getBody().readString());
//
//            msg.acknowledge();
//         }
//
//         assertEquals(0, getMessageCount(service, ADDRESS.toString()));
//         
//         //This should also work the other way around
//         
//         cons1.close();
//         
//         cons2.close();
//         
//         cons1 = session1.createConsumer(ADDRESS);
//         
//         //Note we make sure we send the messages *before* cons2 is created
//         
//         for (int i = 0; i < numberOfMessages; i++)
//         {
//            prod.send(createTextMessage(session1, "Msg" + i));
//         }
//
//         cons2 = session2.createConsumer(ADDRESS);
//         
//         //Now we receive on cons2 first
//         
//         for (int i = 0; i < numberOfMessages; i += 2)
//         {
//            ClientMessage msg = cons2.receive(1000);
//            assertNotNull("expected message at i = " + i, msg);
//
//            assertEquals("Msg" + i, msg.getBody().readString());
//
//            msg.acknowledge();
//         }
//
//         for (int i = 1; i < numberOfMessages; i += 2)
//         {
//            ClientMessage msg = cons1.receive(1000);
//
//            assertNotNull("expected message at i = " + i, msg);
//
//            assertEquals("Msg" + i, msg.getBody().readString());
//
//            msg.acknowledge();
//         }
//
//         assertEquals(0, getMessageCount(service, ADDRESS.toString()));
//         
//         
//      }
//      finally
//      {
//         try
//         {
//            if (session1 != null)
//               session1.close();
//            if (session2 != null)
//               session2.close();
//         }
//         catch (Exception ignored)
//         {
//         }
//
//         if (service.isStarted())
//         {
//            service.stop();
//         }
//      }
//   }

}
