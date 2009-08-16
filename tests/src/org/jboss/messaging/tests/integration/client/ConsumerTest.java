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
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Interceptor;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.utils.SimpleString;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class ConsumerTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(ConsumerTest.class);

   private MessagingServer server;

   private final SimpleString QUEUE = new SimpleString("ConsumerTestQueue");

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      server = createServer(false);

      server.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      server.stop();

      server = null;

      super.tearDown();
   }

   
  
   public void testConsumerAckImmediateAutoCommitTrue() throws Exception
   {
      ClientSessionFactory sf = createInVMFactory();

      ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(1000);

         assertEquals("m" + i, message2.getBody().readString());
      }
      // assert that all the messages are there and none have been acked
      assertEquals(0,
                   ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      assertEquals(0,
                   ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getMessageCount());

      session.close();
   }

   public void testConsumerAckImmediateAutoCommitFalse() throws Exception
   {

      ClientSessionFactory sf = createInVMFactory();

      ClientSession session = sf.createSession(false, true, false, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(1000);

         assertEquals("m" + i, message2.getBody().readString());
      }
      // assert that all the messages are there and none have been acked
      assertEquals(0,
                   ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      assertEquals(0,
                   ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getMessageCount());

      session.close();
   }

   public void testConsumerAckImmediateAckIgnored() throws Exception
   {

      ClientSessionFactory sf = createInVMFactory();

      ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(1000);

         assertEquals("m" + i, message2.getBody().readString());
         if (i < 50)
         {
            message2.acknowledge();
         }
      }
      // assert that all the messages are there and none have been acked
      assertEquals(0,
                   ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      assertEquals(0,
                   ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getMessageCount());

      session.close();
   }

   public void testConsumerAckImmediateCloseSession() throws Exception
   {

      ClientSessionFactory sf = createInVMFactory();

      ClientSession session = sf.createSession(false, true, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive(1000);

         assertEquals("m" + i, message2.getBody().readString());
         if (i < 50)
         {
            message2.acknowledge();
         }
      }
      // assert that all the messages are there and none have been acked
      assertEquals(0,
                   ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      assertEquals(0,
                   ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getMessageCount());

      session.close();

      assertEquals(0,
                   ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      assertEquals(0,
                   ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getMessageCount());
   }

   public void testAcksWithSmallSendWindow() throws Exception
   {
      ClientSessionFactory sf = createInVMFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 10000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         producer.send(message);
      }
      session.close();
      sf.close();
      final CountDownLatch latch = new CountDownLatch(numMessages);
      server.getRemotingService().addInterceptor(new Interceptor()
      {
         public boolean intercept(Packet packet, RemotingConnection connection) throws MessagingException
         {
            if(packet.getType() == PacketImpl.SESS_ACKNOWLEDGE)
            {
               latch.countDown();
            }
            return true;
         }
      });
      ClientSessionFactory sfReceive = createInVMFactory();
      sfReceive.setProducerWindowSize(100);
      sfReceive.setAckBatchSize(-1);
      ClientSession sessionRec = sfReceive.createSession(false, true, true);
      ClientConsumer consumer = sessionRec.createConsumer(QUEUE);
      consumer.setMessageHandler(new MessageHandler()
      {
         public void onMessage(ClientMessage message)
         {
            try
            {
               message.acknowledge();
            }
            catch (MessagingException e)
            {
               e.printStackTrace();
            }
         }
      });
      sessionRec.start();
      assertTrue(latch.await(5, TimeUnit.SECONDS));
      sessionRec.close();
   }

}
