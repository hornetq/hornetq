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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.*;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class ConsumerTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(ConsumerTest.class);

   private HornetQServer server;

   private final SimpleString QUEUE = new SimpleString("ConsumerTestQueue");

   private ServerLocator locator;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      server = createServer(false);

      server.start();

      locator = createInVMNonHALocator();
   }

   @Override
   protected void tearDown() throws Exception
   {
      locator.close();

      server.stop();

      server = null;

      super.tearDown();
   }
   
   
   public void testConsumerAckImmediateAutoCommitTrue() throws Exception
   {
      ClientSessionFactory sf = locator.createSessionFactory();

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

         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
      }
      // assert that all the messages are there and none have been acked
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getMessageCount());

      session.close();
   }

   public void testConsumerAckImmediateAutoCommitFalse() throws Exception
   {

      ClientSessionFactory sf = locator.createSessionFactory();

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

         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
      }
      // assert that all the messages are there and none have been acked
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getMessageCount());

      session.close();
   }

   public void testConsumerAckImmediateAckIgnored() throws Exception
   {

      ClientSessionFactory sf = locator.createSessionFactory();

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

         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
         if (i < 50)
         {
            message2.acknowledge();
         }
      }
      // assert that all the messages are there and none have been acked
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getMessageCount());

      session.close();
   }

   public void testConsumerAckImmediateCloseSession() throws Exception
   {

      ClientSessionFactory sf = locator.createSessionFactory();

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

         Assert.assertEquals("m" + i, message2.getBodyBuffer().readString());
         if (i < 50)
         {
            message2.acknowledge();
         }
      }
      // assert that all the messages are there and none have been acked
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getMessageCount());

      session.close();

      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(QUEUE).getBindable()).getMessageCount());
   }

   public void testAcksWithSmallSendWindow() throws Exception
   {
      ClientSessionFactory sf = locator.createSessionFactory();

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
         public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
         {
            if (packet.getType() == PacketImpl.SESS_ACKNOWLEDGE)
            {
               latch.countDown();
            }
            return true;
         }
      });
      ServerLocator locator = createInVMNonHALocator();
      locator.setConfirmationWindowSize(100);
      locator.setAckBatchSize(-1);
      ClientSessionFactory sfReceive = locator.createSessionFactory();
      ClientSession sessionRec = sfReceive.createSession(false, true, true);
      ClientConsumer consumer = sessionRec.createConsumer(QUEUE);
      consumer.setMessageHandler(new MessageHandler()
      {
         public void onMessage(final ClientMessage message)
         {
            try
            {
               message.acknowledge();
            }
            catch (HornetQException e)
            {
               e.printStackTrace();
            }
         }
      });
      sessionRec.start();
      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));
      sessionRec.close();
      locator.close();
   }
   
   // https://jira.jboss.org/browse/HORNETQ-410
   public void testConsumeWithNoConsumerFlowControl() throws Exception
   {
      
      ServerLocator locator = createInVMNonHALocator();
      
      locator.setConsumerWindowSize(-1);
      
      ClientSessionFactory sf = locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);
      
      session.start();

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = createTextMessage("m" + i, session);
         producer.send(message);
      }
            
      ClientConsumer consumer = session.createConsumer(QUEUE);
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(10000);
         assertNotNull(message);
         message.acknowledge();
      }
      
      session.close();
      sf.close();
      locator.close();
      
   }

   public void testClearListener() throws Exception
   {
      ClientSessionFactory sf = locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientConsumer consumer = session.createConsumer(QUEUE);

      consumer.setMessageHandler(new MessageHandler()
      {
         public void onMessage(final ClientMessage msg)
         {
         }
      });

      consumer.setMessageHandler(null);
      consumer.receiveImmediate();

      session.close();
   }

   public void testNoReceiveWithListener() throws Exception
   {
      ClientSessionFactory sf = locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientConsumer consumer = session.createConsumer(QUEUE);

      consumer.setMessageHandler(new MessageHandler()
      {
         public void onMessage(final ClientMessage msg)
         {
         }
      });

      try
      {
         consumer.receiveImmediate();
         Assert.fail("Should throw exception");
      }
      catch (HornetQException me)
      {
         if (me.getCode() == HornetQException.ILLEGAL_STATE)
         {
            // Ok
         }
         else
         {
            Assert.fail("Wrong exception code");
         }
      }

      session.close();
   }

   // https://jira.jboss.org/jira/browse/HORNETQ-111
   // Test that, on rollback credits are released for messages cleared in the buffer
   public void testConsumerCreditsOnRollback() throws Exception
   {
      locator.setConsumerWindowSize(10000);

      ClientSessionFactory sf = locator.createSessionFactory();

      ClientSession session = sf.createTransactedSession();

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      final byte[] bytes = new byte[1000];

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         message.getBodyBuffer().writeBytes(bytes);

         message.putIntProperty("count", i);

         producer.send(message);
      }

      session.commit();

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();

      int commited = 0;
      int rollbacked = 0;
      for (int i = 0; i < 110; i++)
      {
         ClientMessage message = consumer.receive();

         int count = message.getIntProperty("count");

         boolean redelivered = message.getDeliveryCount() > 1;

         if (count % 2 == 0 && !redelivered)
         {
            session.rollback();
            rollbacked++;
         }
         else
         {
            session.commit();
            commited++;
         }
      }

      session.close();
   }

   // https://jira.jboss.org/jira/browse/HORNETQ-111
   // Test that, on rollback credits are released for messages cleared in the buffer
   public void testConsumerCreditsOnRollbackLargeMessages() throws Exception
   {

      locator.setConsumerWindowSize(10000);
      locator.setMinLargeMessageSize(1000);

      ClientSessionFactory sf = locator.createSessionFactory();

      ClientSession session = sf.createTransactedSession();

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      final byte[] bytes = new byte[10000];

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(false);

         message.getBodyBuffer().writeBytes(bytes);

         message.putIntProperty("count", i);

         producer.send(message);
      }

      session.commit();

      ClientConsumer consumer = session.createConsumer(QUEUE);
      session.start();

      int commited = 0;
      int rollbacked = 0;
      for (int i = 0; i < 110; i++)
      {
         ClientMessage message = consumer.receive();

         int count = message.getIntProperty("count");

         boolean redelivered = message.getDeliveryCount() > 1;

         if (count % 2 == 0 && !redelivered)
         {
            session.rollback();
            rollbacked++;
         }
         else
         {
            session.commit();
            commited++;
         }
      }

      session.close();
   }

}
