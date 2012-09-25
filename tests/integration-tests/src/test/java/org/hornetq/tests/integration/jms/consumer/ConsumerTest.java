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
package org.hornetq.tests.integration.jms.consumer;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.HornetQJMSConstants;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.server.Queue;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.util.JMSTestBase;
import org.hornetq.utils.ReusableLatch;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ConsumerTest extends JMSTestBase
{
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private static final String Q_NAME = "ConsumerTestQueue";

   private javax.jms.Queue jBossQueue;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      jmsServer.createQueue(false, ConsumerTest.Q_NAME, null, true, ConsumerTest.Q_NAME);
      cf = (ConnectionFactory)HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));
   }

   @Override
   protected void tearDown() throws Exception
   {
      cf = null;

      super.tearDown();
   }

   public void testPreCommitAcks() throws Exception
   {
      conn = cf.createConnection();
      Session session = conn.createSession(false, HornetQJMSConstants.PRE_ACKNOWLEDGE);
      jBossQueue = HornetQJMSClient.createQueue(ConsumerTest.Q_NAME);
      MessageProducer producer = session.createProducer(jBossQueue);
      MessageConsumer consumer = session.createConsumer(jBossQueue);
      int noOfMessages = 100;
      for (int i = 0; i < noOfMessages; i++)
      {
         producer.send(session.createTextMessage("m" + i));
      }

      conn.start();
      for (int i = 0; i < noOfMessages; i++)
      {
         Message m = consumer.receive(500);
         Assert.assertNotNull(m);
      }

      SimpleString queueName = new SimpleString(HornetQDestination.JMS_QUEUE_ADDRESS_PREFIX + ConsumerTest.Q_NAME);
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(queueName).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(queueName).getBindable()).getMessageCount());
   }

   public void testIndividualACK() throws Exception
   {
      Connection conn = cf.createConnection();
      Session session = conn.createSession(false, HornetQJMSConstants.INDIVIDUAL_ACKNOWLEDGE);
      jBossQueue = HornetQJMSClient.createQueue(ConsumerTest.Q_NAME);
      MessageProducer producer = session.createProducer(jBossQueue);
      MessageConsumer consumer = session.createConsumer(jBossQueue);
      int noOfMessages = 100;
      for (int i = 0; i < noOfMessages; i++)
      {
         producer.send(session.createTextMessage("m" + i));
      }

      conn.start();

      // Consume even numbers first
      for (int i = 0; i < noOfMessages; i++)
      {
         Message m = consumer.receive(500);
         Assert.assertNotNull(m);
         if (i % 2 == 0)
         {
            m.acknowledge();
         }
      }

      session.close();

      session = conn.createSession(false, HornetQJMSConstants.INDIVIDUAL_ACKNOWLEDGE);

      consumer = session.createConsumer(jBossQueue);

      // Consume odd numbers first
      for (int i = 0; i < noOfMessages; i++)
      {
         if (i % 2 == 0)
         {
            continue;
         }

         TextMessage m = (TextMessage) consumer.receive(1000);
         assertNotNull(m);
         m.acknowledge();
         assertEquals("m" + i, m.getText());
      }

      SimpleString queueName = new SimpleString(HornetQDestination.JMS_QUEUE_ADDRESS_PREFIX + ConsumerTest.Q_NAME);
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(queueName).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(queueName).getBindable()).getMessageCount());
      conn.close();
   }

   public void testIndividualACKMessageConsumer() throws Exception
   {
      Connection conn = cf.createConnection();
      Session session = conn.createSession(false, HornetQJMSConstants.INDIVIDUAL_ACKNOWLEDGE);
      jBossQueue = HornetQJMSClient.createQueue(ConsumerTest.Q_NAME);
      MessageProducer producer = session.createProducer(jBossQueue);
      MessageConsumer consumer = session.createConsumer(jBossQueue);
      int noOfMessages = 100;
      for (int i = 0; i < noOfMessages; i++)
      {
         producer.setPriority(2);
         producer.send(session.createTextMessage("m" + i));
      }

      conn.start();

      final AtomicInteger errors = new AtomicInteger(0);
      final ReusableLatch latch = new ReusableLatch();
      latch.setCount(noOfMessages);

      class MessageAckEven implements MessageListener
      {
         int count = 0;

         public void onMessage(Message msg)
         {
            try
            {
               TextMessage txtmsg = (TextMessage)msg;
               if (!txtmsg.getText().equals("m" + count))
               {

                  errors.incrementAndGet();
               }

               if (count % 2 == 0)
               {
                  msg.acknowledge();
               }

               count ++;
            }
            catch (Exception e)
            {
               errors.incrementAndGet();
            }
            finally
            {
               latch.countDown();
            }
         }

      }

      consumer.setMessageListener(new MessageAckEven());

      assertTrue(latch.await(5000));

      session.close();

      session = conn.createSession(false, HornetQJMSConstants.INDIVIDUAL_ACKNOWLEDGE);

      consumer = session.createConsumer(jBossQueue);

      // Consume odd numbers first
      for (int i = 0; i < noOfMessages; i++)
      {
         if (i % 2 == 0)
         {
            continue;
         }

         TextMessage m = (TextMessage) consumer.receive(1000);
         assertNotNull(m);
         m.acknowledge();
         assertEquals("m" + i, m.getText());
      }

      SimpleString queueName = new SimpleString(HornetQDestination.JMS_QUEUE_ADDRESS_PREFIX + ConsumerTest.Q_NAME);
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(queueName).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(queueName).getBindable()).getMessageCount());
      conn.close();
   }

   public void testPreCommitAcksSetOnConnectionFactory() throws Exception
   {
      ((HornetQConnectionFactory)cf).setPreAcknowledge(true);
      conn = cf.createConnection();

      Session session = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      jBossQueue = HornetQJMSClient.createQueue(ConsumerTest.Q_NAME);
      MessageProducer producer = session.createProducer(jBossQueue);
      MessageConsumer consumer = session.createConsumer(jBossQueue);
      int noOfMessages = 100;
      for (int i = 0; i < noOfMessages; i++)
      {
         producer.send(session.createTextMessage("m" + i));
      }

      conn.start();
      for (int i = 0; i < noOfMessages; i++)
      {
         Message m = consumer.receive(500);
         Assert.assertNotNull(m);
      }

      // Messages should all have been acked since we set pre ack on the cf
      SimpleString queueName = new SimpleString(HornetQDestination.JMS_QUEUE_ADDRESS_PREFIX + ConsumerTest.Q_NAME);
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(queueName).getBindable()).getDeliveringCount());
      Assert.assertEquals(0, ((Queue)server.getPostOffice().getBinding(queueName).getBindable()).getMessageCount());
   }

   public void testPreCommitAcksWithMessageExpiry() throws Exception
   {
      ConsumerTest.log.info("starting test");

      conn = cf.createConnection();
      Session session = conn.createSession(false, HornetQJMSConstants.PRE_ACKNOWLEDGE);
      jBossQueue = HornetQJMSClient.createQueue(ConsumerTest.Q_NAME);
      MessageProducer producer = session.createProducer(jBossQueue);
      MessageConsumer consumer = session.createConsumer(jBossQueue);
      int noOfMessages = 1000;
      for (int i = 0; i < noOfMessages; i++)
      {
         TextMessage textMessage = session.createTextMessage("m" + i);
         producer.setTimeToLive(1);
         producer.send(textMessage);
      }

      Thread.sleep(2);

      conn.start();

      Message m = consumer.receiveNoWait();
      Assert.assertNull(m);

      // Asserting delivering count is zero is bogus since messages might still be being delivered and expired at this
      // point
      // which can cause delivering count to flip to 1

   }

   public void testPreCommitAcksWithMessageExpirySetOnConnectionFactory() throws Exception
   {
      ((HornetQConnectionFactory)cf).setPreAcknowledge(true);
      conn = cf.createConnection();
      Session session = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      jBossQueue = HornetQJMSClient.createQueue(ConsumerTest.Q_NAME);
      MessageProducer producer = session.createProducer(jBossQueue);
      MessageConsumer consumer = session.createConsumer(jBossQueue);
      int noOfMessages = 1000;
      for (int i = 0; i < noOfMessages; i++)
      {
         TextMessage textMessage = session.createTextMessage("m" + i);
         producer.setTimeToLive(1);
         producer.send(textMessage);
      }

      Thread.sleep(2);

      conn.start();
      Message m = consumer.receiveNoWait();
      Assert.assertNull(m);

      // Asserting delivering count is zero is bogus since messages might still be being delivered and expired at this
      // point
      // which can cause delivering count to flip to 1
   }

   public void testClearExceptionListener() throws Exception
   {
      conn = cf.createConnection();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      jBossQueue = HornetQJMSClient.createQueue(ConsumerTest.Q_NAME);
      MessageConsumer consumer = session.createConsumer(jBossQueue);
      consumer.setMessageListener(new MessageListener()
      {
         public void onMessage(final Message msg)
         {
         }
      });

      consumer.setMessageListener(null);
      consumer.receiveNoWait();
   }

   public void testCantReceiveWhenListenerIsSet() throws Exception
   {
      conn = cf.createConnection();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      jBossQueue = HornetQJMSClient.createQueue(ConsumerTest.Q_NAME);
      MessageConsumer consumer = session.createConsumer(jBossQueue);
      consumer.setMessageListener(new MessageListener()
      {
         public void onMessage(final Message msg)
         {
         }
      });

      try
      {
         consumer.receiveNoWait();
         Assert.fail("Should throw exception");
      }
      catch (JMSException e)
      {
         // Ok
      }
   }
}
