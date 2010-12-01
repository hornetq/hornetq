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

import junit.framework.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.HornetQJMSConstants;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.Queue;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.jms.server.impl.JMSFactoryType;
import org.hornetq.tests.util.JMSTestBase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ConsumerTest extends JMSTestBase
{
   private static final Logger log = Logger.getLogger(ConsumerTest.class);

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
      Connection conn = cf.createConnection();
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
      conn.close();
   }

   public void testPreCommitAcksSetOnConnectionFactory() throws Exception
   {
      ((HornetQConnectionFactory)cf).setPreAcknowledge(true);
      Connection conn = cf.createConnection();

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
      conn.close();
   }

   public void testPreCommitAcksWithMessageExpiry() throws Exception
   {
      ConsumerTest.log.info("starting test");

      Connection conn = cf.createConnection();
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

      conn.close();
   }

   public void testPreCommitAcksWithMessageExpirySetOnConnectionFactory() throws Exception
   {
      ((HornetQConnectionFactory)cf).setPreAcknowledge(true);
      Connection conn = cf.createConnection();
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

      conn.close();
   }

   public void testClearExceptionListener() throws Exception
   {
      Connection conn = cf.createConnection();
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

      conn.close();
   }

   public void testCantReceiveWhenListenerIsSet() throws Exception
   {
      Connection conn = cf.createConnection();
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

      conn.close();
   }
}
