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
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.Enumeration;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
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

   private static final String T_NAME = "ConsumerTestTopic";

   private static final String T2_NAME = "ConsumerTestTopic2";

   private javax.jms.Queue jBossQueue;
   private javax.jms.Topic topic;
   private javax.jms.Topic topic2;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();


      topic = HornetQJMSClient.createTopic(T_NAME);
      topic2 = HornetQJMSClient.createTopic(T2_NAME);


      jmsServer.createQueue(false, ConsumerTest.Q_NAME, null, true, ConsumerTest.Q_NAME);
      jmsServer.createTopic(true, T_NAME, "/topic/" +T_NAME);
      jmsServer.createTopic(true, T2_NAME, "/topic/" +T2_NAME);
      cf = (ConnectionFactory)HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      cf = null;

      super.tearDown();
   }

   @Test
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

   @Test
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

   @Test
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

   @Test
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

   @Test
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

   @Test
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


   @Test
   public void testBrowserAndConsumerSimultaneous() throws Exception
   {
      ((HornetQConnectionFactory)cf).setConsumerWindowSize(0);
      conn = cf.createConnection();

      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      jBossQueue = HornetQJMSClient.createQueue(ConsumerTest.Q_NAME);
      MessageProducer producer = session.createProducer(jBossQueue);

      QueueBrowser browser = session.createBrowser(jBossQueue);
      Enumeration enumMessages = browser.getEnumeration();


      MessageConsumer consumer = session.createConsumer(jBossQueue);
      int noOfMessages = 10;
      for (int i = 0; i < noOfMessages; i++)
      {
         TextMessage textMessage = session.createTextMessage("m" + i);
         textMessage.setIntProperty("i", i);
         producer.send(textMessage);
      }

      conn.start();
      for (int i = 0 ; i < noOfMessages; i++)
      {
         TextMessage msg = (TextMessage)enumMessages.nextElement();
         assertNotNull(msg);
         assertEquals(i, msg.getIntProperty("i"));

         conn.start();
         TextMessage recvMessage = (TextMessage)consumer.receiveNoWait();
         assertNotNull(recvMessage);
         conn.stop();
         assertEquals(i, msg.getIntProperty("i"));
      }

      assertNull(consumer.receiveNoWait());
      assertFalse(enumMessages.hasMoreElements());

      conn.close();

      // Asserting delivering count is zero is bogus since messages might still be being delivered and expired at this
      // point
      // which can cause delivering count to flip to 1
   }

   @Test
   public void testBrowserAndConsumerSimultaneousDifferentConnections() throws Exception
   {
      ((HornetQConnectionFactory)cf).setConsumerWindowSize(0);
      conn = cf.createConnection();

      Connection connConsumer = cf.createConnection();
      Session sessionConsumer = connConsumer.createSession(false, Session.AUTO_ACKNOWLEDGE);

      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      jBossQueue = HornetQJMSClient.createQueue(ConsumerTest.Q_NAME);
      MessageProducer producer = session.createProducer(jBossQueue);
      MessageConsumer consumer = sessionConsumer.createConsumer(jBossQueue);
      int noOfMessages = 1000;
      for (int i = 0; i < noOfMessages; i++)
      {
         TextMessage textMessage = session.createTextMessage("m" + i);
         textMessage.setIntProperty("i", i);
         producer.send(textMessage);
      }

      connConsumer.start();

      QueueBrowser browser = session.createBrowser(jBossQueue);
      Enumeration enumMessages = browser.getEnumeration();

      for (int i = 0 ; i < noOfMessages; i++)
      {
         TextMessage msg = (TextMessage)enumMessages.nextElement();
         assertNotNull(msg);
         assertEquals(i, msg.getIntProperty("i"));

         TextMessage recvMessage = (TextMessage)consumer.receiveNoWait();
         assertNotNull(recvMessage);
         assertEquals(i, msg.getIntProperty("i"));
      }

      Message m = consumer.receiveNoWait();
      assertFalse(enumMessages.hasMoreElements());
      Assert.assertNull(m);

      conn.close();
   }

   @Test
   public void testBrowserOnly() throws Exception
   {
      ((HornetQConnectionFactory)cf).setConsumerWindowSize(0);
      conn = cf.createConnection();

      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      jBossQueue = HornetQJMSClient.createQueue(ConsumerTest.Q_NAME);
      MessageProducer producer = session.createProducer(jBossQueue);
      int noOfMessages = 10;
      for (int i = 0; i < noOfMessages; i++)
      {
         TextMessage textMessage = session.createTextMessage("m" + i);
         textMessage.setIntProperty("i", i);
         producer.send(textMessage);
      }

      QueueBrowser browser = session.createBrowser(jBossQueue);
      Enumeration enumMessages = browser.getEnumeration();

      for (int i = 0 ; i < noOfMessages; i++)
      {
         assertTrue(enumMessages.hasMoreElements());
         TextMessage msg = (TextMessage)enumMessages.nextElement();
         assertNotNull(msg);
         assertEquals(i, msg.getIntProperty("i"));

      }

      assertFalse(enumMessages.hasMoreElements());

      conn.close();

      // Asserting delivering count is zero is bogus since messages might still be being delivered and expired at this
      // point
      // which can cause delivering count to flip to 1
   }

   @Test
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

   @Test
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

   @Test
   public void testSharedConsumer() throws Exception
   {
      conn = cf.createConnection();
      conn.start();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      topic = HornetQJMSClient.createTopic(T_NAME);


      MessageConsumer cons = session.createSharedConsumer(topic, "test1");

      MessageProducer producer = session.createProducer(topic);

      producer.send(session.createTextMessage("test"));

      TextMessage txt = (TextMessage)cons.receive(5000);

      assertNotNull(txt);
   }

   @Test
   public void testSharedDurableConsumer() throws Exception
   {
      conn = cf.createConnection();
      conn.start();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      topic = HornetQJMSClient.createTopic(T_NAME);


      MessageConsumer cons = session.createSharedDurableConsumer(topic, "test1");

      MessageProducer producer = session.createProducer(topic);

      producer.send(session.createTextMessage("test"));

      TextMessage txt = (TextMessage)cons.receive(5000);

      assertNotNull(txt);
   }

   @Test
   public void testSharedDurableConsumerWithClientID() throws Exception
   {
      conn = cf.createConnection();
      conn.setClientID("C1");
      conn.start();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      Connection conn2 = cf.createConnection();
      conn2.setClientID("C2");
      Session session2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

      {
         Connection conn3 = cf.createConnection();

         boolean exception = false;
         try
         {
            conn3.setClientID("C2");
         }
         catch (Exception e)
         {
            exception = true;
         }

         assertTrue(exception);
         conn3.close();
      }

      topic = HornetQJMSClient.createTopic(T_NAME);


      MessageConsumer cons = session.createSharedDurableConsumer(topic, "test1");

      MessageProducer producer = session.createProducer(topic);

      producer.send(session.createTextMessage("test"));

      TextMessage txt = (TextMessage)cons.receive(5000);

      assertNotNull(txt);
   }

   @Test
   public void testValidateExceptionsThroughSharedConsumers() throws Exception
   {
      conn = cf.createConnection();
      conn.setClientID("C1");
      conn.start();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Connection conn2 = cf.createConnection();
      conn2.setClientID("C2");



       MessageConsumer cons = session.createSharedConsumer(topic, "cons1");
      boolean exceptionHappened = false;
      try
      {
         MessageConsumer cons2Error = session.createSharedConsumer(topic2, "cons1");
      }
      catch (JMSException e)
      {
         exceptionHappened = true;
      }

      assertTrue(exceptionHappened);


      MessageProducer producer = session.createProducer(topic2);

      // This is durable, different than the one on topic... So it should go through
      MessageConsumer cons2 = session.createSharedDurableConsumer(topic2, "cons1");

      conn.start();


      producer.send(session.createTextMessage("hello!"));

      TextMessage msg = (TextMessage)cons2.receive(5000);
      assertNotNull(msg);


      exceptionHappened = false;
      try
      {
         session.unsubscribe("cons1");
      }
      catch (JMSException e)
      {
         exceptionHappened = true;
      }


      assertTrue(exceptionHappened);
      cons2.close();
      conn.close();
      conn2.close();

   }

   @Test
   public void testUnsubscribeDurable() throws Exception
   {
      conn = cf.createConnection();
      conn.setClientID("C1");
      conn.start();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageConsumer cons = session.createSharedDurableConsumer(topic, "c1");

      MessageProducer prod = session.createProducer(topic);

      for (int i = 0 ; i < 100; i++)
      {
         prod.send(topic, session.createTextMessage("msg" + i));
      }

      assertNotNull(cons.receive(5000));

      cons.close();

      session.unsubscribe("c1");

      cons = session.createSharedDurableConsumer(topic, "c1");

      // it should be null since the queue was deleted through unsubscribe
      assertNull(cons.receiveNoWait());
   }

   @Test
   public void testShareDurale() throws Exception
   {
      ((HornetQConnectionFactory)cf).setConsumerWindowSize(0);
      conn = cf.createConnection();
      conn.start();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session session2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageConsumer cons = session.createSharedDurableConsumer(topic, "c1");
      MessageConsumer cons2 = session2.createSharedDurableConsumer(topic, "c1");

      MessageProducer prod = session.createProducer(topic);

      for (int i = 0 ; i < 100; i++)
      {
         prod.send(topic, session.createTextMessage("msg" + i));
      }


      for (int i = 0; i < 50; i++)
      {
         Message msg = cons.receive(5000);
         assertNotNull(msg);
         msg = cons2.receive(5000);
         assertNotNull(msg);
      }

      assertNull(cons.receiveNoWait());
      assertNull(cons2.receiveNoWait());

      cons.close();

      boolean exceptionHappened = false;

      try
      {
         session.unsubscribe("c1");
      }
      catch (JMSException e)
      {
         exceptionHappened = true;
      }

      assertTrue(exceptionHappened);

      cons2.close();

      for (int i = 0 ; i < 100; i++)
      {
         prod.send(topic, session.createTextMessage("msg" + i));
      }


      session.unsubscribe("c1");

      cons = session.createSharedDurableConsumer(topic, "c1");

      // it should be null since the queue was deleted through unsubscribe
      assertNull(cons.receiveNoWait());
   }
}
