/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.tests.integration.jms.client;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.util.JMSTestBase;
import org.hornetq.tests.util.RandomUtil;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class NoLocalSubscriberTest extends JMSTestBase
{
   // Constants -----------------------------------------------------

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;


   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Test that a message created from the same connection than a nolocal consumer
    * can be sent by *another* connection and will be received by the nolocal consumer
    */
   @Test
   public void testNoLocal() throws Exception
   {
      if (log.isTraceEnabled())
      {
         log.trace("testNoLocal");
      }

      Connection defaultConn = null;
      Connection newConn = null;

      try
      {
         Topic topic1 = createTopic("topic1");
         defaultConn = cf.createConnection();
         Session defaultSess = defaultConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer defaultConsumer = defaultSess.createConsumer(topic1);
         MessageConsumer noLocalConsumer = defaultSess.createConsumer(topic1, null, true);
         MessageProducer defaultProd = defaultSess.createProducer(topic1);

         defaultConn.start();

         String text = RandomUtil.randomString();
         // message is created only once from the same connection than the noLocalConsumer
         TextMessage messageSent = defaultSess.createTextMessage(text);
         for (int i = 0; i < 10; i++)
         {
            defaultProd.send(messageSent);
         }

         Message received = null;
         for (int i = 0; i < 10; i++)
         {
            received = defaultConsumer.receive(5000);
            assertNotNull(received);
            assertEquals(text, ((TextMessage)received).getText());
         }

         newConn = cf.createConnection();
         Session newSession = newConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer newProd = newSession.createProducer(topic1);
         MessageConsumer newConsumer = newSession.createConsumer(topic1);

         newConn.start();

         text = RandomUtil.randomString();
         messageSent.setText(text);
         defaultProd.send(messageSent);

         received = newConsumer.receive(5000);
         assertNotNull(received);
         assertEquals(text, ((TextMessage)received).getText());

         text = RandomUtil.randomString();
         messageSent.setText(text);
         // we send the message created at the start of the test but on the *newConn* this time
         newProd.send(messageSent);
         newConn.close();

         received = noLocalConsumer.receive(5000);
         assertNotNull("nolocal consumer did not get message", received);
         assertEquals(text, ((TextMessage)received).getText());
      }
      finally
      {
         if (defaultConn != null)
         {
            defaultConn.close();
         }
         if (newConn != null)
         {
            newConn.close();
         }
      }
   }

   /**
    * Create a ConnectionFactory with a preset client ID
    * and test the functionality of noLocal consumer with it.
    */
   @Test
   public void testNoLocalWithClientIDPreset() throws Exception
   {
      if (log.isTraceEnabled())
      {
         log.trace("testNoLocal");
      }

      ConnectionFactory clientIDCF = createCFwithClientID("myClientID");

      final String testName = "testNoLocalWithClientIDPreset()";
      Connection defaultConn = null;
      Connection newConn = null;
      Session newSess = null;
      MessageConsumer noLocalConsumer = null;
      MessageConsumer selectConsumer = null;
      MessageConsumer defaultConsumer = null;
      MessageProducer newPub = null;
      TextMessage messageSent = null;
      TextMessage messageReceived = null;
      long timeout = 5000;

      try
      {
         Topic topic1 = createTopic("topic1");
         newConn = clientIDCF.createConnection();

         newSess = newConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         noLocalConsumer = newSess.createConsumer(topic1, null, true);
         selectConsumer  = newSess.createConsumer(topic1,"TEST = 'test'", false);
         defaultConsumer = newSess.createConsumer(topic1);
         newPub = newSess.createProducer(topic1);

         defaultConn = clientIDCF.createConnection();

         Session defaultSess = defaultConn.createSession();
         MessageProducer defaultProd = defaultSess.createProducer(topic1);

         defaultConn.start();
         newConn.start();

         //Create and send two messages from new connection
         messageSent = defaultSess.createTextMessage();
         messageSent.setText("Just a test");
         messageSent.setStringProperty("COM_SUN_JMS_TESTNAME", testName);

         messageSent.setBooleanProperty("lastMessage", false);
         newPub.send(messageSent);

         messageSent.setStringProperty("TEST", "test");
         messageSent.setBooleanProperty("lastMessage", true);
         newPub.send(messageSent);

         //Verify that noLocalConsumer cannot receive any message
         messageReceived = (TextMessage)noLocalConsumer.receive(timeout);
         assertTrue(messageReceived == null);

         //Verify that defaultConsumer received correct messages
         for (int i = 0; i < 2; i++)
         {
             messageReceived = (TextMessage)defaultConsumer.receive(timeout);
             assertNotNull(messageReceived);
             assertTrue(messageReceived.getText().equals(messageSent.getText()));
         }

         // Verify that selectConsumer only receive the last message
         messageReceived = (TextMessage)selectConsumer.receive(timeout);
         assertNotNull(messageReceived);
         assertTrue(messageReceived.getText().equals(messageSent.getText()));

         // send message from default connection
         messageSent.setBooleanProperty("newConnection", true);
         defaultProd.send(messageSent);

         //Verify that noLocalConsumer now can receive message from second connection
         messageReceived = (TextMessage)noLocalConsumer.receive(timeout);
         assertNotNull(messageReceived);
         assertTrue(messageReceived.getText().equals(messageSent.getText()));

         noLocalConsumer.close();
         defaultConsumer.close();
         selectConsumer.close();
         
         newConn.close();
      }
      finally
      {
         if (defaultConn != null)
         {
            defaultConn.close();
         }
         if (newConn != null)
         {
            newConn.close();
         }
      }
   }

   @Test
   public void testNoLocalReconnect() throws Exception
   {
      ConnectionFactory connectionFactory = cf;
      String uniqueID = Long.toString(System.currentTimeMillis());
      String topicName = "exampleTopic";
      String clientID = "myClientID_" + uniqueID;
      String subscriptionName = "mySub_" + uniqueID;

      boolean noLocal = true;
      String messageSelector = "";
      Topic topic = createTopic(topicName);

      {
         // Create durable subscription
         Connection connection = connectionFactory.createConnection("guest", "guest");
         connection.setClientID(clientID);
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         TopicSubscriber topicSubscriber =
            session.createDurableSubscriber(topic, subscriptionName, messageSelector, noLocal);
         topicSubscriber.close();
         connection.close();
      }

      {
         // create a connection using the same client ID and send a message
         // to the topic
         // this will not be added to the durable subscription
         Connection connection = connectionFactory.createConnection("guest", "guest");
         connection.setClientID(clientID);
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer messageProducer = session.createProducer(topic);
         messageProducer.send(session.createTextMessage("M3"));
         connection.close();
      }

      {
         // create a connection using a different client ID and send a
         // message to the topic
         // this will be added to the durable subscription
         Connection connection = connectionFactory.createConnection("guest", "guest");
         connection.setClientID(clientID + "_different");
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer messageProducer = session.createProducer(topic);
         messageProducer.send(session.createTextMessage("M4"));
         connection.close();
      }

      {
         Connection connection = connectionFactory.createConnection("guest", "guest");
         connection.setClientID(clientID);
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         TopicSubscriber topicSubscriber =
            session.createDurableSubscriber(topic, subscriptionName, messageSelector, noLocal);
         connection.start();

         // now drain the subscription
         // we should not receive message M3, but we should receive message M4
         // However for some reason HornetMQ doesn't receive either
         TextMessage textMessage = (TextMessage)topicSubscriber.receive(1000);
         assertNotNull(textMessage);

         assertEquals("M4", textMessage.getText());

         assertNull(topicSubscriber.receiveNoWait());

         connection.close();
      }
   }

   @Test
   public void testNoLocalReconnect2() throws Exception
   {

      ConnectionFactory connectionFactory = cf;
      String uniqueID = Long.toString(System.currentTimeMillis());
      String topicName = "exampleTopic";
      String clientID = "myClientID_" + uniqueID;
      String subscriptionName = "mySub_" + uniqueID;

      boolean noLocal = true;
      String messageSelector = "";
      Topic topic = createTopic(topicName);

      Connection originalConnection;

      {
         // Create durable subscription
         originalConnection = connectionFactory.createConnection("guest", "guest");
         originalConnection.setClientID(clientID);
         Session session = originalConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         TopicSubscriber topicSubscriber =
            session.createDurableSubscriber(topic, subscriptionName, messageSelector, noLocal);
         topicSubscriber.close();
         session.close();
      }

      {
         // create a connection using the same client ID and send a message
         // to the topic
         // this will not be added to the durable subscription
         Session session = originalConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer messageProducer = session.createProducer(topic);
         messageProducer.send(session.createTextMessage("M3"));
         session.close();
      }

      {
         // create a connection using a different client ID and send a
         // message to the topic
         // this will be added to the durable subscription
         Connection connection = connectionFactory.createConnection("guest", "guest");
         connection.setClientID(clientID + "_different");
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer messageProducer = session.createProducer(topic);
         messageProducer.send(session.createTextMessage("M4"));
         connection.close();
      }

      {
         Session session = originalConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         TopicSubscriber topicSubscriber =
            session.createDurableSubscriber(topic, subscriptionName, messageSelector, noLocal);
         originalConnection.start();

         // now drain the subscription
         // we should not receive message M3, but we should receive message M4
         // However for some reason HornetMQ doesn't receive either
         TextMessage textMessage = (TextMessage)topicSubscriber.receive(1000);
         assertNotNull(textMessage);

         assertEquals("M4", textMessage.getText());

         assertNull(topicSubscriber.receiveNoWait());

         originalConnection.close();
      }
   }

   private ConnectionFactory createCFwithClientID(String clientID) throws Exception
   {

      List<TransportConfiguration> connectorConfigs = new ArrayList<TransportConfiguration>();
      connectorConfigs.add(new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      createCF(connectorConfigs, "/cfWithClientID");

      HornetQConnectionFactory factory = (HornetQConnectionFactory)namingContext.lookup("/cfWithClientID");
      factory.setClientID(clientID);
      
      return factory;
   }

}
