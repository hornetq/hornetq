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

package org.hornetq.tests.integration.jms.client;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.hornetq.tests.util.JMSTestBase;

public class NoLocalTest extends JMSTestBase
{
   // Constants -----------------------------------------------------
   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testNoLocal() throws Exception
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

   // This one would work with the current version
   public void testNoLocal2() throws Exception
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
   // Inner classes -------------------------------------------------
}
