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

package org.hornetq.tests.integration.jms.server.management;

import static org.hornetq.tests.util.RandomUtil.randomString;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import junit.framework.Assert;

import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.jms.client.HornetQConnectionFactory;

/**
 * A JMSUtil
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 14 nov. 2008 13:48:08
 *
 *
 */
public class JMSUtil
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public static Connection createConnection(String connectorFactory) throws JMSException
   {
      HornetQConnectionFactory cf = new HornetQConnectionFactory(new TransportConfiguration(connectorFactory));
      
      cf.setBlockOnNonPersistentSend(true);
      cf.setBlockOnPersistentSend(true);
      cf.setBlockOnAcknowledge(true);

      return cf.createConnection();
   }
   
   public static ConnectionFactory createFactory(String connectorFactory, long connectionTTL, long clientFailureCheckPeriod) throws JMSException
   {
      HornetQConnectionFactory cf = new HornetQConnectionFactory(new TransportConfiguration(connectorFactory));
      
      cf.setBlockOnNonPersistentSend(true);
      cf.setBlockOnPersistentSend(true);
      cf.setBlockOnAcknowledge(true);
      cf.setConnectionTTL(connectionTTL);
      cf.setClientFailureCheckPeriod(clientFailureCheckPeriod);

      return cf;
   }
   
   static MessageConsumer createConsumer(Connection connection, Destination destination, String connectorFactory) throws JMSException
   {
      Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      return s.createConsumer(destination);
   }

   public static MessageConsumer createConsumer(Connection connection, Destination destination) throws JMSException
   {
      return createConsumer(connection, destination, InVMConnectorFactory.class.getName());
   }

   static TopicSubscriber createDurableSubscriber(Connection connection,
                                                  Topic topic,
                                                  String clientID,
                                                  String subscriptionName) throws JMSException
   {
      connection.setClientID(clientID);
      Session s = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      return s.createDurableSubscriber(topic, subscriptionName);
   }

   public static String[] sendMessages(Destination destination, int messagesToSend) throws Exception
   {
      HornetQConnectionFactory cf = new HornetQConnectionFactory(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      return sendMessages(cf, destination, messagesToSend);
   }

   public static String[] sendMessages(ConnectionFactory cf, Destination destination, int messagesToSend) throws Exception
   {
      String[] messageIDs = new String[messagesToSend];

      Connection conn = cf.createConnection();

      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = s.createProducer(destination);

      for (int i = 0; i < messagesToSend; i++)
      {
         Message m = s.createTextMessage(randomString());
         producer.send(m);
         messageIDs[i] = m.getJMSMessageID();
      }

      conn.close();

      return messageIDs;
   }

   public static Message sendMessageWithProperty(Session session, Destination destination, String key, long value) throws JMSException
   {
      MessageProducer producer = session.createProducer(destination);
      Message message = session.createMessage();
      message.setLongProperty(key, value);
      producer.send(message);
      return message;
   }

   public static void consumeMessages(int expected, Destination dest) throws JMSException
   {
      Connection connection = createConnection(InVMConnectorFactory.class.getName());
      try
      {
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(dest);

         connection.start();

         Message m = null;
         for (int i = 0; i < expected; i++)
         {
            m = consumer.receive(500);
            Assert.assertNotNull("expected to received " + expected + " messages, got only " + (i + 1), m);
         }
         m = consumer.receive(500);
         Assert.assertNull("received one more message than expected (" + expected + ")", m);
      }
      finally
      {
         if (connection != null)
         {
            connection.close();
         }
      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
