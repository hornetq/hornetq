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

package org.jboss.messaging.tests.integration.jms.management;

import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_INITIAL_CONNECT_ATTEMPTS;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PING_PERIOD;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RECONNECT_ATTEMPTS;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

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

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.jms.client.JBossConnectionFactory;

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
      JBossConnectionFactory cf = new JBossConnectionFactory(new TransportConfiguration(connectorFactory),
                                                             null,
                                                             DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                                             DEFAULT_PING_PERIOD,
                                                             DEFAULT_CONNECTION_TTL,
                                                             DEFAULT_CALL_TIMEOUT,
                                                             null,
                                                             DEFAULT_ACK_BATCH_SIZE,
                                                             DEFAULT_ACK_BATCH_SIZE,
                                                             DEFAULT_CONSUMER_WINDOW_SIZE,
                                                             DEFAULT_CONSUMER_MAX_RATE,
                                                             DEFAULT_SEND_WINDOW_SIZE,
                                                             DEFAULT_PRODUCER_MAX_RATE,
                                                             DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                             DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                                             true,
                                                             true,
                                                             DEFAULT_AUTO_GROUP,
                                                             DEFAULT_MAX_CONNECTIONS,
                                                             DEFAULT_PRE_ACKNOWLEDGE,                                                  
                                                             DEFAULT_RETRY_INTERVAL,
                                                             DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                             DEFAULT_INITIAL_CONNECT_ATTEMPTS,
                                                             DEFAULT_RECONNECT_ATTEMPTS);

      return cf.createConnection();
   }

   static MessageConsumer createConsumer(Destination destination, boolean startConnection, String connectorFactory) throws JMSException
   {
      JBossConnectionFactory cf = new JBossConnectionFactory(new TransportConfiguration(connectorFactory),
                                                             null,
                                                             DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                                             DEFAULT_PING_PERIOD,
                                                             DEFAULT_CONNECTION_TTL,
                                                             DEFAULT_CALL_TIMEOUT,
                                                             null,
                                                             DEFAULT_ACK_BATCH_SIZE,
                                                             DEFAULT_ACK_BATCH_SIZE,
                                                             DEFAULT_CONSUMER_WINDOW_SIZE,
                                                             DEFAULT_CONSUMER_MAX_RATE,
                                                             DEFAULT_SEND_WINDOW_SIZE,
                                                             DEFAULT_PRODUCER_MAX_RATE,
                                                             DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                             true,
                                                             true,
                                                             true,
                                                             DEFAULT_AUTO_GROUP,
                                                             DEFAULT_MAX_CONNECTIONS,
                                                             DEFAULT_PRE_ACKNOWLEDGE,                                                
                                                             DEFAULT_RETRY_INTERVAL,
                                                             DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                             DEFAULT_INITIAL_CONNECT_ATTEMPTS,
                                                             DEFAULT_RECONNECT_ATTEMPTS);

      Connection conn = cf.createConnection();

      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      if (startConnection)
      {
         conn.start();
      }

      return s.createConsumer(destination);
   }

   public static MessageConsumer createConsumer(Destination destination, boolean startConnection) throws JMSException
   {
      return createConsumer(destination,
                            startConnection,
                            InVMConnectorFactory.class.getName());
   }

   static TopicSubscriber createDurableSubscriber(Topic topic, String clientID, String subscriptionName) throws JMSException
   {
      JBossConnectionFactory cf = new JBossConnectionFactory(new TransportConfiguration(InVMConnectorFactory.class.getName()),
                                                             null,
                                                             DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                                             DEFAULT_PING_PERIOD,
                                                             DEFAULT_CONNECTION_TTL,
                                                             DEFAULT_CALL_TIMEOUT,
                                                             null,
                                                             DEFAULT_ACK_BATCH_SIZE,
                                                             DEFAULT_ACK_BATCH_SIZE,
                                                             DEFAULT_CONSUMER_WINDOW_SIZE,
                                                             DEFAULT_CONSUMER_MAX_RATE,
                                                             DEFAULT_SEND_WINDOW_SIZE,
                                                             DEFAULT_PRODUCER_MAX_RATE,
                                                             DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                             DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                                             DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                                             true,
                                                             DEFAULT_AUTO_GROUP,
                                                             DEFAULT_MAX_CONNECTIONS,
                                                             DEFAULT_PRE_ACKNOWLEDGE,                                                       
                                                             DEFAULT_RETRY_INTERVAL,
                                                             DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                             DEFAULT_INITIAL_CONNECT_ATTEMPTS,
                                                             DEFAULT_RECONNECT_ATTEMPTS);

      Connection conn = cf.createConnection();

      conn.setClientID(clientID);
      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      return s.createDurableSubscriber(topic, subscriptionName);
   }

   public static String[] sendMessages(Destination destination, int messagesToSend) throws Exception
   {
      JBossConnectionFactory cf = new JBossConnectionFactory(new TransportConfiguration(InVMConnectorFactory.class.getName()));
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
      try{
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
