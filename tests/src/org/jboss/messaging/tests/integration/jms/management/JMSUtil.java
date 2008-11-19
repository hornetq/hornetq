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

import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
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

   static MessageConsumer createConsumer(Destination destination, boolean startConnection) throws JMSException
   {
      JBossConnectionFactory cf = new JBossConnectionFactory(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                             null,
                                                             ClientSessionFactoryImpl.DEFAULT_PING_PERIOD,
                                                             ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                                                             null,
                                                             ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                                             ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE,
                                                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE,
                                                             ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE,
                                                             ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE,
                                                             ClientSessionFactoryImpl.DEFAULT_BIG_MESSAGE_SIZE,
                                                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                                             true,
                                                             ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP,
                                                             ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS,
                                                             ClientSessionFactoryImpl.DEFAULT_PRE_COMMIT_ACKS);

      Connection conn = cf.createConnection();

      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      if (startConnection)
      {
         conn.start();
      }

      return s.createConsumer(destination);
   }

   static TopicSubscriber createDurableSubscriber(Topic topic, String clientID, String subscriptionName) throws JMSException
   {
      JBossConnectionFactory cf = new JBossConnectionFactory(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                             null,
                                                             ClientSessionFactoryImpl.DEFAULT_PING_PERIOD,
                                                             ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                                                             null,
                                                             ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                                             ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE,
                                                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE,
                                                             ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE,
                                                             ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE,
                                                             ClientSessionFactoryImpl.DEFAULT_BIG_MESSAGE_SIZE,
                                                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                                             true,
                                                             ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP,
                                                             ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS,
                                                             ClientSessionFactoryImpl.DEFAULT_PRE_COMMIT_ACKS);

      Connection conn = cf.createConnection();

      conn.setClientID(clientID);
      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      return s.createDurableSubscriber(topic, subscriptionName);
   }

   static void sendMessages(Destination destination, int messagesToSend) throws Exception
   {
      JBossConnectionFactory cf = new JBossConnectionFactory(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                             null,
                                                             ClientSessionFactoryImpl.DEFAULT_PING_PERIOD,
                                                             ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                                                             null,
                                                             ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                                             ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE,
                                                             ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE,
                                                             ClientSessionFactoryImpl.DEFAULT_SEND_WINDOW_SIZE,
                                                             ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE,
                                                             ClientSessionFactoryImpl.DEFAULT_BIG_MESSAGE_SIZE,
                                                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                                             true,
                                                             ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP,
                                                             ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS,
                                                             ClientSessionFactoryImpl.DEFAULT_PRE_COMMIT_ACKS);

      Connection conn = cf.createConnection();

      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = s.createProducer(destination);

      for (int i = 0; i < messagesToSend; i++)
      {
         producer.send(s.createTextMessage(randomString()));
      }
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
