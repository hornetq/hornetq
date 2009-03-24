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
package org.jboss.messaging.tests.integration.jms.bridge;

import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.management.ObjectName;
import javax.transaction.TransactionManager;

import com.arjuna.ats.internal.jta.transaction.arjunacore.TransactionManagerImple;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.management.ObjectNames;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.jms.bridge.ConnectionFactoryFactory;
import org.jboss.messaging.jms.bridge.DestinationFactory;
import org.jboss.messaging.jms.bridge.QualityOfServiceMode;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.jms.server.management.JMSQueueControlMBean;
import org.jboss.messaging.jms.server.management.TopicControlMBean;
import org.jboss.messaging.tests.unit.util.InVMContext;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * 
 * A BridgeTestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public abstract class BridgeTestBase extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(BridgeTestBase.class);

   protected static ConnectionFactoryFactory cff0, cff1;

   protected static ConnectionFactory cf0, cf1;

   protected static DestinationFactory sourceQueueFactory, targetQueueFactory, localTargetQueueFactory,
            sourceTopicFactory;

   protected static Queue sourceQueue, targetQueue, localTargetQueue;

   protected static Topic sourceTopic;

   protected static boolean firstTime = true;

   protected MessagingServiceImpl server0;

   protected JMSServerManagerImpl jmsServer0;

   protected MessagingServiceImpl server1;

   protected JMSServerManagerImpl jmsServer1;

   private InVMContext context0;

   protected InVMContext context1;

   private HashMap<String, Object> params1;

   protected void setUp() throws Exception
   {
      super.setUp();

      // Start the servers
      Configuration conf0 = new ConfigurationImpl();
      conf0.setSecurityEnabled(false);
      conf0.getAcceptorConfigurations()
           .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      server0 = Messaging.newNullStorageMessagingService(conf0);
      server0.start();

      context0 = new InVMContext();
      jmsServer0 = JMSServerManagerImpl.newJMSServerManagerImpl(server0.getServer());
      jmsServer0.start();
      jmsServer0.setContext(context0);

      Configuration conf1 = new ConfigurationImpl();
      conf1.setSecurityEnabled(false);
      params1 = new HashMap<String, Object>();
      params1.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      conf1.getAcceptorConfigurations()
           .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory", params1));

      server1 = Messaging.newNullStorageMessagingService(conf1);
      server1.start();

      context1 = new InVMContext();
      jmsServer1 = JMSServerManagerImpl.newJMSServerManagerImpl(server1.getServer());
      jmsServer1.start();
      jmsServer1.setContext(context1);

      createQueue("sourceQueue", 0);

      jmsServer0.createTopic("sourceTopic", "/topic/sourceTopic");

      createQueue("localTargetQueue", 0);

      createQueue("targetQueue", 1);

      setUpAdministeredObjects();

      // We need a local transaction and recovery manager
      // We must start this after the remote servers have been created or it won't
      // have deleted the database and the recovery manager may attempt to recover transactions

      firstTime = false;

   }

   protected void createQueue(String queueName, int index) throws Exception
   {
      JMSServerManagerImpl server = jmsServer0;
      if (index == 1)
      {
         server = jmsServer1;
      }
      server.createQueue(queueName, "/queue/" + queueName);
   }

   protected void tearDown() throws Exception
   {
      checkEmpty(sourceQueue, 0);
      checkEmpty(localTargetQueue, 0);
      checkEmpty(targetQueue, 1);

      // Check no subscriptions left lying around

      checkNoSubscriptions(sourceTopic, 0);

      server1.stop();
      server0.stop();

      super.tearDown();
   }

   protected void setUpAdministeredObjects() throws Exception
   {
      cff0 = new ConnectionFactoryFactory()
      {
         public ConnectionFactory createConnectionFactory() throws Exception
         {
            //Note! We disable automatic reconnection on the session factory. The bridge needs to do the reconnection
            return new JBossConnectionFactory(new TransportConfiguration(InVMConnectorFactory.class.getName()),
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
                                              1,
                                              0);
         }

      };

      cf0 = cff0.createConnectionFactory();

      cff1 = new ConnectionFactoryFactory()
      {

         public ConnectionFactory createConnectionFactory() throws Exception
         {
            //Note! We disable automatic reconnection on the session factory. The bridge needs to do the reconnection
            return new JBossConnectionFactory(new TransportConfiguration(InVMConnectorFactory.class.getName(), params1),
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
                                              1,
                                              0);
         }
      };

      cf1 = cff1.createConnectionFactory();

      sourceQueueFactory = new DestinationFactory()
      {
         public Destination createDestination() throws Exception
         {
            return (Destination)context0.lookup("/queue/sourceQueue");
         }
      };

      sourceQueue = (Queue)sourceQueueFactory.createDestination();

      targetQueueFactory = new DestinationFactory()
      {
         public Destination createDestination() throws Exception
         {
            return (Destination)context1.lookup("/queue/targetQueue");
         }
      };

      targetQueue = (Queue)targetQueueFactory.createDestination();

      sourceTopicFactory = new DestinationFactory()
      {
         public Destination createDestination() throws Exception
         {
            return (Destination)context0.lookup("/topic/sourceTopic");
         }
      };

      sourceTopic = (Topic)sourceTopicFactory.createDestination();

      localTargetQueueFactory = new DestinationFactory()
      {
         public Destination createDestination() throws Exception
         {
            return (Destination)context0.lookup("/queue/localTargetQueue");
         }
      };

      localTargetQueue = (Queue)localTargetQueueFactory.createDestination();
   }

   protected void sendMessages(ConnectionFactory cf, Destination dest, int start, int numMessages, boolean persistent) throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sess.createProducer(dest);

         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         for (int i = start; i < start + numMessages; i++)
         {
            TextMessage tm = sess.createTextMessage("message" + i);

            prod.send(tm);
         }
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   protected void checkMessagesReceived(ConnectionFactory cf,
                                        Destination dest,
                                        QualityOfServiceMode qosMode,
                                        int numMessages,
                                        boolean longWaitForFirst) throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();

         conn.start();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = sess.createConsumer(dest);

         // Consume the messages

         Set msgs = new HashSet();

         int count = 0;

         // We always wait longer for the first one - it may take some time to arrive especially if we are
         // waiting for recovery to kick in
         while (true)
         {
            TextMessage tm = (TextMessage)cons.receive(count == 0 ? (longWaitForFirst ? 60000 : 10000) : 5000);

            if (tm == null)
            {
               break;
            }

            // log.info("Got message " + tm.getText());

            msgs.add(tm.getText());

            count++;

         }

         if (qosMode == QualityOfServiceMode.ONCE_AND_ONLY_ONCE || qosMode == QualityOfServiceMode.DUPLICATES_OK)
         {
            // All the messages should be received

            for (int i = 0; i < numMessages; i++)
            {
               assertTrue("" + i, msgs.contains("message" + i));
            }

            // Should be no more
            if (qosMode == QualityOfServiceMode.ONCE_AND_ONLY_ONCE)
            {
               assertEquals(numMessages, msgs.size());
            }
         }
         else if (qosMode == QualityOfServiceMode.AT_MOST_ONCE)
         {
            // No *guarantee* that any messages will be received
            // but you still might get some depending on how/where the crash occurred
         }

         log.trace("Check complete");

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   protected void checkAllMessageReceivedInOrder(ConnectionFactory cf, Destination dest, int start, int numMessages) throws Exception
   {
      Connection conn = null;
      try
      {
         conn = cf.createConnection();

         conn.start();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = sess.createConsumer(dest);

         // Consume the messages

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage tm = (TextMessage)cons.receive(30000);

            assertNotNull(tm);

            // log.info("Got message " + tm.getText());

            assertEquals("message" + (i + start), tm.getText());
         }
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public boolean checkEmpty(Queue queue, int index) throws Exception
   {
      ManagementService managementService = server0.getServer().getManagementService();
      if (index == 1)
      {
         managementService = server1.getServer().getManagementService();
      }
      ObjectName objectName = ObjectNames.getJMSQueueObjectName(queue.getQueueName());
      JMSQueueControlMBean queueControl = (JMSQueueControlMBean)managementService.getResource(objectName);

      Integer messageCount = queueControl.getMessageCount();

      if (messageCount > 0)
      {
         queueControl.removeAllMessages();
      }
      return true;
   }

   protected void checkNoSubscriptions(Topic topic, int index) throws Exception
   {
      ManagementService managementService = server0.getServer().getManagementService();
      if (index == 1)
      {
         managementService = server1.getServer().getManagementService();
      }
      ObjectName objectName = ObjectNames.getJMSTopicObjectName(topic.getTopicName());
      TopicControlMBean topicControl = (TopicControlMBean)managementService.getResource(objectName);
      assertEquals(0, topicControl.getSubcriptionsCount());

   }

   protected void removeAllMessages(String queueName, int index) throws Exception
   {
      ManagementService managementService = server0.getServer().getManagementService();
      if (index == 1)
      {
         managementService = server1.getServer().getManagementService();
      }
      ObjectName objectName = ObjectNames.getJMSQueueObjectName(queueName);
      JMSQueueControlMBean queueControl = (JMSQueueControlMBean)managementService.getResource(objectName);
      queueControl.removeAllMessages();
   }

   protected TransactionManager newTransactionManager()
   {
      return new TransactionManagerImple();
   }

   // Inner classes -------------------------------------------------------------------

}
