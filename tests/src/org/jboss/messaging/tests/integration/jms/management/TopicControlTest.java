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

import java.lang.management.ManagementFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.management.MBeanServerInvocationHandler;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.jms.server.management.TopicControlMBean;
import org.jboss.messaging.jms.server.management.impl.JMSManagementServiceImpl;

/**
 * A TopicControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 13 nov. 2008 16:50:53
 *
 *
 */
public class TopicControlTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingService service;

   private JMSServerManagerImpl serverManager;

   private String clientID;

   private String subscriptionName;

   private Topic topic;

   // Static --------------------------------------------------------

   private static TopicControlMBean createTopicControl(Topic topic) throws Exception
   {
      TopicControlMBean topicControl = (TopicControlMBean)MBeanServerInvocationHandler.newProxyInstance(ManagementFactory.getPlatformMBeanServer(),
                                                                                                        JMSManagementServiceImpl.getJMSTopicObjectName(topic.getTopicName()),
                                                                                                        TopicControlMBean.class,
                                                                                                        false);
      return topicControl;
   }

   private static TopicSubscriber createDurableSubscriber(Topic topic, String clientID, String subscriptionName) throws JMSException
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
                                                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                                             true,
                                                             ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP,
                                                             ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS);

      Connection conn = cf.createConnection();

      conn.setClientID(clientID);
      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      return s.createDurableSubscriber(topic, subscriptionName);
   }

   private static TopicSubscriber createSubscriber(Topic topic) throws JMSException
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
                                                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                                             true,
                                                             ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP,
                                                             ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS);

      TopicConnection conn = cf.createTopicConnection();

      TopicSession s = conn.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);

      return s.createSubscriber(topic);
   }

   private static void sendMessages(Topic topic, int messagesToSend) throws Exception
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
                                                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                                             ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                                             true,
                                                             ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP,
                                                             ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS);

      Connection conn = cf.createConnection();

      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = s.createProducer(topic);

      for (int i = 0; i < messagesToSend; i++)
      {
         producer.send(s.createTextMessage(randomString()));
      }
   }
   
   // Constructors --------------------------------------------------

   public TopicControlTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testGetXXXSubscriptionsCount() throws Exception
   {
      // 1 non-durable subscriber, 2 durable subscribers
      createSubscriber(topic);
      createDurableSubscriber(topic, clientID, subscriptionName);
      createDurableSubscriber(topic, clientID, subscriptionName + "2");

      TopicControlMBean topicControl = createTopicControl(topic);
      assertEquals(3, topicControl.getSubcriptionsCount());
      assertEquals(1, topicControl.getNonDurableSubcriptionsCount());
      assertEquals(2, topicControl.getDurableSubcriptionsCount());
   }

   public void testGetXXXMessagesCount() throws Exception
   {
      // 1 non-durable subscriber, 2 durable subscribers
      createSubscriber(topic);
      createDurableSubscriber(topic, clientID, subscriptionName);
      createDurableSubscriber(topic, clientID, subscriptionName + "2");

      TopicControlMBean topicControl = createTopicControl(topic);

      assertEquals(0, topicControl.getMessageCount());
      assertEquals(0, topicControl.getNonDurableMessagesCount());
      assertEquals(0, topicControl.getDurableMessagesCount());

      sendMessages(topic, 2);

      assertEquals(3 * 2, topicControl.getMessageCount());
      assertEquals(1 * 2, topicControl.getNonDurableMessagesCount());
      assertEquals(2 * 2, topicControl.getDurableMessagesCount());
   }

   public void testDropDurableSubscriptionWithExistingSubscription() throws Exception
   {
      createDurableSubscriber(topic, clientID, subscriptionName);

      TopicControlMBean topicControl = createTopicControl(topic);
      assertEquals(1, topicControl.getDurableSubcriptionsCount());

      topicControl.dropDurableSubscription(clientID, subscriptionName);

      assertEquals(0, topicControl.getDurableSubcriptionsCount());
   }

   public void testDropDurableSubscriptionWithUnknownSubscription() throws Exception
   {
      createDurableSubscriber(topic, clientID, subscriptionName);

      TopicControlMBean topicControl = createTopicControl(topic);
      assertEquals(1, topicControl.getDurableSubcriptionsCount());

      try
      {
         topicControl.dropDurableSubscription(clientID, "this subscription does not exist");
         fail("should throw an exception");
      }
      catch (Exception e)
      {

      }

      assertEquals(1, topicControl.getDurableSubcriptionsCount());
   }

   public void testDropAllSubscriptions() throws Exception
   {
      createSubscriber(topic);
      createDurableSubscriber(topic, clientID, subscriptionName);
      createDurableSubscriber(topic, clientID, subscriptionName + "2");

      TopicControlMBean topicControl = createTopicControl(topic);
      assertEquals(3, topicControl.getSubcriptionsCount());

      topicControl.dropAllSubscriptions();

      assertEquals(0, topicControl.getSubcriptionsCount());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations()
          .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      service = MessagingServiceImpl.newNullStorageMessagingServer(conf);
      service.start();

      serverManager = JMSServerManagerImpl.newJMSServerManagerImpl(service.getServer());
      serverManager.start();
      serverManager.setInitialContext(new NullInitialContext());

      clientID = randomString();
      subscriptionName = randomString();

      String topicName = randomString();
      serverManager.createTopic(topicName, topicName);
      topic = new JBossTopic(topicName);
   }

   @Override
   protected void tearDown() throws Exception
   {
      service.stop();

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
