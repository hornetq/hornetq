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

package org.jboss.messaging.tests.integration.jms.server.management;

import static org.jboss.messaging.core.config.impl.ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import javax.jms.Connection;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TopicSubscriber;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.management.ResourceNames;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.tests.integration.management.ManagementTestBase;

public class TopicControlUsingJMSTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingServer server;

   private JMSServerManagerImpl serverManager;

   private String clientID;

   private String subscriptionName;

   protected JBossTopic topic;

   protected JMSMessagingProxy proxy;

   private QueueConnection connection;

   private QueueSession session;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetAttributes() throws Exception
   {
      assertEquals(topic.getTopicName(), proxy.retrieveAttributeValue("Name"));
      assertEquals(topic.getAddress(), proxy.retrieveAttributeValue("Address"));
      assertEquals(topic.isTemporary(), proxy.retrieveAttributeValue("Temporary"));
      assertEquals(topic.getName(), proxy.retrieveAttributeValue("JNDIBinding"));
   }

   public void testGetXXXSubscriptionsCount() throws Exception
   {
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());

      // 1 non-durable subscriber, 2 durable subscribers
      JMSUtil.createConsumer(connection_1, topic);

      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_2, topic, clientID, subscriptionName);
      Connection connection_3 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_3, topic, clientID, subscriptionName + "2");

      assertEquals(3, proxy.retrieveAttributeValue("SubscriptionCount"));
      assertEquals(1, proxy.retrieveAttributeValue("NonDurableSubscriptionCount"));
      assertEquals(2, proxy.retrieveAttributeValue("DurableSubscriptionCount"));

      connection_1.close();
      connection_2.close();
      connection_3.close();
   }

   public void testGetXXXMessagesCount() throws Exception
   {
      // 1 non-durable subscriber, 2 durable subscribers
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createConsumer(connection_1, topic);
      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_2, topic, clientID, subscriptionName);
      Connection connection_3 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_3, topic, clientID, subscriptionName + "2");

      assertEquals(0, proxy.retrieveAttributeValue("MessageCount"));
      assertEquals(0, proxy.retrieveAttributeValue("NonDurableMessageCount"));
      assertEquals(0, proxy.retrieveAttributeValue("DurableMessageCount"));

      JMSUtil.sendMessages(topic, 2);

      assertEquals(3 * 2, proxy.retrieveAttributeValue("MessageCount"));
      assertEquals(1 * 2, proxy.retrieveAttributeValue("NonDurableMessageCount"));
      assertEquals(2 * 2, proxy.retrieveAttributeValue("DurableMessageCount"));

      connection_1.close();
      connection_2.close();
      connection_3.close();
   }

   public void testListXXXSubscriptionsCount() throws Exception
   {
      // 1 non-durable subscriber, 2 durable subscribers
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createConsumer(connection_1, topic);
      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_2, topic, clientID, subscriptionName);
      Connection connection_3 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_3, topic, clientID, subscriptionName + "2");

      assertEquals(3, ((Object[])proxy.invokeOperation("listAllSubscriptions")).length);
      assertEquals(1, ((Object[])proxy.invokeOperation("listNonDurableSubscriptions")).length);
      assertEquals(2, ((Object[])proxy.invokeOperation("listDurableSubscriptions")).length);

      connection_1.close();
      connection_2.close();
      connection_3.close();
   }

   public void testCountMessagesForSubscription() throws Exception
   {
      String key = "key";
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection, topic, clientID, subscriptionName);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      JMSUtil.sendMessageWithProperty(session, topic, key, matchingValue);
      JMSUtil.sendMessageWithProperty(session, topic, key, unmatchingValue);
      JMSUtil.sendMessageWithProperty(session, topic, key, matchingValue);

      assertEquals(3, proxy.retrieveAttributeValue("MessageCount"));

      assertEquals(2, proxy.invokeOperation("countMessagesForSubscription", clientID, subscriptionName, key + " =" +
                                                                                                        matchingValue));
      assertEquals(1,
                   proxy.invokeOperation("countMessagesForSubscription", clientID, subscriptionName, key + " =" +
                                                                                                     unmatchingValue));

      connection.close();
   }

   public void testCountMessagesForUnknownSubscription() throws Exception
   {
      String unknownSubscription = randomString();

      try
      {
         proxy.invokeOperation("countMessagesForSubscription", clientID, unknownSubscription, null);
         fail();
      }
      catch (Exception e)
      {
      }
   }

   public void testCountMessagesForUnknownClientID() throws Exception
   {
      String unknownClientID = randomString();

      try
      {
         proxy.invokeOperation("countMessagesForSubscription", unknownClientID, subscriptionName, null);
         fail();
      }
      catch (Exception e)
      {
      }
   }

   public void testDropDurableSubscriptionWithExistingSubscription() throws Exception
   {
      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());

      JMSUtil.createDurableSubscriber(connection, topic, clientID, subscriptionName);

      assertEquals(1, proxy.retrieveAttributeValue("DurableSubscriptionCount"));

      connection.close();

      proxy.invokeOperation("dropDurableSubscription", clientID, subscriptionName);

      assertEquals(0, proxy.retrieveAttributeValue("DurableSubscriptionCount"));
   }

   public void testDropDurableSubscriptionWithUnknownSubscription() throws Exception
   {
      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());

      JMSUtil.createDurableSubscriber(connection, topic, clientID, subscriptionName);

      assertEquals(1, proxy.retrieveAttributeValue("DurableSubscriptionCount"));

      try
      {
         proxy.invokeOperation("dropDurableSubscription", clientID, "this subscription does not exist");
         fail("should throw an exception");
      }
      catch (Exception e)
      {

      }

      assertEquals(1, proxy.retrieveAttributeValue("DurableSubscriptionCount"));

      connection.close();
   }

   public void testDropAllSubscriptions() throws Exception
   {
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      TopicSubscriber durableSubscriber_1 = JMSUtil.createDurableSubscriber(connection_1,
                                                                            topic,
                                                                            clientID,
                                                                            subscriptionName);
      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      TopicSubscriber durableSubscriber_2 = JMSUtil.createDurableSubscriber(connection_2,
                                                                            topic,
                                                                            clientID,
                                                                            subscriptionName + "2");

      assertEquals(2, proxy.retrieveAttributeValue("SubscriptionCount"));

      durableSubscriber_1.close();
      durableSubscriber_2.close();

      assertEquals(2, proxy.retrieveAttributeValue("SubscriptionCount"));
      proxy.invokeOperation("dropAllSubscriptions");

      assertEquals(0, proxy.retrieveAttributeValue("SubscriptionCount"));

      connection_1.close();
      connection_2.close();
   }

   public void testRemoveAllMessages() throws Exception
   {
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_1, topic, clientID, subscriptionName);
      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_2, topic, clientID, subscriptionName + "2");

      JMSUtil.sendMessages(topic, 3);

      assertEquals(3 * 2, proxy.retrieveAttributeValue("MessageCount"));

      int removedCount = (Integer)proxy.invokeOperation("removeAllMessages");
      assertEquals(3 * 2, removedCount);
      assertEquals(0, proxy.retrieveAttributeValue("MessageCount"));

      connection_1.close();
      connection_2.close();
   }

   public void testListMessagesForSubscription() throws Exception
   {
      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());

      JMSUtil.createDurableSubscriber(connection, topic, clientID, subscriptionName);

      JMSUtil.sendMessages(topic, 3);

      Object[] data = (Object[])proxy.invokeOperation("listMessagesForSubscription",
                                                      JBossTopic.createQueueNameForDurableSubscription(clientID,
                                                                                                       subscriptionName));
      assertEquals(3, data.length);
   }

   public void testListMessagesForSubscriptionWithUnknownClientID() throws Exception
   {
      String unknownClientID = randomString();

      try
      {
         proxy.invokeOperation("listMessagesForSubscription",
                               JBossTopic.createQueueNameForDurableSubscription(unknownClientID, subscriptionName));
         fail();
      }
      catch (Exception e)
      {
      }
   }

   public void testListMessagesForSubscriptionWithUnknownSubscription() throws Exception
   {
      String unknownSubscription = randomString();

      try
      {
         proxy.invokeOperation("listMessagesForSubscription",
                               JBossTopic.createQueueNameForDurableSubscription(clientID, unknownSubscription));
         fail();
      }
      catch (Exception e)
      {
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations()
          .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      server = Messaging.newMessagingServer(conf, mbeanServer, false);
      server.start();

      serverManager = new JMSServerManagerImpl(server);
      serverManager.start();
      serverManager.setContext(new NullInitialContext());
      serverManager.activated();

      clientID = randomString();
      subscriptionName = randomString();

      String topicName = randomString();
      serverManager.createTopic(topicName, topicName);
      topic = new JBossTopic(topicName);

      JBossConnectionFactory cf = new JBossConnectionFactory(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      connection = cf.createQueueConnection();
      session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      connection.start();

      JBossQueue managementQueue = new JBossQueue(DEFAULT_MANAGEMENT_ADDRESS.toString(),
                                                  DEFAULT_MANAGEMENT_ADDRESS.toString());
      proxy = new JMSMessagingProxy(session, managementQueue, ResourceNames.JMS_TOPIC + topic.getTopicName());
   }

   @Override
   protected void tearDown() throws Exception
   {
      connection.close();

      server.stop();

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
