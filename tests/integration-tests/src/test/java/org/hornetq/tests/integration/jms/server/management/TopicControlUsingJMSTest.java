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
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import static org.hornetq.tests.util.RandomUtil.randomString;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TopicSubscriber;

import org.junit.Assert;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.management.ResourceNames;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.jms.client.HornetQQueue;
import org.hornetq.jms.client.HornetQTopic;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.integration.management.ManagementTestBase;
import org.hornetq.tests.unit.util.InVMNamingContext;
import org.hornetq.tests.util.RandomUtil;

public class TopicControlUsingJMSTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server;

   private JMSServerManagerImpl serverManager;

   private String clientID;

   private String subscriptionName;

   protected HornetQTopic topic;

   protected JMSMessagingProxy proxy;

   private QueueConnection connection;

   private QueueSession session;

   private final String topicBinding = "/topic/" + randomString();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testGetAttributes() throws Exception
   {
      Assert.assertEquals(topic.getTopicName(), proxy.retrieveAttributeValue("name"));
      Assert.assertEquals(topic.getAddress(), proxy.retrieveAttributeValue("address"));
      Assert.assertEquals(topic.isTemporary(), proxy.retrieveAttributeValue("temporary"));
      Object[] bindings = (Object[])proxy.retrieveAttributeValue("JNDIBindings");
      assertEquals(1, bindings.length);
      Assert.assertEquals(topicBinding, bindings[0]);
   }

   @Test
   public void testGetXXXSubscriptionsCount() throws Exception
   {
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());

      // 1 non-durable subscriber, 2 durable subscribers
      JMSUtil.createConsumer(connection_1, topic);

      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_2, topic, clientID, subscriptionName);
      Connection connection_3 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_3, topic, clientID + "2", subscriptionName + "2");

      Assert.assertEquals(3, proxy.retrieveAttributeValue("subscriptionCount"));
      Assert.assertEquals(1, proxy.retrieveAttributeValue("nonDurableSubscriptionCount"));
      Assert.assertEquals(2, proxy.retrieveAttributeValue("durableSubscriptionCount"));

      connection_1.close();
      connection_2.close();
      connection_3.close();
   }

   @Test
   public void testGetXXXMessagesCount() throws Exception
   {
      // 1 non-durable subscriber, 2 durable subscribers
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createConsumer(connection_1, topic);
      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_2, topic, clientID, subscriptionName);
      Connection connection_3 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_3, topic, clientID + "2", subscriptionName + "2");

      Assert.assertEquals(0, proxy.retrieveAttributeValue("messageCount"));
      Assert.assertEquals(0, proxy.retrieveAttributeValue("nonDurableMessageCount"));
      Assert.assertEquals(0, proxy.retrieveAttributeValue("durableMessageCount"));

      JMSUtil.sendMessages(topic, 2);

      Assert.assertEquals(3 * 2, proxy.retrieveAttributeValue("messageCount"));
      Assert.assertEquals(1 * 2, proxy.retrieveAttributeValue("nonDurableMessageCount"));
      Assert.assertEquals(2 * 2, proxy.retrieveAttributeValue("durableMessageCount"));

      connection_1.close();
      connection_2.close();
      connection_3.close();
   }

   @Test
   public void testListXXXSubscriptionsCount() throws Exception
   {
      // 1 non-durable subscriber, 2 durable subscribers
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createConsumer(connection_1, topic);
      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_2, topic, clientID, subscriptionName);
      Connection connection_3 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_3, topic, clientID + "2", subscriptionName + "2");

      Assert.assertEquals(3, ((Object[])proxy.invokeOperation("listAllSubscriptions")).length);
      Assert.assertEquals(1, ((Object[])proxy.invokeOperation("listNonDurableSubscriptions")).length);
      Assert.assertEquals(2, ((Object[])proxy.invokeOperation("listDurableSubscriptions")).length);

      connection_1.close();
      connection_2.close();
      connection_3.close();
   }

   @Test
   public void testCountMessagesForSubscription() throws Exception
   {
      String key = "key";
      long matchingValue = RandomUtil.randomLong();
      long unmatchingValue = matchingValue + 1;

      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection, topic, clientID, subscriptionName);

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      JMSUtil.sendMessageWithProperty(session, topic, key, matchingValue);
      JMSUtil.sendMessageWithProperty(session, topic, key, unmatchingValue);
      JMSUtil.sendMessageWithProperty(session, topic, key, matchingValue);

      Assert.assertEquals(3, proxy.retrieveAttributeValue("messageCount"));

      Assert.assertEquals(2,
                          proxy.invokeOperation("countMessagesForSubscription", clientID, subscriptionName, key + " =" +
                                                                                                            matchingValue));
      Assert.assertEquals(1,
                          proxy.invokeOperation("countMessagesForSubscription", clientID, subscriptionName, key + " =" +
                                                                                                            unmatchingValue));

      connection.close();
   }

   @Test
   public void testCountMessagesForUnknownSubscription() throws Exception
   {
      String unknownSubscription = RandomUtil.randomString();

      try
      {
         proxy.invokeOperation("countMessagesForSubscription", clientID, unknownSubscription, null);
         Assert.fail();
      }
      catch (Exception e)
      {
      }
   }

   @Test
   public void testCountMessagesForUnknownClientID() throws Exception
   {
      String unknownClientID = RandomUtil.randomString();

      try
      {
         proxy.invokeOperation("countMessagesForSubscription", unknownClientID, subscriptionName, null);
         Assert.fail();
      }
      catch (Exception e)
      {
      }
   }

   @Test
   public void testDropDurableSubscriptionWithExistingSubscription() throws Exception
   {
      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());

      JMSUtil.createDurableSubscriber(connection, topic, clientID, subscriptionName);

      Assert.assertEquals(1, proxy.retrieveAttributeValue("durableSubscriptionCount"));

      connection.close();

      proxy.invokeOperation("dropDurableSubscription", clientID, subscriptionName);

      Assert.assertEquals(0, proxy.retrieveAttributeValue("durableSubscriptionCount"));
   }

   @Test
   public void testDropDurableSubscriptionWithUnknownSubscription() throws Exception
   {
      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());

      JMSUtil.createDurableSubscriber(connection, topic, clientID, subscriptionName);

      Assert.assertEquals(1, proxy.retrieveAttributeValue("durableSubscriptionCount"));

      try
      {
         proxy.invokeOperation("dropDurableSubscription", clientID, "this subscription does not exist");
         Assert.fail("should throw an exception");
      }
      catch (Exception e)
      {

      }

      Assert.assertEquals(1, proxy.retrieveAttributeValue("durableSubscriptionCount"));

      connection.close();
   }

   @Test
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
                                                                            clientID + "2",
                                                                            subscriptionName + "2");

      Assert.assertEquals(2, proxy.retrieveAttributeValue("subscriptionCount"));

      durableSubscriber_1.close();
      durableSubscriber_2.close();

      Assert.assertEquals(2, proxy.retrieveAttributeValue("subscriptionCount"));
      proxy.invokeOperation("dropAllSubscriptions");

      Assert.assertEquals(0, proxy.retrieveAttributeValue("subscriptionCount"));

      connection_1.close();
      connection_2.close();
   }

   @Test
   public void testRemoveAllMessages() throws Exception
   {
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_1, topic, clientID, subscriptionName);
      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_2, topic, clientID + "2", subscriptionName + "2");

      JMSUtil.sendMessages(topic, 3);

      Assert.assertEquals(3 * 2, proxy.retrieveAttributeValue("messageCount"));

      int removedCount = (Integer)proxy.invokeOperation("removeMessages", "");
      Assert.assertEquals(3 * 2, removedCount);
      Assert.assertEquals(0, proxy.retrieveAttributeValue("messageCount"));

      connection_1.close();
      connection_2.close();
   }

   @Test
   public void testListMessagesForSubscription() throws Exception
   {
      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());

      JMSUtil.createDurableSubscriber(connection, topic, clientID, subscriptionName);

      JMSUtil.sendMessages(topic, 3);

      Object[] data = (Object[])proxy.invokeOperation("listMessagesForSubscription",
                                                      HornetQDestination.createQueueNameForDurableSubscription(clientID,
                                                                                                               subscriptionName));
      Assert.assertEquals(3, data.length);

      connection.close();
   }

   @Test
   public void testListMessagesForSubscriptionWithUnknownClientID() throws Exception
   {
      String unknownClientID = RandomUtil.randomString();

      try
      {
         proxy.invokeOperation("listMessagesForSubscription",
                               HornetQDestination.createQueueNameForDurableSubscription(unknownClientID,
                                                                                        subscriptionName));
         Assert.fail();
      }
      catch (Exception e)
      {
      }
   }

   @Test
   public void testListMessagesForSubscriptionWithUnknownSubscription() throws Exception
   {
      String unknownSubscription = RandomUtil.randomString();

      try
      {
         proxy.invokeOperation("listMessagesForSubscription",
                               HornetQDestination.createQueueNameForDurableSubscription(clientID, unknownSubscription));
         Assert.fail();
      }
      catch (Exception e)
      {
      }
   }

   @Test
   public void testGetMessagesAdded() throws Exception
   {
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createConsumer(connection_1, topic);
      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_2, topic, clientID, subscriptionName);
      Connection connection_3 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      JMSUtil.createDurableSubscriber(connection_3, topic, clientID + "2", subscriptionName + "2");

      assertEquals(0, proxy.retrieveAttributeValue("messagesAdded"));

      JMSUtil.sendMessages(topic, 2);

      assertEquals(3 * 2, proxy.retrieveAttributeValue("messagesAdded"));

      connection_1.close();
      connection_2.close();
      connection_3.close();
   }

   @Test
   public void testGetMessagesDelivering() throws Exception
   {
      Connection connection_1 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      MessageConsumer cons_1 = JMSUtil.createConsumer(connection_1, topic, Session.CLIENT_ACKNOWLEDGE);
      Connection connection_2 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      MessageConsumer cons_2 = JMSUtil.createDurableSubscriber(connection_2,
                                                               topic,
                                                               clientID,
                                                               subscriptionName,
                                                               Session.CLIENT_ACKNOWLEDGE);
      Connection connection_3 = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      MessageConsumer cons_3 = JMSUtil.createDurableSubscriber(connection_3,
                                                               topic,
                                                               clientID + "2",
                                                               subscriptionName + "2",
                                                               Session.CLIENT_ACKNOWLEDGE);

      assertEquals(0, proxy.retrieveAttributeValue("deliveringCount"));

      JMSUtil.sendMessages(topic, 2);

      assertEquals(0, proxy.retrieveAttributeValue("deliveringCount"));

      connection_1.start();
      connection_2.start();
      connection_3.start();

      Message msg_1 = null;
      Message msg_2 = null;
      Message msg_3 = null;
      for (int i = 0; i < 2; i++)
      {
         msg_1 = cons_1.receive(5000);
         assertNotNull(msg_1);
         msg_2 = cons_2.receive(5000);
         assertNotNull(msg_2);
         msg_3 = cons_3.receive(5000);
         assertNotNull(msg_3);
      }

      assertEquals(3 * 2, proxy.retrieveAttributeValue("deliveringCount"));

      msg_1.acknowledge();
      assertEquals(2 * 2, proxy.retrieveAttributeValue("deliveringCount"));
      msg_2.acknowledge();
      assertEquals(1 * 2, proxy.retrieveAttributeValue("deliveringCount"));
      msg_3.acknowledge();
      assertEquals(0, proxy.retrieveAttributeValue("deliveringCount"));

      connection_1.close();
      connection_2.close();
      connection_3.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = createBasicConfig();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations()
          .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory"));
      server = HornetQServers.newHornetQServer(conf, mbeanServer, false);
      server.start();

      serverManager = new JMSServerManagerImpl(server);
      serverManager.start();
      serverManager.setContext(new InVMNamingContext());
      serverManager.activated();

      clientID = RandomUtil.randomString();
      subscriptionName = RandomUtil.randomString();

      String topicName = RandomUtil.randomString();
      serverManager.createTopic(false, topicName, topicBinding);
      topic = (HornetQTopic)HornetQJMSClient.createTopic(topicName);

      HornetQConnectionFactory cf =
               HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,
                                                                 new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      connection = cf.createQueueConnection();
      session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      connection.start();

      HornetQQueue managementQueue = (HornetQQueue)HornetQJMSClient.createQueue("hornetq.management");
      proxy = new JMSMessagingProxy(session, managementQueue, ResourceNames.JMS_TOPIC + topic.getTopicName());
   }

   @Override
   @After
   public void tearDown() throws Exception
   {

      session.close();

      connection.close();

      serverManager.stop();

      server.stop();

      serverManager = null;

      server = null;

      session = null;

      connection = null;

      proxy = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
