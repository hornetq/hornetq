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

import static org.jboss.messaging.tests.integration.management.ManagementControlHelper.createTopicControl;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import javax.jms.Connection;
import javax.jms.Session;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.jms.server.management.TopicControlMBean;
import org.jboss.messaging.tests.integration.management.ManagementTestBase;
import org.jboss.messaging.tests.util.UnitTestCase;

/**
 * A TopicControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 13 nov. 2008 16:50:53
 *
 *
 */
public class TopicControlTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingService service;

   private JMSServerManagerImpl serverManager;

   private String clientID;

   private String subscriptionName;

   protected JBossTopic topic;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetAttributes() throws Exception
   {
      TopicControlMBean topicControl = createManagementControl();

      assertEquals(topic.getTopicName(), topicControl.getName());
      assertEquals(topic.getAddress(), topicControl.getAddress());
      assertEquals(topic.isTemporary(), topicControl.isTemporary());
      assertEquals(topic.getName(), topicControl.getJNDIBinding());
   }
   
   public void testGetXXXSubscriptionsCount() throws Exception
   {
      // 1 non-durable subscriber, 2 durable subscribers
      JMSUtil.createConsumer(topic, false);
      JMSUtil.createDurableSubscriber(topic, clientID, subscriptionName);
      JMSUtil.createDurableSubscriber(topic, clientID, subscriptionName + "2");

      TopicControlMBean topicControl = createManagementControl();
      assertEquals(3, topicControl.getSubcriptionsCount());
      assertEquals(1, topicControl.getNonDurableSubcriptionsCount());
      assertEquals(2, topicControl.getDurableSubcriptionsCount());
   }

   public void testGetXXXMessagesCount() throws Exception
   {
      // 1 non-durable subscriber, 2 durable subscribers
      JMSUtil.createConsumer(topic, true);
      JMSUtil.createDurableSubscriber(topic, clientID, subscriptionName);
      JMSUtil.createDurableSubscriber(topic, clientID, subscriptionName + "2");

      TopicControlMBean topicControl = createManagementControl();

      assertEquals(0, topicControl.getMessageCount());
      assertEquals(0, topicControl.getNonDurableMessagesCount());
      assertEquals(0, topicControl.getDurableMessagesCount());

      JMSUtil.sendMessages(topic, 2);

      assertEquals(3 * 2, topicControl.getMessageCount());
      assertEquals(1 * 2, topicControl.getNonDurableMessagesCount());
      assertEquals(2 * 2, topicControl.getDurableMessagesCount());
   }

   public void testListXXXSubscriptionsCount() throws Exception
   {
      // 1 non-durable subscriber, 2 durable subscribers
      JMSUtil.createConsumer(topic, false);
      JMSUtil.createDurableSubscriber(topic, clientID, subscriptionName);
      JMSUtil.createDurableSubscriber(topic, clientID, subscriptionName + "2");

      TopicControlMBean topicControl = createManagementControl();
      assertEquals(3, topicControl.listAllSubscriptions().size());
      assertEquals(1, topicControl.listNonDurableSubscriptions().size());
      assertEquals(2, topicControl.listDurableSubscriptions().size());
   }
   
   public void testCountMessagesForSubscription() throws Exception
   {
      String key = "key";
      long matchingValue = randomLong();
      long unmatchingValue = matchingValue + 1;

      JMSUtil.createDurableSubscriber(topic, clientID, subscriptionName);
      
      Connection connection = JMSUtil.createConnection(InVMConnectorFactory.class.getName());
      connection.setClientID(clientID);
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      JMSUtil.sendMessageWithProperty(session, topic, key, matchingValue);
      JMSUtil.sendMessageWithProperty(session, topic, key, unmatchingValue);
      JMSUtil.sendMessageWithProperty(session, topic, key, matchingValue);
      
      TopicControlMBean topicControl = createManagementControl();

      assertEquals(3, topicControl.getMessageCount());

      assertEquals(2, topicControl.countMessagesForSubscription(clientID, subscriptionName, key + " =" + matchingValue));
      assertEquals(1, topicControl.countMessagesForSubscription(clientID, subscriptionName, key + " =" + unmatchingValue));
   }

   public void testCountMessagesForUnknownSubscription() throws Exception
   {
      String unknownSubscription = randomString();

      TopicControlMBean topicControl = createManagementControl();

      try
      {
         topicControl.countMessagesForSubscription(clientID, unknownSubscription, null);
         fail();
      }
      catch (Exception e)
      {
      }
   }
   
   public void testCountMessagesForUnknownClientID() throws Exception
   {
      String unknownClientID = randomString();

      TopicControlMBean topicControl = createManagementControl();

      try
      {
         topicControl.countMessagesForSubscription(unknownClientID, subscriptionName, null);
         fail();
      }
      catch (Exception e)
      {
      }
   }

   public void testDropDurableSubscriptionWithExistingSubscription() throws Exception
   {
      JMSUtil.createDurableSubscriber(topic, clientID, subscriptionName);

      TopicControlMBean topicControl = createManagementControl();
      assertEquals(1, topicControl.getDurableSubcriptionsCount());

      topicControl.dropDurableSubscription(clientID, subscriptionName);

      assertEquals(0, topicControl.getDurableSubcriptionsCount());
   }

   public void testDropDurableSubscriptionWithUnknownSubscription() throws Exception
   {
      JMSUtil.createDurableSubscriber(topic, clientID, subscriptionName);

      TopicControlMBean topicControl = createManagementControl();
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
      JMSUtil.createConsumer(topic, true);
      JMSUtil.createDurableSubscriber(topic, clientID, subscriptionName);
      JMSUtil.createDurableSubscriber(topic, clientID, subscriptionName + "2");

      TopicControlMBean topicControl = createManagementControl();
      assertEquals(3, topicControl.getSubcriptionsCount());

      topicControl.dropAllSubscriptions();

      assertEquals(0, topicControl.getSubcriptionsCount());
   }
   
   public void testRemoveAllMessages() throws Exception
   {
      JMSUtil.createDurableSubscriber(topic, clientID, subscriptionName);
      JMSUtil.createDurableSubscriber(topic, clientID, subscriptionName + "2");
      
      JMSUtil.sendMessages(topic, 3);
      
      TopicControlMBean topicControl = createManagementControl();
      assertEquals(3  * 2, topicControl.getMessageCount());

      int removedCount = topicControl.removeAllMessages();
      assertEquals(3 * 2, removedCount);
      assertEquals(0, topicControl.getMessageCount());
   }
   
   public void testListMessagesForSubscription() throws Exception
   {
      JMSUtil.createDurableSubscriber(topic, clientID, subscriptionName);

      JMSUtil.sendMessages(topic, 3);

      TopicControlMBean topicControl = createManagementControl();      
      TabularData tabularData = topicControl.listMessagesForSubscription(JBossTopic.createQueueNameForDurableSubscription(clientID, subscriptionName));
      assertEquals(3, tabularData.size());
   }
   
   public void testListMessagesForSubscriptionWithUnknownClientID() throws Exception
   {
      String unknownClientID = randomString();
      
      TopicControlMBean topicControl = createManagementControl();      

      try
      {
         topicControl.listMessagesForSubscription(JBossTopic.createQueueNameForDurableSubscription(unknownClientID, subscriptionName));
         fail();
      }
      catch (Exception e)
      {
      }
   }

   public void testListMessagesForSubscriptionWithUnknownSubscription() throws Exception
   {
      String unknownSubscription = randomString();
      
      TopicControlMBean topicControl = createManagementControl();      

      try
      {
         topicControl.listMessagesForSubscription(JBossTopic.createQueueNameForDurableSubscription(clientID, unknownSubscription));
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
      service = Messaging.newNullStorageMessagingService(conf, mbeanServer);
      service.start();

      serverManager = JMSServerManagerImpl.newJMSServerManagerImpl(service.getServer());
      serverManager.start();
      serverManager.setContext(new NullInitialContext());

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
   
   protected TopicControlMBean createManagementControl() throws Exception
   {
      return createTopicControl(topic, mbeanServer);
   }


   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
