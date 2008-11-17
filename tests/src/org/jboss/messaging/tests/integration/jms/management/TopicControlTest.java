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

import javax.jms.Topic;
import javax.management.MBeanServerInvocationHandler;

import junit.framework.TestCase;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.jms.JBossTopic;
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
   
   // Constructors --------------------------------------------------

   public TopicControlTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testGetXXXSubscriptionsCount() throws Exception
   {
      // 1 non-durable subscriber, 2 durable subscribers
      JMSUtil.createConsumer(topic, false);
      JMSUtil.createDurableSubscriber(topic, clientID, subscriptionName);
      JMSUtil.createDurableSubscriber(topic, clientID, subscriptionName + "2");

      TopicControlMBean topicControl = createTopicControl(topic);
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

      TopicControlMBean topicControl = createTopicControl(topic);

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

      TopicControlMBean topicControl = createTopicControl(topic);
      assertEquals(3, topicControl.listAllSubscriptionInfos().length);
      assertEquals(1, topicControl.listNonDurableSubscriptionInfos().length);
      assertEquals(2, topicControl.listDurableSubscriptionInfos().length);
   }
   
   public void testDropDurableSubscriptionWithExistingSubscription() throws Exception
   {
      JMSUtil.createDurableSubscriber(topic, clientID, subscriptionName);

      TopicControlMBean topicControl = createTopicControl(topic);
      assertEquals(1, topicControl.getDurableSubcriptionsCount());

      topicControl.dropDurableSubscription(clientID, subscriptionName);

      assertEquals(0, topicControl.getDurableSubcriptionsCount());
   }

   public void testDropDurableSubscriptionWithUnknownSubscription() throws Exception
   {
      JMSUtil.createDurableSubscriber(topic, clientID, subscriptionName);

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
      JMSUtil.createConsumer(topic, true);
      JMSUtil.createDurableSubscriber(topic, clientID, subscriptionName);
      JMSUtil.createDurableSubscriber(topic, clientID, subscriptionName + "2");

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
