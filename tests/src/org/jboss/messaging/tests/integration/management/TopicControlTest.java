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

package org.jboss.messaging.tests.integration.management;

import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.lang.management.ManagementFactory;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;
import javax.management.MBeanServerInvocationHandler;

import org.jboss.messaging.jms.server.management.TopicControlMBean;
import org.jboss.messaging.jms.server.management.impl.JMSManagementServiceImpl;
import org.jboss.test.messaging.jms.JMSTestCase;

/**
 * A TopicControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 13 nov. 2008 16:50:53
 *
 *
 */
public class TopicControlTest extends JMSTestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private String clientID;

   private String subscriptionName;

   // Static --------------------------------------------------------

   private static void assertDurableSubscriptions(Topic topic, int expectedCount) throws Exception
   {
      TopicControlMBean topicControl = (TopicControlMBean)MBeanServerInvocationHandler.newProxyInstance(ManagementFactory.getPlatformMBeanServer(),
                                                                                                        JMSManagementServiceImpl.getJMSTopicObjectName(topic.getTopicName()),
                                                                                                        TopicControlMBean.class,
                                                                                                        false);
     assertEquals(expectedCount, topicControl.getDurableSubcriptionsCount());
   }
   
   // Constructors --------------------------------------------------

   public TopicControlTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testDropDurableSubscriptionWithExistingSubscription() throws Exception
   {
      createDurableSubscriber(clientID, subscriptionName);

      assertDurableSubscriptions(topic1, 1);
      
      TopicControlMBean topicControl = (TopicControlMBean)MBeanServerInvocationHandler.newProxyInstance(ManagementFactory.getPlatformMBeanServer(),
                                                                                                        JMSManagementServiceImpl.getJMSTopicObjectName(topic1.getTopicName()),
                                                                                                        TopicControlMBean.class,
                                                                                                        false);
      topicControl.dropDurableSubscription(clientID, subscriptionName);
      
      assertDurableSubscriptions(topic1, 0);
   }

   public void testDropDurableSubscriptionWithUnknownSubscription() throws Exception
   {
      createDurableSubscriber(clientID, subscriptionName);

      assertDurableSubscriptions(topic1, 1);

      TopicControlMBean topicControl = (TopicControlMBean)MBeanServerInvocationHandler.newProxyInstance(ManagementFactory.getPlatformMBeanServer(),
                                                                                                        JMSManagementServiceImpl.getJMSTopicObjectName(topic1.getTopicName()),
                                                                                                        TopicControlMBean.class,
                                                                                                        false);
      try
      {
         topicControl.dropDurableSubscription(clientID, "this subscription does not exist");
         fail("should throw an exception");
      }
      catch (Exception e)
      {

      }

      assertDurableSubscriptions(topic1, 1);
   }

   public void testDropAllSubscriptions() throws Exception
   {
      createDurableSubscriber(clientID, subscriptionName);
      createDurableSubscriber(clientID, subscriptionName + "2");
      
      assertDurableSubscriptions(topic1, 2);

      TopicControlMBean topicControl = (TopicControlMBean)MBeanServerInvocationHandler.newProxyInstance(ManagementFactory.getPlatformMBeanServer(),
                                                                                                        JMSManagementServiceImpl.getJMSTopicObjectName(topic1.getTopicName()),
                                                                                                        TopicControlMBean.class,
                                                                                                        false);
      
      topicControl.dropAllSubscriptions();

      assertDurableSubscriptions(topic1, 0);
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      clientID = randomString();
      subscriptionName = randomString();
   }
   
   @Override
   protected void tearDown() throws Exception
   {
      destroyAllSubscriptions();
      
      super.tearDown();
   }
   
   // Private -------------------------------------------------------

   private void createDurableSubscriber(String clientID, String subscriptionName) throws JMSException
   {
      Connection conn = cf.createConnection();

      conn.setClientID(clientID);
      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      s.createDurableSubscriber(topic1, subscriptionName);

   }
   
   private void destroyAllSubscriptions() throws Exception
   {
      // we leave the server in a clean state wrt to subscriptions
      TopicControlMBean topicControl = (TopicControlMBean)MBeanServerInvocationHandler.newProxyInstance(ManagementFactory.getPlatformMBeanServer(),
                                                                                                        JMSManagementServiceImpl.getJMSTopicObjectName(topic1.getTopicName()),
                                                                                                        TopicControlMBean.class,
                                                                                                        false);
      
      topicControl.dropAllSubscriptions();
   }


   // Inner classes -------------------------------------------------

}
