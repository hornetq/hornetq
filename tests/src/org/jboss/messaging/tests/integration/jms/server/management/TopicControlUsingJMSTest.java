/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.management.ResourceNames;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.management.TopicControlMBean;

/**
 * A JMSQueueControlUsingJMSTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class TopicControlUsingJMSTest extends TopicControlTest
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private QueueConnection connection;

   private QueueSession session;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // JMSServerControlTest overrides --------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      JBossConnectionFactory cf = new JBossConnectionFactory(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      connection = cf.createQueueConnection();
      session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      connection.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      connection.close();

      super.tearDown();
   }

   @Override
   protected TopicControlMBean createManagementControl() throws Exception
   {
      JBossQueue managementQueue = new JBossQueue(DEFAULT_MANAGEMENT_ADDRESS.toString(),
                                                  DEFAULT_MANAGEMENT_ADDRESS.toString());
      final JMSMessagingProxy proxy = new JMSMessagingProxy(session,
                                                            managementQueue,
                                                            ResourceNames.JMS_TOPIC + topic.getTopicName());

      return new TopicControlMBean()
      {
         public int countMessagesForSubscription(String clientID, String subscriptionName, String filter) throws Exception
         {
            return (Integer)proxy.invokeOperation("countMessagesForSubscription", clientID, subscriptionName, filter);
         }

         public void dropAllSubscriptions() throws Exception
         {
            proxy.invokeOperation("dropAllSubscriptions");
         }

         public void dropDurableSubscription(String clientID, String subscriptionName) throws Exception
         {
            proxy.invokeOperation("dropDurableSubscription", clientID, subscriptionName);
         }

         public int getDurableMessagesCount()
         {
            return (Integer)proxy.retrieveAttributeValue("DurableMessagesCount");
         }

         public int getDurableSubcriptionsCount()
         {
            return (Integer)proxy.retrieveAttributeValue("DurableSubcriptionsCount");
         }

         public int getNonDurableMessagesCount()
         {
            return (Integer)proxy.retrieveAttributeValue("NonDurableMessagesCount");
         }

         public int getNonDurableSubcriptionsCount()
         {
            return (Integer)proxy.retrieveAttributeValue("NonDurableSubcriptionsCount");
         }

         public int getSubcriptionsCount()
         {
            return (Integer)proxy.retrieveAttributeValue("SubcriptionsCount");
         }

         public TabularData listAllSubscriptions() throws Exception
         {
            return (TabularData)proxy.invokeOperation("listAllSubscriptions");
         }

         public TabularData listDurableSubscriptions() throws Exception
         {
            return (TabularData)proxy.invokeOperation("listDurableSubscriptions");
         }

         public TabularData listMessagesForSubscription(String queueName) throws Exception
         {
            return (TabularData)proxy.invokeOperation("listMessagesForSubscription", queueName);
         }

         public TabularData listNonDurableSubscriptions() throws Exception
         {
            return (TabularData)proxy.invokeOperation("listNonDurableSubscriptions");
         }

         public String getAddress()
         {
            return (String)proxy.retrieveAttributeValue("Address");
         }

         public String getJNDIBinding()
         {
            return (String)proxy.retrieveAttributeValue("JNDIBinding");
         }

         public int getMessageCount() throws Exception
         {
            return (Integer)proxy.retrieveAttributeValue("MessageCount");
         }

         public String getName()
         {
            return (String)proxy.retrieveAttributeValue("Name");
         }

         public boolean isTemporary()
         {
            return (Boolean)proxy.retrieveAttributeValue("Temporary");
         }

         public int removeAllMessages() throws Exception
         {
            return (Integer)proxy.invokeOperation("removeAllMessages");
         }

      };
   }
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
