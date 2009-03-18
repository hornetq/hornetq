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

package org.jboss.messaging.tests.integration.jms.management;

import static org.jboss.messaging.core.config.impl.ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS;

import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.management.ObjectNames;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.management.JMSQueueControlMBean;

/**
 * A JMSQueueControlUsingJMSTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class JMSQueueControlUsingJMSTest extends JMSQueueControlTest
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
   protected JMSQueueControlMBean createManagementControl() throws Exception
   {
      JBossQueue managementQueue = new JBossQueue(DEFAULT_MANAGEMENT_ADDRESS.toString(),
                     DEFAULT_MANAGEMENT_ADDRESS.toString());
      final JMSMessagingProxy proxy = new JMSMessagingProxy(session,
                                                            managementQueue,
                                                            ObjectNames.getJMSQueueObjectName(queue.getQueueName()));
      
      return new JMSQueueControlMBean()
      {

         public boolean changeMessagePriority(String messageID, int newPriority) throws Exception
         {
            return (Boolean)proxy.invokOperation("changeMessagePriority", messageID, newPriority);
         }

         public int countMessages(String filter) throws Exception
         {
            return (Integer)proxy.invokOperation("countMessages", filter);
         }

         public boolean expireMessage(String messageID) throws Exception
         {
            return (Boolean)proxy.invokOperation("expireMessage", messageID);
         }

         public int expireMessages(String filter) throws Exception
         {
            return (Integer)proxy.invokOperation("expireMessages", filter);
         }

         public int getConsumerCount()
         {
            return (Integer)proxy.retriveAttributeValue("ConsumerCount");
         }

         public String getDeadLetterAddress()
         {
            return (String)proxy.retriveAttributeValue("DeadLetterAddress");
         }

         public int getDeliveringCount()
         {
            return (Integer)proxy.retriveAttributeValue("DeliveringCount");
         }

         public String getExpiryAddress()
         {
            return (String)proxy.retriveAttributeValue("ExpiryAddress");
         }

         public int getMessageCount()
         {
            return (Integer)proxy.retriveAttributeValue("MessageCount");
         }

         public int getMessagesAdded()
         {
            return (Integer)proxy.retriveAttributeValue("MessagesAdded");
         }

         public String getName()
         {
            return (String)proxy.retriveAttributeValue("Name");
         }

         public long getScheduledCount()
         {
            return (Long)proxy.retriveAttributeValue("ScheduledCount");
         }

         public boolean isDurable()
         {
            return (Boolean)proxy.retriveAttributeValue("Durable");
         }

         public boolean isTemporary()
         {
            return (Boolean)proxy.retriveAttributeValue("Temporary");
         }

         public TabularData listAllMessages() throws Exception
         {
            return (TabularData)proxy.invokOperation("listAllMessages");
         }

         public CompositeData listMessageCounter() throws Exception
         {
            return (CompositeData)proxy.invokOperation("listMessageCounter");
         }

         public String listMessageCounterAsHTML() throws Exception
         {
            return (String)proxy.invokOperation("listMessageCounterAsHTML");
         }

         public TabularData listMessageCounterHistory() throws Exception
         {
            return (TabularData)proxy.invokOperation("listMessageCounterHistory");
         }

         public String listMessageCounterHistoryAsHTML() throws Exception
         {
            return (String)proxy.invokOperation("listMessageCounterHistoryAsHTML");
         }

         public TabularData listMessages(String filter) throws Exception
         {
            return (TabularData)proxy.invokOperation("listMessages", filter);
         }

         public int moveAllMessages(String otherQueueName) throws Exception
         {
            return (Integer)proxy.invokOperation("moveAllMessages", otherQueueName);
         }

         public int moveMatchingMessages(String filter, String otherQueueName) throws Exception
         {
            return (Integer)proxy.invokOperation("moveMatchingMessages", filter, otherQueueName);
         }

         public boolean moveMessage(String messageID, String otherQueueName) throws Exception
         {
            return (Boolean)proxy.invokOperation("moveMessage", messageID, otherQueueName);
         }

         public int removeMatchingMessages(String filter) throws Exception
         {
            return (Integer)proxy.invokOperation("removeMatchingMessages", filter);
         }

         public boolean removeMessage(String messageID) throws Exception
         {
            return (Boolean)proxy.invokOperation("removeMessage", messageID);
         }

         public boolean sendMessageToDLQ(String messageID) throws Exception
         {
            return (Boolean)proxy.invokOperation("sendMessageToDLQ", messageID);
         }

         public void setDeadLetterAddress(String deadLetterAddress) throws Exception
         {
            proxy.invokOperation("setDeadLetterAddress", deadLetterAddress);
         }

         public void setExpiryAddress(String expiryAddress) throws Exception
         {
            proxy.invokOperation("setExpiryAddress", expiryAddress);
         }

         public String getAddress()
         {
            return (String)proxy.retriveAttributeValue("Address");
         }

         public String getJNDIBinding()
         {
            return (String)proxy.retriveAttributeValue("JNDIBinding");
         }

         public int removeAllMessages() throws Exception
         {
            return (Integer)proxy.invokOperation("removeAllMessages");
         }
         
      };

     
   }
   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
