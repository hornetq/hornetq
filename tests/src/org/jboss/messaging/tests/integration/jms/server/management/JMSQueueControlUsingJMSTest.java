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

import java.util.Map;

import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.management.ResourceNames;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.jms.server.management.JMSQueueControl;

/**
 * 
 * A JMSQueueControlUsingJMSTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class JMSQueueControlUsingJMSTest extends JMSQueueControlTest
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private QueueConnection connection;

   private QueueSession session;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

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
   protected JMSQueueControl createManagementControl() throws Exception
   {
      JBossQueue managementQueue = new JBossQueue(DEFAULT_MANAGEMENT_ADDRESS.toString(),
                                                  DEFAULT_MANAGEMENT_ADDRESS.toString());
      final JMSMessagingProxy proxy = new JMSMessagingProxy(session,
                                                            managementQueue,
                                                            ResourceNames.JMS_QUEUE + queue.getQueueName());

      return new JMSQueueControl()
      {
         public boolean changeMessagePriority(String messageID, int newPriority) throws Exception
         {
            return (Boolean)proxy.invokeOperation("changeMessagePriority", messageID, newPriority);
         }

         public int countMessages(String filter) throws Exception
         {
            return (Integer)proxy.invokeOperation("countMessages", filter);
         }

         public boolean expireMessage(String messageID) throws Exception
         {
            return (Boolean)proxy.invokeOperation("expireMessage", messageID);
         }

         public int expireMessages(String filter) throws Exception
         {
            return (Integer)proxy.invokeOperation("expireMessages", filter);
         }

         public int getConsumerCount()
         {
            return (Integer)proxy.retrieveAttributeValue("consumerCount");
         }

         public String getDeadLetterAddress()
         {
            return (String)proxy.retrieveAttributeValue("deadLetterAddress");
         }

         public int getDeliveringCount()
         {
            return (Integer)proxy.retrieveAttributeValue("deliveringCount");
         }

         public String getExpiryAddress()
         {
            return (String)proxy.retrieveAttributeValue("expiryAddress");
         }

         public int getMessageCount()
         {
            return (Integer)proxy.retrieveAttributeValue("messageCount");
         }

         public int getMessagesAdded()
         {
            return (Integer)proxy.retrieveAttributeValue("messagesAdded");
         }

         public String getName()
         {
            return (String)proxy.retrieveAttributeValue("name");
         }

         public long getScheduledCount()
         {
            return (Long)proxy.retrieveAttributeValue("scheduledCount");
         }

         public boolean isDurable()
         {
            return (Boolean)proxy.retrieveAttributeValue("durable");
         }

         public boolean isTemporary()
         {
            return (Boolean)proxy.retrieveAttributeValue("temporary");
         }

         public Map<String, Object>[] listAllMessages() throws Exception
         {
            Object[] res = (Object[])proxy.invokeOperation("listAllMessages");
            Map<String, Object>[] results = new Map[res.length];
            for (int i = 0; i < res.length; i++)
            {
               results[i] = (Map<String, Object>)res[i];
            }
            return results;
         }
         
         public String listAllMessagesAsJSON() throws Exception
         {
            return (String)proxy.invokeOperation("listAllMessagesAsJSON");
         }

         public String listMessageCounter() throws Exception
         {
            return (String)proxy.invokeOperation("listMessageCounter");
         }

         public String listMessageCounterAsHTML() throws Exception
         {
            return (String)proxy.invokeOperation("listMessageCounterAsHTML");
         }

         public String listMessageCounterHistory() throws Exception
         {
            return (String)proxy.invokeOperation("listMessageCounterHistory");
         }

         public String listMessageCounterHistoryAsHTML() throws Exception
         {
            return (String)proxy.invokeOperation("listMessageCounterHistoryAsHTML");
         }

         public Map<String, Object>[] listMessages(String filter) throws Exception
         {
            Object[] res = (Object[])proxy.invokeOperation("listMessages", filter);
            Map<String, Object>[] results = new Map[res.length];
            for (int i = 0; i < res.length; i++)
            {
               results[i] = (Map<String, Object>)res[i];
            }
            return results;
         }

         public String listMessagesAsJSON(String filter) throws Exception
         {
            return (String)proxy.invokeOperation("listMessagesAsJSON", filter);
         }
         
         public int moveAllMessages(String otherQueueName) throws Exception
         {
            return (Integer)proxy.invokeOperation("moveAllMessages", otherQueueName);
         }

         public int moveMatchingMessages(String filter, String otherQueueName) throws Exception
         {
            return (Integer)proxy.invokeOperation("moveMatchingMessages", filter, otherQueueName);
         }

         public boolean moveMessage(String messageID, String otherQueueName) throws Exception
         {
            return (Boolean)proxy.invokeOperation("moveMessage", messageID, otherQueueName);
         }

         public int removeMatchingMessages(String filter) throws Exception
         {
            return (Integer)proxy.invokeOperation("removeMatchingMessages", filter);
         }

         public boolean removeMessage(String messageID) throws Exception
         {
            return (Boolean)proxy.invokeOperation("removeMessage", messageID);
         }

         public boolean sendMessageToDeadLetterAddress(String messageID) throws Exception
         {
            return (Boolean)proxy.invokeOperation("sendMessageToDeadLetterAddress", messageID);
         }

         public void setDeadLetterAddress(String deadLetterAddress) throws Exception
         {
            proxy.invokeOperation("setDeadLetterAddress", deadLetterAddress);
         }

         public void setExpiryAddress(String expiryAddress) throws Exception
         {
            proxy.invokeOperation("setExpiryAddress", expiryAddress);
         }

         public String getAddress()
         {
            return (String)proxy.retrieveAttributeValue("address");
         }

         public String getJNDIBinding()
         {
            return (String)proxy.retrieveAttributeValue("JNDIBinding");
         }

         public int removeAllMessages() throws Exception
         {
            return (Integer)proxy.invokeOperation("removeAllMessages");
         }

      };
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}