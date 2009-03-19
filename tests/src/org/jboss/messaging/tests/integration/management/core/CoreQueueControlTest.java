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

package org.jboss.messaging.tests.integration.management.core;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.management.ObjectNames;
import org.jboss.messaging.core.management.QueueControlMBean;
import org.jboss.messaging.tests.integration.management.QueueControlTest;
import org.jboss.messaging.utils.SimpleString;

/**
 * A JMXQueueControlTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class CoreQueueControlTest extends QueueControlTest
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // QueueControlTestBase overrides --------------------------------

   @Override
   protected QueueControlMBean createManagementControl(final SimpleString address, final SimpleString queue) throws Exception
   {

      return new QueueControlMBean()
      {

         private final CoreMessagingProxy proxy = new CoreMessagingProxy(session,
                                                                         ObjectNames.getQueueObjectName(address, queue));

         public boolean changeMessagePriority(long messageID, int newPriority) throws Exception
         {
            return (Boolean)proxy.invokeOperation("changeMessagePriority", messageID, newPriority);
         }

         public int countMessages(String filter) throws Exception
         {
            return (Integer)proxy.invokeOperation("countMessages", filter);
         }

         public boolean expireMessage(long messageID) throws Exception
         {
            return (Boolean)proxy.invokeOperation("expireMessage", messageID);
         }

         public int expireMessages(String filter) throws Exception
         {
            return (Integer)proxy.invokeOperation("expireMessages", filter);
         }

         public String getAddress()
         {
            return (String)proxy.retrieveAttributeValue("Address");
         }

         public int getConsumerCount()
         {
            return (Integer)proxy.retrieveAttributeValue("ConsumerCount");
         }

         public String getDeadLetterAddress()
         {
            return (String)proxy.retrieveAttributeValue("DeadLetterAddress");
         }

         public int getDeliveringCount()
         {
            return (Integer)proxy.retrieveAttributeValue("DeliveringCount");
         }

         public String getExpiryAddress()
         {
            return (String)proxy.retrieveAttributeValue("ExpiryAddress");
         }

         public String getFilter()
         {
            return (String)proxy.retrieveAttributeValue("Filter");
         }

         public int getMessageCount()
         {
            return (Integer)proxy.retrieveAttributeValue("MessageCount");
         }

         public int getMessagesAdded()
         {
            return (Integer)proxy.retrieveAttributeValue("MessagesAdded");
         }

         public String getName()
         {
            return (String)proxy.retrieveAttributeValue("Name");
         }

         public long getPersistenceID()
         {
            return (Long)proxy.retrieveAttributeValue("PersistenceID");
         }

         public long getScheduledCount()
         {
            return (Long)proxy.retrieveAttributeValue("ScheduledCount");
         }

         public boolean isBackup()
         {
            return (Boolean)proxy.retrieveAttributeValue("Backup");
         }

         public boolean isDurable()
         {
            return (Boolean)proxy.retrieveAttributeValue("Durable");
         }

         public boolean isTemporary()
         {
            return (Boolean)proxy.retrieveAttributeValue("Temporary");
         }

         public TabularData listAllMessages() throws Exception
         {
            return (TabularData)proxy.invokeOperation("listAllMessages");
         }

         public CompositeData listMessageCounter() throws Exception
         {
            return (CompositeData)proxy.invokeOperation("listMessageCounter");
         }

         public String listMessageCounterAsHTML() throws Exception
         {
            return (String)proxy.invokeOperation("listMessageCounterAsHTML");
         }

         public TabularData listMessageCounterHistory() throws Exception
         {
            return (TabularData)proxy.invokeOperation("listMessageCounterHistory");
         }

         public String listMessageCounterHistoryAsHTML() throws Exception
         {
            return (String)proxy.invokeOperation("listMessageCounterHistoryAsHTML");
         }

         public TabularData listMessages(String filter) throws Exception
         {
            return (TabularData)proxy.invokeOperation("listMessages", filter);
         }

         public TabularData listScheduledMessages() throws Exception
         {
            return (TabularData)proxy.invokeOperation("listScheduledMessages");
         }

         public int moveAllMessages(String otherQueueName) throws Exception
         {
            return (Integer)proxy.invokeOperation("moveAllMessages", otherQueueName);
         }

         public int moveMatchingMessages(String filter, String otherQueueName) throws Exception
         {
            return (Integer)proxy.invokeOperation("moveMatchingMessages", filter, otherQueueName);
         }

         public boolean moveMessage(long messageID, String otherQueueName) throws Exception
         {
            return (Boolean)proxy.invokeOperation("moveMessage", messageID, otherQueueName);
         }

         public int removeAllMessages() throws Exception
         {
            return (Integer)proxy.invokeOperation("removeAllMessages");
         }

         public int removeMatchingMessages(String filter) throws Exception
         {
            return (Integer)proxy.invokeOperation("removeMatchingMessages", filter);
         }

         public boolean removeMessage(long messageID) throws Exception
         {
            return (Boolean)proxy.invokeOperation("removeMessage", messageID);
         }

         public boolean sendMessageToDeadLetterAddress(long messageID) throws Exception
         {
            return (Boolean)proxy.invokeOperation("sendMessageToDeadLetterAddress", messageID);
         }

         public void setDeadLetterAddress(String deadLetterAddress) throws Exception
         {
            proxy.invokeOperation("setDeadLetterAddress", deadLetterAddress);
         }

         public void setExpiryAddress(String expiryAddres) throws Exception
         {
            proxy.invokeOperation("setExpiryAddress", expiryAddres);
         }
      };
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
