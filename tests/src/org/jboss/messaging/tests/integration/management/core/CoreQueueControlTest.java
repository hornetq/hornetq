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

import static org.jboss.messaging.tests.integration.management.core.CoreMessagingProxy.fromNullableSimpleString;

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
            return (Boolean)proxy.invokOperation("changeMessagePriority", messageID, newPriority);
         }

         public int countMessages(String filter) throws Exception
         {
            return (Integer)proxy.invokOperation("countMessages", filter);
         }

         public boolean expireMessage(long messageID) throws Exception
         {
            return (Boolean)proxy.invokOperation("expireMessage", messageID);
         }

         public int expireMessages(String filter) throws Exception
         {
            return (Integer)proxy.invokOperation("expireMessages", filter);
         }

         public String getAddress()
         {
            return fromNullableSimpleString((SimpleString)proxy.retriveAttributeValue("Address"));
         }

         public int getConsumerCount()
         {
            return (Integer)proxy.retriveAttributeValue("ConsumerCount");
         }

         public String getDeadLetterAddress()
         {
            return fromNullableSimpleString((SimpleString)proxy.retriveAttributeValue("DeadLetterAddress"));
         }

         public int getDeliveringCount()
         {
            return (Integer)proxy.retriveAttributeValue("DeliveringCount");
         }

         public String getExpiryAddress()
         {
            return fromNullableSimpleString((SimpleString)proxy.retriveAttributeValue("ExpiryAddress"));
         }

         public String getFilter()
         {
            return fromNullableSimpleString((SimpleString)proxy.retriveAttributeValue("Filter"));
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
            return fromNullableSimpleString((SimpleString)proxy.retriveAttributeValue("Name"));
         }

         public long getPersistenceID()
         {
            return (Long)proxy.retriveAttributeValue("PersistenceID");
         }

         public long getScheduledCount()
         {
            return (Long)proxy.retriveAttributeValue("ScheduledCount");
         }

         public boolean isBackup()
         {
            return (Boolean)proxy.retriveAttributeValue("Backup");
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
            return proxy.invokeTabularOperation("listAllMessages");
         }

         public CompositeData listMessageCounter() throws Exception
         {
            return proxy.invokeCompositeOperation("listMessageCounter");
         }

         public String listMessageCounterAsHTML() throws Exception
         {
            return fromNullableSimpleString((SimpleString)proxy.invokOperation("listMessageCounterAsHTML"));
         }

         public TabularData listMessageCounterHistory() throws Exception
         {
            return proxy.invokeTabularOperation("listMessageCounterHistory");
         }

         public String listMessageCounterHistoryAsHTML() throws Exception
         {
            return fromNullableSimpleString((SimpleString)proxy.invokOperation("listMessageCounterHistoryAsHTML"));
         }

         public TabularData listMessages(String filter) throws Exception
         {
            return proxy.invokeTabularOperation("listMessages", filter);
         }

         public TabularData listScheduledMessages() throws Exception
         {
            return proxy.invokeTabularOperation("listScheduledMessages");
         }

         public int moveAllMessages(String otherQueueName) throws Exception
         {
            return (Integer)proxy.invokOperation("moveAllMessages", otherQueueName);
         }

         public int moveMatchingMessages(String filter, String otherQueueName) throws Exception
         {
            return (Integer)proxy.invokOperation("moveMatchingMessages", filter, otherQueueName);
         }

         public boolean moveMessage(long messageID, String otherQueueName) throws Exception
         {
            return (Boolean)proxy.invokOperation("moveMessage", messageID, otherQueueName);
         }

         public int removeAllMessages() throws Exception
         {
            return (Integer)proxy.invokOperation("removeAllMessages");
         }

         public int removeMatchingMessages(String filter) throws Exception
         {
            return (Integer)proxy.invokOperation("removeMatchingMessages", filter);
         }

         public boolean removeMessage(long messageID) throws Exception
         {
            return (Boolean)proxy.invokOperation("removeMessage", messageID);
         }

         public boolean sendMessageToDeadLetterAddress(long messageID) throws Exception
         {
            return (Boolean)proxy.invokOperation("sendMessageToDeadLetterAddress", messageID);
         }

         public void setDeadLetterAddress(String deadLetterAddress) throws Exception
         {
            proxy.invokOperation("setDeadLetterAddress", deadLetterAddress);
         }

         public void setExpiryAddress(String expiryAddres) throws Exception
         {
            proxy.invokOperation("setExpiryAddress", expiryAddres);
         }
      };
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
