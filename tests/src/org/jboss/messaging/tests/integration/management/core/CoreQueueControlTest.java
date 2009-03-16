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

import static org.jboss.messaging.core.config.impl.ConfigurationImpl.DEFAULT_MANAGEMENT_ADDRESS;

import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientRequestor;
import org.jboss.messaging.core.client.management.impl.ManagementHelper;
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

   private static String fromNullableSimpleString(SimpleString sstring)
   {
      if (sstring == null)
      {
         return null;
      }
      else
      {
         return sstring.toString();
      }
   }

   // Constructors --------------------------------------------------

   // QueueControlTestBase overrides --------------------------------

   @Override
   protected QueueControlMBean createManagementControl(final SimpleString address, final SimpleString queue) throws Exception
   {

      final ClientRequestor requestor = new ClientRequestor(session, DEFAULT_MANAGEMENT_ADDRESS);

      return new QueueControlMBean()
      {

         private ObjectName queueON = ObjectNames.getQueueObjectName(address, queue);

         public boolean changeMessagePriority(long messageID, int newPriority) throws Exception
         {
            ClientMessage m = session.createClientMessage(false);
            ManagementHelper.putOperationInvocation(m, queueON, "changeMessagePriority", messageID, newPriority);
            ClientMessage reply = requestor.request(m);
            return (Boolean)reply.getProperty(new SimpleString("changeMessagePriority"));
         }

         public int countMessages(String filter) throws Exception
         {
            return (Integer)invokOperation("countMessages", filter);
         }

         public boolean expireMessage(long messageID) throws Exception
         {
            return (Boolean)invokOperation("expireMessage", messageID);
         }

         public int expireMessages(String filter) throws Exception
         {
            return (Integer)invokOperation("expireMessages", filter);
         }

         public String getAddress()
         {
            return fromNullableSimpleString((SimpleString)retriveAttributeValue("Address"));
         }

         public int getConsumerCount()
         {
            return (Integer)retriveAttributeValue("ConsumerCount");
         }

         public String getDeadLetterAddress()
         {
            return fromNullableSimpleString((SimpleString)retriveAttributeValue("DeadLetterAddress"));
         }

         public int getDeliveringCount()
         {
            return (Integer)retriveAttributeValue("DeliveringCount");
         }

         public String getExpiryAddress()
         {
            return fromNullableSimpleString((SimpleString)retriveAttributeValue("ExpiryAddress"));
         }

         public String getFilter()
         {
            return fromNullableSimpleString((SimpleString)retriveAttributeValue("Filter"));
         }

         public int getMessageCount()
         {
            return (Integer)retriveAttributeValue("MessageCount");
         }

         public int getMessagesAdded()
         {
            return (Integer)retriveAttributeValue("MessagesAdded");
         }

         public String getName()
         {
            return fromNullableSimpleString((SimpleString)retriveAttributeValue("Name"));
         }

         public long getPersistenceID()
         {
            return (Long)retriveAttributeValue("PersistenceID");
         }

         public long getScheduledCount()
         {
            return (Long)retriveAttributeValue("ScheduledCount");
         }

         public boolean isBackup()
         {
            return (Boolean)retriveAttributeValue("Backup");
         }

         public boolean isDurable()
         {
            return (Boolean)retriveAttributeValue("Durable");
         }

         public boolean isTemporary()
         {
            return (Boolean)retriveAttributeValue("Temporary");
         }

         public TabularData listAllMessages() throws Exception
         {
            ClientMessage m = session.createClientMessage(false);
            ManagementHelper.putOperationInvocation(m, queueON, "listAllMessages");
            ClientMessage reply = requestor.request(m);
            return (TabularData)ManagementHelper.getTabularDataProperty(reply, "listAllMessages");
         }

         public CompositeData listMessageCounter() throws Exception
         {
            ClientMessage m = session.createClientMessage(false);
            ManagementHelper.putOperationInvocation(m, queueON, "listMessageCounter");
            ClientMessage reply = requestor.request(m);
            return (CompositeData)ManagementHelper.getCompositeDataProperty(reply, "listMessageCounter");
         }

         public String listMessageCounterAsHTML() throws Exception
         {
            return fromNullableSimpleString((SimpleString)invokOperation("listMessageCounterAsHTML"));
         }

         public TabularData listMessageCounterHistory() throws Exception
         {
            ClientMessage m = session.createClientMessage(false);
            ManagementHelper.putOperationInvocation(m, queueON, "listMessageCounterHistory");
            ClientMessage reply = requestor.request(m);
            return (TabularData)ManagementHelper.getTabularDataProperty(reply, "listMessageCounterHistory");
         }

         public String listMessageCounterHistoryAsHTML() throws Exception
         {
            return fromNullableSimpleString((SimpleString)invokOperation("listMessageCounterHistoryAsHTML"));
         }

         public TabularData listMessages(String filter) throws Exception
         {
            ClientMessage m = session.createClientMessage(false);
            ManagementHelper.putOperationInvocation(m, queueON, "listMessages", filter);
            ClientMessage reply = requestor.request(m);
            return (TabularData)ManagementHelper.getTabularDataProperty(reply, "listMessages");
         }

         public TabularData listScheduledMessages() throws Exception
         {
            ClientMessage m = session.createClientMessage(false);
            ManagementHelper.putOperationInvocation(m, queueON, "listScheduledMessages");
            ClientMessage reply = requestor.request(m);
            return (TabularData)ManagementHelper.getTabularDataProperty(reply, "listScheduledMessages");
         }

         public int moveAllMessages(String otherQueueName) throws Exception
         {
            return (Integer)invokOperation("moveAllMessages", otherQueueName);
         }

         public int moveMatchingMessages(String filter, String otherQueueName) throws Exception
         {
            return (Integer)invokOperation("moveMatchingMessages", filter, otherQueueName);
         }

         public boolean moveMessage(long messageID, String otherQueueName) throws Exception
         {
            return (Boolean)invokOperation("moveMessage", messageID, otherQueueName);
         }

         public int removeAllMessages() throws Exception
         {
            return (Integer)invokOperation("removeAllMessages");
         }

         public int removeMatchingMessages(String filter) throws Exception
         {
            return (Integer)invokOperation("removeMatchingMessages", filter);
         }

         public boolean removeMessage(long messageID) throws Exception
         {
            return (Boolean)invokOperation("removeMessage", messageID);
         }

         public boolean sendMessageToDeadLetterAddress(long messageID) throws Exception
         {
            return (Boolean)invokOperation("sendMessageToDeadLetterAddress", messageID);
         }

         public void setDeadLetterAddress(String deadLetterAddress) throws Exception
         {
            ClientMessage m = session.createClientMessage(false);
            ManagementHelper.putOperationInvocation(m, queueON, "setDeadLetterAddress", deadLetterAddress);
            requestor.request(m);
         }

         public void setExpiryAddress(String expiryAddres) throws Exception
         {
            ClientMessage m = session.createClientMessage(false);
            ManagementHelper.putOperationInvocation(m, queueON, "setExpiryAddress", expiryAddres);
            requestor.request(m);
         }

         private Object retriveAttributeValue(String attributeName)
         {
            ClientMessage m = session.createClientMessage(false);
            ManagementHelper.putAttributes(m, queueON, attributeName);
            ClientMessage reply;
            try
            {
               reply = requestor.request(m);
               Object attributeValue = reply.getProperty(new SimpleString(attributeName));
               if (attributeValue.equals(new SimpleString("null")))
               {
                  return null;
               }
               return attributeValue;
            }
            catch (Exception e)
            {
               throw new IllegalStateException(e);
            }
         }

         public Object invokOperation(String operationName, Object... args) throws Exception
         {
            ClientMessage m = session.createClientMessage(false);
            ManagementHelper.putOperationInvocation(m, queueON, operationName, args);
            ClientMessage reply = requestor.request(m);
            return reply.getProperty(new SimpleString(operationName));
         }
      };
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
