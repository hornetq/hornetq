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

import java.util.Map;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.QueueControl;
import org.jboss.messaging.core.management.ResourceNames;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.utils.SimpleString;

/**
 * A QueueControlTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class QueueControlUsingCoreTest extends QueueControlTest
{

   // Constants -----------------------------------------------------
   
   private static final Logger log = Logger.getLogger(QueueControlUsingCoreTest.class);

   // Attributes ----------------------------------------------------

   protected ClientSession session;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected QueueControl createManagementControl(final SimpleString address, final SimpleString queue) throws Exception
   {
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      session = sf.createSession(false, true, true);
      session.start();

      return new QueueControl()
      {
         private final CoreMessagingProxy proxy = new CoreMessagingProxy(session,
                                                                         ResourceNames.CORE_QUEUE + queue);

         public boolean changeMessagePriority(long messageID, int newPriority) throws Exception
         {
            return (Boolean)proxy.invokeOperation("changeMessagePriority", messageID, newPriority);
         }

         public int changeMessagesPriority(String filter, int newPriority) throws Exception
         {
            return (Integer)proxy.invokeOperation("changeMessagesPriority", filter, newPriority);
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
            return (Long)proxy.retrieveAttributeValue("ScheduledCount", Long.class);
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

         public Map<String, Object>[] listScheduledMessages() throws Exception
         {
            Object[] res = (Object[])proxy.invokeOperation("listScheduledMessages");
            Map<String, Object>[] results = new Map[res.length];
            for (int i = 0; i < res.length; i++)
            {
               results[i] = (Map<String, Object>)res[i];
            }
            return results;
         }

         public String listScheduledMessagesAsJSON() throws Exception
         {
            return (String)proxy.invokeOperation("listScheduledMessagesAsJSON");
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

         public void resetMessageCounter() throws Exception
         {
            proxy.invokeOperation("resetMessageCounter");
         }

         public boolean sendMessageToDeadLetterAddress(long messageID) throws Exception
         {
            return (Boolean)proxy.invokeOperation("sendMessageToDeadLetterAddress", messageID);
         }

         public int sendMessagesToDeadLetterAddress(String filterStr) throws Exception
         {
            return (Integer)proxy.invokeOperation("sendMessagesToDeadLetterAddress", filterStr);
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

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void tearDown() throws Exception
   {
      if (session != null)
      {
         session.close();
      }
      
      super.tearDown();
   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
