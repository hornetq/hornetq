/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.integration.management;

import java.util.Map;

import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.QueueControl;
import org.hornetq.core.management.ResourceNames;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.utils.SimpleString;

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
            return (String)proxy.retrieveAttributeValue("address");
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

         public String getFilter()
         {
            return (String)proxy.retrieveAttributeValue("filter");
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

         public long getID()
         {
            return (Long)proxy.retrieveAttributeValue("ID");
         }

         public long getScheduledCount()
         {
            return (Long)proxy.retrieveAttributeValue("scheduledCount", Long.class);
         }

         public boolean isBackup()
         {
            return (Boolean)proxy.retrieveAttributeValue("backup");
         }

         public boolean isDurable()
         {
            return (Boolean)proxy.retrieveAttributeValue("durable");
         }

         public boolean isTemporary()
         {
            return (Boolean)proxy.retrieveAttributeValue("temporary");
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

         public int moveMessages(String filter, String otherQueueName) throws Exception
         {
            return (Integer)proxy.invokeOperation("moveMessages", filter, otherQueueName);
         }

         public boolean moveMessage(long messageID, String otherQueueName) throws Exception
         {
            return (Boolean)proxy.invokeOperation("moveMessage", messageID, otherQueueName);
         }

         public int removeMessages(String filter) throws Exception
         {
            return (Integer)proxy.invokeOperation("removeMessages", filter);
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

         public void pause() throws Exception
         {
            proxy.invokeOperation("pause");
         }

         public void resume() throws Exception
         {
            proxy.invokeOperation("resume");
         }

         public boolean isPaused() throws Exception
         {
            return (Boolean)proxy.invokeOperation("isPaused");
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
      
      session = null;
      
      super.tearDown();
   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
