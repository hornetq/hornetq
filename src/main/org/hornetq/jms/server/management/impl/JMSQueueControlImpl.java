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

package org.hornetq.jms.server.management.impl;

import java.util.Map;

import javax.management.StandardMBean;

import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.MessageCounterInfo;
import org.hornetq.core.management.QueueControl;
import org.hornetq.core.messagecounter.MessageCounter;
import org.hornetq.core.messagecounter.impl.MessageCounterHelper;
import org.hornetq.jms.HornetQQueue;
import org.hornetq.jms.client.HornetQMessage;
import org.hornetq.jms.client.SelectorTranslator;
import org.hornetq.jms.server.management.JMSQueueControl;
import org.hornetq.utils.json.JSONArray;
import org.hornetq.utils.json.JSONObject;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JMSQueueControlImpl extends StandardMBean implements JMSQueueControl
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(JMSQueueControlImpl.class);

   // Attributes ----------------------------------------------------

   private final HornetQQueue managedQueue;

   private final QueueControl coreQueueControl;

   private final String binding;

   private final MessageCounter counter;

   // Static --------------------------------------------------------

   /**
    * Returns null if the string is null or empty
    */
   public static String createFilterFromJMSSelector(final String selectorStr) throws HornetQException
   {
      return (selectorStr == null || selectorStr.trim().length() == 0) ? null
                                                                      : SelectorTranslator.convertToHornetQFilterString(selectorStr);
   }

   private static String createFilterForJMSMessageID(String jmsMessageID) throws Exception
   {
      return HornetQMessage.HORNETQ_MESSAGE_ID + " = '" + jmsMessageID + "'";
   }

   static String toJSON(Map<String, Object>[] messages)
   {
      JSONArray array = new JSONArray();
      for (int i = 0; i < messages.length; i++)
      {
         Map<String, Object> message = messages[i];
         array.put(new JSONObject(message));
      }
      return array.toString();
   }

   // Constructors --------------------------------------------------

   public JMSQueueControlImpl(final HornetQQueue managedQueue,
                              final QueueControl coreQueueControl,
                              final String jndiBinding,
                              final MessageCounter counter) throws Exception
   {
      super(JMSQueueControl.class);
      this.managedQueue = managedQueue;
      this.coreQueueControl = coreQueueControl;
      this.binding = jndiBinding;
      this.counter = counter;
   }

   // Public --------------------------------------------------------

   // ManagedJMSQueueMBean implementation ---------------------------

   public String getName()
   {
      return managedQueue.getName();
   }

   public String getAddress()
   {
      return managedQueue.getAddress();
   }

   public boolean isTemporary()
   {
      return managedQueue.isTemporary();
   }

   public int getMessageCount()
   {
      return coreQueueControl.getMessageCount();
   }

   public int getMessagesAdded()
   {
      return coreQueueControl.getMessagesAdded();
   }

   public int getConsumerCount()
   {
      return coreQueueControl.getConsumerCount();
   }

   public int getDeliveringCount()
   {
      return coreQueueControl.getDeliveringCount();
   }

   public long getScheduledCount()
   {
      return coreQueueControl.getScheduledCount();
   }

   public boolean isDurable()
   {
      return coreQueueControl.isDurable();
   }

   public String getJNDIBinding()
   {
      return binding;
   }

   public String getDeadLetterAddress()
   {
      return coreQueueControl.getDeadLetterAddress();
   }

   public void setDeadLetterAddress(String deadLetterAddress) throws Exception
   {
      coreQueueControl.setDeadLetterAddress(deadLetterAddress);
   }

   public String getExpiryAddress()
   {
      return coreQueueControl.getExpiryAddress();
   }

   public void setExpiryAddress(String expiryAddres) throws Exception
   {
      coreQueueControl.setExpiryAddress(expiryAddres);
   }

   public boolean removeMessage(final String messageID) throws Exception
   {
      String filter = createFilterForJMSMessageID(messageID);
      int removed = coreQueueControl.removeMessages(filter);
      if (removed != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }
      return true;
   }

   public int removeMessages(String filterStr) throws Exception
   {
      String filter = createFilterFromJMSSelector(filterStr);
      return coreQueueControl.removeMessages(filter);
   }

   public Map<String, Object>[] listMessages(final String filterStr) throws Exception
   {
      try
      {
         String filter = createFilterFromJMSSelector(filterStr);
         Map<String, Object>[] coreMessages = coreQueueControl.listMessages(filter);

         Map<String, Object>[] jmsMessages = new Map[coreMessages.length];

         int i = 0;

         for (Map<String, Object> coreMessage : coreMessages)
         {
            Map<String, Object> jmsMessage = HornetQMessage.coreMaptoJMSMap(coreMessage);
            jmsMessages[i++] = jmsMessage;
         }
         return jmsMessages;
      }
      catch (HornetQException e)
      {
         throw new IllegalStateException(e.getMessage());
      }
   }

   public String listMessagesAsJSON(String filter) throws Exception
   {
      return toJSON(listMessages(filter));
   }

   public int countMessages(final String filterStr) throws Exception
   {
      String filter = createFilterFromJMSSelector(filterStr);
      return coreQueueControl.countMessages(filter);
   }

   public boolean expireMessage(final String messageID) throws Exception
   {
      String filter = createFilterForJMSMessageID(messageID);
      int expired = coreQueueControl.expireMessages(filter);
      if (expired != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }
      return true;
   }

   public int expireMessages(final String filterStr) throws Exception
   {
      String filter = createFilterFromJMSSelector(filterStr);
      return coreQueueControl.expireMessages(filter);
   }

   public boolean sendMessageToDeadLetterAddress(final String messageID) throws Exception
   {
      String filter = createFilterForJMSMessageID(messageID);
      int dead = coreQueueControl.sendMessagesToDeadLetterAddress(filter);
      if (dead != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }
      return true;
   }

   public boolean changeMessagePriority(final String messageID, final int newPriority) throws Exception
   {
      String filter = createFilterForJMSMessageID(messageID);
      int changed = coreQueueControl.changeMessagesPriority(filter, newPriority);
      if (changed != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }
      return true;
   }

   public boolean moveMessage(String messageID, String otherQueueName) throws Exception
   {
      String filter = createFilterForJMSMessageID(messageID);
      HornetQQueue otherQueue = new HornetQQueue(otherQueueName);
      int moved = coreQueueControl.moveMessages(filter, otherQueue.getAddress());
      if (moved != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }

      return true;
   }

   public int moveMessages(String filterStr, String otherQueueName) throws Exception
   {
      String filter = createFilterFromJMSSelector(filterStr);
      HornetQQueue otherQueue = new HornetQQueue(otherQueueName);
      return coreQueueControl.moveMessages(filter, otherQueue.getAddress());
   }

   public String listMessageCounter()
   {
      try
      {
         return MessageCounterInfo.toJSon(counter);
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
   }

   public String listMessageCounterAsHTML()
   {
      return MessageCounterHelper.listMessageCounterAsHTML(new MessageCounter[] { counter });
   }

   public String listMessageCounterHistory() throws Exception
   {
      return MessageCounterHelper.listMessageCounterHistory(counter);
   }

   public String listMessageCounterHistoryAsHTML()
   {
      return MessageCounterHelper.listMessageCounterHistoryAsHTML(new MessageCounter[] { counter });
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
