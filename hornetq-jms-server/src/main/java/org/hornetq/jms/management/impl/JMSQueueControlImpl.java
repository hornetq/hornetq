/*
 * Copyright 2005-2014 Red Hat, Inc.
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
package org.hornetq.jms.management.impl;

import javax.management.MBeanInfo;
import javax.management.StandardMBean;
import java.util.HashMap;
import java.util.Map;

import org.hornetq.api.core.FilterConstants;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.management.MessageCounterInfo;
import org.hornetq.api.core.management.Operation;
import org.hornetq.api.core.management.QueueControl;
import org.hornetq.api.jms.management.JMSQueueControl;
import org.hornetq.core.management.impl.MBeanInfoHelper;
import org.hornetq.core.messagecounter.MessageCounter;
import org.hornetq.core.messagecounter.impl.MessageCounterHelper;
import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.jms.client.HornetQMessage;
import org.hornetq.jms.client.SelectorTranslator;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.utils.json.JSONArray;
import org.hornetq.utils.json.JSONObject;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class JMSQueueControlImpl extends StandardMBean implements JMSQueueControl
{
   private final HornetQDestination managedQueue;

   private final JMSServerManager jmsServerManager;

   private final QueueControl coreQueueControl;

   private final MessageCounter counter;

   // Static --------------------------------------------------------

   /**
    * Returns null if the string is null or empty
    */
   public static String createFilterFromJMSSelector(final String selectorStr) throws HornetQException
   {
      return selectorStr == null || selectorStr.trim().length() == 0 ? null
         : SelectorTranslator.convertToHornetQFilterString(selectorStr);
   }

   private static String createFilterForJMSMessageID(final String jmsMessageID) throws Exception
   {
      return FilterConstants.HORNETQ_USERID + " = '" + jmsMessageID + "'";
   }

   static String toJSON(final Map<String, Object>[] messages)
   {
      JSONArray array = new JSONArray();
      for (Map<String, Object> message : messages)
      {
         array.put(new JSONObject(message));
      }
      return array.toString();
   }

   // Constructors --------------------------------------------------

   public JMSQueueControlImpl(final HornetQDestination managedQueue,
                              final QueueControl coreQueueControl,
                              final JMSServerManager jmsServerManager,
                              final MessageCounter counter) throws Exception
   {
      super(JMSQueueControl.class);
      this.managedQueue = managedQueue;
      this.jmsServerManager = jmsServerManager;
      this.coreQueueControl = coreQueueControl;
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

   public long getMessageCount()
   {
      return coreQueueControl.getMessageCount();
   }

   public long getMessagesAdded()
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

   public String getDeadLetterAddress()
   {
      return coreQueueControl.getDeadLetterAddress();
   }

   public void setDeadLetterAddress(final String deadLetterAddress) throws Exception
   {
      coreQueueControl.setDeadLetterAddress(deadLetterAddress);
   }

   public String getExpiryAddress()
   {
      return coreQueueControl.getExpiryAddress();
   }

   public void setExpiryAddress(final String expiryAddress) throws Exception
   {
      coreQueueControl.setExpiryAddress(expiryAddress);
   }

   public String getFirstMessageAsJSON() throws Exception
   {
      return coreQueueControl.getFirstMessageAsJSON();
   }

   public Long getFirstMessageTimestamp() throws Exception
   {
      return coreQueueControl.getFirstMessageTimestamp();
   }

   public Long getFirstMessageAge() throws Exception
   {
      return coreQueueControl.getFirstMessageAge();
   }

   @Override
   public void addJNDI(String jndi) throws Exception
   {
      jmsServerManager.addQueueToJndi(managedQueue.getName(), jndi);
   }

   public void removeJNDI(String jndi) throws Exception
   {
      jmsServerManager.removeQueueFromJNDI(managedQueue.getName(), jndi);
   }

   public String[] getJNDIBindings()
   {
      return jmsServerManager.getJNDIOnQueue(managedQueue.getName());
   }

   public boolean removeMessage(final String messageID) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterForJMSMessageID(messageID);
      int removed = coreQueueControl.removeMessages(filter);
      if (removed != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }
      return true;
   }

   public int removeMessages(final String filterStr) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterFromJMSSelector(filterStr);
      return coreQueueControl.removeMessages(filter);
   }

   public Map<String, Object>[] listMessages(final String filterStr) throws Exception
   {
      try
      {
         String filter = JMSQueueControlImpl.createFilterFromJMSSelector(filterStr);
         Map<String, Object>[] coreMessages = coreQueueControl.listMessages(filter);

         return toJMSMap(coreMessages);
      }
      catch (HornetQException e)
      {
         throw new IllegalStateException(e.getMessage());
      }
   }

   private Map<String, Object>[] toJMSMap(Map<String, Object>[] coreMessages)
   {
      Map<String, Object>[] jmsMessages = new Map[coreMessages.length];

      int i = 0;

      for (Map<String, Object> coreMessage : coreMessages)
      {
         Map<String, Object> jmsMessage = HornetQMessage.coreMaptoJMSMap(coreMessage);
         jmsMessages[i++] = jmsMessage;
      }
      return jmsMessages;
   }

   @Override
   public Map<String, Object>[] listScheduledMessages() throws Exception
   {
      Map<String, Object>[] coreMessages = coreQueueControl.listScheduledMessages();

      return toJMSMap(coreMessages);
   }

   @Override
   public String listScheduledMessagesAsJSON() throws Exception
   {
      return coreQueueControl.listScheduledMessagesAsJSON();
   }

   @Override
   public Map<String, Map<String, Object>[]> listDeliveringMessages() throws Exception
   {
      try
      {
         Map<String, Map<String, Object>[]> returnMap = new HashMap<String, Map<String, Object>[]>();


         // the workingMap from the queue-control
         Map<String, Map<String, Object>[]> workingMap = coreQueueControl.listDeliveringMessages();

         for (Map.Entry<String, Map<String, Object>[]> entry : workingMap.entrySet())
         {
            returnMap.put(entry.getKey(), toJMSMap(entry.getValue()));
         }

         return returnMap;
      }
      catch (HornetQException e)
      {
         throw new IllegalStateException(e.getMessage());
      }
   }

   @Override
   public String listDeliveringMessagesAsJSON() throws Exception
   {
      return coreQueueControl.listDeliveringMessagesAsJSON();
   }



   public String listMessagesAsJSON(final String filter) throws Exception
   {
      return JMSQueueControlImpl.toJSON(listMessages(filter));
   }

   public long countMessages(final String filterStr) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterFromJMSSelector(filterStr);
      return coreQueueControl.countMessages(filter);
   }

   public boolean expireMessage(final String messageID) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterForJMSMessageID(messageID);
      int expired = coreQueueControl.expireMessages(filter);
      if (expired != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }
      return true;
   }

   public int expireMessages(final String filterStr) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterFromJMSSelector(filterStr);
      return coreQueueControl.expireMessages(filter);
   }

   public boolean sendMessageToDeadLetterAddress(final String messageID) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterForJMSMessageID(messageID);
      int dead = coreQueueControl.sendMessagesToDeadLetterAddress(filter);
      if (dead != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }
      return true;
   }

   public int sendMessagesToDeadLetterAddress(final String filterStr) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterFromJMSSelector(filterStr);
      return coreQueueControl.sendMessagesToDeadLetterAddress(filter);
   }

   public boolean changeMessagePriority(final String messageID, final int newPriority) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterForJMSMessageID(messageID);
      int changed = coreQueueControl.changeMessagesPriority(filter, newPriority);
      if (changed != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }
      return true;
   }

   public int changeMessagesPriority(final String filterStr, final int newPriority) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterFromJMSSelector(filterStr);
      return coreQueueControl.changeMessagesPriority(filter, newPriority);
   }

   public boolean moveMessage(final String messageID, final String otherQueueName) throws Exception
   {
      return moveMessage(messageID, otherQueueName, false);
   }

   public boolean moveMessage(final String messageID, final String otherQueueName, final boolean rejectDuplicates) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterForJMSMessageID(messageID);
      HornetQDestination otherQueue = HornetQDestination.createQueue(otherQueueName);
      int moved = coreQueueControl.moveMessages(filter, otherQueue.getAddress(), rejectDuplicates);
      if (moved != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }

      return true;
   }

   public int moveMessages(final String filterStr, final String otherQueueName, final boolean rejectDuplicates) throws Exception
   {
      String filter = JMSQueueControlImpl.createFilterFromJMSSelector(filterStr);
      HornetQDestination otherQueue = HornetQDestination.createQueue(otherQueueName);
      return coreQueueControl.moveMessages(filter, otherQueue.getAddress(), rejectDuplicates);
   }


   public int moveMessages(final String filterStr, final String otherQueueName) throws Exception
   {
      return moveMessages(filterStr, otherQueueName, false);
   }

   @Operation(desc = "List all the existent consumers on the Queue")
   public String listConsumersAsJSON() throws Exception
   {
      return coreQueueControl.listConsumersAsJSON();
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

   public void resetMessageCounter() throws Exception
   {
      coreQueueControl.resetMessageCounter();
   }

   public String listMessageCounterAsHTML()
   {
      return MessageCounterHelper.listMessageCounterAsHTML(new MessageCounter[]{counter});
   }

   public String listMessageCounterHistory() throws Exception
   {
      return MessageCounterHelper.listMessageCounterHistory(counter);
   }

   public String listMessageCounterHistoryAsHTML()
   {
      return MessageCounterHelper.listMessageCounterHistoryAsHTML(new MessageCounter[]{counter});
   }

   public boolean isPaused() throws Exception
   {
      return coreQueueControl.isPaused();
   }

   public void pause() throws Exception
   {
      coreQueueControl.pause();
   }

   public void resume() throws Exception
   {
      coreQueueControl.resume();
   }

   public String getSelector()
   {
      return coreQueueControl.getFilter();
   }

   public void flushExecutor()
   {
      coreQueueControl.flushExecutor();
   }

   @Override
   public MBeanInfo getMBeanInfo()
   {
      MBeanInfo info = super.getMBeanInfo();
      return new MBeanInfo(info.getClassName(),
                           info.getDescription(),
                           info.getAttributes(),
                           info.getConstructors(),
                           MBeanInfoHelper.getMBeanOperationsInfo(JMSQueueControl.class),
                           info.getNotifications());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
