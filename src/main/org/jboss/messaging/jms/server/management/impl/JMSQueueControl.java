/*
 * JBoss, Home of Professional Open Source
 * Copyright 2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.jms.server.management.impl;

import java.util.HashMap;
import java.util.Map;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.MessageCounterInfo;
import org.jboss.messaging.core.management.QueueControlMBean;
import org.jboss.messaging.core.messagecounter.MessageCounter;
import org.jboss.messaging.core.messagecounter.impl.MessageCounterHelper;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.client.JBossMessage;
import org.jboss.messaging.jms.client.SelectorTranslator;
import org.jboss.messaging.jms.server.management.JMSQueueControlMBean;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class JMSQueueControl implements JMSQueueControlMBean
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(JMSQueueControl.class);

   // Attributes ----------------------------------------------------

   private final JBossQueue managedQueue;

   private final QueueControlMBean coreQueueControl;

   private final String binding;

   private final MessageCounter counter;

   // Static --------------------------------------------------------

   public static String createFilterFromJMSSelector(final String selectorStr) throws MessagingException
   {
      return (selectorStr == null) ? null : SelectorTranslator.convertToJBMFilterString(selectorStr);
   }

   private static String createFilterForJMSMessageID(String jmsMessageID) throws Exception
   {
      return JBossMessage.JBM_MESSAGE_ID + " = '" + jmsMessageID + "'";
   }

   // Constructors --------------------------------------------------

   public JMSQueueControl(final JBossQueue managedQueue,
                          final QueueControlMBean coreQueueControl,
                          final String jndiBinding,
                          final MessageCounter counter)
   {
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
      int removed = coreQueueControl.removeMatchingMessages(filter);
      if (removed != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }
      return true;
   }

   public int removeMatchingMessages(String filterStr) throws Exception
   {
      String filter = createFilterFromJMSSelector(filterStr);
      return coreQueueControl.removeMatchingMessages(filter);
   }

   public int removeAllMessages() throws Exception
   {
      return coreQueueControl.removeAllMessages();
   }

   public Map<String, Object>[] listAllMessages() throws Exception
   {
      return listMessages(null);
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
            Map<String, Object> jmsMessage = JBossMessage.coreMaptoJMSMap(coreMessage);
            jmsMessages[i++] = jmsMessage;
         }
         return jmsMessages;
      }
      catch (MessagingException e)
      {
         throw new IllegalStateException(e.getMessage());
      }
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

   public boolean sendMessageToDLQ(final String messageID) throws Exception
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
      JBossQueue otherQueue = new JBossQueue(otherQueueName);
      int moved = coreQueueControl.moveMatchingMessages(filter, otherQueue.getAddress());
      if (moved != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }

      return true;
   }

   public int moveMatchingMessages(String filterStr, String otherQueueName) throws Exception
   {
      String filter = createFilterFromJMSSelector(filterStr);
      JBossQueue otherQueue = new JBossQueue(otherQueueName);
      return coreQueueControl.moveMatchingMessages(filter, otherQueue.getAddress());
   }

   public int moveAllMessages(String otherQueueName) throws Exception
   {
      return moveMatchingMessages(null, otherQueueName);
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
