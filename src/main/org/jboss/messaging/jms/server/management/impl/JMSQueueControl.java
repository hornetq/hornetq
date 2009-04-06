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

import java.util.ArrayList;
import java.util.List;

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.MessageCounterInfo;
import org.jboss.messaging.core.messagecounter.MessageCounter;
import org.jboss.messaging.core.messagecounter.impl.MessageCounterHelper;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.client.JBossMessage;
import org.jboss.messaging.jms.client.SelectorTranslator;
import org.jboss.messaging.jms.server.management.JMSMessageInfo;
import org.jboss.messaging.jms.server.management.JMSQueueControlMBean;
import org.jboss.messaging.utils.SimpleString;

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

   private final Queue coreQueue;

   private final String binding;

   private final PostOffice postOffice;

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private final MessageCounter counter;

   // Static --------------------------------------------------------

   public static Filter createFilterFromJMSSelector(final String selectorStr) throws MessagingException
   {
      String filterStr = (selectorStr == null) ? null : SelectorTranslator.convertToJBMFilterString(selectorStr);
      return FilterImpl.createFilter(filterStr);
   }

   private static Filter createFilterForJMSMessageID(String jmsMessageID) throws Exception
   {
      return new FilterImpl(new SimpleString(JBossMessage.JBM_MESSAGE_ID + " = '" + jmsMessageID + "'"));
   }

   // Constructors --------------------------------------------------

   public JMSQueueControl(final JBossQueue queue,
                          final Queue coreQueue,
                          final String jndiBinding,
                          final PostOffice postOffice,                          
                          final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                          final MessageCounter counter)
   {
      this.managedQueue = queue;
      this.coreQueue = coreQueue;
      this.binding = jndiBinding;
      this.postOffice = postOffice;
      this.addressSettingsRepository = addressSettingsRepository;
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
      return coreQueue.getMessageCount();
   }

   public int getMessagesAdded()
   {
      return coreQueue.getMessagesAdded();
   }

   public int getConsumerCount()
   {
      return coreQueue.getConsumerCount();
   }

   public int getDeliveringCount()
   {
      return coreQueue.getDeliveringCount();
   }

   public long getScheduledCount()
   {
      return coreQueue.getScheduledCount();
   }

   public boolean isDurable()
   {
      return coreQueue.isDurable();
   }

   public String getJNDIBinding()
   {
      return binding;
   }

   public String getDeadLetterAddress()
   {
      AddressSettings addressSettings = addressSettingsRepository.getMatch(getAddress());
      if (addressSettings != null && addressSettings.getDeadLetterAddress() != null)
      {
         return addressSettings.getDeadLetterAddress().toString();
      }
      else
      {
         return null;
      }
   }

   public void setDeadLetterAddress(String deadLetterAddress) throws Exception
   {
      AddressSettings addressSettings = addressSettingsRepository.getMatch(getAddress());

      if (deadLetterAddress != null)
      {
         addressSettings.setDeadLetterAddress(new SimpleString(deadLetterAddress));
      }
   }

   public String getExpiryAddress()
   {
      AddressSettings addressSettings = addressSettingsRepository.getMatch(getAddress());
      if (addressSettings != null && addressSettings.getExpiryAddress() != null)
      {
         return addressSettings.getExpiryAddress().toString();
      }
      else
      {
         return null;
      }
   }

   public void setExpiryAddress(String expiryQueueName)
   {
      AddressSettings addressSettings = addressSettingsRepository.getMatch(getAddress());

      if (expiryQueueName != null)
      {
         addressSettings.setExpiryAddress(new SimpleString(expiryQueueName));
      }
   }

   public boolean removeMessage(final String messageID) throws Exception
   {
      Filter filter = createFilterForJMSMessageID(messageID);
      List<MessageReference> refs = coreQueue.list(filter);
      if (refs.size() != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }
      return coreQueue.deleteReference(refs.get(0).getMessage().getMessageID());
   }

   public int removeMatchingMessages(String filterStr) throws Exception
   {
      try
      {
         Filter filter = createFilterFromJMSSelector(filterStr);
         return coreQueue.deleteMatchingReferences(filter);
      }
      catch (MessagingException e)
      {
         throw new IllegalStateException(e.getMessage());
      }
   }

   public int removeAllMessages() throws Exception
   {
      return coreQueue.deleteAllReferences();
   }

   public TabularData listAllMessages() throws Exception
   {
      return listMessages(null);
   }

   public TabularData listMessages(final String filterStr) throws Exception
   {
      try
      {
         Filter filter = createFilterFromJMSSelector(filterStr);

         List<MessageReference> messageRefs = coreQueue.list(filter);
         List<JMSMessageInfo> infos = new ArrayList<JMSMessageInfo>(messageRefs.size());
         for (MessageReference messageRef : messageRefs)
         {
            ServerMessage message = messageRef.getMessage();
            JMSMessageInfo info = JMSMessageInfo.fromServerMessage(message);
            infos.add(info);
         }
         return JMSMessageInfo.toTabularData(infos);
      }
      catch (MessagingException e)
      {
         throw new IllegalStateException(e.getMessage());
      }
   }

   public int countMessages(final String filterStr) throws Exception
   {
      Filter filter = createFilterFromJMSSelector(filterStr);
      List<MessageReference> messageRefs = coreQueue.list(filter);
      return messageRefs.size();
   }

   public boolean expireMessage(final String messageID) throws Exception
   {
      Filter filter = createFilterForJMSMessageID(messageID);
      List<MessageReference> refs = coreQueue.list(filter);
      if (refs.size() != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }
      return coreQueue.expireReference(refs.get(0).getMessage().getMessageID());
   }

   public int expireMessages(final String filterStr) throws Exception
   {
      try
      {
         Filter filter = createFilterFromJMSSelector(filterStr);
         return coreQueue.expireReferences(filter);
      }
      catch (MessagingException e)
      {
         throw new IllegalStateException(e.getMessage());
      }
   }

   public boolean sendMessageToDLQ(final String messageID) throws Exception
   {
      Filter filter = createFilterForJMSMessageID(messageID);
      List<MessageReference> refs = coreQueue.list(filter);
      if (refs.size() != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }
      return coreQueue.sendMessageToDeadLetterAddress(refs.get(0).getMessage().getMessageID());
   }

   public boolean changeMessagePriority(final String messageID, final int newPriority) throws Exception
   {
      if (newPriority < 0 || newPriority > 9)
      {
         throw new IllegalArgumentException("invalid newPriority value: " + newPriority +
                                            ". It must be between 0 and 9 (both included)");
      }
      Filter filter = createFilterForJMSMessageID(messageID);
      List<MessageReference> refs = coreQueue.list(filter);
      if (refs.size() != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }
      return coreQueue.changeReferencePriority(refs.get(0).getMessage().getMessageID(), (byte)newPriority);
   }

   public boolean moveMessage(String messageID, String otherQueueName) throws Exception
   {
      JBossQueue otherQueue = new JBossQueue(otherQueueName);
      Binding binding = postOffice.getBinding(otherQueue.getSimpleAddress());
      if (binding == null)
      {
         throw new IllegalArgumentException("No queue found for " + otherQueueName);
      }
      Filter filter = createFilterForJMSMessageID(messageID);
      List<MessageReference> refs = coreQueue.list(filter);
      if (refs.size() != 1)
      {
         throw new IllegalArgumentException("No message found for JMSMessageID: " + messageID);
      }

      return coreQueue.moveReference(refs.get(0).getMessage().getMessageID(), binding.getAddress());
   }

   public int moveMatchingMessages(String filterStr, String otherQueueName) throws Exception
   {
      JBossQueue otherQueue = new JBossQueue(otherQueueName);
      Binding otherBinding = postOffice.getBinding(otherQueue.getSimpleAddress());
      if (otherBinding == null)
      {
         throw new IllegalArgumentException("No queue found for " + otherQueueName);
      }

      Filter filter = createFilterFromJMSSelector(filterStr);
      return coreQueue.moveReferences(filter, otherBinding.getAddress());
   }

   public int moveAllMessages(String otherQueueName) throws Exception
   {
      return moveMatchingMessages(null, otherQueueName);
   }

   public CompositeData listMessageCounter()
   {
      return MessageCounterInfo.toCompositeData(counter);
   }

   public String listMessageCounterAsHTML()
   {
      return MessageCounterHelper.listMessageCounterAsHTML(new MessageCounter[] { counter });
   }

   public TabularData listMessageCounterHistory() throws Exception
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
