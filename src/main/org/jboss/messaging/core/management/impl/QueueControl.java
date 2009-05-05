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

package org.jboss.messaging.core.management.impl;

import java.util.List;
import java.util.Map;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.management.QueueControlMBean;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.messagecounter.MessageCounter;
import org.jboss.messaging.core.messagecounter.impl.MessageCounterHelper;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.AddressSettings;
import org.jboss.messaging.utils.SimpleString;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class QueueControl implements QueueControlMBean
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final Queue queue;

   private final String address;

   private final PostOffice postOffice;

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private MessageCounter counter;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public QueueControl(final Queue queue,
                       final String address,
                       final PostOffice postOffice,
                       final HierarchicalRepository<AddressSettings> addressSettingsRepository)
   {
      this.queue = queue;
      this.address = address;
      this.postOffice = postOffice;
      this.addressSettingsRepository = addressSettingsRepository;
   }

   // Public --------------------------------------------------------

   public void setMessageCounter(MessageCounter counter)
   {
      this.counter = counter;
   }

   // QueueControlMBean implementation ------------------------------

   public String getName()
   {
      return queue.getName().toString();
   }

   public String getAddress()
   {
      return address;
   }

   public String getFilter()
   {
      Filter filter = queue.getFilter();

      return (filter != null) ? filter.getFilterString().toString() : null;
   }

   public boolean isDurable()
   {
      return queue.isDurable();
   }

   public boolean isTemporary()
   {
      return queue.isTemporary();
   }

   public boolean isBackup()
   {
      return queue.isBackup();
   }

   public int getMessageCount()
   {
      return queue.getMessageCount();
   }

   public int getConsumerCount()
   {
      return queue.getConsumerCount();
   }

   public int getDeliveringCount()
   {
      return queue.getDeliveringCount();
   }

   public int getMessagesAdded()
   {
      return queue.getMessagesAdded();
   }

   public long getPersistenceID()
   {
      return queue.getPersistenceID();
   }

   public long getScheduledCount()
   {
      return queue.getScheduledCount();
   }

   public String getDeadLetterAddress()
   {
      AddressSettings addressSettings = addressSettingsRepository.getMatch(address);

      if (addressSettings != null && addressSettings.getDeadLetterAddress() != null)
      {
         return addressSettings.getDeadLetterAddress().toString();
      }
      else
      {
         return null;
      }
   }

   public void setDeadLetterAddress(final String deadLetterAddress) throws Exception
   {
      AddressSettings addressSettings = addressSettingsRepository.getMatch(address);

      if (deadLetterAddress != null)
      {
         addressSettings.setDeadLetterAddress(new SimpleString(deadLetterAddress));
      }
   }

   public String getExpiryAddress()
   {
      AddressSettings addressSettings = addressSettingsRepository.getMatch(address);

      if (addressSettings != null && addressSettings.getExpiryAddress() != null)
      {
         return addressSettings.getExpiryAddress().toString();
      }
      else
      {
         return null;
      }
   }

   public void setExpiryAddress(final String expiryAddres) throws Exception
   {
      AddressSettings addressSettings = addressSettingsRepository.getMatch(address);

      if (expiryAddres != null)
      {
         addressSettings.setExpiryAddress(new SimpleString(expiryAddres));
      }
   }

   public Map<String, Object>[] listAllMessages() throws Exception
   {
      return listMessages(null);
   }

   public Map<String, Object>[] listScheduledMessages() throws Exception
   {
      List<MessageReference> refs = queue.getScheduledMessages();
      Map<String, Object>[] messages = new Map[refs.size()];
      int i = 0;
      for (MessageReference ref : refs)
      {
         Message message = ref.getMessage();
         messages[i++] = message.toMap();
      }
      return messages;
   }

   public Map[] listMessages(final String filterStr) throws Exception
   {
      try
      {
         Filter filter = FilterImpl.createFilter(filterStr);
         List<MessageReference> refs = queue.list(filter);
         Map<String, Object>[] messages = new Map[refs.size()];
         int i = 0;
         for (MessageReference ref : refs)
         {
            Message message = ref.getMessage();
            messages[i++] = message.toMap();
         }
         return messages;
      }
      catch (MessagingException e)
      {
         throw new IllegalStateException(e.getMessage());
      }
   }

   public int countMessages(final String filterStr) throws Exception
   {
      Filter filter = FilterImpl.createFilter(filterStr);
      List<MessageReference> refs = queue.list(filter);
      return refs.size();
   }

   public int removeAllMessages() throws Exception
   {
      try
      {
         return queue.deleteAllReferences();
      }
      catch (MessagingException e)
      {
         throw new IllegalStateException(e.getMessage());
      }
   }

   public boolean removeMessage(final long messageID) throws Exception
   {
      try
      {
         return queue.deleteReference(messageID);
      }
      catch (MessagingException e)
      {
         throw new IllegalStateException(e.getMessage());
      }
   }

   public int removeMatchingMessages(final String filterStr) throws Exception
   {
      Filter filter = FilterImpl.createFilter(filterStr);

      return queue.deleteMatchingReferences(filter);
   }

   public boolean expireMessage(final long messageID) throws Exception
   {
      return queue.expireReference(messageID);
   }

   public int expireMessages(final String filterStr) throws Exception
   {
      try
      {
         Filter filter = FilterImpl.createFilter(filterStr);

         return queue.expireReferences(filter);
      }
      catch (MessagingException e)
      {
         throw new IllegalStateException(e.getMessage());
      }
   }

   public boolean moveMessage(final long messageID, final String otherQueueName) throws Exception
   {
      Binding binding = postOffice.getBinding(new SimpleString(otherQueueName));

      if (binding == null)
      {
         throw new IllegalArgumentException("No queue found for " + otherQueueName);
      }

      return queue.moveReference(messageID, binding.getAddress());
   }

   public int moveMatchingMessages(final String filterStr, final String otherQueueName) throws Exception
   {
      Filter filter = FilterImpl.createFilter(filterStr);

      Binding binding = postOffice.getBinding(new SimpleString(otherQueueName));

      if (binding == null)
      {
         throw new IllegalArgumentException("No queue found for " + otherQueueName);
      }

      return queue.moveReferences(filter, binding.getAddress());
   }

   public int moveAllMessages(String otherQueueName) throws Exception
   {
      return moveMatchingMessages(null, otherQueueName);
   }

   public int sendMessagesToDeadLetterAddress(final String filterStr) throws Exception
   {
      Filter filter = filterStr == null ? null : new FilterImpl(new SimpleString(filterStr));

      List<MessageReference> refs = queue.list(filter);

      for (MessageReference ref : refs)
      {
         sendMessageToDeadLetterAddress(ref.getMessage().getMessageID());
      }

      return refs.size();
   }

   public boolean sendMessageToDeadLetterAddress(final long messageID) throws Exception
   {
      return queue.sendMessageToDeadLetterAddress(messageID);
   }

   public int changeMessagesPriority(String filterStr, int newPriority) throws Exception
   {
      Filter filter = filterStr == null ? null : new FilterImpl(new SimpleString(filterStr));

      List<MessageReference> refs = queue.list(filter);

      for (MessageReference ref : refs)
      {
         changeMessagePriority(ref.getMessage().getMessageID(), newPriority);
      }

      return refs.size();
   }

   public boolean changeMessagePriority(final long messageID, final int newPriority) throws Exception
   {
      if (newPriority < 0 || newPriority > 9)
      {
         throw new IllegalArgumentException("invalid newPriority value: " + newPriority +
                                            ". It must be between 0 and 9 (both included)");
      }
      return queue.changeReferencePriority(messageID, (byte)newPriority);
   }

   public Object[] listMessageCounter()
   {
      Object[] counterData = new Object[] { counter.getDestinationName(),
                                           counter.getDestinationSubscription(),
                                           counter.isDestinationDurable(),
                                           counter.getCount(),
                                           counter.getCountDelta(),
                                           counter.getMessageCount(),
                                           counter.getMessageCountDelta(),
                                           counter.getLastAddedMessageTime(),
                                           counter.getLastUpdate() };
      return counterData;
   }

   public void resetMessageCounter()
   {
      counter.resetCounter();
   }

   public String listMessageCounterAsHTML()
   {
      return MessageCounterHelper.listMessageCounterAsHTML(new MessageCounter[] { counter });
   }

   public Object[] listMessageCounterHistory() throws Exception
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
