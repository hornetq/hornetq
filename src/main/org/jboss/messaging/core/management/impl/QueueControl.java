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

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.management.MessageCounterInfo;
import org.jboss.messaging.core.management.MessageInfo;
import org.jboss.messaging.core.management.QueueControlMBean;
import org.jboss.messaging.core.messagecounter.MessageCounter;
import org.jboss.messaging.core.messagecounter.impl.MessageCounterHelper;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
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

   private final MessageCounter counter;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public QueueControl(final Queue queue,
                       final String address,
                       final PostOffice postOffice,
                       final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                       final MessageCounter counter)
   {
      this.queue = queue;
      this.address = address;
      this.postOffice = postOffice;
      this.addressSettingsRepository = addressSettingsRepository;
      this.counter = counter;
   }

   // Public --------------------------------------------------------

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

   public TabularData listAllMessages() throws Exception
   {
      return listMessages(null);
   }

   public TabularData listScheduledMessages() throws Exception
   {
      List<MessageReference> refs = queue.getScheduledMessages();
      MessageInfo[] infos = new MessageInfo[refs.size()];
      for (int i = 0; i < refs.size(); i++)
      {
         MessageReference ref = refs.get(i);
         ServerMessage message = ref.getMessage();
         MessageInfo info = new MessageInfo(message.getMessageID(),
                                            message.getDestination().toString(),
                                            message.isDurable(),
                                            message.getTimestamp(),
                                            message.getType(),
                                            message.getEncodeSize(),
                                            message.getPriority(),
                                            message.isExpired(),
                                            message.getExpiration());
         for (SimpleString key : message.getPropertyNames())
         {
            Object value = message.getProperty(key);
            String valueStr = value == null ? null : value.toString();
            info.putProperty(key.toString(), valueStr);
         }
         infos[i] = info;
      }
      return MessageInfo.toTabularData(infos);
   }

   public TabularData listMessages(final String filterStr) throws Exception
   {
      try
      {
         Filter filter = FilterImpl.createFilter(filterStr);
         List<MessageReference> refs = queue.list(filter);
         MessageInfo[] infos = new MessageInfo[refs.size()];
         for (int i = 0; i < refs.size(); i++)
         {
            MessageReference ref = refs.get(i);
            ServerMessage message = ref.getMessage();
            MessageInfo info = new MessageInfo(message.getMessageID(),
                                               message.getDestination().toString(),
                                               message.isDurable(),
                                               message.getTimestamp(),
                                               message.getType(),
                                               message.getEncodeSize(),
                                               message.getPriority(),
                                               message.isExpired(),
                                               message.getExpiration());
            for (SimpleString key : message.getPropertyNames())
            {
               Object value = message.getProperty(key);
               String valueStr = value == null ? null : value.toString();
               info.putProperty(key.toString(), valueStr);
            }
            infos[i] = info;
         }
         return MessageInfo.toTabularData(infos);
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
      return queue.expireMessage(messageID);
   }

   public int expireMessages(final String filterStr) throws Exception
   {
      try
      {
         Filter filter = FilterImpl.createFilter(filterStr);
         
         return queue.expireMessages(filter);
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

      return queue.moveMessage(messageID, binding.getAddress());
   }

   public int moveMatchingMessages(final String filterStr, final String otherQueueName) throws Exception
   {
      Filter filter = FilterImpl.createFilter(filterStr);
      
      Binding binding = postOffice.getBinding(new SimpleString(otherQueueName));
      
      if (binding == null)
      {
         throw new IllegalArgumentException("No queue found for " + otherQueueName);
      }

      return queue.moveMessages(filter, binding.getAddress());
   }

   public int moveAllMessages(String otherQueueName) throws Exception
   {
      return moveMatchingMessages(null, otherQueueName);
   }

   public boolean sendMessageToDeadLetterAddress(final long messageID) throws Exception
   {
      return queue.sendMessageToDeadLetterAddress(messageID);
   }

   public boolean changeMessagePriority(final long messageID, final int newPriority) throws Exception
   {
      if (newPriority < 0 || newPriority > 9)
      {
         throw new IllegalArgumentException("invalid newPriority value: " + newPriority +
                                            ". It must be between 0 and 9 (both included)");
      }
      return queue.changeMessagePriority(messageID, (byte)newPriority);
   }

   public CompositeData listMessageCounter()
   {
      return MessageCounterInfo.toCompositeData(counter);
   }
   
   public void resetMessageCounter()
   {
      counter.resetCounter();
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
