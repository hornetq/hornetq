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

import java.text.DateFormat;
import java.util.GregorianCalendar;
import java.util.List;

import javax.management.MBeanInfo;
import javax.management.NotCompliantMBeanException;
import javax.management.StandardMBean;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.management.DayCounterInfo;
import org.jboss.messaging.core.management.MessageCounterInfo;
import org.jboss.messaging.core.management.MessageInfo;
import org.jboss.messaging.core.management.QueueControlMBean;
import org.jboss.messaging.core.messagecounter.MessageCounter;
import org.jboss.messaging.core.messagecounter.MessageCounter.DayCounter;
import org.jboss.messaging.core.messagecounter.impl.MessageCounterHelper;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.util.SimpleString;

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
   private final StorageManager storageManager;
   private final PostOffice postOffice;
   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;
   private final MessageCounter counter;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public QueueControl(final Queue queue, final StorageManager storageManager,
         final PostOffice postOffice,
         final HierarchicalRepository<QueueSettings> queueSettingsRepository,
         final MessageCounter counter) throws NotCompliantMBeanException
   {
      this.queue = queue;
      this.storageManager = storageManager;
      this.postOffice = postOffice;
      this.queueSettingsRepository = queueSettingsRepository;
      this.counter = counter;
   }

   // Public --------------------------------------------------------

   // QueueControlMBean implementation ------------------------------

   public String getName()
   {
      return queue.getName().toString();
   }

   public String getFilter()
   {
      Filter filter = queue.getFilter();
      return (filter != null) ? filter.getFilterString().toString() : null;
   }

   public boolean isClustered()
   {
      return queue.isClustered();
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

   public long getSizeBytes()
   {
      return queue.getSizeBytes();
   }

   public String getDeadLetterAddress()
   {
      return queueSettingsRepository.getMatch(getName()).getDeadLetterAddress().toString();
   }

   public String getExpiryQueue()
   {
      return queueSettingsRepository.getMatch(getName()).getExpiryAddress()
            .toString();
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
         MessageInfo info = new MessageInfo(message.getMessageID(), message
                                            .getDestination().toString(), message.isDurable(), message
                                            .getTimestamp(), message.getType(), message.getEncodeSize(),
                                            message.getPriority(), message.isExpired(), message
                                            .getExpiration());
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
            MessageInfo info = new MessageInfo(message.getMessageID(), message
                  .getDestination().toString(), message.isDurable(), message
                  .getTimestamp(), message.getType(), message.getEncodeSize(),
                  message.getPriority(), message.isExpired(), message
                        .getExpiration());
            for (SimpleString key : message.getPropertyNames())
            {
               Object value = message.getProperty(key);
               String valueStr = value == null ? null : value.toString();
               info.putProperty(key.toString(), valueStr);
            }
            infos[i] = info;
         }
         return MessageInfo.toTabularData(infos);
      } catch (MessagingException e)
      {
         throw new IllegalStateException(e.getMessage());
      }
   }

   public int removeAllMessages() throws Exception
   {
      try
      {
         return queue.deleteAllReferences(storageManager);
      } catch (MessagingException e)
      {
         throw new IllegalStateException(e.getMessage());
      }
   }

   public boolean removeMessage(final long messageID) throws Exception
   {
      try
      {
         return queue.deleteReference(messageID, storageManager);
      } catch (MessagingException e)
      {
         throw new IllegalStateException(e.getMessage());
      }
   }
   
   public int removeMatchingMessages(String filterStr) throws Exception
   {
      Filter filter = FilterImpl.createFilter(filterStr);
      return queue.deleteMatchingReferences(filter, storageManager);
   }

   public boolean expireMessage(final long messageID) throws Exception
   {
      return queue.expireMessage(messageID, storageManager, postOffice,
            queueSettingsRepository);
   }

   public int expireMessages(final String filterStr) throws Exception
   {
      try
      {
         Filter filter = FilterImpl.createFilter(filterStr);
         return queue.expireMessages(filter, storageManager, postOffice, queueSettingsRepository);
      } catch (MessagingException e)
      {
         throw new IllegalStateException(e.getMessage());
      }
   }

   public boolean moveMessage(final long messageID, final String otherQueueName)
         throws Exception
   {
      Binding binding = postOffice.getBinding(new SimpleString(otherQueueName));
      if (binding == null)
      {
         throw new IllegalArgumentException("No queue found for "
               + otherQueueName);
      }

      return queue.moveMessage(messageID, binding.getAddress(), storageManager, postOffice);
   }
   
   public int moveMatchingMessages(String filterStr, String otherQueueName) throws Exception
   {
      Filter filter = FilterImpl.createFilter(filterStr);
      Binding binding = postOffice.getBinding(new SimpleString(otherQueueName));
      if (binding == null)
      {
         throw new IllegalArgumentException("No queue found for "
               + otherQueueName);
      }

      return queue.moveMessages(filter, binding.getAddress(), storageManager, postOffice);
   }
   
   public int moveAllMessages(String otherQueueName) throws Exception
   {
      return moveMatchingMessages(null, otherQueueName);
   }

   public boolean sendMessageToDeadLetterAddress(final long messageID) throws Exception
   {
      return queue.sendMessageToDeadLetterAddress(messageID, storageManager, postOffice,
            queueSettingsRepository);
   }

   public boolean changeMessagePriority(final long messageID,
         final int newPriority) throws Exception
   {
      if (newPriority < 0 || newPriority > 9)
      {
         throw new IllegalArgumentException("invalid newPriority value: "
               + newPriority + ". It must be between 0 and 9 (both included)");
      }
      return queue.changeMessagePriority(messageID, (byte) newPriority,
            storageManager, postOffice, queueSettingsRepository);
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
      try {
         List<DayCounter> history = counter.getHistory();
         DayCounterInfo[] infos = new DayCounterInfo[history.size()];
         for (int i = 0; i < infos.length; i++)
         {
            DayCounter dayCounter = history.get(i);
            int[] counters = dayCounter.getCounters();
            GregorianCalendar date = dayCounter.getDate();

            DateFormat dateFormat = DateFormat.getDateInstance(DateFormat.SHORT);
            String strData = dateFormat.format(date.getTime());
            infos[i] = new DayCounterInfo(strData, counters);
         }
         return DayCounterInfo.toTabularData(infos);
      } catch (Throwable t)
      {
         t.printStackTrace();
         return null;
      }
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
