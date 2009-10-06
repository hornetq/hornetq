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

package org.hornetq.core.management.impl;

import java.util.List;
import java.util.Map;

import javax.management.StandardMBean;

import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.filter.impl.FilterImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.MessageCounterInfo;
import org.hornetq.core.management.QueueControl;
import org.hornetq.core.message.Message;
import org.hornetq.core.messagecounter.MessageCounter;
import org.hornetq.core.messagecounter.impl.MessageCounterHelper;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.json.JSONArray;
import org.hornetq.utils.json.JSONObject;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 * 
 */
public class QueueControlImpl extends StandardMBean implements QueueControl
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(QueueControlImpl.class);

   // Attributes ----------------------------------------------------

   private final Queue queue;

   private final String address;

   private final PostOffice postOffice;

   private final HierarchicalRepository<AddressSettings> addressSettingsRepository;

   private MessageCounter counter;

   // Static --------------------------------------------------------

   private static String toJSON(Map<String, Object>[] messages)
   {
      JSONArray array = new JSONArray();
      for (int i = 0; i < messages.length; i++)
      {
         Map<String, Object> message = messages[i];
         array.put(new JSONObject(message));
      }
      return array.toString();
   }

   /**
    * Returns null if the string is null or empty
    */
   public static Filter createFilter(final String filterStr) throws HornetQException
   {
      if (filterStr == null || filterStr.trim().length() == 0)
      {
         return null;
      }
      else
      {
         return new FilterImpl(new SimpleString(filterStr));
      }
   }

   // Constructors --------------------------------------------------

   public QueueControlImpl(final Queue queue,
                           final String address,
                           final PostOffice postOffice,
                           final HierarchicalRepository<AddressSettings> addressSettingsRepository) throws Exception
   {
      super(QueueControl.class);
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

   public long getID()
   {
      return queue.getID();
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

   public void setExpiryAddress(final String expiryAddress) throws Exception
   {
      AddressSettings addressSettings = addressSettingsRepository.getMatch(address);

      SimpleString sExpiryAddress = new SimpleString(expiryAddress);

      if (expiryAddress != null)
      {
         addressSettings.setExpiryAddress(sExpiryAddress);
      }

      queue.setExpiryAddress(sExpiryAddress);
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

   public String listScheduledMessagesAsJSON() throws Exception
   {
      return toJSON(listScheduledMessages());
   }

   public Map<String, Object>[] listMessages(final String filterStr) throws Exception
   {
      try
      {
         Filter filter = createFilter(filterStr);
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
      Filter filter = createFilter(filterStr);
      List<MessageReference> refs = queue.list(filter);
      return refs.size();
   }

   public boolean removeMessage(final long messageID) throws Exception
   {
      try
      {
         return queue.deleteReference(messageID);
      }
      catch (HornetQException e)
      {
         throw new IllegalStateException(e.getMessage());
      }
   }

   public int removeMessages(final String filterStr) throws Exception
   {
      Filter filter = createFilter(filterStr);
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
         Filter filter = createFilter(filterStr);
         return queue.expireReferences(filter);
      }
      catch (HornetQException e)
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

   public int moveMessages(final String filterStr, final String otherQueueName) throws Exception
   {
      Filter filter = createFilter(filterStr);

      Binding binding = postOffice.getBinding(new SimpleString(otherQueueName));

      if (binding == null)
      {
         throw new IllegalArgumentException("No queue found for " + otherQueueName);
      }

      return queue.moveReferences(filter, binding.getAddress());
   }

   public int sendMessagesToDeadLetterAddress(final String filterStr) throws Exception
   {
      Filter filter = createFilter(filterStr);

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
      Filter filter = createFilter(filterStr);

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

   public void resetMessageCounter()
   {
      counter.resetCounter();
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

   public void pause()
   {
      queue.pause();
   }

   public void resume()
   {
      queue.resume();
   }

   public boolean isPaused() throws Exception
   {
      return queue.isPaused();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
