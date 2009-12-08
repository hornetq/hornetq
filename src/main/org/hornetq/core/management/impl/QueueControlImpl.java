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

import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.filter.impl.FilterImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.MessageCounterInfo;
import org.hornetq.core.management.QueueControl;
import org.hornetq.core.message.Message;
import org.hornetq.core.messagecounter.MessageCounter;
import org.hornetq.core.messagecounter.impl.MessageCounterHelper;
import org.hornetq.core.persistence.StorageManager;
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
public class QueueControlImpl extends AbstractControl implements QueueControl
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

   private static String toJSON(final Map<String, Object>[] messages)
   {
      JSONArray array = new JSONArray();
      for (Map<String, Object> message : messages)
      {
         array.put(new JSONObject(message));
      }
      return array.toString();
   }

   // Constructors --------------------------------------------------

   public QueueControlImpl(final Queue queue,
                           final String address,
                           final PostOffice postOffice,
                           final StorageManager storageManager,
                           final HierarchicalRepository<AddressSettings> addressSettingsRepository) throws Exception
   {
      super(QueueControl.class, storageManager);
      this.queue = queue;
      this.address = address;
      this.postOffice = postOffice;
      this.addressSettingsRepository = addressSettingsRepository;
   }

   // Public --------------------------------------------------------

   public void setMessageCounter(final MessageCounter counter)
   {
      this.counter = counter;
   }

   // QueueControlMBean implementation ------------------------------

   public String getName()
   {
      clearIO();
      try
      {
         return queue.getName().toString();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getAddress()
   {
      return address;
   }

   public String getFilter()
   {
      clearIO();
      try
      {
         Filter filter = queue.getFilter();

         return filter != null ? filter.getFilterString().toString() : null;
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isDurable()
   {
      clearIO();
      try
      {
         return queue.isDurable();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isTemporary()
   {
      clearIO();
      try
      {
         return queue.isTemporary();
      }
      finally
      {
         blockOnIO();
      }
   }

   public int getMessageCount()
   {
      clearIO();
      try
      {
         return queue.getMessageCount();
      }
      finally
      {
         blockOnIO();
      }
   }

   public int getConsumerCount()
   {
      clearIO();
      try
      {
         return queue.getConsumerCount();
      }
      finally
      {
         blockOnIO();
      }
   }

   public int getDeliveringCount()
   {
      clearIO();
      try
      {
         return queue.getDeliveringCount();
      }
      finally
      {
         blockOnIO();
      }
   }

   public int getMessagesAdded()
   {
      clearIO();
      try
      {
         return queue.getMessagesAdded();
      }
      finally
      {
         blockOnIO();
      }
   }

   public long getID()
   {
      clearIO();
      try
      {
         return queue.getID();
      }
      finally
      {
         blockOnIO();
      }
   }

   public long getScheduledCount()
   {
      clearIO();
      try
      {
         return queue.getScheduledCount();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getDeadLetterAddress()
   {
      clearIO();
      try
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
      finally
      {
         blockOnIO();
      }
   }

   public void setDeadLetterAddress(final String deadLetterAddress) throws Exception
   {
      clearIO();
      try
      {
         AddressSettings addressSettings = addressSettingsRepository.getMatch(address);

         if (deadLetterAddress != null)
         {
            addressSettings.setDeadLetterAddress(new SimpleString(deadLetterAddress));
         }
      }
      finally
      {
         blockOnIO();
      }
   }

   public String getExpiryAddress()
   {
      clearIO();
      try
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
      finally
      {
         blockOnIO();
      }
   }

   public void setExpiryAddress(final String expiryAddress) throws Exception
   {
      clearIO();
      try
      {
         AddressSettings addressSettings = addressSettingsRepository.getMatch(address);

         SimpleString sExpiryAddress = new SimpleString(expiryAddress);

         if (expiryAddress != null)
         {
            addressSettings.setExpiryAddress(sExpiryAddress);
         }

         queue.setExpiryAddress(sExpiryAddress);
      }
      finally
      {
         blockOnIO();
      }
   }

   public Map<String, Object>[] listScheduledMessages() throws Exception
   {
      clearIO();
      try
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
      finally
      {
         blockOnIO();
      }
   }

   public String listScheduledMessagesAsJSON() throws Exception
   {
      clearIO();
      try
      {
         return QueueControlImpl.toJSON(listScheduledMessages());
      }
      finally
      {
         blockOnIO();
      }
   }

   public Map<String, Object>[] listMessages(final String filterStr) throws Exception
   {
      clearIO();
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
      catch (HornetQException e)
      {
         throw new IllegalStateException(e.getMessage());
      }
      finally
      {
         blockOnIO();
      }
   }

   public String listMessagesAsJSON(final String filter) throws Exception
   {
      clearIO();
      try
      {
         return QueueControlImpl.toJSON(listMessages(filter));
      }
      finally
      {
         blockOnIO();
      }
   }

   public int countMessages(final String filterStr) throws Exception
   {
      clearIO();
      try
      {
         Filter filter = FilterImpl.createFilter(filterStr);
         List<MessageReference> refs = queue.list(filter);
         return refs.size();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean removeMessage(final long messageID) throws Exception
   {
      clearIO();
      try
      {
         return queue.deleteReference(messageID);
      }
      catch (HornetQException e)
      {
         throw new IllegalStateException(e.getMessage());
      }
      finally
      {
         blockOnIO();
      }
   }

   public int removeMessages(final String filterStr) throws Exception
   {
      clearIO();
      try
      {
         Filter filter = FilterImpl.createFilter(filterStr);

         return queue.deleteMatchingReferences(filter);
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean expireMessage(final long messageID) throws Exception
   {
      clearIO();
      try
      {
         return queue.expireReference(messageID);
      }
      finally
      {
         blockOnIO();
      }
   }

   public int expireMessages(final String filterStr) throws Exception
   {
      clearIO();
      try
      {
         Filter filter = FilterImpl.createFilter(filterStr);
         return queue.expireReferences(filter);
      }
      catch (HornetQException e)
      {
         throw new IllegalStateException(e.getMessage());
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean moveMessage(final long messageID, final String otherQueueName) throws Exception
   {
      clearIO();
      try
      {
         Binding binding = postOffice.getBinding(new SimpleString(otherQueueName));

         if (binding == null)
         {
            throw new IllegalArgumentException("No queue found for " + otherQueueName);
         }

         return queue.moveReference(messageID, binding.getAddress());
      }
      finally
      {
         blockOnIO();
      }

   }

   public int moveMessages(final String filterStr, final String otherQueueName) throws Exception
   {
      clearIO();
      try
      {
         Filter filter = FilterImpl.createFilter(filterStr);

         Binding binding = postOffice.getBinding(new SimpleString(otherQueueName));

         if (binding == null)
         {
            throw new IllegalArgumentException("No queue found for " + otherQueueName);
         }

         int retValue = queue.moveReferences(filter, binding.getAddress());

         return retValue;
      }
      finally
      {
         blockOnIO();
      }

   }

   public int sendMessagesToDeadLetterAddress(final String filterStr) throws Exception
   {
      clearIO();
      try
      {
         Filter filter = FilterImpl.createFilter(filterStr);

         List<MessageReference> refs = queue.list(filter);

         for (MessageReference ref : refs)
         {
            sendMessageToDeadLetterAddress(ref.getMessage().getMessageID());
         }

         return refs.size();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean sendMessageToDeadLetterAddress(final long messageID) throws Exception
   {
      clearIO();
      try
      {

         boolean retValue = queue.sendMessageToDeadLetterAddress(messageID);

         return retValue;
      }
      finally
      {
         blockOnIO();
      }
   }

   public int changeMessagesPriority(final String filterStr, final int newPriority) throws Exception
   {
      clearIO();
      try
      {
         Filter filter = FilterImpl.createFilter(filterStr);

         List<MessageReference> refs = queue.list(filter);

         for (MessageReference ref : refs)
         {
            changeMessagePriority(ref.getMessage().getMessageID(), newPriority);
         }

         return refs.size();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean changeMessagePriority(final long messageID, final int newPriority) throws Exception
   {
      clearIO();
      try
      {
         if (newPriority < 0 || newPriority > 9)
         {
            throw new IllegalArgumentException("invalid newPriority value: " + newPriority +
                                               ". It must be between 0 and 9 (both included)");
         }
         return queue.changeReferencePriority(messageID, (byte)newPriority);
      }
      finally
      {
         blockOnIO();
      }
   }

   public String listMessageCounter()
   {
      clearIO();
      try
      {
         return MessageCounterInfo.toJSon(counter);
      }
      catch (Exception e)
      {
         throw new IllegalStateException(e);
      }
      finally
      {
         blockOnIO();
      }
   }

   public void resetMessageCounter()
   {
      clearIO();
      try
      {
         counter.resetCounter();
      }
      finally
      {
         blockOnIO();
      }
   }

   public String listMessageCounterAsHTML()
   {
      clearIO();
      try
      {
         return MessageCounterHelper.listMessageCounterAsHTML(new MessageCounter[] { counter });
      }
      finally
      {
         blockOnIO();
      }
   }

   public String listMessageCounterHistory() throws Exception
   {
      clearIO();
      try
      {
         return MessageCounterHelper.listMessageCounterHistory(counter);
      }
      finally
      {
         blockOnIO();
      }
   }

   public String listMessageCounterHistoryAsHTML()
   {
      clearIO();
      try
      {
         return MessageCounterHelper.listMessageCounterHistoryAsHTML(new MessageCounter[] { counter });
      }
      finally
      {
         blockOnIO();
      }
   }

   public void pause()
   {
      clearIO();
      try
      {
         queue.pause();
      }
      finally
      {
         blockOnIO();
      }
   }

   public void resume()
   {
      clearIO();
      try
      {
         queue.resume();
      }
      finally
      {
         blockOnIO();
      }
   }

   public boolean isPaused() throws Exception
   {
      clearIO();
      try
      {
         return queue.isPaused();
      }
      finally
      {
         blockOnIO();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
