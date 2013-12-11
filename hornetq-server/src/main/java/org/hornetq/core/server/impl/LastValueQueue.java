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
package org.hornetq.core.server.impl;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.api.core.Message;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.message.impl.MessageImpl;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.core.transaction.Transaction;

/**
 * A queue that will discard messages if a newer message with the same
 * {@link MessageImpl#HDR_LAST_VALUE_NAME} property value. In other words it only retains the last
 * value
 * <p>
 * This is useful for example, for stock prices, where you're only interested in the latest value
 * for a particular stock
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> rewrite
 */
public class LastValueQueue extends QueueImpl
{
   private final Map<SimpleString, HolderReference> map = new ConcurrentHashMap<SimpleString, HolderReference>();

   public LastValueQueue(final long persistenceID,
                         final SimpleString address,
                         final SimpleString name,
                         final Filter filter,
                         final PageSubscription pageSubscription,
                         final boolean durable,
                         final boolean temporary,
                         final ScheduledExecutorService scheduledExecutor,
                         final PostOffice postOffice,
                         final StorageManager storageManager,
                         final HierarchicalRepository<AddressSettings> addressSettingsRepository,
                         final Executor executor)
   {
      super(persistenceID,
            address,
            name,
            filter,
            pageSubscription,
            durable,
            temporary,
            scheduledExecutor,
            postOffice,
            storageManager,
            addressSettingsRepository,
            executor);
      new Exception ("LastValueQeue " + this ).toString();
   }

   @Override
   public synchronized void addTail(final MessageReference ref, final boolean direct)
   {
      SimpleString prop = ref.getMessage().getSimpleStringProperty(Message.HDR_LAST_VALUE_NAME);

      if (prop != null)
      {
         HolderReference hr = map.get(prop);

         if (hr != null)
         {
            // We need to overwrite the old ref with the new one and ack the old one

            MessageReference oldRef = hr.getReference();

            referenceHandled();

            try
            {
               oldRef.acknowledge();
            }
            catch (Exception e)
            {
               HornetQServerLogger.LOGGER.errorAckingOldReference(e);
            }

            hr.setReference(ref);

         }
         else
         {
            hr = new HolderReference(prop, ref);

            map.put(prop, hr);

            super.addTail(hr, direct);
         }
      }
      else
      {
         super.addTail(ref, direct);
      }
   }

   @Override
   public synchronized void addHead(final MessageReference ref)
   {
      SimpleString prop = ref.getMessage().getSimpleStringProperty(Message.HDR_LAST_VALUE_NAME);

      if (prop != null)
      {
         HolderReference hr = map.get(prop);

         if (hr != null)
         {
            // We keep the current ref and ack the one we are returning

            super.referenceHandled();

            try
            {
               super.acknowledge(ref);
            }
            catch (Exception e)
            {
               HornetQServerLogger.LOGGER.errorAckingOldReference(e);
            }
         }
         else
         {
            map.put(prop, (HolderReference)ref);

            super.addHead(ref);
         }
      }
      else
      {
         super.addHead(ref);
      }
   }


   @Override
   protected void refRemoved(MessageReference ref)
   {
      synchronized (this)
      {
         SimpleString prop = ref.getMessage().getSimpleStringProperty(Message.HDR_LAST_VALUE_NAME);

         if (prop != null)
         {
            map.remove(prop);
         }
      }

      super.refRemoved(ref);
   }

   private class HolderReference implements MessageReference
   {
      private final SimpleString prop;

      private volatile MessageReference ref;

      private Long consumerId;

      HolderReference(final SimpleString prop, final MessageReference ref)
      {
         this.prop = prop;

         this.ref = ref;
      }

      MessageReference getReference()
      {
         return ref;
      }

      public void handled()
      {
         ref.handled();
         // We need to remove the entry from the map just before it gets delivered
         map.remove(prop);
      }

      void setReference(final MessageReference ref)
      {
         this.ref = ref;
      }

      public MessageReference copy(final Queue queue)
      {
         return ref.copy(queue);
      }

      public void decrementDeliveryCount()
      {
         ref.decrementDeliveryCount();
      }

      public int getDeliveryCount()
      {
         return ref.getDeliveryCount();
      }

      public ServerMessage getMessage()
      {
         return ref.getMessage();
      }

      public Queue getQueue()
      {
         return ref.getQueue();
      }

      public long getScheduledDeliveryTime()
      {
         return ref.getScheduledDeliveryTime();
      }

      public void incrementDeliveryCount()
      {
         ref.incrementDeliveryCount();
      }

      public void setDeliveryCount(final int deliveryCount)
      {
         ref.setDeliveryCount(deliveryCount);
      }

      public void setScheduledDeliveryTime(final long scheduledDeliveryTime)
      {
         ref.setScheduledDeliveryTime(scheduledDeliveryTime);
      }

      public void setPersistedCount(int count)
      {
         ref.setPersistedCount(count);
      }

      public int getPersistedCount()
      {
         return ref.getPersistedCount();
      }

      public boolean isPaged()
      {
         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.MessageReference#acknowledge(org.hornetq.core.server.MessageReference)
       */
      public void acknowledge() throws Exception
      {
         ref.getQueue().acknowledge(this);
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.MessageReference#acknowledge(org.hornetq.core.transaction.Transaction, org.hornetq.core.server.MessageReference)
       */
      public void acknowledge(Transaction tx) throws Exception
      {
         ref.getQueue().acknowledge(tx, this);
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.MessageReference#getMessageMemoryEstimate()
       */
      public int getMessageMemoryEstimate()
      {
         return ref.getMessage().getMemoryEstimate();
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.MessageReference#setConsumerId(java.lang.Long)
       */
      @Override
      public void setConsumerId(Long consumerID)
      {
         this.consumerId = consumerID;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.MessageReference#getConsumerId()
       */
      @Override
      public Long getConsumerId()
      {
         return this.consumerId;
      }
   }

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((map == null) ? 0 : map.hashCode());
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
      {
         return true;
      }
      if (!super.equals(obj))
      {
         return false;
      }
      if (!(obj instanceof LastValueQueue))
      {
         return false;
      }
      LastValueQueue other = (LastValueQueue)obj;
      if (map == null)
      {
         if (other.map != null)
         {
            return false;
         }
      }
      else if (!map.equals(other.map))
      {
         return false;
      }
      return true;
   }
}
