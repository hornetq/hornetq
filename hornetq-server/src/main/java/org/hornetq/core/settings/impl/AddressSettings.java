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

package org.hornetq.core.settings.impl;

import java.io.Serializable;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.settings.Mergeable;
import org.hornetq.utils.BufferHelper;

/**
 * Configuration settings that are applied on the address level
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class AddressSettings implements Mergeable<AddressSettings>, Serializable, EncodingSupport
{
   private static final long serialVersionUID = 1607502280582336366L;


   /**
    * defaults used if null, this allows merging
    */
   public static final long DEFAULT_MAX_SIZE_BYTES = -1;

   public static final AddressFullMessagePolicy DEFAULT_ADDRESS_FULL_MESSAGE_POLICY = AddressFullMessagePolicy.PAGE;

   public static final long DEFAULT_PAGE_SIZE = 10 * 1024 * 1024;

   public static final int DEFAULT_MAX_DELIVERY_ATTEMPTS = 10;
   
   public static final int DEFAULT_PAGE_MAX_CACHE = 5;

   public static final int DEFAULT_MESSAGE_COUNTER_HISTORY_DAY_LIMIT = 0;

   public static final long DEFAULT_REDELIVER_DELAY = 0L;

   public static final double DEFAULT_REDELIVER_MULTIPLIER = 1.0;

   public static final boolean DEFAULT_LAST_VALUE_QUEUE = false;

   public static final long DEFAULT_REDISTRIBUTION_DELAY = -1;

   public static final long DEFAULT_EXPIRY_DELAY = -1;

   public static final boolean DEFAULT_SEND_TO_DLA_ON_NO_ROUTE = false;

   private AddressFullMessagePolicy addressFullMessagePolicy = null;

   private Long maxSizeBytes = null;

   private Long pageSizeBytes = null;
   
   private Integer pageMaxCache = null;

   private Boolean dropMessagesWhenFull = null;

   private Integer maxDeliveryAttempts = null;

   private Integer messageCounterHistoryDayLimit = null;

   private Long redeliveryDelay = null;
   
   private Double redeliveryMultiplier = null;
   
   private Long maxRedeliveryDelay = null;
   
   private SimpleString deadLetterAddress = null;

   private SimpleString expiryAddress = null;

   private Long expiryDelay = AddressSettings.DEFAULT_EXPIRY_DELAY;

   private Boolean lastValueQueue = null;

   private Long redistributionDelay = null;

   private Boolean sendToDLAOnNoRoute = null;

   public boolean isLastValueQueue()
   {
      return lastValueQueue != null ? lastValueQueue : AddressSettings.DEFAULT_LAST_VALUE_QUEUE;
   }

   public void setLastValueQueue(final boolean lastValueQueue)
   {
      this.lastValueQueue = lastValueQueue;
   }

   public AddressFullMessagePolicy getAddressFullMessagePolicy()
   {
      return addressFullMessagePolicy != null ? addressFullMessagePolicy
                                             : AddressSettings.DEFAULT_ADDRESS_FULL_MESSAGE_POLICY;
   }

   public void setAddressFullMessagePolicy(final AddressFullMessagePolicy addressFullMessagePolicy)
   {
      this.addressFullMessagePolicy = addressFullMessagePolicy;
   }

   public long getPageSizeBytes()
   {
      return pageSizeBytes != null ? pageSizeBytes : AddressSettings.DEFAULT_PAGE_SIZE;
   }

   public void setPageSizeBytes(final long pageSize)
   {
      pageSizeBytes = pageSize;
   }
   
   public int getPageCacheMaxSize()
   {
      return pageMaxCache != null ? pageMaxCache : AddressSettings.DEFAULT_PAGE_MAX_CACHE;
   }
   
   public void setPageCacheMaxSize(final int pageMaxCache)
   {
      this.pageMaxCache = pageMaxCache;
   }

   public long getMaxSizeBytes()
   {
      return maxSizeBytes != null ? maxSizeBytes : AddressSettings.DEFAULT_MAX_SIZE_BYTES;
   }

   public void setMaxSizeBytes(final long maxSizeBytes)
   {
      this.maxSizeBytes = maxSizeBytes;
   }

   public int getMaxDeliveryAttempts()
   {
      return maxDeliveryAttempts != null ? maxDeliveryAttempts : AddressSettings.DEFAULT_MAX_DELIVERY_ATTEMPTS;
   }

   public void setMaxDeliveryAttempts(final int maxDeliveryAttempts)
   {
      this.maxDeliveryAttempts = maxDeliveryAttempts;
   }

   public int getMessageCounterHistoryDayLimit()
   {
      return messageCounterHistoryDayLimit != null ? messageCounterHistoryDayLimit
                                                  : AddressSettings.DEFAULT_MESSAGE_COUNTER_HISTORY_DAY_LIMIT;
   }

   public void setMessageCounterHistoryDayLimit(final int messageCounterHistoryDayLimit)
   {
      this.messageCounterHistoryDayLimit = messageCounterHistoryDayLimit;
   }

   public long getRedeliveryDelay()
   {
      return redeliveryDelay != null ? redeliveryDelay : AddressSettings.DEFAULT_REDELIVER_DELAY;
   }

   public void setRedeliveryDelay(final long redeliveryDelay)
   {
      this.redeliveryDelay = redeliveryDelay;
   }

   public double getRedeliveryMultiplier()
   {
      return redeliveryMultiplier != null ? redeliveryMultiplier : AddressSettings.DEFAULT_REDELIVER_MULTIPLIER;
   }

   public void setRedeliveryMultiplier(final double redeliveryMultiplier)
   {
      this.redeliveryMultiplier = redeliveryMultiplier;
   }

   public long getMaxRedeliveryDelay()
   {
      return maxRedeliveryDelay != null ? maxRedeliveryDelay : getRedeliveryDelay();
   }

   public void setMaxRedeliveryDelay(final long maxRedeliveryDelay)
   {
      this.maxRedeliveryDelay = maxRedeliveryDelay;
   }

   public SimpleString getDeadLetterAddress()
   {
      return deadLetterAddress;
   }

   public void setDeadLetterAddress(final SimpleString deadLetterAddress)
   {
      this.deadLetterAddress = deadLetterAddress;
   }

   public SimpleString getExpiryAddress()
   {
      return expiryAddress;
   }

   public void setExpiryAddress(final SimpleString expiryAddress)
   {
      this.expiryAddress = expiryAddress;
   }

   public Long getExpiryDelay()
   {
      return expiryDelay;
   }

   public void setExpiryDelay(final Long expiryDelay)
   {
      this.expiryDelay = expiryDelay;
   }

   public boolean isSendToDLAOnNoRoute()
   {
      return sendToDLAOnNoRoute != null ? sendToDLAOnNoRoute : AddressSettings.DEFAULT_SEND_TO_DLA_ON_NO_ROUTE;
   }

   public void setSendToDLAOnNoRoute(final boolean value)
   {
      sendToDLAOnNoRoute = value;
   }

   public long getRedistributionDelay()
   {
      return redistributionDelay != null ? redistributionDelay : AddressSettings.DEFAULT_REDISTRIBUTION_DELAY;
   }

   public void setRedistributionDelay(final long redistributionDelay)
   {
      this.redistributionDelay = redistributionDelay;
   }

   /**
    * merge 2 objects in to 1
    * @param merged
    */
   public void merge(final AddressSettings merged)
   {
      if (maxDeliveryAttempts == null)
      {
         maxDeliveryAttempts = merged.maxDeliveryAttempts;
      }
      if (dropMessagesWhenFull == null)
      {
         dropMessagesWhenFull = merged.dropMessagesWhenFull;
      }
      if (maxSizeBytes == null)
      {
         maxSizeBytes = merged.maxSizeBytes;
      }
      if (pageMaxCache == null)
      {
         pageMaxCache = merged.pageMaxCache;
      }
      if (pageSizeBytes == null)
      {
         pageSizeBytes = merged.getPageSizeBytes();
      }
      if (messageCounterHistoryDayLimit == null)
      {
         messageCounterHistoryDayLimit = merged.messageCounterHistoryDayLimit;
      }
      if (redeliveryDelay == null)
      {
         redeliveryDelay = merged.redeliveryDelay;
      }
      if (redeliveryMultiplier == null)
      {
         redeliveryMultiplier = merged.redeliveryMultiplier;
      }
      if(maxRedeliveryDelay == null)
      {
         maxRedeliveryDelay = merged.maxRedeliveryDelay;
      }
      if (deadLetterAddress == null)
      {
         deadLetterAddress = merged.deadLetterAddress;
      }
      if (expiryAddress == null)
      {
         expiryAddress = merged.expiryAddress;
      }
      if (expiryDelay == null)
      {
         expiryDelay = merged.expiryDelay;
      }
      if (redistributionDelay == null)
      {
         redistributionDelay = merged.redistributionDelay;
      }
      if (sendToDLAOnNoRoute == null)
      {
         sendToDLAOnNoRoute = merged.sendToDLAOnNoRoute;
      }
      if (addressFullMessagePolicy == null)
      {
         addressFullMessagePolicy = merged.addressFullMessagePolicy;
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.EncodingSupport#decode(org.hornetq.api.core.HornetQBuffer)
    */
   public void decode(HornetQBuffer buffer)
   {
      SimpleString policyStr = buffer.readNullableSimpleString();

      if (policyStr != null)
      {
         addressFullMessagePolicy = AddressFullMessagePolicy.valueOf(policyStr.toString());
      }
      else
      {
         addressFullMessagePolicy = null;
      }

      maxSizeBytes = BufferHelper.readNullableLong(buffer);

      pageSizeBytes = BufferHelper.readNullableLong(buffer);
      
      pageMaxCache = BufferHelper.readNullableInteger(buffer);

      dropMessagesWhenFull = BufferHelper.readNullableBoolean(buffer);

      maxDeliveryAttempts = BufferHelper.readNullableInteger(buffer);

      messageCounterHistoryDayLimit = BufferHelper.readNullableInteger(buffer);

      redeliveryDelay = BufferHelper.readNullableLong(buffer);

      redeliveryMultiplier = BufferHelper.readNullableDouble(buffer);
      
      maxRedeliveryDelay = BufferHelper.readNullableLong(buffer);

      deadLetterAddress = buffer.readNullableSimpleString();

      expiryAddress = buffer.readNullableSimpleString();

      expiryDelay = BufferHelper.readNullableLong(buffer);

      lastValueQueue = BufferHelper.readNullableBoolean(buffer);

      redistributionDelay = BufferHelper.readNullableLong(buffer);

      sendToDLAOnNoRoute = BufferHelper.readNullableBoolean(buffer);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.EncodingSupport#getEncodeSize()
    */
   public int getEncodeSize()
   {

      return BufferHelper.sizeOfNullableSimpleString(addressFullMessagePolicy != null ? addressFullMessagePolicy.toString()
                                                                                     : null) + BufferHelper.sizeOfNullableLong(maxSizeBytes) +
             BufferHelper.sizeOfNullableLong(pageSizeBytes) +
             BufferHelper.sizeOfNullableInteger(pageMaxCache) +
             BufferHelper.sizeOfNullableBoolean(dropMessagesWhenFull) +
             BufferHelper.sizeOfNullableInteger(maxDeliveryAttempts) +
             BufferHelper.sizeOfNullableInteger(messageCounterHistoryDayLimit) +
             BufferHelper.sizeOfNullableLong(redeliveryDelay) +
             BufferHelper.sizeOfNullableDouble(redeliveryMultiplier) +
             BufferHelper.sizeOfNullableLong(maxRedeliveryDelay) +
             SimpleString.sizeofNullableString(deadLetterAddress) +
             SimpleString.sizeofNullableString(expiryAddress) +
             BufferHelper.sizeOfNullableLong(expiryDelay) +
             BufferHelper.sizeOfNullableBoolean(lastValueQueue) +
             BufferHelper.sizeOfNullableLong(redistributionDelay) +
             BufferHelper.sizeOfNullableBoolean(sendToDLAOnNoRoute);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.EncodingSupport#encode(org.hornetq.api.core.HornetQBuffer)
    */
   public void encode(HornetQBuffer buffer)
   {
      buffer.writeNullableSimpleString(addressFullMessagePolicy != null ? new SimpleString(addressFullMessagePolicy.toString())
                                                                       : null);

      BufferHelper.writeNullableLong(buffer, maxSizeBytes);

      BufferHelper.writeNullableLong(buffer, pageSizeBytes);

      BufferHelper.writeNullableInteger(buffer, pageMaxCache);

      BufferHelper.writeNullableBoolean(buffer, dropMessagesWhenFull);

      BufferHelper.writeNullableInteger(buffer, maxDeliveryAttempts);

      BufferHelper.writeNullableInteger(buffer, messageCounterHistoryDayLimit);

      BufferHelper.writeNullableLong(buffer, redeliveryDelay);
      
      BufferHelper.writeNullableDouble(buffer, redeliveryMultiplier);
      
      BufferHelper.writeNullableLong(buffer, maxRedeliveryDelay);

      buffer.writeNullableSimpleString(deadLetterAddress);

      buffer.writeNullableSimpleString(expiryAddress);

      BufferHelper.writeNullableLong(buffer, expiryDelay);

      BufferHelper.writeNullableBoolean(buffer, lastValueQueue);

      BufferHelper.writeNullableLong(buffer, redistributionDelay);

      BufferHelper.writeNullableBoolean(buffer, sendToDLAOnNoRoute);
   }

   /* (non-Javadoc)
    * @see java.lang.Object#hashCode()
    */
   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((addressFullMessagePolicy == null) ? 0 : addressFullMessagePolicy.hashCode());
      result = prime * result + ((deadLetterAddress == null) ? 0 : deadLetterAddress.hashCode());
      result = prime * result + ((dropMessagesWhenFull == null) ? 0 : dropMessagesWhenFull.hashCode());
      result = prime * result + ((expiryAddress == null) ? 0 : expiryAddress.hashCode());
      result = prime * result + ((expiryDelay == null) ? 0 : expiryDelay.hashCode());
      result = prime * result + ((lastValueQueue == null) ? 0 : lastValueQueue.hashCode());
      result = prime * result + ((maxDeliveryAttempts == null) ? 0 : maxDeliveryAttempts.hashCode());
      result = prime * result + ((maxSizeBytes == null) ? 0 : maxSizeBytes.hashCode());
      result = prime * result +
               ((messageCounterHistoryDayLimit == null) ? 0 : messageCounterHistoryDayLimit.hashCode());
      result = prime * result + ((pageSizeBytes == null) ? 0 : pageSizeBytes.hashCode());
      result = prime * result + ((pageMaxCache == null) ? 0 : pageMaxCache.hashCode());
      result = prime * result + ((redeliveryDelay == null) ? 0 : redeliveryDelay.hashCode());
      result = prime * result + ((redeliveryMultiplier == null) ? 0 : redeliveryMultiplier.hashCode());
      result = prime * result + ((maxRedeliveryDelay == null) ? 0 : maxRedeliveryDelay.hashCode());
      result = prime * result + ((redistributionDelay == null) ? 0 : redistributionDelay.hashCode());
      result = prime * result + ((sendToDLAOnNoRoute == null) ? 0 : sendToDLAOnNoRoute.hashCode());
      return result;
   }

   /* (non-Javadoc)
    * @see java.lang.Object#equals(java.lang.Object)
    */
   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      AddressSettings other = (AddressSettings)obj;
      if (addressFullMessagePolicy == null)
      {
         if (other.addressFullMessagePolicy != null)
            return false;
      }
      else if (!addressFullMessagePolicy.equals(other.addressFullMessagePolicy))
         return false;
      if (deadLetterAddress == null)
      {
         if (other.deadLetterAddress != null)
            return false;
      }
      else if (!deadLetterAddress.equals(other.deadLetterAddress))
         return false;
      if (dropMessagesWhenFull == null)
      {
         if (other.dropMessagesWhenFull != null)
            return false;
      }
      else if (!dropMessagesWhenFull.equals(other.dropMessagesWhenFull))
         return false;
      if (expiryAddress == null)
      {
         if (other.expiryAddress != null)
            return false;
      }
      else if (!expiryAddress.equals(other.expiryAddress))
         return false;
      if (expiryDelay == null)
      {
         if (other.expiryDelay != null)
            return false;
      }
      else if (!expiryDelay.equals(other.expiryDelay))
         return false;
      if (lastValueQueue == null)
      {
         if (other.lastValueQueue != null)
            return false;
      }
      else if (!lastValueQueue.equals(other.lastValueQueue))
         return false;
      if (maxDeliveryAttempts == null)
      {
         if (other.maxDeliveryAttempts != null)
            return false;
      }
      else if (!maxDeliveryAttempts.equals(other.maxDeliveryAttempts))
         return false;
      if (maxSizeBytes == null)
      {
         if (other.maxSizeBytes != null)
            return false;
      }
      else if (!maxSizeBytes.equals(other.maxSizeBytes))
         return false;
      if (messageCounterHistoryDayLimit == null)
      {
         if (other.messageCounterHistoryDayLimit != null)
            return false;
      }
      else if (!messageCounterHistoryDayLimit.equals(other.messageCounterHistoryDayLimit))
         return false;
      if (pageSizeBytes == null)
      {
         if (other.pageSizeBytes != null)
            return false;
      }
      else if (!pageSizeBytes.equals(other.pageSizeBytes))
         return false;
      if (pageMaxCache == null)
      {
         if (other.pageMaxCache != null)
            return false;
      }
      else if (!pageMaxCache.equals(other.pageMaxCache))
         return false;
      if (redeliveryDelay == null)
      {
         if (other.redeliveryDelay != null)
            return false;
      }
      else if (!redeliveryDelay.equals(other.redeliveryDelay))
         return false;
      if (redeliveryMultiplier == null)
      {
         if (other.redeliveryMultiplier != null)
            return false;
      }
      else if (!redeliveryMultiplier.equals(other.redeliveryMultiplier))
         return false;
      if (maxRedeliveryDelay == null)
      {
         if (other.maxRedeliveryDelay != null)
            return false;
      }
      else if (!maxRedeliveryDelay.equals(other.maxRedeliveryDelay))
         return false;
      if (redistributionDelay == null)
      {
         if (other.redistributionDelay != null)
            return false;
      }
      else if (!redistributionDelay.equals(other.redistributionDelay))
         return false;
      if (sendToDLAOnNoRoute == null)
      {
         if (other.sendToDLAOnNoRoute != null)
            return false;
      }
      else if (!sendToDLAOnNoRoute.equals(other.sendToDLAOnNoRoute))
         return false;
      return true;
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      return "AddressSettings [addressFullMessagePolicy=" + addressFullMessagePolicy +
             ", deadLetterAddress=" +
             deadLetterAddress +
             ", dropMessagesWhenFull=" +
             dropMessagesWhenFull +
             ", expiryAddress=" +
             expiryAddress +
             ", expiryDelay=" +
             expiryDelay +
             ", lastValueQueue=" +
             lastValueQueue +
             ", maxDeliveryAttempts=" +
             maxDeliveryAttempts +
             ", maxSizeBytes=" +
             maxSizeBytes +
             ", messageCounterHistoryDayLimit=" +
             messageCounterHistoryDayLimit +
             ", pageSizeBytes=" +
             pageSizeBytes +
             ", pageMaxCache=" +
             pageMaxCache +
             ", redeliveryDelay=" +
             redeliveryDelay +
             ", redeliveryMultiplier=" +
            redeliveryMultiplier +
             ", maxRedeliveryDelay=" +
            maxRedeliveryDelay +
             ", redistributionDelay=" +
             redistributionDelay +
             ", sendToDLAOnNoRoute=" +
             sendToDLAOnNoRoute +
             "]";
   }
}
