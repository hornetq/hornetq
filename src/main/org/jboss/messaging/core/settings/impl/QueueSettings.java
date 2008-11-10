/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.settings.impl;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.DistributionPolicy;
import org.jboss.messaging.core.server.impl.RoundRobinDistributionPolicy;
import org.jboss.messaging.core.settings.Mergeable;
import org.jboss.messaging.util.SimpleString;

/**
 * The Queue Settings that will be used to configure a queue
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class QueueSettings implements Mergeable<QueueSettings>
{
   private static Logger log = Logger.getLogger(QueueSettings.class);

   /**
    * defaults used if null, this allows merging
    */
   public static final Class<?> DEFAULT_DISTRIBUTION_POLICY_CLASS = new RoundRobinDistributionPolicy().getClass();

   public static final Boolean DEFAULT_CLUSTERED = false;

   public static final Integer DEFAULT_MAX_SIZE_BYTES = -1;

   public static final Boolean DEFAULT_DROP_MESSAGES_WHEN_FULL = Boolean.FALSE;

   public static final Integer DEFAULT_PAGE_SIZE_BYTES = 10 * 1024 * 1024; // 10M Bytes

   public static final Integer DEFAULT_MAX_DELIVERY_ATTEMPTS = 10;

   public static final Integer DEFAULT_MESSAGE_COUNTER_HISTORY_DAY_LIMIT = 0;

   public static final Long DEFAULT_REDELIVER_DELAY = 0L;

   private Boolean clustered = null;

   private Integer maxSizeBytes = null;

   private Integer pageSizeBytes = null;

   private Boolean dropMessagesWhenFull = null;

   private String distributionPolicyClass = null;

   private Integer maxDeliveryAttempts = null;

   private Integer messageCounterHistoryDayLimit = null;

   private Long redeliveryDelay = null;

   private SimpleString DLQ = null;

   private SimpleString ExpiryQueue = null;

   public Boolean isClustered()
   {
      return clustered != null ? clustered : DEFAULT_CLUSTERED;
   }

   public void setClustered(Boolean clustered)
   {
      this.clustered = clustered;
   }

   public Integer getPageSizeBytes()
   {
      return pageSizeBytes != null ? pageSizeBytes : DEFAULT_PAGE_SIZE_BYTES;
   }

   public Boolean isDropMessagesWhenFull()
   {
      return dropMessagesWhenFull != null ? this.dropMessagesWhenFull : DEFAULT_DROP_MESSAGES_WHEN_FULL;
   }

   public void setDropMessagesWhenFull(Boolean value)
   {
      this.dropMessagesWhenFull = value;
   }

   public void setPageSizeBytes(Integer pageSize)
   {
      this.pageSizeBytes = pageSize;
   }

   public Integer getMaxSizeBytes()
   {
      return maxSizeBytes != null ? maxSizeBytes : DEFAULT_MAX_SIZE_BYTES;
   }

   public void setMaxSizeBytes(Integer maxSizeBytes)
   {
      this.maxSizeBytes = maxSizeBytes;
   }

   public Integer getMaxDeliveryAttempts()
   {
      return maxDeliveryAttempts != null ? maxDeliveryAttempts : DEFAULT_MAX_DELIVERY_ATTEMPTS;
   }

   public void setMaxDeliveryAttempts(Integer maxDeliveryAttempts)
   {
      this.maxDeliveryAttempts = maxDeliveryAttempts;
   }

   public Integer getMessageCounterHistoryDayLimit()
   {
      return messageCounterHistoryDayLimit != null ? messageCounterHistoryDayLimit
                                                  : DEFAULT_MESSAGE_COUNTER_HISTORY_DAY_LIMIT;
   }

   public void setMessageCounterHistoryDayLimit(Integer messageCounterHistoryDayLimit)
   {
      this.messageCounterHistoryDayLimit = messageCounterHistoryDayLimit;
   }

   public Long getRedeliveryDelay()
   {
      return redeliveryDelay != null ? redeliveryDelay : DEFAULT_REDELIVER_DELAY;
   }

   public void setRedeliveryDelay(Long redeliveryDelay)
   {
      this.redeliveryDelay = redeliveryDelay;
   }

   public String getDistributionPolicyClass()
   {
      return distributionPolicyClass;
   }

   public void setDistributionPolicyClass(String distributionPolicyClass)
   {
      this.distributionPolicyClass = distributionPolicyClass;
   }

   public SimpleString getDLQ()
   {
      return DLQ;
   }

   public void setDLQ(SimpleString DLQ)
   {
      this.DLQ = DLQ;
   }

   public SimpleString getExpiryQueue()
   {
      return ExpiryQueue;
   }

   public void setExpiryQueue(SimpleString expiryQueue)
   {
      ExpiryQueue = expiryQueue;
   }

   public DistributionPolicy getDistributionPolicy()
   {
      try
      {
         if (distributionPolicyClass != null)
         {
            return (DistributionPolicy)getClass().getClassLoader().loadClass(distributionPolicyClass).newInstance();
         }
         else
         {
            return (DistributionPolicy)DEFAULT_DISTRIBUTION_POLICY_CLASS.newInstance();
         }
      }
      catch (Exception e)
      {
         throw new IllegalArgumentException("Error instantiating distribution policy '" + e + " '");
      }
   }

   /**
    * merge 2 objects in to 1
    * @param merged
    */
   public void merge(QueueSettings merged)
   {
      if (clustered == null)
      {
         clustered = merged.clustered;
      }
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
      if (distributionPolicyClass == null)
      {
         distributionPolicyClass = merged.distributionPolicyClass;
      }
      if (DLQ == null)
      {
         DLQ = merged.DLQ;
      }
      if (ExpiryQueue == null)
      {
         ExpiryQueue = merged.ExpiryQueue;
      }
   }

}
