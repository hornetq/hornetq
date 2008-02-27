/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
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

import org.jboss.logging.Logger;
import org.jboss.messaging.core.server.DistributionPolicy;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.impl.RoundRobinDistributionPolicy;
import org.jboss.messaging.core.settings.Mergeable;

/**
 * The Queue Settings that will be used to configure a queue
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class QueueSettings implements Mergeable<QueueSettings>
{
   private static Logger log = Logger.getLogger(QueueSettings.class);
   /**
    * defaults used if null, this allows merging
    */
   public static final DistributionPolicy DEFAULT_DISTRIBUTION_POLICY = new RoundRobinDistributionPolicy();
   public static final Boolean DEFAULT_CLUSTERED = false;
   public static final Integer DEFAULT_MAX_SIZE = -1;
   public static final Integer DEFAULT_MAX_DELIVERY_ATTEMPTS = 10;
   public static final Integer DEFAULT_MESSAGE_COUNTER_HISTORY_DAY_LIMIT = 0;
   public static final Long DEFAULT_REDELIVER_DELAY = (long) 500;

   private Boolean clustered = false;
   private Integer maxSize = null;
   private String distributionPolicyClass = null;
   private Integer maxDeliveryAttempts = null;
   private Integer messageCounterHistoryDayLimit = null;
   private Long redeliveryDelay = null;
   private Queue DLQ = null;
   private Queue ExpiryQueue = null;


   public Boolean isClustered()
   {
      return clustered != null?clustered:DEFAULT_CLUSTERED;
   }

   public void setClustered(Boolean clustered)
   {
      this.clustered = clustered;
   }

   public Integer getMaxSize()
   {
      return maxSize != null?maxSize:DEFAULT_MAX_SIZE;
   }

   public void setMaxSize(Integer maxSize)
   {
      this.maxSize = maxSize;
   }

   public Integer getMaxDeliveryAttempts()
   {
      return maxDeliveryAttempts != null?maxDeliveryAttempts:DEFAULT_MAX_DELIVERY_ATTEMPTS;
   }

   public void setMaxDeliveryAttempts(Integer maxDeliveryAttempts)
   {
      this.maxDeliveryAttempts = maxDeliveryAttempts;
   }

   public Integer getMessageCounterHistoryDayLimit()
   {
      return messageCounterHistoryDayLimit!=null?messageCounterHistoryDayLimit:DEFAULT_MESSAGE_COUNTER_HISTORY_DAY_LIMIT;
   }

   public void setMessageCounterHistoryDayLimit(Integer messageCounterHistoryDayLimit)
   {
      this.messageCounterHistoryDayLimit = messageCounterHistoryDayLimit;
   }

   public Long getRedeliveryDelay()
   {
      return redeliveryDelay!=null?redeliveryDelay:DEFAULT_REDELIVER_DELAY;
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


   public Queue getDLQ()
   {
      return DLQ;
   }

   public void setDLQ(Queue DLQ)
   {
      this.DLQ = DLQ;
   }

   public Queue getExpiryQueue()
   {
      return ExpiryQueue;
   }

   public void setExpiryQueue(Queue expiryQueue)
   {
      ExpiryQueue = expiryQueue;
   }

   public DistributionPolicy getDistributionPolicy()
   {
      if(distributionPolicyClass != null)
      {
         try
         {
            return (DistributionPolicy) getClass().getClassLoader().loadClass(distributionPolicyClass).newInstance();
         }
         catch (Exception e)
         {
            log.warn("unable to create Distribution Policy using default", e);
         }
      }
      return DEFAULT_DISTRIBUTION_POLICY;
   }


   

   /**
    * merge 2 objects in to 1
    * @param merged
    */
   public void merge(QueueSettings merged)
   {
      if(!DEFAULT_CLUSTERED.equals(merged.clustered))
      {
         clustered = merged.clustered;
      }
      if(!DEFAULT_MAX_DELIVERY_ATTEMPTS.equals(merged.maxDeliveryAttempts) && merged.maxDeliveryAttempts != null)
      {
         maxDeliveryAttempts = merged.maxDeliveryAttempts;
      }
      if(!DEFAULT_MAX_SIZE.equals(merged.maxSize) && merged.maxSize != null)
      {
         maxSize = merged.maxSize;
      }
      if(!DEFAULT_MESSAGE_COUNTER_HISTORY_DAY_LIMIT.equals(merged.messageCounterHistoryDayLimit) && merged.messageCounterHistoryDayLimit != null)
      {
         messageCounterHistoryDayLimit = merged.messageCounterHistoryDayLimit;
      }
      if(!DEFAULT_REDELIVER_DELAY.equals(merged.redeliveryDelay) && merged.redeliveryDelay != null && merged.redeliveryDelay != null)
      {
         redeliveryDelay = merged.redeliveryDelay;
      }
      if(merged.distributionPolicyClass != null)
      {
         distributionPolicyClass = merged.distributionPolicyClass;
      }
      if(merged.DLQ != null)
      {
         DLQ = merged.DLQ;
      }
      if(merged.ExpiryQueue != null)
      {
         ExpiryQueue = merged.ExpiryQueue;
      }
   }
}
