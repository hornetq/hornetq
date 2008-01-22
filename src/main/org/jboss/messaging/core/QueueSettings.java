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
package org.jboss.messaging.core;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.impl.RoundRobinDistributionPolicy;

/**
 * The Queue Settings that will be used to configure a queue
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class QueueSettings
{
   private static Logger log = Logger.getLogger(QueueSettings.class);
   private static final DistributionPolicy DEFAULT_DISTRIBUTION_POLICY = new RoundRobinDistributionPolicy();
   boolean clustered = false;
   int maxSize = -1;
   String distributionPolicyClass = null;
   int maxDeliveryAttempts = 10;
   int messageCounterHistoryDayLimit = 0;
   long redeliveryDelay = 500;
   private String DLQ;
   private String ExpiryQueue;


   public boolean isClustered()
   {
      return clustered;
   }

   public void setClustered(boolean clustered)
   {
      this.clustered = clustered;
   }

   public int getMaxSize()
   {
      return maxSize;
   }

   public void setMaxSize(int maxSize)
   {
      this.maxSize = maxSize;
   }

   public int getMaxDeliveryAttempts()
   {
      return maxDeliveryAttempts;
   }

   public void setMaxDeliveryAttempts(int maxDeliveryAttempts)
   {
      this.maxDeliveryAttempts = maxDeliveryAttempts;
   }

   public int getMessageCounterHistoryDayLimit()
   {
      return messageCounterHistoryDayLimit;
   }

   public void setMessageCounterHistoryDayLimit(int messageCounterHistoryDayLimit)
   {
      this.messageCounterHistoryDayLimit = messageCounterHistoryDayLimit;
   }

   public long getRedeliveryDelay()
   {
      return redeliveryDelay;
   }

   public void setRedeliveryDelay(long redeliveryDelay)
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


   public String getDLQ()
   {
      return DLQ;
   }

   public void setDLQ(String DLQ)
   {
      this.DLQ = DLQ;
   }

   public String getExpiryQueue()
   {
      return ExpiryQueue;
   }

   public void setExpiryQueue(String expiryQueue)
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


   public boolean equals(Object o)
   {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      QueueSettings that = (QueueSettings) o;

      if (clustered != that.clustered) return false;
      if (maxDeliveryAttempts != that.maxDeliveryAttempts) return false;
      if (maxSize != that.maxSize) return false;
      if (messageCounterHistoryDayLimit != that.messageCounterHistoryDayLimit) return false;
      if (redeliveryDelay != that.redeliveryDelay) return false;
      if (DLQ != null ? !DLQ.equals(that.DLQ) : that.DLQ != null) return false;
      if (ExpiryQueue != null ? !ExpiryQueue.equals(that.ExpiryQueue) : that.ExpiryQueue != null) return false;
      if (distributionPolicyClass != null ? !distributionPolicyClass.equals(that.distributionPolicyClass) : that.distributionPolicyClass != null)
         return false;

      return true;
   }

   public int hashCode()
   {
      int result;
      result = (clustered ? 1 : 0);
      result = 31 * result + maxSize;
      result = 31 * result + (distributionPolicyClass != null ? distributionPolicyClass.hashCode() : 0);
      result = 31 * result + maxDeliveryAttempts;
      result = 31 * result + messageCounterHistoryDayLimit;
      result = 31 * result + (int) (redeliveryDelay ^ (redeliveryDelay >>> 32));
      result = 31 * result + (DLQ != null ? DLQ.hashCode() : 0);
      result = 31 * result + (ExpiryQueue != null ? ExpiryQueue.hashCode() : 0);
      return result;
   }
}
