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
package org.jboss.jms.util;

import java.io.Serializable;

import javax.jms.DeliveryMode;

/**
 * 
 * Class that holds the parameters used in the performance examples
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class PerfParams implements Serializable
{
   private static final long serialVersionUID = -4336539641012356002L;
   
   private int noOfMessagesToSend = 1000;
   private int noOfWarmupMessages;
   private int messageSize = 1024; // in bytes
   private int deliveryMode = DeliveryMode.NON_PERSISTENT;
   private boolean isSessionTransacted = false;
   private int transactionBatchSize = 5000;
   private boolean drainQueue = true;
   private String queueLookup = "/queue/testPerfQueue";
   private String connectionFactoryLookup = "/ConnectionFactory";
   private boolean dupsOk;
   private int throttleRate;

   public int getNoOfMessagesToSend()
   {
      return noOfMessagesToSend;
   }

   public void setNoOfMessagesToSend(final int noOfMessagesToSend)
   {
      this.noOfMessagesToSend = noOfMessagesToSend;
   }

   public int getNoOfWarmupMessages()
   {
      return noOfWarmupMessages;
   }

   public void setNoOfWarmupMessages(final int noOfWarmupMessages)
   {
      this.noOfWarmupMessages = noOfWarmupMessages;
   }

   public int getMessageSize()
   {
      return messageSize;
   }
   
   public void setMessageSize(int messageSize)
   {
      this.messageSize = messageSize;
   }
   
   public int getDeliveryMode()
   {
      return deliveryMode;
   }

   public void setDeliveryMode(final int deliveryMode)
   {
      this.deliveryMode = deliveryMode;
   }

   public boolean isSessionTransacted()
   {
      return isSessionTransacted;
   }

   public void setSessionTransacted(final boolean sessionTransacted)
   {
      isSessionTransacted = sessionTransacted;
   }

   public int getTransactionBatchSize()
   {
      return transactionBatchSize;
   }

   public void setTransactionBatchSize(final int transactionBatchSize)
   {
      this.transactionBatchSize = transactionBatchSize;
   }

   public boolean isDrainQueue()
   {
      return drainQueue;
   }

   public void setDrainQueue(final boolean drainQueue)
   {
      this.drainQueue = drainQueue;
   }

   public String getQueueLookup()
   {
      return queueLookup;
   }

   public void setQueueLookup(final String queueLookup)
   {
      this.queueLookup = queueLookup;
   }

   public String getConnectionFactoryLookup()
   {
      return connectionFactoryLookup;
   }

   public void setConnectionFactoryLookup(final String connectionFactoryLookup)
   {
      this.connectionFactoryLookup = connectionFactoryLookup;
   }

   public boolean isDupsOk()
   {
      return dupsOk;
   }

   public void setDupsOk(final boolean dupsOk)
   {
      this.dupsOk = dupsOk;
   }
   
   public int getThrottleRate()
   {
      return throttleRate;
   }
   
   public void setThrottleRate(final int throttleRate)
   {
      this.throttleRate = throttleRate;
   }

   public String toString()
   {
      return "message to send = " + noOfMessagesToSend + ", DeliveryMode = " +
              (deliveryMode == DeliveryMode.PERSISTENT ? "PERSISTENT" : "NON_PERSISTENT") + ", session transacted = " + isSessionTransacted +
              (isSessionTransacted ? ", transaction batch size = " + transactionBatchSize : "") + ", drain queue = " + drainQueue +
              ", queue lookup = " + queueLookup + ", connection factory lookup = " + connectionFactoryLookup +
              ", Session Acknowledge mode = " + (dupsOk ? "DUPS_OK_ACKNOWLEDGE" : "AUTO_ACKNOWLEDGE") + 
              ", Throttle rate = " + throttleRate;
   }


}
