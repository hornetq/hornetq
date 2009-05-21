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
package org.jboss.core.example;

import java.io.Serializable;

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
   private boolean durable = false;
   private boolean isSessionTransacted = false;
   private int batchSize = 5000;
   private boolean drainQueue = true;
   private String queueName = "perfQueue";  
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
   
   public boolean isDurable()
   {
      return durable;
   }

   public void setDurable(final boolean durable)
   {
      this.durable = durable;
   }

   public boolean isSessionTransacted()
   {
      return isSessionTransacted;
   }

   public void setSessionTransacted(final boolean sessionTransacted)
   {
      isSessionTransacted = sessionTransacted;
   }

   public int getBatchSize()
   {
      return batchSize;
   }

   public void setBatchSize(final int batchSize)
   {
      this.batchSize = batchSize;
   }

   public boolean isDrainQueue()
   {
      return drainQueue;
   }

   public void setDrainQueue(final boolean drainQueue)
   {
      this.drainQueue = drainQueue;
   }

   public String getQueueName()
   {
      return queueName;
   }

   public void setQueueName(final String queueName)
   {
      this.queueName = queueName;
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
      return "message to send = " + noOfMessagesToSend + ", Durable = " +
              durable + ", session transacted = " + isSessionTransacted +
              (isSessionTransacted ? ", transaction batch size = " + batchSize : "") + ", drain queue = " + drainQueue +
              ", queue name = " + queueName + 
              ", Throttle rate = " + throttleRate;
   }


}
