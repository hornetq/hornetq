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
package org.jboss.jms.example;

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
   private String connectionFactoryLookup;
   private String destinationLookup;
   private int throttleRate;
   private boolean disableMessageID;
   private boolean disableTimestamp;
   private boolean dupsOK;
   
   public synchronized int getNoOfMessagesToSend()
   {
      return noOfMessagesToSend;
   }
   public synchronized void setNoOfMessagesToSend(int noOfMessagesToSend)
   {
      this.noOfMessagesToSend = noOfMessagesToSend;
   }
   public synchronized int getNoOfWarmupMessages()
   {
      return noOfWarmupMessages;
   }
   public synchronized void setNoOfWarmupMessages(int noOfWarmupMessages)
   {
      this.noOfWarmupMessages = noOfWarmupMessages;
   }
   public synchronized int getMessageSize()
   {
      return messageSize;
   }
   public synchronized void setMessageSize(int messageSize)
   {
      this.messageSize = messageSize;
   }
   public synchronized boolean isDurable()
   {
      return durable;
   }
   public synchronized void setDurable(boolean durable)
   {
      this.durable = durable;
   }
   public synchronized boolean isSessionTransacted()
   {
      return isSessionTransacted;
   }
   public synchronized void setSessionTransacted(boolean isSessionTransacted)
   {
      this.isSessionTransacted = isSessionTransacted;
   }
   public synchronized int getBatchSize()
   {
      return batchSize;
   }
   public synchronized void setBatchSize(int batchSize)
   {
      this.batchSize = batchSize;
   }
   public synchronized boolean isDrainQueue()
   {
      return drainQueue;
   }
   public synchronized void setDrainQueue(boolean drainQueue)
   {
      this.drainQueue = drainQueue;
   }
   public synchronized String getConnectionFactoryLookup()
   {
      return connectionFactoryLookup;
   }
   public synchronized void setConnectionFactoryLookup(String connectionFactoryLookup)
   {
      this.connectionFactoryLookup = connectionFactoryLookup;
   }
   public synchronized String getDestinationLookup()
   {
      return destinationLookup;
   }
   public synchronized void setDestinationLookup(String destinationLookup)
   {
      this.destinationLookup = destinationLookup;
   }
   public synchronized int getThrottleRate()
   {
      return throttleRate;
   }
   public synchronized void setThrottleRate(int throttleRate)
   {
      this.throttleRate = throttleRate;
   }
   public synchronized boolean isDisableMessageID()
   {
      return disableMessageID;
   }
   public synchronized void setDisableMessageID(boolean disableMessageID)
   {
      this.disableMessageID = disableMessageID;
   }
   public synchronized boolean isDisableTimestamp()
   {
      return disableTimestamp;
   }
   public synchronized void setDisableTimestamp(boolean disableTimestamp)
   {
      this.disableTimestamp = disableTimestamp;
   }
   public synchronized boolean isDupsOK()
   {
      return dupsOK;
   }
   public synchronized void setDupsOK(boolean dupsOK)
   {
      this.dupsOK = dupsOK;
   }   
     


}
