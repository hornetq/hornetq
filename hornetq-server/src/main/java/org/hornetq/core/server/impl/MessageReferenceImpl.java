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

import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.utils.MemorySize;

/**
 * Implementation of a MessageReference
 *
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 */
public class MessageReferenceImpl implements MessageReference
{
   private final AtomicInteger deliveryCount = new AtomicInteger();

   private volatile int persistedCount;

   private volatile long scheduledDeliveryTime;

   private final ServerMessage message;

   private final Queue queue;

   private Long consumerID;

   // Static --------------------------------------------------------

   private static final int memoryOffset;

   static
   {
      // This is an estimate of how much memory a ServerMessageImpl takes up, exclusing body and properties
      // Note, it is only an estimate, it's not possible to be entirely sure with Java
      // This figure is calculated using the test utilities in org.hornetq.tests.unit.util.sizeof
      // The value is somewhat higher on 64 bit architectures, probably due to different alignment

      if (MemorySize.is64bitArch())
      {
         memoryOffset = 48;
      }
      else
      {
         memoryOffset = 32;
      }
   }

   // Constructors --------------------------------------------------

   public MessageReferenceImpl()
   {
      queue = null;

      message = null;
   }

   public MessageReferenceImpl(final MessageReferenceImpl other, final Queue queue)
   {
      deliveryCount.set(other.deliveryCount.get());

      scheduledDeliveryTime = other.scheduledDeliveryTime;

      message = other.message;

      this.queue = queue;
   }

   protected MessageReferenceImpl(final ServerMessage message, final Queue queue)
   {
      this.message = message;

      this.queue = queue;
   }

   // MessageReference implementation -------------------------------

   /**
    * @return the persistedCount
    */
   public int getPersistedCount()
   {
      return persistedCount;
   }

   /**
    * @param persistedCount the persistedCount to set
    */
   public void setPersistedCount(int persistedCount)
   {
      this.persistedCount = persistedCount;
   }

   public MessageReference copy(final Queue queue)
   {
      return new MessageReferenceImpl(this, queue);
   }

   public static int getMemoryEstimate()
   {
      return MessageReferenceImpl.memoryOffset;
   }

   public int getDeliveryCount()
   {
      return deliveryCount.get();
   }

   public void setDeliveryCount(final int deliveryCount)
   {
      this.deliveryCount.set(deliveryCount);
      this.persistedCount = this.deliveryCount.get();
   }

   public void incrementDeliveryCount()
   {
      deliveryCount.incrementAndGet();
   }

   public void decrementDeliveryCount()
   {
      deliveryCount.decrementAndGet();
   }

   public long getScheduledDeliveryTime()
   {
      return scheduledDeliveryTime;
   }

   public void setScheduledDeliveryTime(final long scheduledDeliveryTime)
   {
      this.scheduledDeliveryTime = scheduledDeliveryTime;
   }

   public ServerMessage getMessage()
   {
      return message;
   }

   public Queue getQueue()
   {
      return queue;
   }

   public void handled()
   {
      queue.referenceHandled();
   }

   public boolean isPaged()
   {
      return false;
   }

   public void acknowledge() throws Exception
   {
      queue.acknowledge(this);
   }

   @Override
   public void setConsumerId(Long consumerID)
   {
      this.consumerID = consumerID;
   }

   @Override
   public Long getConsumerId()
   {
      return this.consumerID;
   }

   public int getMessageMemoryEstimate()
   {
      return message.getMemoryEstimate();
   }

   @Override
   public String toString()
   {
      return "Reference[" + getMessage().getMessageID() +
             "]:" +
             (getMessage().isDurable() ? "RELIABLE" : "NON-RELIABLE") +
             ":" +
             getMessage();
   }
}