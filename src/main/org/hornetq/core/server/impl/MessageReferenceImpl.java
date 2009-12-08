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

import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.utils.MemorySize;

/**
 * Implementation of a MessageReference
 *
 * @author <a href="mailto:tim.fox@jboss.com>Tim Fox</a>
 * @version <tt>1.3</tt>
 *
 * MessageReferenceImpl.java,v 1.3 2006/02/23 17:45:57 timfox Exp
 */
public class MessageReferenceImpl implements MessageReference
{
   private static final Logger log = Logger.getLogger(MessageReferenceImpl.class);

   // Attributes ----------------------------------------------------

   private volatile int deliveryCount;

   private volatile long scheduledDeliveryTime;

   private final ServerMessage message;

   private final Queue queue;

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
      deliveryCount = other.deliveryCount;

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
      return deliveryCount;
   }

   public void setDeliveryCount(final int deliveryCount)
   {
      this.deliveryCount = deliveryCount;
   }

   public void incrementDeliveryCount()
   {
      deliveryCount++;
   }

   public void decrementDeliveryCount()
   {
      deliveryCount--;
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

   // Public --------------------------------------------------------

   @Override
   public String toString()
   {
      return "Reference[" + getMessage().getMessageID() +
             "]:" +
             (getMessage().isDurable() ? "RELIABLE" : "NON-RELIABLE");
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}