/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.core.paging.cursor;

import java.lang.ref.WeakReference;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.Message;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.transaction.Transaction;

/**
 * A InternalReference
 *
 * @author clebert
 *
 *
 */
public class PagedReferenceImpl implements PagedReference
{

   private static final long serialVersionUID = -8640232251318264710L;

   private static final Logger log = Logger.getLogger(PagedReferenceImpl.class);

   private static final boolean isTrace = log.isTraceEnabled();

   private final PagePosition position;

   private WeakReference<PagedMessage> message;

   private Long deliveryTime = null;

   private int persistedCount;

   private AtomicInteger deliveryCount = new AtomicInteger(0);

   private final PageSubscription subscription;

   public ServerMessage getMessage()
   {
      return getPagedMessage().getMessage();
   }

   public synchronized PagedMessage getPagedMessage()
   {
      PagedMessage returnMessage = message != null ? message.get() : null;

      // We only keep a few references on the Queue from paging...
      // Besides those references are SoftReferenced on page cache...
      // So, this will unlikely be null,
      // unless the Queue has stalled for some time after paging
      if (returnMessage == null)
      {
         // reference is gone, we will reconstruct it
         returnMessage = subscription.queryMessage(position);
         message = new WeakReference<PagedMessage>(returnMessage);
      }
      return returnMessage;
   }

   public PagePosition getPosition()
   {
      return position;
   }

   public PagedReferenceImpl(final PagePosition position,
                             final PagedMessage message,
                             final PageSubscription subscription)
   {
      this.position = position;
      this.message = new WeakReference<PagedMessage>(message);
      this.subscription = subscription;
   }

   public boolean isPaged()
   {
      return true;
   }

   public void setPersistedCount(int count)
   {
      this.persistedCount = count;
   }

   public int getPersistedCount()
   {
      return persistedCount;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#copy(org.hornetq.core.server.Queue)
    */
   public MessageReference copy(final Queue queue)
   {
      return new PagedReferenceImpl(this.position, this.getPagedMessage(), this.subscription);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#getScheduledDeliveryTime()
    */
   public long getScheduledDeliveryTime()
   {
      if (deliveryTime == null)
      {
         ServerMessage msg = getMessage();
         if (msg.containsProperty(Message.HDR_SCHEDULED_DELIVERY_TIME))
         {
            deliveryTime = getMessage().getLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME);
         }
         else
         {
            deliveryTime = 0l;
         }
      }
      return deliveryTime;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#setScheduledDeliveryTime(long)
    */
   public void setScheduledDeliveryTime(final long scheduledDeliveryTime)
   {
      deliveryTime = scheduledDeliveryTime;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#getDeliveryCount()
    */
   public int getDeliveryCount()
   {
      return deliveryCount.get();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#setDeliveryCount(int)
    */
   public void setDeliveryCount(final int deliveryCount)
   {
      this.deliveryCount.set(deliveryCount);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#incrementDeliveryCount()
    */
   public void incrementDeliveryCount()
   {
      deliveryCount.incrementAndGet();
      if (isTrace)
      {
         log.trace("deliveryCount = " + deliveryCount + " for " + this);
      }

   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#decrementDeliveryCount()
    */
   public void decrementDeliveryCount()
   {
      deliveryCount.decrementAndGet();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#getQueue()
    */
   public Queue getQueue()
   {
      return subscription.getQueue();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#handled()
    */
   public void handled()
   {
      getQueue().referenceHandled();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#acknowledge()
    */
   public void acknowledge() throws Exception
   {
      subscription.ack(this);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.MessageReference#acknowledge(org.hornetq.core.transaction.Transaction)
    */
   public void acknowledge(final Transaction tx) throws Exception
   {
      subscription.ackTx(tx, this);
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      return "PagedReferenceImpl [position=" + position +
             ", message=" +
             message +
             ", deliveryTime=" +
             deliveryTime +
             ", persistedCount=" +
             persistedCount +
             ", deliveryCount=" +
             deliveryCount +
             ", subscription=" +
             subscription +
             "]";
   }

}
