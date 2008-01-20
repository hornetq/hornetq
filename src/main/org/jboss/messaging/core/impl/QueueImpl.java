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
package org.jboss.messaging.core.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.ListIterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.jboss.jms.server.MessagingTimeoutFactory;
import org.jboss.messaging.util.Logger;
import org.jboss.messaging.core.Consumer;
import org.jboss.messaging.core.DistributionPolicy;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.HandleStatus;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.PriorityLinkedList;
import org.jboss.messaging.core.Queue;
import org.jboss.util.timeout.Timeout;
import org.jboss.util.timeout.TimeoutTarget;

/**
 * 
 * Implementation of a Queue
 * 
 * TODO use Java 5 concurrent queue
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class QueueImpl implements Queue
{
   private static final Logger log = Logger.getLogger(QueueImpl.class);

   private static final boolean trace = log.isTraceEnabled();
      
   protected volatile long id = -1;
   
   protected String name;
   
   protected volatile int maxSize = -1;
   
   protected boolean clustered;
   
   protected boolean temporary;
   
   protected boolean durable;
   
   protected Filter filter;
   
   protected PriorityLinkedList<MessageReference> messageReferences;
   
   protected List<Consumer> consumers;
   
   protected Set<Timeout> scheduledTimeouts;
   
   protected DistributionPolicy distributionPolicy;
   
   protected boolean direct;
   
   protected boolean promptDelivery;
   
   private int pos;
   
   private AtomicInteger messagesAdded = new AtomicInteger(0);
   
   private AtomicInteger deliveringCount = new AtomicInteger(0);
         
   // ---------
   
   private Queue dlq;
   
   private Queue expiryQueue;
   
   private int maxDeliveryAttempts;
   
   private long redeliveryDelay;
   
   private int messageCounterHistoryDayLimit;
      
   
   public QueueImpl(long id, String name, Filter filter, boolean clustered,
                    boolean durable, boolean temporary, int maxSize)
   {
      this.id = id;
      
      this.name = name;
      
      this.filter = filter;
      
      this.clustered = clustered;
      
      this.durable = durable;
      
      this.temporary = temporary;
      
      this.maxSize = maxSize;
      
      //TODO - use a wait free concurrent queue
      messageReferences = new PriorityLinkedListImpl<MessageReference>(NUM_PRIORITIES);
      
      consumers = new ArrayList<Consumer>();
      
      scheduledTimeouts = new HashSet<Timeout>();
      
      distributionPolicy = new RoundRobinDistributionPolicy();
      
      direct = true;
   }
   
   // Queue implementation -------------------------------------------------------------------
      
   public boolean isClustered()
   {
      return clustered;
   }

   public boolean isDurable()
   {
      return durable;
   }

   public boolean isTemporary()
   {
      return temporary;
   }
   
   public String getName()
   {
      return name;
   }
   
   public synchronized HandleStatus addLast(MessageReference ref)
   {
      return add(ref, false);
   }

   public synchronized HandleStatus addFirst(MessageReference ref)
   {
      return add(ref, true);
   }
              
   /*
    * Attempt to deliver all the messages in the queue
    * @see org.jboss.messaging.newcore.intf.Queue#deliver()
    */
   public synchronized void deliver()
   {
      MessageReference reference;
      
      ListIterator<MessageReference> iterator = null;
      
      while (true)
      {
         if (iterator == null)
         {
            reference = messageReferences.peekFirst();
         }
         else
         {
            if (iterator.hasNext())
            {
               reference = iterator.next();
            }
            else
            {
               reference = null;
            }
         }
         
         if (reference == null)
         {
            if (iterator == null)
            {
               //We delivered all the messages - go into direct delivery
               direct = true;
               
               promptDelivery = false;
            } 
            return;
         }
         
         HandleStatus status = deliver(reference);
         
         if (status == HandleStatus.HANDLED)
         {
            if (iterator == null)
            {
               messageReferences.removeFirst();
            }
            else
            {
               iterator.remove();
            }
         }
         else if (status == HandleStatus.BUSY)
         {
            //All consumers busy - give up
            break;
         }
         else if (status == HandleStatus.NO_MATCH && iterator == null)
         {
            //Consumers not all busy - but filter not accepting - iterate back through the queue
            iterator = messageReferences.iterator();
         }
      }               
   }
   
   public synchronized void addConsumer(Consumer consumer)
   {
      consumers.add(consumer);
   }
   
   public synchronized boolean removeConsumer(Consumer consumer)
   {
      boolean removed = consumers.remove(consumer);
      
      if (pos == consumers.size())
      {
         pos = 0;
      }
      
      if (consumers.isEmpty())
      {
         promptDelivery = false;
      }
      
      return removed;
   }
   
   public synchronized int getConsumerCount()
   {
      return consumers.size();
   }

   public synchronized List<MessageReference> list(Filter filter)
   {
      if (filter == null)
      {
         return new ArrayList<MessageReference>(messageReferences.getAll());
      }
      else
      {
         ArrayList<MessageReference> list = new ArrayList<MessageReference>();
         
         for (MessageReference ref: messageReferences.getAll())
         {
            if (filter.match(ref.getMessage()))
            {
               list.add(ref);
            }
         }
         
         return list;
      }
   }

   public synchronized void removeAllReferences()
   {
      messageReferences.clear();
      
      if (!this.scheduledTimeouts.isEmpty())
      {
         Set<Timeout> clone = new HashSet<Timeout>(scheduledTimeouts);
         
         for (Timeout timeout: clone)
         {
            timeout.cancel();
         }
         
         scheduledTimeouts.clear();
      }
   }

   public long getPersistenceID()
   {
      return id;
   }
   
   public void setPersistenceID(long id)
   {
      this.id = id;
   }
   
   public synchronized Filter getFilter()
   {
      return filter;
   }
   
   public synchronized void setFilter(Filter filter)
   {
      this.filter = filter;
   }

   public synchronized int getMessageCount()
   {
      return messageReferences.size() + getScheduledCount() + getDeliveringCount();
   }
   
   public synchronized int getScheduledCount()
   {
      return scheduledTimeouts.size();
   }
   
   public int getDeliveringCount()
   {
      return deliveringCount.get();
   }
   
   public void referenceAcknowledged()
   {
      deliveringCount.decrementAndGet();
   }

   public synchronized int getMaxSize()
   {
      return maxSize;
   }

   public synchronized void setMaxSize(int maxSize)
   {
      int num = messageReferences.size() + scheduledTimeouts.size();
      
      if (maxSize < num)
      {
         throw new IllegalArgumentException("Cannot set maxSize to " + maxSize + " since there are " + num + " refs");
      }
      this.maxSize = maxSize;
   }
     
   public synchronized DistributionPolicy getDistributionPolicy()
   {
      return distributionPolicy;
   }

   public synchronized void setDistributionPolicy(DistributionPolicy distributionPolicy)
   {
      this.distributionPolicy = distributionPolicy;
   }
   
   public int getMessagesAdded()
   {
      return messagesAdded.get();
   }
              
   public boolean equals(Object other)
   {
      QueueImpl qother = (QueueImpl)other;
      
      return name.equals(qother.name);
   }
   
   public int hashCode()
   {
      return name.hashCode();
   }
   
   // Private ------------------------------------------------------------------------------
   
   private HandleStatus add(MessageReference ref, boolean first)
   {
      if (!checkFull())
      {
         return HandleStatus.BUSY;
      }
            
      if (!first)
      {
         messagesAdded.incrementAndGet();
      }
      
      if (!checkAndSchedule(ref))
      {           
         boolean add = false;
         
         if (direct)
         {
            //Deliver directly
            
            HandleStatus status = deliver(ref);
            
            if (status == HandleStatus.HANDLED)
            {
               //Ok
            }
            else if (status == HandleStatus.BUSY)
            {
               add = true;
            }
            else if (status == HandleStatus.NO_MATCH)
            {
               add = true;
            }
            
            if (add)
            {
               direct = false;
            }
         }
         else
         {
            add = true;
         }
         
         if (add)
         {
            if (first)
            {
               messageReferences.addFirst(ref, ref.getMessage().getPriority());
            }
            else
            {
               messageReferences.addLast(ref, ref.getMessage().getPriority());
            }
            
            if (!direct && promptDelivery)
            {               
               //We have consumers with filters which don't match, so we need to prompt delivery every time
               //a new message arrives - this is why you really shouldn't use filters with queues - in most cases
               //it's an ant-pattern since it would cause a queue scan on each message
               deliver();
            }
         }
      }
      
      return HandleStatus.HANDLED;
   }
             
   private boolean checkAndSchedule(MessageReference ref)
   {
      if (ref.getScheduledDeliveryTime() > System.currentTimeMillis())
      {      
         if (trace) { log.trace("Scheduling delivery for " + ref + " to occur at " + ref.getScheduledDeliveryTime()); }
         
         // Schedule the cancel to actually occur at the specified time. 
            
         Timeout timeout =
            MessagingTimeoutFactory.instance.getFactory().
               schedule(ref.getScheduledDeliveryTime(), new DeliverRefTimeoutTarget(ref));
         
         scheduledTimeouts.add(timeout);
                       
         return true;
      }
      else
      {
         return false;
      }
   }
   
   private boolean checkFull()
   {
      if (maxSize != -1 && (messageReferences.size() + scheduledTimeouts.size()) >= maxSize)
      {
         if (trace) { log.trace(this + " queue is full, rejecting message"); }
         
         return false;
      }
      else
      {
         return true;
      }
   }
   
   private HandleStatus deliver(MessageReference reference)
   {
      if (consumers.isEmpty())
      {
         return HandleStatus.BUSY;
      }
      
      int startPos = pos;
      
      boolean filterRejected = false;
      
      while (true)
      {               
         Consumer consumer = consumers.get(pos);
         
         pos = distributionPolicy.select(consumers, pos);                  
                  
         HandleStatus status;
         
         try
         {
            status = consumer.handle(reference);
         }
         catch (Throwable t)
         {
            //If the consumer throws an exception we remove the consumer
            removeConsumer(consumer);
            
            return HandleStatus.BUSY;
         }
         
         if (status == null)
         {
            throw new IllegalStateException("Consumer.handle() should never return null");
         }
         
         if (status == HandleStatus.HANDLED)
         {
            deliveringCount.incrementAndGet();
            
            return HandleStatus.HANDLED;
         }
         else if (status == HandleStatus.NO_MATCH)
         {
            promptDelivery = true;
            
            filterRejected = true;
         }       
         
         if (pos == startPos)
         {
            //Tried all of them
            if (filterRejected)
            {
               return HandleStatus.NO_MATCH;
            }
            else
            {
               //Give up - all consumers busy
               return HandleStatus.BUSY;
            }
         }
      }     
   }
   
   // Inner classes --------------------------------------------------------------------------
   
   private class DeliverRefTimeoutTarget implements TimeoutTarget
   {
      private MessageReference ref;

      public DeliverRefTimeoutTarget(MessageReference ref)
      {
         this.ref = ref;
      }

      public void timedOut(Timeout timeout)
      {
         if (trace) { log.trace("Scheduled delivery timeout " + ref); }
         
         synchronized (scheduledTimeouts)
         {
            boolean removed = scheduledTimeouts.remove(timeout);
            
            if (!removed)
            {
               throw new IllegalStateException("Failed to remove timeout " + timeout);
            }
         }
              
         ref.setScheduledDeliveryTime(0);
                  
         HandleStatus status = deliver(ref);
                  
         if (HandleStatus.HANDLED != status)
         {
            //Add back to the front of the queue
            
            addFirst(ref);
         }
         else
         {
            if (trace) { log.trace("Delivered scheduled delivery at " + System.currentTimeMillis() + " for " + ref); }
         }
      }
   }

   // -------------------------------
   
   
   //TODO - these can probably all be managed by the queue settings manager
   
   public Queue getDLQ()
   {
      return dlq;
   }

   public void setDLQ(Queue dlq)
   {
      this.dlq = dlq;
   }

   public Queue getExpiryQueue()
   {
      return expiryQueue;
   }

   public void setExpiryQueue(Queue expiryQueue)
   {
      this.expiryQueue = expiryQueue;
   }

   public int getMaxDeliveryAttempts()
   {
      return maxDeliveryAttempts;
   }

   public void setMaxDeliveryAttempts(int maxDeliveryAttempts)
   {
      this.maxDeliveryAttempts = maxDeliveryAttempts;
   }

   public long getRedeliveryDelay()
   {
      return redeliveryDelay;
   }

   public void setRedeliveryDelay(long redeliveryDelay)
   {
      this.redeliveryDelay = redeliveryDelay;
   }

   public int getMessageCounterHistoryDayLimit()
   {
      return messageCounterHistoryDayLimit;
   }

   public void setMessageCounterHistoryDayLimit(int messageCounterHistoryDayLimit)
   {
      this.messageCounterHistoryDayLimit = messageCounterHistoryDayLimit;
   }
   
   // -------------------------------------------------------
   
   
   

}
