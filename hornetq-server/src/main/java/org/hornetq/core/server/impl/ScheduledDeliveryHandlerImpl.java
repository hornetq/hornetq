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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.hornetq.core.filter.Filter;
import org.hornetq.core.server.HornetQServerLogger;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ScheduledDeliveryHandler;

/**
 * Handles scheduling deliveries to a queue at the correct time.
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="clebert.suconic@jboss.com">Clebert Suconic</a>
 */
public class ScheduledDeliveryHandlerImpl implements ScheduledDeliveryHandler
{
   private static final boolean trace = HornetQServerLogger.LOGGER.isTraceEnabled();

   private final ScheduledExecutorService scheduledExecutor;
   
   private final Object lockDelivery = new Object();
   
   private final LinkedList<MessageReference> scheduledReferences = new LinkedList<MessageReference>();

   public ScheduledDeliveryHandlerImpl(final ScheduledExecutorService scheduledExecutor)
   {
      this.scheduledExecutor = scheduledExecutor;
   }

   public boolean checkAndSchedule(final MessageReference ref, final boolean tail)
   {
      long deliveryTime = ref.getScheduledDeliveryTime();

      if (deliveryTime > 0 && scheduledExecutor != null)
      {
         if (ScheduledDeliveryHandlerImpl.trace)
         {
            HornetQServerLogger.LOGGER.trace("Scheduling delivery for " + ref + " to occur at " + deliveryTime);
         }

         ScheduledDeliveryRunnable runnable = new ScheduledDeliveryRunnable(ref.getScheduledDeliveryTime());

         synchronized (scheduledReferences)
         {
            if (tail)
            {
               // We do the opposite what the parameter says as the Runnable will always add it to the head
               scheduledReferences.addFirst(ref);
            }
            else
            {
               // We do the opposite what the parameter says as the Runnable will always add it to the head
               scheduledReferences.add(ref);
            }
         }

         scheduleDelivery(runnable, deliveryTime);

         return true;
      }
      return false;
   }

   public int getScheduledCount()
   {
      synchronized (scheduledReferences)
      {
         return scheduledReferences.size();
      }
   }

   public List<MessageReference> getScheduledReferences()
   {
      List<MessageReference> refs = new ArrayList<MessageReference>();

      synchronized (scheduledReferences)
      {
         refs.addAll(scheduledReferences);
      }
      return refs;
   }

   public List<MessageReference> cancel(final Filter filter)
   {
      List<MessageReference> refs = new ArrayList<MessageReference>();

      synchronized (scheduledReferences)
      {
         Iterator<MessageReference> iter = scheduledReferences.iterator();
         
         while (iter.hasNext())
         {
            MessageReference ref = iter.next();
            if (filter == null || filter.match(ref.getMessage()))
            {
               iter.remove();
               refs.add(ref);
            }
         }
      }
      return refs;
   }

   public MessageReference removeReferenceWithID(final long id)
   {
      synchronized (scheduledReferences)
      {
         Iterator<MessageReference> iter = scheduledReferences.iterator();
         while (iter.hasNext())
         {
            MessageReference ref = iter.next();
            if (ref.getMessage().getMessageID() == id)
            {
               iter.remove();
               return ref;
            }
         }
      }
      
      return null;
   }

   private void scheduleDelivery(final ScheduledDeliveryRunnable runnable, final long deliveryTime)
   {
      long now = System.currentTimeMillis();

      long delay = deliveryTime - now;
      
      if (delay < 0)
      {
         delay = 0;
      }

      scheduledExecutor.schedule(runnable, delay, TimeUnit.MILLISECONDS);
   }

   private class ScheduledDeliveryRunnable implements Runnable
   {
      private final long scheduledTime;

      public ScheduledDeliveryRunnable(final long scheduledTime)
      {
         this.scheduledTime = scheduledTime;
      }

      public void run()
      {
         HashMap<Queue, LinkedList<MessageReference>> refs = new HashMap<Queue, LinkedList<MessageReference>>();

         synchronized (lockDelivery)
         {
            synchronized (scheduledReferences)
            {
               
               Iterator<MessageReference> iter = scheduledReferences.iterator();
               while (iter.hasNext())
               {
                  MessageReference reference = iter.next();
                  if (reference.getScheduledDeliveryTime() <= this.scheduledTime)
                  {
                     iter.remove();
   
                     reference.setScheduledDeliveryTime(0);
                     
                     LinkedList<MessageReference> references = refs.get(reference.getQueue());
                     
                     if (references == null)
                     {
                        references = new LinkedList<MessageReference>();
                        refs.put(reference.getQueue(), references);
                     }
                     
                     references.add(reference);
                  }
               }
            }

            for (Map.Entry<Queue, LinkedList<MessageReference>> entry : refs.entrySet())
            {
               entry.getKey().addHead(entry.getValue());
            }
            
            // Just to speed up GC
            refs.clear();
         }
      }
   }
}
