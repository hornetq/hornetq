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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.hornetq.core.filter.Filter;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.MessageReference;
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
   private static final Logger log = Logger.getLogger(ScheduledDeliveryHandlerImpl.class);

   private static final boolean trace = ScheduledDeliveryHandlerImpl.log.isTraceEnabled();

   private final ScheduledExecutorService scheduledExecutor;

   private final Map<Long, ScheduledDeliveryRunnable> scheduledRunnables = new LinkedHashMap<Long, ScheduledDeliveryRunnable>();

   public ScheduledDeliveryHandlerImpl(final ScheduledExecutorService scheduledExecutor)
   {
      this.scheduledExecutor = scheduledExecutor;
   }

   public boolean checkAndSchedule(final MessageReference ref)
   {
      long deliveryTime = ref.getScheduledDeliveryTime();

      if (deliveryTime > 0 && scheduledExecutor != null)
      {
         if (ScheduledDeliveryHandlerImpl.trace)
         {
            ScheduledDeliveryHandlerImpl.log.trace("Scheduling delivery for " + ref + " to occur at " + deliveryTime);
         }

         ScheduledDeliveryRunnable runnable = new ScheduledDeliveryRunnable(ref);

         synchronized (scheduledRunnables)
         {
            scheduledRunnables.put(ref.getMessage().getMessageID(), runnable);
         }

         scheduleDelivery(runnable, deliveryTime);

         return true;
      }
      return false;
   }

   public int getScheduledCount()
   {
      return scheduledRunnables.size();
   }

   public List<MessageReference> getScheduledReferences()
   {
      List<MessageReference> refs = new ArrayList<MessageReference>();

      synchronized (scheduledRunnables)
      {
         for (ScheduledDeliveryRunnable scheduledRunnable : scheduledRunnables.values())
         {
            refs.add(scheduledRunnable.getReference());
         }
      }
      return refs;
   }

   public List<MessageReference> cancel(final Filter filter)
   {
      List<MessageReference> refs = new ArrayList<MessageReference>();

      synchronized (scheduledRunnables)
      {
         Map<Long, ScheduledDeliveryRunnable> copy = new LinkedHashMap<Long, ScheduledDeliveryRunnable>(scheduledRunnables);
         for (ScheduledDeliveryRunnable runnable : copy.values())
         {
            if (filter == null || filter.match(runnable.getReference().getMessage()))
            {
               runnable.cancel();

               refs.add(runnable.getReference());
            }
         }
         for (MessageReference ref : refs)
         {
            scheduledRunnables.remove(ref.getMessage().getMessageID());
         }
      }
      return refs;
   }

   public MessageReference removeReferenceWithID(final long id)
   {
      synchronized (scheduledRunnables)
      {
         ScheduledDeliveryRunnable runnable = scheduledRunnables.remove(id);
         if (runnable == null)
         {
            return null;
         }
         else
         {
            return runnable.getReference();
         }
      }
   }

   private void scheduleDelivery(final ScheduledDeliveryRunnable runnable, final long deliveryTime)
   {
      long now = System.currentTimeMillis();

      long delay = deliveryTime - now;

      Future<?> future = scheduledExecutor.schedule(runnable, delay, TimeUnit.MILLISECONDS);

      runnable.setFuture(future);
   }

   private class ScheduledDeliveryRunnable implements Runnable
   {
      private final MessageReference ref;

      private volatile Future<?> future;

      private boolean cancelled;

      public ScheduledDeliveryRunnable(final MessageReference ref)
      {
         this.ref = ref;
      }

      public synchronized void setFuture(final Future<?> future)
      {
         if (cancelled)
         {
            future.cancel(false);
         }
         else
         {
            this.future = future;
         }
      }

      public synchronized void cancel()
      {
         if (future != null)
         {
            future.cancel(false);
         }

         cancelled = true;
      }

      public MessageReference getReference()
      {
         return ref;
      }

      public void run()
      {
         if (ScheduledDeliveryHandlerImpl.trace)
         {
            ScheduledDeliveryHandlerImpl.log.trace("Scheduled delivery timeout " + ref);
         }

         synchronized (scheduledRunnables)
         {
            Object removed = scheduledRunnables.remove(ref.getMessage().getMessageID());

            if (removed == null)
            {
               ScheduledDeliveryHandlerImpl.log.warn("Failed to remove timeout " + this);

               return;
            }
         }

         ref.setScheduledDeliveryTime(0);
         // TODO - need to replicate this so backup node also adds back to
         // front of queue
         ref.getQueue().addFirst(ref);
      }
   }
}
