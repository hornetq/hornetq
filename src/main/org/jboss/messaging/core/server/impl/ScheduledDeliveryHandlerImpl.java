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
package org.jboss.messaging.core.server.impl;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.ScheduledDeliveryHandler;

/**
 * Handles scheduling deliveries to a queue at the correct time.
 * 
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ScheduledDeliveryHandlerImpl implements ScheduledDeliveryHandler
{
   private static final Logger log = Logger.getLogger(ScheduledDeliveryHandlerImpl.class);

   private static final boolean trace = log.isTraceEnabled();

   private final ScheduledExecutorService scheduledExecutor;

   private final Set<ScheduledDeliveryRunnable> scheduledRunnables = new LinkedHashSet<ScheduledDeliveryRunnable>();

   public ScheduledDeliveryHandlerImpl(final ScheduledExecutorService scheduledExecutor)
   {
      this.scheduledExecutor = scheduledExecutor;
   }

   public boolean checkAndSchedule(final MessageReference ref, final boolean backup)
   {
      long deliveryTime = ref.getScheduledDeliveryTime();

      if (deliveryTime != 0 && scheduledExecutor != null)
      {
         if (trace)
         {
            log.trace("Scheduling delivery for " + ref + " to occur at " + deliveryTime);
         }

         ScheduledDeliveryRunnable runnable = new ScheduledDeliveryRunnable(ref);

         scheduledRunnables.add(runnable);

         if (!backup)
         {
            scheduleDelivery(runnable, deliveryTime);
         }

         return true;
      }
      return false;
   }

   public void reSchedule()
   {
      for (ScheduledDeliveryRunnable runnable : scheduledRunnables)
      {
         scheduleDelivery(runnable, runnable.getReference().getScheduledDeliveryTime());
      }
   }

   public int getScheduledCount()
   {
      return scheduledRunnables.size();
   }

   public List<MessageReference> getScheduledMessages()
   {
      List<MessageReference> refs = new ArrayList<MessageReference>();
      synchronized (scheduledRunnables)
      {
         for (ScheduledDeliveryRunnable scheduledRunnable : scheduledRunnables)
         {
            refs.add(scheduledRunnable.getReference());
         }
      }
      return refs;
   }

   public List<MessageReference> cancel()
   {
      List<MessageReference> refs = new ArrayList<MessageReference>();
      synchronized (scheduledRunnables)
      {
         for (ScheduledDeliveryRunnable runnable : scheduledRunnables)
         {
            runnable.cancel();
            refs.add(runnable.getReference());
         }

         scheduledRunnables.clear();
      }
      return refs;
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
         if (trace)
         {
            log.trace("Scheduled delivery timeout " + ref);
         }

         synchronized (scheduledRunnables)
         {
            boolean removed = scheduledRunnables.remove(this);

            if (!removed)
            {
               log.warn("Failed to remove timeout " + this);

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
