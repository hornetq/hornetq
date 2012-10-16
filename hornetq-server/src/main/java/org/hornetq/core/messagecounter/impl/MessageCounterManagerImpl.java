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
package org.hornetq.core.messagecounter.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.core.messagecounter.MessageCounter;
import org.hornetq.core.messagecounter.MessageCounterManager;

/**
 *
 * A MessageCounterManager
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision: 3307 $</tt>
 *
 * $Id: MessageCounterManager.java 3307 2007-11-09 20:43:00Z timfox $
 *
 */
public class MessageCounterManagerImpl implements MessageCounterManager
{

   public static final long DEFAULT_SAMPLE_PERIOD = HornetQDefaultConfiguration.DEFAULT_MESSAGE_COUNTER_SAMPLE_PERIOD;

   public static final long MIN_SAMPLE_PERIOD = 1000;

   public static final int DEFAULT_MAX_DAY_COUNT = HornetQDefaultConfiguration.DEFAULT_MESSAGE_COUNTER_MAX_DAY_HISTORY;

   private final Map<String, MessageCounter> messageCounters;

   private boolean started;

   private long period = MessageCounterManagerImpl.DEFAULT_SAMPLE_PERIOD;

   private MessageCountersPinger messageCountersPinger;

   private int maxDayCount = MessageCounterManagerImpl.DEFAULT_MAX_DAY_COUNT;

   private final ScheduledExecutorService scheduledThreadPool;

   public MessageCounterManagerImpl(final ScheduledExecutorService scheduledThreadPool)
   {
      messageCounters = new HashMap<String, MessageCounter>();

      this.scheduledThreadPool = scheduledThreadPool;
   }

   public synchronized void start()
   {
      if (started)
      {
         return;
      }

      messageCountersPinger = new MessageCountersPinger();

      Future<?> future = scheduledThreadPool.scheduleAtFixedRate(messageCountersPinger,
                                                                 0,
                                                                 period,
                                                                 TimeUnit.MILLISECONDS);
      messageCountersPinger.setFuture(future);

      started = true;
   }

   public synchronized void stop()
   {
      if (!started)
      {
         return;
      }

      messageCountersPinger.stop();

      started = false;
   }

   public synchronized void clear()
   {
      messageCounters.clear();
   }

   public synchronized void reschedule(final long newPeriod)
   {
      boolean wasStarted = started;

      if (wasStarted)
      {
         stop();
      }

      period = newPeriod;

      if (wasStarted)
      {
         start();
      }
   }

   public long getSamplePeriod()
   {
      return period;
   }

   public int getMaxDayCount()
   {
      return maxDayCount;
   }

   public void setMaxDayCount(final int count)
   {
      maxDayCount = count;
   }

   public void registerMessageCounter(final String name, final MessageCounter counter)
   {
      synchronized (messageCounters)
      {
         messageCounters.put(name, counter);
      }
   }

   public MessageCounter unregisterMessageCounter(final String name)
   {
      synchronized (messageCounters)
      {
         return messageCounters.remove(name);
      }
   }

   public Set<MessageCounter> getMessageCounters()
   {
      synchronized (messageCounters)
      {
         return new HashSet<MessageCounter>(messageCounters.values());
      }
   }

   public void resetAllCounters()
   {
      synchronized (messageCounters)
      {
         Iterator<MessageCounter> iter = messageCounters.values().iterator();

         while (iter.hasNext())
         {
            MessageCounter counter = iter.next();

            counter.resetCounter();
         }
      }
   }

   public void resetAllCounterHistories()
   {
      synchronized (messageCounters)
      {
         Iterator<MessageCounter> iter = messageCounters.values().iterator();

         while (iter.hasNext())
         {
            MessageCounter counter = iter.next();

            counter.resetHistory();
         }
      }
   }

   private class MessageCountersPinger implements Runnable
   {
      private boolean closed = false;

      private Future<?> future;

      public synchronized void run()
      {
         if (closed)
         {
            return;
         }

         synchronized (messageCounters)
         {
            Iterator<MessageCounter> iter = messageCounters.values().iterator();

            while (iter.hasNext())
            {
               MessageCounter counter = iter.next();

               counter.onTimer();
            }
         }
      }

      public void setFuture(final Future<?> future)
      {
         this.future = future;
      }

      synchronized void stop()
      {
         if (future != null)
         {
            future.cancel(false);
         }

         closed = true;
      }
   }

}
