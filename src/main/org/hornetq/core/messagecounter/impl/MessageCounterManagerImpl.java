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
package org.hornetq.core.messagecounter.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
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
   
   public static final long DEFAULT_SAMPLE_PERIOD = ConfigurationImpl.DEFAULT_MESSAGE_COUNTER_SAMPLE_PERIOD;

   public static final long MIN_SAMPLE_PERIOD = 1000;

   public static final int DEFAULT_MAX_DAY_COUNT = ConfigurationImpl.DEFAULT_MESSAGE_COUNTER_MAX_DAY_HISTORY;

   private static final Logger log = Logger.getLogger(MessageCounterManagerImpl.class);
   
   private Map<String, MessageCounter> messageCounters;
   
   private boolean started;
   
   private long period = DEFAULT_SAMPLE_PERIOD;
   
   private MessageCountersPinger messageCountersPinger;

   private int maxDayCount = DEFAULT_MAX_DAY_COUNT;

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
      
      Future<?> future = scheduledThreadPool.scheduleAtFixedRate(messageCountersPinger, 0, period, TimeUnit.MILLISECONDS);
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
   
   public synchronized void reschedule(long newPeriod)
   {
      boolean wasStarted = this.started;
      
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
   
   public void setMaxDayCount(int count)
   {
      maxDayCount = count;  
   }
   
   public void registerMessageCounter(String name, MessageCounter counter)
   {
      synchronized (messageCounters)
      {
         messageCounters.put(name, counter);
      }
   }
   
   public MessageCounter unregisterMessageCounter(String name)
   {
      synchronized (messageCounters)
      {
         return (MessageCounter)messageCounters.remove(name);
      }
   }
   
   public Set<MessageCounter> getMessageCounters()
   {
      synchronized (messageCounters)
      {
         return new HashSet<MessageCounter>(messageCounters.values());
      }
   }
   
   public MessageCounter getMessageCounter(String name)
   {
      synchronized (messageCounters)
      {
         return (MessageCounter)messageCounters.get(name);
      }
   }
   
   public void resetAllCounters()
   {
      synchronized (messageCounters)
      {
         Iterator<MessageCounter> iter = messageCounters.values().iterator();
         
         while (iter.hasNext())
         {
            MessageCounter counter = (MessageCounter)iter.next();
            
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
            MessageCounter counter = (MessageCounter)iter.next();
            
            counter.resetHistory();
         }
      }
   }
   
   class MessageCountersPinger implements Runnable
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
               MessageCounter counter = (MessageCounter)iter.next();
               
               counter.onTimer();
            }
         }
      }  
                        
      public void setFuture(Future<?> future)
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
