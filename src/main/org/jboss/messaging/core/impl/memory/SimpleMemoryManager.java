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
package org.jboss.messaging.core.impl.memory;

import org.jboss.messaging.util.Logger;
import org.jboss.messaging.core.MemoryManager;

/**
 * A MemoryManager

 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class SimpleMemoryManager implements MemoryManager
{
   private static final Logger log = Logger.getLogger(SimpleMemoryManager.class);
   
   private static final long DEFAULT_MEASURE_INTERVAL = 3000;
   
   private static final int DEFAULT_FREE_MEMORY_PERCENT = 25;
       
   private Runtime runtime;
   
   //TODO Should be configurable
   private long measureInterval;
   
   //TODO Should be configurable
   private int freeMemoryPercent;
   
   private volatile boolean started;
   
   private Thread thread;
   
   private volatile boolean low;
   
   public SimpleMemoryManager()
   {
      runtime = Runtime.getRuntime();
      
      this.measureInterval = DEFAULT_MEASURE_INTERVAL;
      
      this.freeMemoryPercent = DEFAULT_FREE_MEMORY_PERCENT;    
   }
   
   public boolean isMemoryLow()
   {
      return low;
   }
    
   
   public synchronized void start()
   {
      log.debug("Starting MemoryManager with MEASURE_INTERVAL: " + measureInterval
               + " FREE_MEMORY_PERCENT: " + freeMemoryPercent);
      
      if (started)
      {
         //Already started
         return;
      }
      
      started = true;
      
      thread = new Thread(new MemoryRunnable());
      
      thread.setDaemon(true);
      
      thread.start();
   }
   
   public synchronized void stop()
   {      
      if (!started)
      {
         //Already stopped
         return;
      }
      
      started = false;
      
      thread.interrupt();
      
      try
      {
         thread.join();
      }
      catch (InterruptedException ignore)
      {         
      }
   }
   
   private class MemoryRunnable implements Runnable
   {
      public void run()
      {
         while (true)
         {
            try
            {
               if (thread.isInterrupted() && !started)
               {
                  break;
               }
               Thread.sleep(measureInterval);
            }
            catch (InterruptedException ignore)
            {
               if (!started)
               {
                  break;
               }
            }
                        
            long freeMemory = runtime.freeMemory();
            
            long maxMemory = runtime.maxMemory();
            
            long totalMemory = runtime.totalMemory();
            
            long availableMemory = freeMemory + maxMemory - totalMemory;
            
            if (100 * availableMemory / totalMemory <= freeMemoryPercent)
            {
               //log.warn("Less than " + freeMemoryPercent + "% of total available memory free");
               low = true;
            }
            else
            {
               low = false;
            }
             
         }
      }
   }
}
