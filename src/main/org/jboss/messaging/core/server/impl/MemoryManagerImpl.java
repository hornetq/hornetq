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
package org.jboss.messaging.core.server.impl;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.MemoryManager;
import org.jboss.messaging.utils.SizeFormatterUtil;

/**
 * A MemoryManager

 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class MemoryManagerImpl implements MemoryManager
{
   private static final Logger log = Logger.getLogger(MemoryManagerImpl.class);
   
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
   
   public MemoryManagerImpl()
   {
      runtime = Runtime.getRuntime();
      
      this.measureInterval = DEFAULT_MEASURE_INTERVAL;
      
      this.freeMemoryPercent = DEFAULT_FREE_MEMORY_PERCENT;    
   }
   
   public boolean isMemoryLow()
   {
      return low;
   }
    
   public synchronized boolean isStarted()
   {
      return started;
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
                                  
            long maxMemory = runtime.maxMemory();
            
            long totalMemory = runtime.totalMemory();
            
            long freeMemory = runtime.freeMemory();
            
            double currentFreeMemoryPercent = 100.0 * freeMemory / totalMemory;
            
            String info = "";
            info += String.format("free memory:      %s\n", SizeFormatterUtil.sizeof(freeMemory));
            info += String.format("max memory:       %s\n", SizeFormatterUtil.sizeof(maxMemory));
            info += String.format("total memory:     %s\n", SizeFormatterUtil.sizeof(totalMemory));
            info += String.format("available memory: %.2f%%\n", currentFreeMemoryPercent);

            if (log.isDebugEnabled())
            {
               log.debug(info);
            }
            
            if (currentFreeMemoryPercent <= freeMemoryPercent)
            {
               log.warn("Less than " + freeMemoryPercent + "%\n" 
                        + info +
                        "\nYou are in danger of running out of RAM. Have you set paging parameters " +
                        "on your addresses? (See user manual \"Paging\" chapter)");
               
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
