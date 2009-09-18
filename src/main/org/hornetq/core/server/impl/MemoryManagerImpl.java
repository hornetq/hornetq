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
import org.hornetq.core.server.MemoryManager;
import org.hornetq.utils.SizeFormatterUtil;

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
   
   private Runtime runtime;
   
   private long measureInterval;
   
   private int memoryWarningThreshold;
   
   private volatile boolean started;
   
   private Thread thread;
   
   private volatile boolean low;
   
   public MemoryManagerImpl(int memoryWarningThreshold, long measureInterval)
   {
      runtime = Runtime.getRuntime();
      
      this.measureInterval = measureInterval;
      
      this.memoryWarningThreshold = memoryWarningThreshold;    
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
               + " FREE_MEMORY_PERCENT: " + memoryWarningThreshold);
      
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
            
            long availableMemory = freeMemory + (maxMemory - totalMemory);
                                    
            double availableMemoryPercent = 100.0 * (double)availableMemory / maxMemory;
            
            String info = "";
            info += String.format("free memory:      %s\n", SizeFormatterUtil.sizeof(freeMemory));
            info += String.format("max memory:       %s\n", SizeFormatterUtil.sizeof(maxMemory));
            info += String.format("total memory:     %s\n", SizeFormatterUtil.sizeof(totalMemory));
            info += String.format("available memory: %.2f%%\n", availableMemoryPercent);

            if (log.isDebugEnabled())
            {
               log.debug(info);
            }
            
            if (availableMemoryPercent <= memoryWarningThreshold)
            {
               log.warn("Less than " + memoryWarningThreshold + "%\n" 
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
