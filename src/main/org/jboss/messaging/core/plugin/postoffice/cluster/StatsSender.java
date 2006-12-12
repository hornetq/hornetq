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
package org.jboss.messaging.core.plugin.postoffice.cluster;

import java.util.Timer;
import java.util.TimerTask;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.plugin.contract.MessagingComponent;

/**
 * A StatsSender
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class StatsSender implements MessagingComponent
{
   private static final Logger log = Logger.getLogger(DefaultClusteredPostOffice.class);   
   
   private PostOfficeInternal office;
   
   private boolean started;
   
   private Timer timer;
   
   private long period;
    
   private SendStatsTimerTask task;
   
   StatsSender(PostOfficeInternal office, long period)
   {
      this.office = office;
      
      this.period = period;
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }
      
      //Needs to be daemon
      timer = new Timer(true);
      
      //Add a random delay to prevent all timers starting at once
      long delay = (long)(period * Math.random());
      
      task = new SendStatsTimerTask();
            
      timer.schedule(task, delay, period);      
      
      started = true;
   }

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;
      }
            
      //Wait for timer task to stop
      
      task.stop();
      
      timer.cancel();
      
      timer = null;
      
      started = false;
   }      
   
   class SendStatsTimerTask extends TimerTask
   {
      public synchronized void run()
      {
         try
         {
            office.sendQueueStats();
         }
         catch (Exception e)
         {
            log.error("Failed to send statistics", e);
         }
      }  
                        
      synchronized void stop()
      {
         cancel();
      }
   }
}
