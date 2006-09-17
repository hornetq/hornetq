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
 * A MessageMover
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */

class MessageRedistributor implements MessagingComponent
{
   private static final Logger log = Logger.getLogger(MessageRedistributor.class);
     
   private PostOfficeInternal office;
   
   private Timer timer;
   
   private long period;
   
   MessageRedistributor(PostOfficeInternal office, long period)
   {
      this.office = office;
      
      this.period = period;
   }
   
   
   // MessagingComponent overrides
   // ---------------------------------------------------
   
   public void start() throws Exception
   {
      timer = new Timer(true);
      
      //Add a random delay to start, to prevent multiple exchanges all calculating at the same time
      
      long delay = (long)(Math.random() * period);
      
      timer.schedule(new RedistributeTimerTask(), delay, period);
   }

   public void stop() throws Exception
   {
      timer.cancel();
   }
   
   class RedistributeTimerTask extends TimerTask
   {
      public void run()
      {
         try
         {
            office.calculateRedistribution();
         }
         catch (Throwable t)
         {
            log.error("Caught Throwable in calculating message redistribution", t);
         }
         try
         {
            office.sendStats();
         }
         catch (Exception e)
         {
            log.error("Caught Exception in calculating/sending queue statistics", e);
         }
      }
      
   }
}
