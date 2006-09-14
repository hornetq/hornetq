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

import java.util.ArrayList;
import java.util.List;

import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.local.Queue;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * 
 * A ClusteredQueue
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class ClusteredQueue extends Queue
{
   private static final int MIN_PERIOD = 1000;
   
   private long lastTime;
   
   private double lastGrowthRate;
   
   private volatile int numberAdded;
   
   private volatile int numberConsumed;
 
   public ClusteredQueue(long id, MessageStore ms, PersistenceManager pm, boolean acceptReliableMessages, boolean recoverable, int fullSize, int pageSize, int downCacheSize, QueuedExecutor executor, Filter filter)
   {
      super(id, ms, pm, acceptReliableMessages, recoverable, fullSize, pageSize,
            downCacheSize, executor, filter);
      
      lastTime = System.currentTimeMillis();      
      
      numberAdded = numberConsumed = 0;
   }
   
   /**
    * 
    * @return The rate of growth in messages per second of the queue
    * Rate of growth is defined as follows:
    * growth = (number of messages added - number of messages consumed) / time
    */
   public synchronized double getGrowthRate()
   {
      long now = System.currentTimeMillis();
      
      long period = now - lastTime;
      
      if (period <= MIN_PERIOD)
      {
         //Cache the value to avoid recalculating too often
         return lastGrowthRate;
      }
      
      lastGrowthRate = 1000 * (numberAdded - numberConsumed) / ((double)period);
      
      lastTime = now;
      
      numberAdded = numberConsumed = 0;
      
      return lastGrowthRate;
   }
   
   public List getDeliveries(int number) throws Exception
   {
      List dels = new ArrayList();
      
      synchronized (refLock)
      {
         synchronized (deliveryLock)
         {
            MessageReference ref;
            
            while ((ref = removeFirstInMemory()) != null)
            {
               SimpleDelivery del = new SimpleDelivery(this, ref);
               
               deliveries.add(del);
               
               dels.add(del);               
            }           
            return dels;
         }
      }          
   }

   protected void addReferenceInMemory(MessageReference ref) throws Exception
   {
      super.addReferenceInMemory(ref);
      
      //This is ok, since the channel ensures only one thread calls this method at once
      numberAdded++;
   }

   protected boolean acknowledgeInMemory(Delivery d)
   {
      boolean acked = super.acknowledgeInMemory(d);
      
      // This is ok, since the channel ensures only one thread calls this method at once
      numberConsumed--;
      
      return acked;
   }  
}
