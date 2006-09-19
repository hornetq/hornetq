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
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Filter;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.local.PagingFilteredQueue;
import org.jboss.messaging.core.plugin.contract.MessageStore;
import org.jboss.messaging.core.plugin.contract.PersistenceManager;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.util.Future;

import EDU.oswego.cs.dl.util.concurrent.QueuedExecutor;

/**
 * 
 * A LocalClusteredQueue
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class LocalClusteredQueue extends PagingFilteredQueue implements ClusteredQueue
{
   private static final int MIN_PERIOD = 1000;
   
   private static final int STATS_DIFFERENCE_MARGIN_PERCENT = 10;   
      
   private String nodeId;
   
   private long lastTime;
   
   private QueueStats lastStats;
   
   private volatile int numberAdded;
   
   private volatile int numberConsumed;
   
   private volatile boolean changedSignificantly;
 
   public LocalClusteredQueue(String nodeId, String name, long id, MessageStore ms, PersistenceManager pm,             
                              boolean acceptReliableMessages, boolean recoverable, QueuedExecutor executor,
                              Filter filter,
                              int fullSize, int pageSize, int downCacheSize)
   {
      super(name, id, ms, pm, acceptReliableMessages, recoverable, executor, filter, fullSize, pageSize, downCacheSize);
     
      this.nodeId = nodeId;
      
      lastTime = System.currentTimeMillis();      
      
      numberAdded = numberConsumed = 0;
   }
   
   public LocalClusteredQueue(String nodeId, String name, long id, MessageStore ms, PersistenceManager pm,             
                              boolean acceptReliableMessages, boolean recoverable, QueuedExecutor executor,
                              Filter filter)
   {
      super(name, id, ms, pm, acceptReliableMessages, recoverable, executor, filter);
      
      this.nodeId = nodeId;
      
      lastTime = System.currentTimeMillis();      
      
      numberAdded = numberConsumed = 0;
   }
   
   public QueueStats getStats()
   {
      long now = System.currentTimeMillis();
      
      long period = now - lastTime;
      
      if (period <= MIN_PERIOD)
      {
         //Cache the value to avoid recalculating too often
         return lastStats;
      }
      
      float addRate = 1000 * numberAdded / ((float)period);
      
      float consumeRate = 1000 * numberConsumed / ((float)period);
      
      int cnt = messageCount();      
      
      if (lastStats != null)
      {
         if (checkSignificant(lastStats.getAddRate(), addRate) ||
             checkSignificant(lastStats.getConsumeRate(), consumeRate) ||
             checkSignificant(lastStats.getMessageCount(), cnt))
         {
            changedSignificantly = true; 
         }
         else
         {
            changedSignificantly = false;
         }
      }
      else
      {
         changedSignificantly = true;
      }
            
      lastStats = new QueueStats(name, addRate, consumeRate, messageCount());
      
      lastTime = now;
      
      numberAdded = numberConsumed = 0;
      
      return lastStats;
   }      
   
   //Have the stats changed significantly since the last time we request them?
   public boolean changedSignificantly()
   {
      return changedSignificantly;
   }
   
   public boolean isLocal()
   {
      return true;
   }
     
   public String getNodeId()
   {
      return nodeId;
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
   
   /*
    * This is the same as the normal handle() method on the Channel except it doesn't
    * persist the message even if it is persistent - this is because persistent messages
    * are always persisted on the sending node before sending.
    */
   public Delivery handleFromCluster(DeliveryObserver sender, MessageReference ref, Transaction tx)
      throws Exception
   {
      if (filter != null && !filter.accept(ref))
      {
         Delivery del = new SimpleDelivery(this, ref, true, false);
         
         return del;
      }
      
      checkClosed();
      
      Future result = new Future();

      if (tx == null)
      {         
         // Instead of executing directly, we add the handle request to the event queue.
         // Since remoting doesn't currently handle non blocking IO, we still have to wait for the
         // result, but when remoting does, we can use a full SEDA approach and get even better
         // throughput.
         this.executor.execute(new HandleRunnable(result, sender, ref, false));
   
         return (Delivery)result.getResult();
      }
      else
      {
         return handleInternal(sender, ref, tx, false);
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
   
   private boolean checkSignificant(float oldValue, float newValue)
   {
      boolean significant = false;
      
      if (oldValue != 0)
      {         
         int percentChange = (int)(100 * (oldValue - newValue) / oldValue);
         
         if (Math.abs(percentChange) >= STATS_DIFFERENCE_MARGIN_PERCENT)
         {
            significant = true;
         }
      }
      else
      {
         if (newValue != 0)
         {
            significant = true;
         }
      }
      return significant;
   }
}
