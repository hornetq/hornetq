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
import java.util.Iterator;
import java.util.List;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.tx.Transaction;

/**
 * 
 * A DefaultRouter
 * 
 * This router always favours the local queue.
 * 
 * If there is no local queue it will round robin between the others.
 * 
 * In the case of a distributed point to point queue deployed at each node in the cluster
 * there will always be a local queue.
 * 
 * In this case, with the assumption that producers and consumers are distributed evenly across the cluster
 * then sending the message to the local queue is the most efficient policy.
 * 
 * The exception to this if there are no consumers on the local queue.
 * 
 * In the case of a durable subscription, there may well be no local queue since the durable subscription lives
 * only on the number of nodes that it is looked up at.
 * 
 * In this case the round robin routing will kick in
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class DefaultRouter implements ClusterRouter
{
   private static final Logger log = Logger.getLogger(DefaultRouter.class);
   
   private boolean trace = log.isTraceEnabled();
      
   //MUST be an arraylist for fast index access
   private ArrayList queues;
   
   private ClusteredQueue localQueue;
   
   private int target;
   
   public DefaultRouter()
   {
      queues = new ArrayList();
   }
   
   public int size()
   {
      return queues.size();
   }
   
   public ClusteredQueue getLocalQueue()
   {
      return localQueue;
   }

   public boolean add(Receiver receiver)
   {
      ClusteredQueue queue = (ClusteredQueue)receiver;
      
      if (queue.isLocal())
      {
         if (localQueue != null)
         {
            throw new IllegalStateException("Already has local queue");
         }
         localQueue = queue;
      }
      
      queues.add(queue); 
      
      target = 0;
      
      return true;
   }

   public void clear()
   {
      queues.clear();
      
      localQueue = null;
      
      target = 0;
   }

   public boolean contains(Receiver queue)
   {
      return queues.contains(queue);
   }

   public Iterator iterator()
   {
      return queues.iterator();
   }

   public boolean remove(Receiver queue)
   {      
      if (queues.remove(queue))
      {
         if (localQueue == queue)
         {
            localQueue = null;
         }
         
         target = 0;
         
         return true;
      }
      else
      {
         return false;
      }
   }

   public Delivery handle(DeliveryObserver observer, MessageReference reference, Transaction tx)
   {
      if (trace) { log.trace(this + " routing ref " + reference); }
      
      //Favour the local queue
           
      if (localQueue != null && localQueue.numberOfReceivers() > 0)
      {
         //The only time the local queue won't accept is if the selector doesn't
         //match - in which case it won't match at any other nodes too so no point
         //in trying them
         
         Delivery del = localQueue.handle(observer, reference, tx);
         
         if (trace) { log.trace(this + " routed to local queue, it returned " + del); }
         
         return del;
      }
      else
      {
         //There is no local shared queue or the local queue has no consumers
          
         //We round robin among the rest
         if ((localQueue == null && !queues.isEmpty()) || (localQueue != null && queues.size() > 1))
         {
            ClusteredQueue queue = (ClusteredQueue)queues.get(target);
            
            if (queue == localQueue)
            {
               //We don't want to choose the local queue
               incTarget();
            }
            
            queue = (ClusteredQueue)queues.get(target);
            
            Delivery del = queue.handle(observer, reference, tx);
             
            if (trace) { log.trace(this + " routed to remote queue, it returned " + del); }
                        
            incTarget();

            //Again, if the selector doesn't match then it won't on any others so no point trying them
            return del;
         }                  
      }
      
      if (trace) { log.trace(this + " no queues to route to so return null"); }
      
      return null;
   }
   
   private void incTarget()
   {
      target++;
      
      if (target == queues.size())
      {
         target = 0;
      }
   }
   
   public List getQueues()
   {
      return queues;
   }

   public int numberOfReceivers()
   {
      return queues.size();
   }
}
