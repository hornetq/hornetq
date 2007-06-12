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
import org.jboss.messaging.core.Queue;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.tx.Transaction;

/**
 * 
 * This router round robins between the queues
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 2319 $</tt>
 *
 * $Id: DefaultRouter.java 2319 2007-02-15 07:16:11Z ovidiu.feodorov@jboss.com $
 *
 */
public class RoundRobinRouter implements ClusterRouter
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(DefaultRouter.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   // ArrayList<>; MUST be an arraylist for fast index access
   private ArrayList queues;

   private Queue localQueue;

   private int target;

   // Constructors ---------------------------------------------------------------------------------

   public RoundRobinRouter()
   {
      queues = new ArrayList();
   }

   // Receiver implementation ----------------------------------------------------------------------

   public Delivery handle(DeliveryObserver observer, MessageReference ref, Transaction tx)
   {
      if (trace) { log.trace(this + " routing " + ref); }

      if (!queues.isEmpty())
      {
         Queue queue = (Queue)queues.get(target);

         Delivery del = queue.handle(observer, ref, tx);

         if (trace) { log.trace(this + " routed to queue, it returned " + del); }

         incTarget();

         // Again, if the selector doesn't match then it won't on any others so no point trying
         // them.

         return del;
      }
      
      if (trace) { log.trace(this + " no queues to route to so return null"); }

      return null;
   }

   // Distributor implementation -------------------------------------------------------------------

   public boolean contains(Receiver queue)
   {
      return queues.contains(queue);
   }

   public Iterator iterator()
   {
      return queues.iterator();
   }

   public boolean add(Receiver receiver)
   {
      return add(receiver, false);
   }

   public boolean remove(Receiver queue)
   {
      if (queue == localQueue)
      {
         localQueue = null;
      }
      
      if (queues.remove(queue))
      {
         if (target >= queues.size() - 1)
         {
            target = queues.size() - 1;
         }
         return true;
      }
      
      return false;
   }

   public void clear()
   {
      queues.clear();
      
      localQueue = null;
      
      target = 0;
   }

   public int getNumberOfReceivers()
   {
      return queues.size();
   }

   // ClusterRouter implementation -----------------------------------------------------------------

   public List getQueues()
   {
      return queues;
   }

   public boolean add(Receiver receiver, boolean failedOver)
   {
      Queue queue = (Queue)receiver;
      
      queues.add(receiver);
            
      if (queue instanceof ClusteredQueue)
      {
      	ClusteredQueue clusteredQueue = (ClusteredQueue)queue;
      	
      	if (clusteredQueue.isLocal())
      	{
      		 if (localQueue != null)
             {
                throw new IllegalStateException(this + " already has local queue");
             }
      		localQueue = clusteredQueue;
      	}
      }
      else
      {
      	 if (localQueue != null)
          {
             throw new IllegalStateException(this + " already has local queue");
          }
      	localQueue = queue;
      }      
            
      return true;
   }
   
   public Queue getLocalQueue()
   {
      return localQueue;
   }

   // Public ---------------------------------------------------------------------------------------

   public int size()
   {
      return queues.size();
   }

   public String toString()
   {
      return "Router[" + Integer.toHexString(hashCode()) + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   private void incTarget()
   {
      target++;

      if (target == queues.size())
      {
         target = 0;
      }
   }

   // Inner classes --------------------------------------------------------------------------------

}


