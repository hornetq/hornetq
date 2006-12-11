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

import javax.jms.TextMessage;

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
 * If there is no local queue, then it will round robin between the non local queues.
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
   private ArrayList nonLocalQueues;

   private ArrayList failedOverQueues;

   private ClusteredQueue localQueue;

   private int target;

   public DefaultRouter()
   {
      nonLocalQueues = new ArrayList();
      failedOverQueues = new ArrayList();
   }

   public int size()
   {
      return nonLocalQueues.size() + (localQueue == null ? 0 : 1);
   }

   public ClusteredQueue getLocalQueue()
   {
      return localQueue;
   }

   public boolean add(Receiver receiver)
   {
      return add(receiver,false);
   }

   public boolean add(Receiver receiver, boolean failedOver)
   {
      ClusteredQueue queue = (ClusteredQueue)receiver;

      if (queue.isLocal())
      {
         if (failedOver)
         {
            failedOverQueues.add(receiver);
         }
         else
         {
            if (localQueue != null)
            {
               throw new IllegalStateException("Already has local queue");
            }
            localQueue = queue;
         }
      }
      else
      {
         nonLocalQueues.add(queue);
      }

      return true;
   }

   public void clear()
   {
      nonLocalQueues.clear();

      localQueue = null;

      target = 0;
   }

   public boolean contains(Receiver queue)
   {
      return localQueue == queue || nonLocalQueues.contains(queue);
   }

   public Iterator iterator()
   {
      List queues = new ArrayList();

      if (localQueue != null)
      {
         queues.add(localQueue);
      }

      queues.addAll(nonLocalQueues);

      return queues.iterator();
   }

   public boolean remove(Receiver queue)
   {
      if (localQueue == queue)
      {
         localQueue = null;

         return true;
      }
      else
      {
         if (nonLocalQueues.remove(queue))
         {
            if (target >= nonLocalQueues.size() - 1)
            {
               target = nonLocalQueues.size() - 1;
            }
            return true;
         }
         else
         {
            return false;
         }
      }
   }

   public Delivery handle(DeliveryObserver observer, MessageReference reference, Transaction tx)
   {
      if (trace) { log.trace(this + " routing ref " + reference); }

      //Favour the local queue or the failedOver queue in round robin

      if (!failedOverQueues.isEmpty())
      {
         if (trace) { log.trace("Round robin on FailedOver queue, currentTarget=" + target);}
         
         LocalClusteredQueue queueToUse = null;

         if (target == -1)
         {
            queueToUse = (LocalClusteredQueue)this.localQueue; 
         }
         else
         {
            queueToUse = (LocalClusteredQueue)failedOverQueues.get(target);
         }

         incTargetFailedOver();
         
         log.info("***************** Routing to failed over queue");
         Delivery del = queueToUse.handle(observer, reference, tx);

         if (trace) { log.trace(this+" routed to failed queue, using failedOver round robbing, returned " + del); }
         
         return del;
      }
      else if (localQueue != null)
      {
         //The only time the local queue won't accept is if the selector doesn't
         //match - in which case it won't match at any other nodes too so no point
         //in trying them
         
         //debug
         try
         {
            TextMessage tm = (TextMessage)reference.getMessage();
            
            log.info("*********** Routing to local queue: " + tm.getText() + " id:" + System.identityHashCode(localQueue) );
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }

         Delivery del = localQueue.handle(observer, reference, tx);

         if (trace) { log.trace(this + " routed to local queue, it returned " + del); }

         return del;
      }
      else
      {
         //There is no local shared queue
         //We round robin among the rest

         if (!nonLocalQueues.isEmpty())
         {
            ClusteredQueue queue = (ClusteredQueue)nonLocalQueues.get(target);

            queue = (ClusteredQueue)nonLocalQueues.get(target);

            log.info("************ Routing to non local queue");
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

   private void incTargetFailedOver()
   {
      target++;

      if (target == failedOverQueues.size())
      {
         target = -1; // use the local queue
      }
   }


   private void incTarget()
   {
      target++;

      if (target == nonLocalQueues.size())
      {
         target = 0;
      }
   }


   public java.util.List getFailedQueues()
   {
      return failedOverQueues;
   }

   public List getQueues()
   {
      List queues = new ArrayList();

      if (localQueue != null)
      {
         queues.add(localQueue);
      }

      queues.addAll(nonLocalQueues);

      return queues;
   }

   public int numberOfReceivers()
   {
      return nonLocalQueues.size() + (localQueue != null ? 1 : 0);
   }
}


