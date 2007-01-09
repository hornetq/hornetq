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
import org.jboss.messaging.core.Routable;
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
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
public class DefaultRouter implements ClusterRouter
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(DefaultRouter.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private boolean trace = log.isTraceEnabled();

   // ArrayList<>; MUST be an arraylist for fast index access
   private ArrayList nonLocalQueues;

   // ArrayList<FailedOverQueue>; MUST be an arraylist for fast index access
   private ArrayList failedOverQueues;

   private ClusteredQueue localQueue;

   private int target;

   // Constructors ---------------------------------------------------------------------------------

   public DefaultRouter()
   {
      nonLocalQueues = new ArrayList();
      failedOverQueues = new ArrayList();
   }

   // Receiver implementation ----------------------------------------------------------------------

   public Delivery handle(DeliveryObserver observer, MessageReference ref, Transaction tx)
   {
      if (trace) { log.trace(this + " routing " + ref); }

      // Favour the local queue or the failedOver queue in round robin

      if (!failedOverQueues.isEmpty())
      {
         // If the message arrived over a failed-over connection, try to send the message to its
         // corresponding "failed-over" queue.

         Integer failedNodeID = (Integer)ref.getHeader(Routable.FAILED_NODE_ID);

         if (failedNodeID != null)
         {
            Delivery del = null;

            LocalClusteredQueue targetFailoverQueue = locateFailoverQueue(failedNodeID.intValue());

            if (targetFailoverQueue != null)
            {
               del = targetFailoverQueue.handle(observer, ref, tx);
            }

            if (trace) { log.trace(this + " routed message to fail-over queue " + targetFailoverQueue + ", returned " + del) ;}

            return del;
         }
         else
         {
            LocalClusteredQueue queueToUse = null;

            if (target == -1)
            {
               queueToUse = (LocalClusteredQueue)localQueue;
            }
            else
            {
               queueToUse = (LocalClusteredQueue)failedOverQueues.get(target);
            }

            incTargetFailedOver();

            Delivery del = queueToUse.handle(observer, ref, tx);

            if (trace) { log.trace(this + " routed message to failed queue, using failed-over round robbing, returned " + del); }

            return del;
         }
      }
      else if (localQueue != null)
      {
         // The only time the local queue won't accept is if the selector doesn't match, in which
         // case it won't match at any other nodes too so no point in trying them

         Delivery del = localQueue.handle(observer, ref, tx);

         if (trace) { log.trace(this + " routed to local queue, it returned " + del); }

         return del;
      }
      else
      {
         // There is no local shared queue. We round robin among the rest.

         if (!nonLocalQueues.isEmpty())
         {
            ClusteredQueue queue = (ClusteredQueue)nonLocalQueues.get(target);

            Delivery del = queue.handle(observer, ref, tx);

            if (trace) { log.trace(this + " routed to remote queue, it returned " + del); }

            incTarget();

            // Again, if the selector doesn't match then it won't on any others so no point trying
            // them.

            return del;
         }
      }

      if (trace) { log.trace(this + " no queues to route to so return null"); }

      return null;
   }

   // Distributor implementation -------------------------------------------------------------------

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

   public boolean add(Receiver receiver)
   {
      return add(receiver,false);
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

   public void clear()
   {
      nonLocalQueues.clear();
      localQueue = null;
      target = 0;
   }

   public int getNumberOfReceivers()
   {
      return nonLocalQueues.size() + (localQueue != null ? 1 : 0);
   }


   // ClusterRouter implementation -----------------------------------------------------------------

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

   public List getFailedQueues()
   {
      return failedOverQueues;
   }

   public ClusteredQueue getLocalQueue()
   {
      return localQueue;
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

   // Public ---------------------------------------------------------------------------------------

   public int size()
   {
      return nonLocalQueues.size() + (localQueue == null ? 0 : 1);
   }

   public String toString()
   {
      return "Router[" + Integer.toHexString(hashCode()) + "]";
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

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

   private FailedOverQueue locateFailoverQueue(int failedNodeID)
   {
      // TODO - this is a VERY slow sequential pass; I am sure we can come with a smarter way to
      //        locate the queue
      for(int i = 0; i < failedOverQueues.size(); i++)
      {
         if (((FailedOverQueue)failedOverQueues.get(i)).getFailedNodeID() == failedNodeID)
         {
            return (FailedOverQueue)failedOverQueues.get(i);
         }
      }
      return null;
   }

   // Inner classes --------------------------------------------------------------------------------

}


