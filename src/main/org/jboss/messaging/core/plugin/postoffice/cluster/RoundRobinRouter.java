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
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.MessageReference;
import org.jboss.messaging.core.tx.Transaction;

/**
 * 
 * This router first favours the failed over queue (if there is one) TODO revisit this
 * 
 * Then it will round robin between the non queues.
 * 
 * FIXME - none of the new failed over functionality has been tested!!
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

   // ArrayList<FailedOverQueue>; MUST be an arraylist for fast index access
   private ArrayList failedOverQueues;
   
   private ClusteredQueue localQueue;

   private int target;

   // Constructors ---------------------------------------------------------------------------------

   public RoundRobinRouter()
   {
      queues = new ArrayList();
      
      //FIXME - this should be a map not an arraylist
      failedOverQueues = new ArrayList();
   }

   // Receiver implementation ----------------------------------------------------------------------

   public Delivery handle(DeliveryObserver observer, MessageReference ref, Transaction tx)
   {
      if (trace) { log.trace(this + " routing " + ref); }

      //TODO - revisit all of this - it doesn't look right to me - Tim
      if (!failedOverQueues.isEmpty())
      {
         // If the message arrived over a failed-over connection, try to send the message to its
         // corresponding "failed-over" queue.

         Integer failedNodeID = (Integer)ref.getMessage().getHeader(Message.FAILED_NODE_ID);

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
      }
      
      // We round robin among the rest.

      if (!queues.isEmpty())
      {
         ClusteredQueue queue = (ClusteredQueue)queues.get(target);

         Delivery del = queue.handle(observer, ref, tx);

         if (trace) { log.trace(this + " routed to remote queue, it returned " + del); }

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
      //FIXME - what about failed over queues??
      return queues.contains(queue);
   }

   public Iterator iterator()
   {
      //FIXME - this is broken - where are the failed over queuues?
      
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
      else
      {
         //Maybe it's a failed over queue
         if (this.failedOverQueues.remove(queue))
         {
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
      queues.clear();
      
      failedOverQueues.clear();
      
      localQueue = null;
      
      target = 0;
   }

   public int getNumberOfReceivers()
   {
      //FIXME - what about failed over queues????
      return queues.size();
   }

   // ClusterRouter implementation -----------------------------------------------------------------

   public List getQueues()
   {
      return queues;
   }

   public List getFailedQueues()
   {
      return failedOverQueues;
   }

   public boolean add(Receiver receiver, boolean failedOver)
   {
      ClusteredQueue queue = (ClusteredQueue)receiver;
      
      if (queue.isLocal())
      {
         if (localQueue == null)
         {
            localQueue = queue;
         }
         else
         {
            throw new IllegalStateException("Local queue already exists");
         }
      }
      
      if (failedOver)
      {
         failedOverQueues.add(receiver);
      }
      else
      {
         queues.add(receiver);
      }
      return true;
   }
   
   public ClusteredQueue getLocalQueue()
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

   private FailedOverQueue locateFailoverQueue(int failedNodeID)
   {
      // TODO - this is a VERY slow sequential pass; I am sure we can come with a smarter way to
      //        locate the queue
      
      // This is rubbish - should be using a Map for the failed over queues
      // or better still rethink the whole way this failed over queue routing works - it is a mess!
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


