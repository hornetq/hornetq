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

import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.tx.Transaction;

/**
 * 
 * A FavourLocalRouter
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class FavourLocalRouter implements ClusterRouter
{
   private List queues;
   
   private ClusteredQueue localQueue;
   
   public FavourLocalRouter()
   {
      queues = new ArrayList();
   }
   
   public int size()
   {
      return queues.size();
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
      
      return true;
   }

   public void clear()
   {
      queues.clear();
      localQueue = null;
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
         return true;
      }
      else
      {
         return false;
      }
   }

   public Delivery handle(DeliveryObserver observer, MessageReference reference, Transaction tx)
   {
      //Favour the local queue
           
      if (localQueue != null)
      {
         //But only if it has consumers
         
         Delivery del = localQueue.handle(observer, reference, tx);
         
         if (del != null && del.isSelectorAccepted())
         {
            return del;
         }
      }
      
      //TODO make this round robin
      
      Iterator iter = queues.iterator();
      
      while (iter.hasNext())
      {
         ClusteredQueue queue = (ClusteredQueue)iter.next();
         
         if (!queue.isLocal())
         {
            Delivery del = queue.handle(observer, reference, tx);
            
            if (del != null && del.isSelectorAccepted())
            {
               return del;
            }
         }
      }
      
      return null;      
   }
   
   public List getQueues()
   {
      return queues;
   }
}
