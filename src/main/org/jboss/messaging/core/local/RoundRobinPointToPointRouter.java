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
package org.jboss.messaging.core.local;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Routable;
import org.jboss.messaging.core.Router;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.tx.Transaction;

/**
 * 
 * This router deliver the reference to a maximum of one of the router's receivers.
 * 
 * The router will always first try the next receiver in the list to the one it tried last time
 * This gives a more balanced distribution than the FirstReceiverPointToPointRouter and is
 * better suited when batching messages to consumers since we will end up with messages interleaved amongst
 * consumers rather than in contiguous blocks.
 *  
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1 $</tt>
 * $Id: $
 */
public class RoundRobinPointToPointRouter implements Router
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(RoundRobinPointToPointRouter.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();

   //It's important that we're actually using an ArrayList for fast array access
   protected ArrayList receivers;
   
   protected int pos;

   // Constructors --------------------------------------------------

   public RoundRobinPointToPointRouter()
   {
      receivers = new ArrayList();
      
      reset();
   }

   // Router implementation -----------------------------------------
   
   public Set handle(DeliveryObserver observer, Routable routable, Transaction tx)
   {
      Set deliveries = new HashSet();
      
      boolean selectorRejected = false;
      
      synchronized(receivers)
      {
         if (receivers.isEmpty())
         {
            return deliveries;
         }
         
         int firstPos = pos;
         
         while (true)
         {
            Receiver receiver = (Receiver)receivers.get(pos);
            
            try
            {
               Delivery d = receiver.handle(observer, routable, tx);

               if (trace) { log.trace("receiver " + receiver + " handled " + routable + " and returned " + d); }
     
               if (d != null && !d.isCancelled())
               {
                  if (d.isSelectorAccepted())
                  {
                     // deliver to the first receiver that accepts
                     deliveries.add(d);
                     
                     incPos();
                     
                     break;
                  }
                  else
                  {
                     selectorRejected = true;
                  }
               }
            }
            catch(Throwable t)
            {
               // broken receiver - log the exception and ignore it
               log.error("The receiver " + receiver + " is broken", t);
            }
            
            incPos();
            
            //If we've tried them all then we break
            
            if (pos == firstPos)
            {
               break;
            }            
         }
      }
      
      if (deliveries.isEmpty() && selectorRejected)
      {
         deliveries.add(new SimpleDelivery(null, null, true, false));
      }

      return deliveries;
   }

   public boolean add(Receiver r)
   {
      synchronized(receivers)
      {
         if (receivers.contains(r))
         {
            return false;
         }
         receivers.add(r);
         
         reset();
      }
      return true;
   }


   public boolean remove(Receiver r)
   {
      synchronized(receivers)
      {
         boolean removed = receivers.remove(r);
         
         if (removed)
         {
            reset();
         }
         
         return removed;
      }
   }

   public void clear()
   {
      synchronized(receivers)
      {
         receivers.clear();
         
         reset();
      }
   }

   public boolean contains(Receiver r)
   {
      synchronized(receivers)
      {
         return receivers.contains(r);
      }
   }

   public Iterator iterator()
   {
      synchronized(receivers)
      {
         return receivers.iterator();
      }
   }


   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected void reset()
   {
      //Reset back to the first one
      
      pos = 0;
   }
   
   protected void incPos()
   {
      pos++;
      
      //Wrap around
      
      if (pos == receivers.size())
      {
         pos = 0;
      }
   }
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}

