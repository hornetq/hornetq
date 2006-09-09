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
import java.util.Iterator;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.MessageReference;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.Router;
import org.jboss.messaging.core.SimpleDelivery;
import org.jboss.messaging.core.tx.Transaction;

/**
 *  
 * The router will always first try the next receiver in the list to the one it tried last time
 * This gives a more balanced distribution than the FirstReceiverPointToPointRouter and is
 * better suited when batching messages to consumers since we will end up with messages interleaved amongst
 * consumers rather than in contiguous blocks.
 *  
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
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

   // it's important that we're actually using an ArrayList for fast array access
   protected ArrayList receivers;
   
   protected int target;

   // Constructors --------------------------------------------------

   public RoundRobinPointToPointRouter()
   {
      receivers = new ArrayList();
      target = 0;
   }

   // Router implementation -----------------------------------------
   
   public Delivery handle(DeliveryObserver observer, MessageReference ref, Transaction tx)
   {
      int initial, current;
      ArrayList receiversCopy;

      synchronized(receivers)
      {
         if (receivers.isEmpty())
         {
            return null;
         }

         // try to release the lock as quickly as possible and make a copy of the receivers array
         // to avoid deadlock (http://jira.jboss.org/jira/browse/JBMESSAGING-491)

         receiversCopy = new ArrayList(receivers.size());
         receiversCopy.addAll(receivers);
         initial = target;
         current = initial;
      }

      Delivery del = null;
      boolean selectorRejected = false;

      while (true)
      {
         Receiver r = (Receiver)receiversCopy.get(current);

         try
         {
            Delivery d = r.handle(observer, ref, tx);

            if (trace) { log.trace("receiver " + r + " handled " + ref + " and returned " + d); }

            if (d != null && !d.isCancelled())
            {
               if (d.isSelectorAccepted())
               {
                  // deliver to the first receiver that accepts
                  del = d;
                  shiftTarget(current);
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
            log.error("The receiver " + r + " is broken", t);
         }

         current = (current + 1) % receiversCopy.size();

         // if we've tried them all then we break
         if (current == initial)
         {
            break;
         }
      }

      if (del == null && selectorRejected)
      {
         del = new SimpleDelivery(null, null, true, false);
      }

      return del;
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
         target = 0;
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
            target = 0;
         }
         
         return removed;
      }
   }

   public void clear()
   {
      synchronized(receivers)
      {
         receivers.clear();
         target = 0;
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

   protected void shiftTarget(int currentTarget)
   {
      synchronized(receivers)
      {
         int size = receivers.size();
         if (size == 0)
         {
            // target has already been reset by remove()
            return;
         }
         target = Math.max((target + 1) % size, (currentTarget + 1) % size);
      }
   }
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}

