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
package org.jboss.messaging.core.impl;

import java.util.ArrayList;
import java.util.Iterator;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Delivery;
import org.jboss.messaging.core.contract.DeliveryObserver;
import org.jboss.messaging.core.contract.Distributor;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.contract.Receiver;
import org.jboss.messaging.core.impl.tx.Transaction;

/**
 *  
 * The distributor will always first try the next receiver in the list to the one it tried last time.
 * This gives a more balanced distribution than the FirstReceiverDistributor and is better
 * suited when batching messages to consumers since we will end up with messages interleaved amongst
 * consumers rather than in contiguous blocks.
 *  
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 1 $</tt>
 * $Id: $
 */
public class RoundRobinDistributor implements Distributor
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(RoundRobinDistributor.class);

   // Static ---------------------------------------------------------------------------------------
   
   // Attributes -----------------------------------------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();

   // it's important that we're actually using an ArrayList for fast array access
   private ArrayList receivers;
   
   private int target;
   
   private volatile boolean makeCopy;
   
   private ArrayList receiversCopy;
   
   // Constructors ---------------------------------------------------------------------------------

   public RoundRobinDistributor()
   {
      receivers = new ArrayList();
      
      target = 0;
      
      makeCopy = true;
   }

   // Distributor implementation ------------------------------------------------------------------------
   
   public Delivery handle(DeliveryObserver observer, MessageReference ref, Transaction tx)
   {             
      if (makeCopy)
      {
         synchronized (this)
         {         
            // We make a copy of the receivers to avoid a race condition:
            // http://jira.jboss.org/jira/browse/JBMESSAGING-505
            // Note that we do not make a copy every time - only when the receivers have changed
         
            receiversCopy = new ArrayList(receivers);
            
            if (target >= receiversCopy.size())
            {
               target = 0;
            }
         
            makeCopy = false;
         }
      }      
      
      if (receiversCopy.isEmpty())
      {
         return null;
      }

      Delivery del = null;
      
      boolean selectorRejected = false;
      
      int initial = target;

      while (true)
      {
         Receiver r = (Receiver)receiversCopy.get(target);

         try
         {
            Delivery d = r.handle(observer, ref, tx);

            if (trace) { log.trace("receiver " + r + " handled " + ref + " and returned " + d); }
            
            if (d != null)
            {
               if (d.isSelectorAccepted())
               {
                  // deliver to the first receiver that accepts
                  del = d;
                  
                  incTarget();
                                                     
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

         incTarget();

         // if we've tried them all then we break
         if (target == initial)
         {
            break;
         }
      }

      if (del == null && selectorRejected)
      {
         del = new SimpleDelivery(null, null, false, false);
      }

      return del;      
   }
   
   public synchronized boolean add(Receiver r)
   {
      if (receivers.contains(r))
      {
         return false;
      }

      receivers.add(r);
      
      makeCopy = true;
      
      return true;      
   }

   public synchronized boolean remove(Receiver r)
   {
      boolean removed = receivers.remove(r);
      
      if (removed)
      {
         makeCopy = true;
      }
      
      return removed;      
   }

   public synchronized void clear()
   {
      receivers.clear();
      
      makeCopy = true;    
   }

   public synchronized boolean contains(Receiver r)
   {
      return receivers.contains(r);     
   }

   public synchronized Iterator iterator()
   {
      return receivers.iterator();      
   }
   
   public synchronized int getNumberOfReceivers()
   {
      return receivers.size();      
   }

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------
   
   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
   
   private void incTarget()
   {
      target++;
      
      if (target == receiversCopy.size())
      {
         target = 0;
      }
   }
   
   // Inner classes --------------------------------------------------------------------------------
}

