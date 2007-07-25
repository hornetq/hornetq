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
import java.util.List;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Delivery;
import org.jboss.messaging.core.contract.DeliveryObserver;
import org.jboss.messaging.core.contract.Distributor;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.contract.Receiver;
import org.jboss.messaging.core.impl.tx.Transaction;

/**
 * 
 * It will always favour the first receiver in the internal list of receivers, but will retry
 * the next one (and the next one...) if a previous one does not want to accept the message.
 * If the router has several receivers (e.g. the case of multiple consumers on a queue)
 * then if the consumers are fast then the first receiver will tend to get most or all of the references
 * 
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1174 $</tt>
 * $Id: PointToPointRouter.java 1174 2006-08-02 14:14:32Z timfox $
 */
public class FirstReceiverDistributor implements Distributor
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(FirstReceiverDistributor.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   private boolean trace = log.isTraceEnabled();

   private List receivers;
   
   private volatile boolean makeCopy;
   
   private ArrayList receiversCopy;

   // Constructors --------------------------------------------------

   public FirstReceiverDistributor()
   {
      receivers = new ArrayList();
      
      makeCopy = true;
   }

   // Router implementation -----------------------------------------

   public Delivery handle(DeliveryObserver observer, MessageReference ref, Transaction tx)
   {
      if (makeCopy)
      {
         synchronized (this)
         {         
            //We make a copy of the receivers to avoid a race condition:
            //http://jira.jboss.org/jira/browse/JBMESSAGING-505
            //Note that we do not make a copy every time - only when the receivers have changed
         
            receiversCopy = new ArrayList(receivers);
            
            makeCopy = false;
         }
      }    
      
      Delivery del = null;
      
      boolean selectorRejected = false;

      for(Iterator i = receiversCopy.iterator(); i.hasNext(); )
      {
         Receiver receiver = (Receiver)i.next();

         try
         {
            Delivery d = receiver.handle(observer, ref, tx);

            if (trace) { log.trace("receiver " + receiver + " handled " + ref + " and returned " + d); }

            if (d != null)
            {
               if (d.isSelectorAccepted())
               {
                  // deliver to the first receiver that accepts
                  del = d;
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


   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
