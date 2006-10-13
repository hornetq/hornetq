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
package org.jboss.messaging.core;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.tx.Transaction;

/**
 * A simple Delivery implementation.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class SimpleDelivery implements Delivery
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   private static final Logger log = Logger.getLogger(SimpleDelivery.class);
   
   // Attributes ----------------------------------------------------

   protected boolean done;
   protected boolean cancelled;
   protected boolean selectorAccepted;
   protected DeliveryObserver observer;
   protected MessageReference reference;

   private boolean trace = log.isTraceEnabled();

   // Constructors --------------------------------------------------

   public SimpleDelivery()
   {
      this(null, null, false);
   }

   public SimpleDelivery(boolean d)
   {
      this(null, null, d);
   }

   public SimpleDelivery(DeliveryObserver observer, MessageReference reference)
   {
      this(observer, reference, false);
   }

   public SimpleDelivery(MessageReference reference, boolean done)
   {
      this(null, reference, done);
   }

   public SimpleDelivery(DeliveryObserver observer, MessageReference reference, boolean done)
   {
      this.done = done;
      this.reference = reference;
      this.observer = observer;
      this.selectorAccepted = true;
   }

   public SimpleDelivery(DeliveryObserver observer, MessageReference reference, boolean done,
                         boolean selectorAccepted)
   {
      this.done = done;
      this.reference = reference;
      this.observer = observer;
      this.selectorAccepted = selectorAccepted;
   }
   

   // Delivery implementation ---------------------------------

   public MessageReference getReference()
   {
      return reference;
   }

   public synchronized boolean isDone()
   {
      return done;
   }
   
   public synchronized boolean isCancelled()
   {
      return cancelled;
   }

   public boolean isSelectorAccepted()
   {
      return selectorAccepted;
   }

   public void setObserver(DeliveryObserver observer)
   {
      this.observer = observer;
   }

   public DeliveryObserver getObserver()
   {
      return observer;
   }

   public synchronized void acknowledge(Transaction tx) throws Throwable
   {        
      if (trace) { log.trace(this + " acknowledging delivery in tx:" + tx); }
      
      // deals with the race condition when acknowledgment arrives before the delivery
      // is returned back to the sending delivery observer      
      observer.acknowledge(this, tx);

      //Important note! We must ALWAYS set done true irrespective of whether we are in a tx or not.
      //Previously we were only setting done to true if there was no transaction.
      //This meant that if the acknowledgement (in the tx) came in before the call to handle()
      //had returned the delivery would end up in the delivery set in the channel and never
      //get removed - causing a leak
      done = true;      
   }

   public synchronized void cancel() throws Throwable
   {
      if (trace) { log.trace(this + " cancelling delivery"); }
      
      // deals with the race condition when cancellation arrives before the delivery
      // is returned back to the sending delivery observer      
      observer.cancel(this);
      cancelled = true;      
   }
   
   // Public --------------------------------------------------------

   public String toString()
   {
      return "Delivery" + (reference == null ? "" : "[" + reference + "]") +
         "(" + (cancelled ? "cancelled" : done ? "done" : "active") + ")";
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
