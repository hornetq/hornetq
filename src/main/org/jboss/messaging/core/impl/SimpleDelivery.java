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

import org.jboss.logging.Logger;
import org.jboss.messaging.core.contract.Delivery;
import org.jboss.messaging.core.contract.DeliveryObserver;
import org.jboss.messaging.core.contract.MessageReference;
import org.jboss.messaging.core.impl.tx.Transaction;

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
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------
   
   private static final Logger log = Logger.getLogger(SimpleDelivery.class);
   
   // Attributes -----------------------------------------------------------------------------------

   private boolean selectorAccepted;
   private DeliveryObserver observer;
   private MessageReference reference;

   private boolean trace = log.isTraceEnabled();

   // Constructors ---------------------------------------------------------------------------------

   public SimpleDelivery()
   {
      this(null, null);
   }

   public SimpleDelivery(DeliveryObserver observer, MessageReference reference)
   {
      this(observer, reference, true);
   }

   public SimpleDelivery(DeliveryObserver observer, MessageReference reference,
                         boolean selectorAccepted)
   {

      this.reference = reference;
      this.observer = observer;
      this.selectorAccepted = selectorAccepted;
   }

   // Delivery implementation ----------------------------------------------------------------------

   public MessageReference getReference()
   {
      return reference;
   }
   
   public boolean isSelectorAccepted()
   {
      return selectorAccepted;
   }

   public DeliveryObserver getObserver()
   {
      return observer;
   }

   public void acknowledge(Transaction tx) throws Throwable
   {        
      if (trace) { log.trace(this + " acknowledging delivery " + ( tx == null ? "non-transactionally" : "in " + tx)); }
      
      observer.acknowledge(this, tx);
   }

   public void cancel() throws Throwable
   {
      if (trace) { log.trace(this + " cancelling delivery"); }
         
      observer.cancel(this);
   }
   
   // Public ---------------------------------------------------------------------------------------

   public String toString()
   {
      return "Delivery" + (reference == null ? "" : "[" + reference + "]");
   }

   // Package protected ----------------------------------------------------------------------------
   
   // Protected ------------------------------------------------------------------------------------
   
   // Private --------------------------------------------------------------------------------------
   
   // Inner classes --------------------------------------------------------------------------------
}
