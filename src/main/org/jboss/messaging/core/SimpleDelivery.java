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

import java.io.Serializable;

import org.jboss.messaging.core.tx.Transaction;

/**
 * A simple Delivery implementation.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> Added tx support
 * @version <tt>$Revision$</tt>
 * 
 * $Id$
 */
public class SimpleDelivery implements Delivery, Serializable
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 4995034535739753957L;

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected boolean done;
   protected DeliveryObserver observer;
   protected MessageReference ref;

   // Constructors --------------------------------------------------

   public SimpleDelivery()
   {
      this(null, null, false);
   }

   public SimpleDelivery(boolean d)
   {
      this(null, null, d);
   }

   public SimpleDelivery(DeliveryObserver observer, MessageReference ref)
   {
      this(observer, ref, false);
   }

   public SimpleDelivery(DeliveryObserver observer, MessageReference ref, boolean done)
   {
      this.done = done;
      this.ref = ref;
      this.observer = observer;
   }

   // Delivery implementation ---------------------------------

   public MessageReference getReference()
   {
      return ref;
   }

   public boolean isDone()
   {
      return done;
   }

   public void setObserver(DeliveryObserver observer)
   {
      this.observer = observer;
   }

   public DeliveryObserver getObserver()
   {
      return observer;
   }

   public void acknowledge(Transaction tx) throws Throwable
   {
      observer.acknowledge(this, tx);
   }

   public boolean cancel() throws Throwable
   {
      return observer.cancel(this);
   }

   public void redeliver(Receiver r) throws Throwable
   {
      observer.redeliver(this, r);
   }

   // Public --------------------------------------------------------

   public String toString()
   {

      return "Delivery[" + ref + "](" + (isDone() ? "done" : "active" ) + ")"; 

   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
