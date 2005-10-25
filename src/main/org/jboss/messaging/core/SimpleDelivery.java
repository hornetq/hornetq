/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

import java.io.Serializable;

import org.jboss.messaging.core.tx.Transaction;

/**
 * A simple Delivery implementation.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
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
