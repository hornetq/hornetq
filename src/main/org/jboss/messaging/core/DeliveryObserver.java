/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

import org.jboss.messaging.core.tx.Transaction;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public interface DeliveryObserver
{

   void acknowledge(Delivery d, Transaction tx);

   boolean cancel(Delivery d) throws Throwable;

   /**
    * Initiate a new delivery, possibly canceling the old one.
    *
    * @param r - the receiver to redeliver to.
    */
   void redeliver(Delivery old, Receiver r) throws Throwable;
}
