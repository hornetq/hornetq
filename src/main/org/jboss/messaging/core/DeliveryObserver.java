/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public interface DeliveryObserver
{
   /**
    * Behaves transactionally in presence of a JTA transaction.
    */
   void acknowledge(Delivery d);

   boolean cancel(Delivery d) throws Throwable;

   /**
    * Initiate a new delivery, possibly canceling the old one.
    *
    * @param r - the receiver to redeliver to.
    */
   void redeliver(Delivery old, Receiver r) throws Throwable;
}
