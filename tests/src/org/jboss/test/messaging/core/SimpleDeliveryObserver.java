/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.core;

import org.jboss.messaging.core.DeliveryObserver;
import org.jboss.messaging.core.Delivery;
import org.jboss.messaging.core.Receiver;
import org.jboss.messaging.core.tx.Transaction;
import org.jboss.messaging.util.NotYetImplementedException;
import org.jboss.logging.Logger;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class SimpleDeliveryObserver implements DeliveryObserver
{
   // Constants -----------------------------------------------------
   private static final Logger log = Logger.getLogger(SimpleDeliveryObserver.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // DeliveryObserver implementation --------------------------

   public synchronized void acknowledge(Delivery d, Transaction tx)
   {
      log.info("Delivery " + d + " was acknowledged");
   }

   public synchronized boolean cancel(Delivery d)
   {
      throw new NotYetImplementedException();
   }

   public synchronized void redeliver(Delivery d, Receiver r)
   {
      throw new NotYetImplementedException();
   }


   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
