/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

import java.util.Set;
import java.util.Iterator;
import java.util.HashSet;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a> $Id$
 * @version <tt>$Revision$</tt>
 */
class CompositeDelivery extends SimpleDelivery
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private Set deliveries;

   // Constructors --------------------------------------------------

   public CompositeDelivery(DeliveryObserver observer, Set deliveries)
   {
      for(Iterator i = deliveries.iterator(); i.hasNext(); )
      {
         Delivery d = (Delivery)i.next();
         d.setObserver(observer);
      }
      this.deliveries = new HashSet(deliveries);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
}
