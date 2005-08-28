/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

import java.util.Iterator;

/**
 * An interface for Receiver management.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface Distributor
{

   boolean contains(Receiver receiver);

   Iterator iterator();

   /**
    * Add the receiver to this distributor.
    *
    * @return true if the distributor did not already contain the specified receiver and the
    *         receiver was added to the distributor, false otherwise.
    */
   boolean add(Receiver receiver);

   /**
    * Remove the receiver from this distributor.
    *
    * @return true if this distributor contained the specified receiver.
    */
   boolean remove(Receiver receiver);

   /**
    * Remove all receivers.
    */
   void clear();

}
