/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.interfaces;

import java.util.Iterator;

/**
 * An interface for Receiver management.
 *
 * TODO: notification infrastructure? A listnere will be notified each time a Receiver is added or
 *       removed?
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface Distributor
{
   /**
    * Add the receiver to this distributor.
    *
    * @param receiver - the reciever to add.
    *
    * @return true if the Distributor did not already contain the specified receiver.
    */
   public boolean add(Receiver receiver);

   /**
    * Remove the receiver from this distributor.
    *
    * @param receiver - the receiver to remove.
    *
    * @return true if the Distributor contained the specified element.
    */
   public boolean remove(Receiver receiver);

   public boolean contains(Receiver receiver);

   public Iterator iterator();

}




