/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import java.util.Iterator;
import java.io.Serializable;

/**
 * An interface for Receiver management.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface Distributor
{
   /**
    * Add the receiver to this distributor. If the receiver is already associated with the
    * distributor, the invocation must be a no-op  - the result of the latest handle() invocation
    * on that receiver must not be affected.
    *
    * @param receiver - the receiver to add.
    *
    * @return true if the Distributor did not already contain the specified receiver.
    */
   public boolean add(Receiver receiver);


   public Receiver get(Serializable receiverID);

   /**
    * Remove the receiver from this distributor.
    *
    * @return the removed receiver, or null if no such receiver exists.
    */
   public Receiver remove(Serializable receiverID);

   public boolean contains(Serializable receiverID);

   /**
    * Returns an iterator containing the receiver IDs.
    */
   public Iterator iterator();

   /**
    * Remove all receivers from this router.
    */
   public void clear();


}




