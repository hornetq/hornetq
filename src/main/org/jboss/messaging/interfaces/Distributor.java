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

   /**
    * Remove all receivers from this router.
    */
   public void clear();

   /**
    * Returns the message acknowledgment status of the last message handled, on a per-receiver
    * basis. The message acknowledgment status is the result of the lastest handle() invocation on
    * this Receiver. If handle() was not yet invoked on the Receiver, returns false.
    * <p>
    * If the caller wants a consistent result relative to the whole set of receivers, it is the
    * caller's responsibility to insure adequate locking of the Distributor between successive
    * acknowledge() invocations. 
    *
    * @return the message acknowledgment status.
    */
   public boolean acknowledged(Receiver receiver);

}




