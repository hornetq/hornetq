/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import java.io.Serializable;

/**
 * A component that handles Routables. Handling means consumption or synchronous/asynchronous
 * forwarding to another Receiver(s).
 *
 * Note. When implementing Receiver, override Object's equals() and hashCode() so two receiver
 * instances with equal IDs will be equal.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface Receiver
{

   /**
    * Returns the receiver ID. The receiver ID will be used by the sender when storing a
    * negative acknowledment for reliable messages.
    * @return this receiver ID.
    */
   public Serializable getReceiverID();

   /**
    * Hand over the routable to the Receiver.
    *
    * If handle() returns false, this doesn't necessarily mean that the Receiver won't eventually
    * handle the message. It usually means that the Receiver is in process of handling the message
    * (asynchronously) and a possible positive acknowledgment could arrive later.
    *
    * 
    *
    * <p>
    * When invoking this method, be prepared to deal with unchecked exceptions the Receiver may
    * throw. This is equivalent with a negative acknowledgement, but the sender may also decide
    * to remove this Receiver from its list and not attempt delivery to it anymore.
    *
    * TODO: specify the behavior when receiving a null routable.
    *
    * @param routable the routable to be handled by the Receiver.
    *
    * @return true if the Receiver acknowledges the Routable and relieves the sender of the
    *         responsibity to deal with the Routable.
    */
   public boolean handle(Routable routable);




}
