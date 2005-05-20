/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import java.io.Serializable;
import java.util.Set;


/**
 * A Router is a message handling component that incapsulates a "routing policy". A Router will
 * always implement is routing policy synchronousy, it will never try to hold a message it cannot
 * route.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface Router extends Distributor
{

   public Serializable getRouterID();

   /**
    * Synchronously routes the message to elected Receivers and returns a set of Acknowledgments
    * (positive or negative). Broken receivers are ignored and do not produce an acknowledgment.
    *
    * @param r - the Routable to forward
    *
    * @return the set containing Acknowlegment instances, one acknowledgment per receiver. The
    *         set may be empty but never null.
    */
   public Set handle(Routable r);

   /**
    * Synchronously routes the message to the specified Receivers and returns a set of
    * Acknowledgments (positive or negative). Broken receivers are ignored and do not produce an
    * acknowledgment.
    *
    * @param r - the Routable to forward
    * @param receiverIDs - set containing the ID of the Receivers to send the message to. If the
    *        set is null, the Router will choose the receivers according its routing policy.
    *
    * @return the set containing Acknowlegment instances, one acknowledgment per receiver. The
    *         set may be empty but never null. 
    */
   public Set handle(Routable r, Set receiverIDs);

   /**
    *
    * @return true if the router forwards just references to the message, or false if the router
    *         forwards the message by making a copy for each individual receiver.
    */
   public boolean isPassByReference();

}
