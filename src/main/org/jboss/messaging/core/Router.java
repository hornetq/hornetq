/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import org.jboss.messaging.core.Distributor;
import org.jboss.messaging.core.Receiver;

/**
 * A Router is a message handling component that incapsulates a "routing policy". A Router will
 * always implement is routing policy synchronousy, it will never try to hold a message it cannot
 * route.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface Router extends Receiver, Distributor
{

   /**
    *
    * @return true if the router forwards just references to the message, or false if the router
    *         forwards the message by making a copy for each individual receiver.
    */
   public boolean isPassByReference();

}
