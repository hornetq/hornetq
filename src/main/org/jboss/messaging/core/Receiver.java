/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

/**
 * A component that handles routable instances. Handling means consumption or
 * synchronous/asynchronous forwarding to another receiver(s).
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface Receiver
{

   /**
    * A receiver can return an active, "done" or null delivery. The method returns null in case
    * the receiver doesn't accept the message.
    *
    * @param observer - the component the delivery should be acknowledged to.
    * 
    * @see org.jboss.messaging.core.Delivery
    */
   Delivery handle(DeliveryObserver observer, Routable routable);

}
