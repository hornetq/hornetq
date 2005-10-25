/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

import org.jboss.messaging.core.tx.Transaction;

/**
 * A component that handles routable instances. Handling means consumption or
 * synchronous/asynchronous forwarding to another receiver(s).
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> Added tx support
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public interface Receiver
{

   /**
    * A receiver can return an active, "done" or null delivery. The method returns null in case
    * the receiver doesn't accept the message. The return value is <i>unspecified</i> when the
    * message is submitted in the context of a transaction (tx not null).
    *
    * @param observer - the component the delivery should be acknowledged to.
    * 
    * @see org.jboss.messaging.core.Delivery
    * @see org.jboss.messaging.core.DeliveryObserver
    */
   Delivery handle(DeliveryObserver observer, Routable routable, Transaction tx);

}
