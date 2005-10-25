/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

import org.jboss.messaging.core.tx.Transaction;

/**
 * A message delivery. It can be "done" or active.
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a> Added tx support
 * @version <tt>$Revision$</tt>
 * $Id$
 */

public interface Delivery
{
   MessageReference getReference();

   boolean isDone();

   void setObserver(DeliveryObserver observer);

   DeliveryObserver getObserver();

   void acknowledge(Transaction tx) throws Throwable;

   boolean cancel() throws Throwable;

   /**
    * Initiate a new delivery, possibly canceling this current one.
    *
    * @param r - the receiver to redeliver to.
    */
   void redeliver(Receiver r) throws Throwable;

}
