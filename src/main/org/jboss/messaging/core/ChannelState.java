/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

import java.util.List;

/**
 * A channel's state.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>1.1</tt>
 * ChannelState.java,v 1.1 2005/08/28 11:07:15 ovidiu Exp
 */
interface ChannelState
{
   /**
    * @return true if the state is able to atomically store undelivered or accept deliveries.
    *         It doesn't necesarily mean the state is reliable, though.
    *
    * @see org.jboss.messaging.core.ChannelState#isReliable()
    */
   boolean isTransactional();

   /**
    * @return true if the state is able to store reliable undelivered. It implies transactionality.
    *
    * @see org.jboss.messaging.core.ChannelState#isTransactional()
    */
   boolean isReliable();

   /**
    * Works transactionally in presence of a JTA transaction.
    */
   void add(Delivery d) throws Throwable;

   /**
    * Works transactionally in presence of a JTA transaction.
    */
   boolean remove(Delivery d) throws Throwable;

   /**
    * A list of undelivered in process of being delivered.
    *
    * @return a <i>copy</i> of the internal storage.
    */
   List delivering();

   /**
    * Works transactionally in presence of a JTA transaction.
    */
   void add(Routable routable) throws Throwable;

   boolean remove(Routable routable);

   /**
    * A list of messages whose delivery has not been started yet.
    *
    * @return a <i>copy</i> of the the internal storage.
    */
   List undelivered();


   List browse();

   /**
    * Clears unreliable state but not persisted state, so a recovery of the channel is possible
    * TODO really?
    */
   void clear();
}
