/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.messaging.core;

import java.util.List;

import org.jboss.messaging.core.tx.Transaction;

/**
 * A channel's state.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
interface State
{

   /**
    * @return true if the state is able to store reliable undelivered. It implies transactionality.
    *
    * @see org.jboss.messaging.core.State#isTransactional()
    */
   boolean isReliable();

   void add(Delivery d) throws Throwable;

   /**
    * Works transactionally in presence of a JTA transaction.
    */
   boolean remove(Delivery d, Transaction tx) throws Throwable;

   /**
    * A list of routables in process of being delivered.
    *
    * @return a <i>copy</i> of the internal storage.
    */
   List delivering(Filter filter);

   void add(MessageReference ref, Transaction tx) throws Throwable;

   boolean remove(MessageReference ref) throws Throwable;

   /**
    * A list of routables that are currently NOT being delivered by the channel.
    *
    * @return a <i>copy</i> of the the internal storage.
    */
   List undelivered(Filter filter);

   /**
    * @param filter - may be null, in which case no filter is applied.
    *
    * @return a List containing messages whose state is maintained by this State instance.
    *         The list includes messages in process of being delivered and messages for which
    *         delivery hasn't been attempted yet.
    */
   List browse(Filter filter);

   /**
    * Clears unreliable state but not persisted state, so a recovery of the channel is possible
    * TODO really?
    */
   void clear();
   
}
