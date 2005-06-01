/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import java.io.Serializable;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface MutableState extends State
{
   /**
    * Submits an ACK. If the corresponding NACK is present, they cancel out. Otherwise, the ACK is
    * stored in the set.
    *
    * If txID is not null, the acknowledgment is accepted, but not enabled until transaction commit.
    */
   public void acknowledge(Serializable receiverID, String txID);

   /**
    * Updates the state.<p>
    *
    * Cancels ACKs if their corresponding NACKs are found and store NACKs for which there are no
    * ACKs. Non-paired ACKs are ignored. To place a ACK in advance, use acknowledge(). If the
    * state update contains TransactionalState instances, they are locally updated.
    */
   public void update(State newState);

   /**
    * Called on txID's commit. Enable delivery on routables that have been waiting for transaction
    * to commit.
    *
    * @param txID - transaction's ID, as it is known to the AcknowledgmentStore.
    */
   public void commit(String txID);

   /**
    * Called on txID's rollback. Discards the routables that have been waiting for transaction
    * to commit.
    *
    * @param txID - transaction's ID, as it is known to the AcknowledgmentStore.
    */
   public void rollback(String txID);
}


