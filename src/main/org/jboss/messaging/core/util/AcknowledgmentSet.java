/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.util;

import java.util.Set;
import java.io.Serializable;

/**
 * Contains a set of Acknowlegments or NonCommitted.
 *
 * TODO: refactor into a coherent State representation (Acknowledgment, ChannelNACK, NonCommitted)
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface AcknowledgmentSet
{
   /**
    * Returns true if at least one receiver saw the message, and returned either a positive or a
    * negative acknowledgment, false for a Channel NACK.
    */
   public boolean isDeliveryAttempted();

   /**
    * Cancels positive acks if their corresponding negative acks are found and store negative
    * acks for which there are no positive acks. Unpaired positive acks are ignored. If you want
    * to place a positive ack in advance, use acknowledge().
    *
    * Null is equivalent with an empty Set and usually means Channel NACK.
    * @param newAcks - a set of positive/negative acknowledgments or NonCommitted instances. An
    *                  empty set means Channel NACK. null is also handled as Channel NACK but its
    *                  use is discouraged.
    */
   public void update(Set newAcks);

   /**
    * Submits a positive ACK. If the corresponding NACK is present, they cancel out. Otherwise,
    * the positive ack is stored in the set.
    * @param receiverID
    */
   public void acknowledge(Serializable receiverID);

   /**
    * Does not count a Channel NACK.
    */
   public int nackCount();

   /**
    *
    * @return a Set of Acknowledgments
    */
   public Set getNACK();

   public int ackCount();

   /**
    *
    * @return a Set of Acknowledgments
    */
   public Set getACK();

   /**
    * Returns the total number of stored acknowlegments (positive or negative). Does not include
    * a Channel NACK.
    * @return
    */
   public int size();

   /**
    * TODO temporary until refactoring
    */
   public void enableNonCommitted(String txID);

   /**
    * TODO temporary until refactoring
    */
   public void discardNonCommitted(String txID);

   /**
    * TODO temporary until refactoring
    */
   public boolean hasNonCommitted();

}


