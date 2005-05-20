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
 * Contains a set of Acknowlegments.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface AcknowledgmentSet
{
   /**
    * Cancels positive acks if their corresponding negative acks are found and store negative
    * acks for which there are no positive acks. Unpaired positive acks are ignored. If you want
    * to place a positive ack in advance, use ACK().
    *
    * Null is equivalent with an empty Set and usually means Channel NACK.
    * @param newAcks - a set of positive/negative acknoweldgments. An empty set means Channel NACK.
    *                  null is also handled as Channel NACK but its use is discouraged.
    */
   public void update(Set newAcks);

   /**
    * Submits a positive ACK. If the corresponding NACK is present, they cancel out. Otherwise,
    * the positive ack is stored in the set.
    * @param receiverID
    */
   public void ACK(Serializable receiverID);

   /**
    * Returns true if at least one receiver saw the message, and returned either a positive or a
    * negative acknowledgment.
    */
   public boolean isDeliveryAttempted();

   public int nackCount();

   /**
    *
    * @return a Set of receiverIDs
    */
   public Set getNACK();

   public int ackCount();

   /**
    *
    * @return a Set of receiverIDs
    */
   public Set getACK();

   /**
    * Returns the total number of stored acknowlegments (positive or negative).
    * @return
    */
   public int size();
}


