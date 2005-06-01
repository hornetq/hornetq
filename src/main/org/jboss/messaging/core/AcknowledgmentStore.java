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
 * An AcknowledgmentStore is a reliable message state repository.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface AcknowledgmentStore
{
   public Serializable getStoreID();

   /**
    * @return a Set containing unacknowledged message IDs. Unacknowledged messages include
    *         ChannelNACKed, NACKed and messages.
    */
   public Set getUnacknowledged(Serializable channelID);

   /**
    * @return true if the specified message has been NACKed by at least one receiver or the
    *         message got a Channel NACK.
    */
   public boolean hasNACK(Serializable channelID, Serializable messageID);

   /**
    * @return a Set receiver IDs or <b>null</b> if is a Channel NACK.
    */
   public Set getNACK(Serializable channelID, Serializable messageID);

   /**
    * @return a Set containg IDs of the receivers for which there are outstanding ACKs in the
    *         store.
    */
   public Set getACK(Serializable channelID, Serializable messageID);

   /**
    * Overwrites currently stored acknowledgments. A positive ACK is ignored, unless the store
    * contains a corresponding NACK or a ChannelNACK. The ACK will cancel the waiting NACK in
    * this case.
    *
    * The metohd can have the lateral effect of removing the message from the acknowledgment store
    * altogether, if all outstanding NACKs are canceled by ACKs.
    * 
    * @param channelID - the ID of the channel tried to deliver the message.
    * @param newState - the new state of the message.
    * @throws Throwable
    */
   public void update(Serializable channelID, Serializable messageID, State newState)
         throws Throwable;

   /**
    * Method to be used for asynchronous positive acknowledgments. A positive acknowlegment
    * submitted this way is explicitely stored, unless there is a waiting NACK.
    *
    * If txID is not null, the acknowledgment is accepted by the store, but not enabled until
    * transaction commits.
    *
    */
   public void acknowledge(Serializable channelID, 
                           Serializable messageID, 
                           Serializable receiverID,
                           String txID)
         throws Throwable;

   /**
    * Remove the message reference from the acknowledgment store, regardless of its outstanding
    * acknowlegments.
    * 
    * @param channelID
    * @param messageID
    * @throws Throwable
    */
   public void remove(Serializable channelID, Serializable messageID) throws Throwable;

   /**
    * Called on txID's commit. Enable delivery on routables that have been waiting for transaction
    * to commit.
    *
    * @param txID - transaction's ID.
    */
   public void commit(Serializable channelID, String txID);

   /**
    * Called on txID's rollback. Discards the routables that have been waiting for transaction
    * to commit.
    *
    * @param txID - transaction's ID.
    */
   public void rollback(Serializable channelID, String txID);
}
