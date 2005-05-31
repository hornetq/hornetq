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
 * An AcknowledgmentStore is a reliable repository for negative acknowledgments.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public interface AcknowledgmentStore
{
   public Serializable getStoreID();

   /**
    * Overwrites currently stored acknowledgments. A positive ACK is ignored, unless the store
    * contains a corresponding NACK. The ACK will cancel the waiting NACK in this case.
    *
    * The metohd can have the lateral effect of removing the message from the acknowledgment store
    * altogether, if all outstanding NACKs are canceled by ACKs.
    * 
    * @param channelID - the ID of the channel tried to deliver the message.
    * @param acks - Set of Acknowledgments, NonCommitted or null (which means ChannelNACK).
    * @throws Throwable
    */
   public void update(Serializable channelID, Serializable messageID, Set acks)
         throws Throwable;

   /**
    * Method to be used for asynchronous positive acknowledgments. A positive acknowlegment
    * submitted this way is explicitely stored, unless there is a waiting NACK.
    */
   public void acknowledge(Serializable channelID, Serializable messageID, Serializable receiverID)
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
    * @return a Set containing unacknowledged message IDs.
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
    * TODO temporary until refactoring
    */
   public void enableNonCommitted(Serializable channelID, String txID);

   /**
    * TODO temporary until refactoring
    */
   public void discardNonCommitted(Serializable channelID, String txID);

}
