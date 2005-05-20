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
    * The metohd can have the lateral effect of removing the message from the acknowledgment store
    * alltogether, if all outstanding NACKs are canceled by ACKs.
    * 
    * @param channelID - the ID of the channel tried to deliver the message.
    * @param acks - Set of Acknowledgments.
    * @throws Throwable
    */
   public void updateAcknowledgments(Serializable channelID,
                                     Serializable messageID,
                                     Set acks)
         throws Throwable;


   /**
    * Removes the message reference from the acknowledgment store, regardless of its outstanding
    * acknowlegments.
    * 
    * @param channelID
    * @param messageID
    * @throws Throwable
    */
   public void remove(Serializable channelID,
                      Serializable messageID)
         throws Throwable;

   /**
    *
    * @param channelID
    * @return a Set containing unacknowledged message IDs.
    */
   public Set getUnacknowledged(Serializable channelID);


   /**
    * @param channelID
    * @param messageID
    * @return true if the specified message has been NACKed by at least one receiver or the
    *         message got a Channel NACK.
    */
   public boolean hasNACK(Serializable channelID, Serializable messageID);

   /**
    *
    * @param channelID
    * @param messageID
    * @return a Set containing receiver IDs or null if is a Channel NACK.
    */
   public Set getNACK(Serializable channelID, Serializable messageID);

}
