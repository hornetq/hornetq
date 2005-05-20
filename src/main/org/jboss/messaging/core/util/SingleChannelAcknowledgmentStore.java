/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.util;

import org.jboss.logging.Logger;

import java.io.Serializable;
import java.util.Set;
import java.util.Collections;

/**
 * An AcknowledgmentStore that stores acknowledgments for a single channel. channelID passed as
 * argument will be ignored.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class SingleChannelAcknowledgmentStore extends InMemoryAcknowledgmentStore
{
   // Constants -----------------------------------------------------

   public static final Logger log = Logger.getLogger(SingleChannelAcknowledgmentStore.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public SingleChannelAcknowledgmentStore(Serializable storeID)
   {
      super(storeID);
   }

   // AcknowledgmentStore implementation ----------------------------

   public synchronized void updateAcknowledgments(Serializable channelID,
                                                  Serializable messageID,
                                                  Set acks)
         throws Throwable
   {
      // channelID is ignored

      AcknowledgmentSet ackSet = (AcknowledgmentSet)map.get(messageID);

      if (ackSet == null)
      {
         ackSet = new MultipleReceiverAcknowledgmentSet();
         map.put(messageID, ackSet);
      }

      ackSet.update(acks);

      if (ackSet.isDeliveryAttempted() && ackSet.size() == 0)
      {
         // the message has been acknowledged by all receivers, delete it from the map
         map.remove(messageID);
      }
   }

   public synchronized void remove(Serializable channelID,
                                   Serializable messageID)
         throws Throwable
   {
      // channelID is ignored

      map.remove(messageID);
   }


   public Set getUnacknowledged(Serializable channelID)
   {
      // channelID is ignored

      return map.keySet();
   }

   public boolean hasNACK(Serializable channelID, Serializable messageID)
   {
      // channelID is ignored

      AcknowledgmentSet ackSet = (AcknowledgmentSet)map.get(messageID);
      if (ackSet == null)
      {
         return false;
      }
      // there is either the Channel NACK or a set or receiver NACKS.
      return !ackSet.isDeliveryAttempted() || ackSet.nackCount() != 0;
   }


   public Set getNACK(Serializable channelID, Serializable messageID)
   {
      // channelID is ignored

      AcknowledgmentSet ackSet = (AcknowledgmentSet)map.get(messageID);
      if (ackSet == null)
      {
         return Collections.EMPTY_SET;
      }

      if (!ackSet.isDeliveryAttempted())
      {
         return null;
      }
      return ackSet.getNACK();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
