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

/**
 * An AcknowledgmentStore that stores acknowledgments for a single channel AND for a single
 * Receiver. channelID passed as argument will be ignored. The acknowledgment sets must have
 * at most one elements, and the ReceiverID contained by Acknowledgment instances is ignores.
 *
 * TODO the implementation can be further optimized by keeping Acknowledgments instead of
 * AcknowlegmentsSet in the map. However, I would have to inforce locally the behavior provided
 * by AcknowledgmentSet.
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class SingleReceiverAcknowledgmentStore extends SingleChannelAcknowledgmentStore
{
   // Constants -----------------------------------------------------

   public static final Logger log = Logger.getLogger(SingleReceiverAcknowledgmentStore.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public SingleReceiverAcknowledgmentStore(Serializable storeID)
   {
      super(storeID);
   }

   // AcknowledgmentStore implementation ----------------------------

   public synchronized void update(Serializable channelID, Serializable messageID, Set acks)
         throws Throwable
   {
      // channelID is ignored

      AcknowledgmentSet ackSet = (AcknowledgmentSet)map.get(messageID);

      if (ackSet == null)
      {
         // receiverID from inside the acknowledgment is ignored, may as well be null
         ackSet = new SingleReceiverAcknowledgmentSet();
         map.put(messageID, ackSet);
      }

      ackSet.update(acks);

      if (ackSet.isDeliveryAttempted() && ackSet.size() == 0)
      {
         // the message has been acknowledged by all receivers, delete it from the map
         map.remove(messageID);
      }
   }

   public void acknowledge(Serializable channelID, Serializable messageID, Serializable receiverID)
         throws Throwable
   {
      // channelID is ignored

      AcknowledgmentSet ackSet = (AcknowledgmentSet)map.get(messageID);

      if (ackSet == null)
      {
         // receiverID from inside the acknowledgment is ignored, may as well be null
         ackSet = new SingleReceiverAcknowledgmentSet();
         map.put(messageID, ackSet);
      }

      ackSet.acknowledge(null);
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
