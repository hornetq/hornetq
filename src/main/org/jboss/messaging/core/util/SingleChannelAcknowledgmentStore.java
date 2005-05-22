/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.util;

import org.jboss.logging.Logger;
import org.jboss.messaging.core.Acknowledgment;

import java.io.Serializable;
import java.util.Set;
import java.util.Collections;
import java.util.Iterator;
import java.util.HashSet;

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

   public synchronized void update(Serializable channelID, Serializable messageID, Set acks)
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

   public void acknowledge(Serializable channelID, Serializable messageID, Serializable receiverID)
         throws Throwable
   {
      // channelID is ignored

      AcknowledgmentSet ackSet = (AcknowledgmentSet)map.get(messageID);

      if (ackSet == null)
      {
         ackSet = new MultipleReceiverAcknowledgmentSet();
         map.put(messageID, ackSet);
      }

      ackSet.acknowledge(receiverID);
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

      Set result = Collections.EMPTY_SET;
      for(Iterator i = map.keySet().iterator(); i.hasNext(); )
      {
         Serializable messageID = (Serializable)i.next();
         AcknowledgmentSet s = (AcknowledgmentSet)map.get(messageID);
         if (s.nackCount() > 0 || !s.isDeliveryAttempted())
         {
            if (result == Collections.EMPTY_SET)
            {
               result = new HashSet();
            }
            result.add(messageID);
         }
      }
      return result;
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

      Set s = Collections.EMPTY_SET;
      for(Iterator i = ackSet.getNACK().iterator(); i.hasNext(); )
      {
         if (s == Collections.EMPTY_SET)
         {
            s = new HashSet();
         }
         s.add(((Acknowledgment)i.next()).getReceiverID());
      }
      return s;
   }

   public Set getACK(Serializable channelID, Serializable messageID)
   {
      // channelID is ignored

      AcknowledgmentSet ackSet = (AcknowledgmentSet)map.get(messageID);
      if (ackSet == null)
      {
         return Collections.EMPTY_SET;
      }

      Set s = Collections.EMPTY_SET;
      for(Iterator i = ackSet.getACK().iterator(); i.hasNext(); )
      {
         if (s == Collections.EMPTY_SET)
         {
            s = new HashSet();
         }
         s.add(((Acknowledgment)i.next()).getReceiverID());
      }
      return s;
   }



   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
