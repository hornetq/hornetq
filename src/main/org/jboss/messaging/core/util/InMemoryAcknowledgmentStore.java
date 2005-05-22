/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.util;

import org.jboss.messaging.core.AcknowledgmentStore;
import org.jboss.messaging.core.Acknowledgment;
import org.jboss.logging.Logger;

import java.io.Serializable;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.Collections;
import java.util.Iterator;
import java.util.HashSet;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class InMemoryAcknowledgmentStore implements AcknowledgmentStore
{
   // Constants -----------------------------------------------------

   public static final Logger log = Logger.getLogger(InMemoryAcknowledgmentStore.class);

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Serializable storeID;
   protected Map map;

   // Constructors --------------------------------------------------

   public InMemoryAcknowledgmentStore(Serializable storeID)
   {
      this.storeID = storeID;
      map = new HashMap();
   }

   // AcknowledgmentStore implementation ----------------------------

   public Serializable getStoreID()
   {
      return storeID;
   }

   public synchronized void update(Serializable channelID,
                                                  Serializable messageID,
                                                  Set acks)
         throws Throwable
   {
      Map channelMap = (Map)map.get(channelID);
      if (channelMap == null)
      {
         channelMap = new HashMap();
         map.put(channelID, channelMap);
      }

      AcknowledgmentSet ackSet = (AcknowledgmentSet)channelMap.get(messageID);

      if (ackSet == null)
      {
         ackSet = new MultipleReceiverAcknowledgmentSet();
         channelMap.put(messageID, ackSet);
      }

      ackSet.update(acks);

      if (ackSet.isDeliveryAttempted() && ackSet.size() == 0)
      {
         // the message has been acknowledged by all receivers, delete it from the map
         channelMap.remove(messageID);
      }
   }


   public void acknowledge(Serializable channelID, Serializable messageID, Serializable receiverID)
         throws Throwable
   {
      Map channelMap = (Map)map.get(channelID);
      if (channelMap == null)
      {
         channelMap = new HashMap();
         map.put(channelID, channelMap);
      }

      AcknowledgmentSet ackSet = (AcknowledgmentSet)channelMap.get(messageID);

      if (ackSet == null)
      {
         ackSet = new MultipleReceiverAcknowledgmentSet();
         channelMap.put(messageID, ackSet);
      }

      ackSet.acknowledge(receiverID);
   }


   public synchronized void remove(Serializable channelID,
                                   Serializable messageID)
         throws Throwable
   {

      Map channelMap = (Map)map.get(channelID);
      if (channelMap == null)
      {
         return;
      }
      channelMap.remove(messageID);
   }


   public Set getUnacknowledged(Serializable channelID)
   {
      Map channelMap = (Map)map.get(channelID);
      if (channelMap == null)
      {
         // TODO - throw an unchecked exception?
         return Collections.EMPTY_SET;
      }

      Set result = Collections.EMPTY_SET;
      for(Iterator i = channelMap.keySet().iterator(); i.hasNext(); )
      {
         Serializable messageID = (Serializable)i.next();
         AcknowledgmentSet s = (AcknowledgmentSet)channelMap.get(messageID);
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
      Map channelMap = (Map)map.get(channelID);
      if (channelMap == null)
      {
         return false;
      }
      AcknowledgmentSet ackSet = (AcknowledgmentSet)channelMap.get(messageID);
      if (ackSet == null)
      {
         return false;
      }
      // there is either the Channel NACK or a set or receiver NACKS.
      return !ackSet.isDeliveryAttempted() || ackSet.nackCount() != 0;
   }


   public Set getNACK(Serializable channelID, Serializable messageID)
   {
      Map channelMap = (Map)map.get(channelID);
      if (channelMap == null)
      {
         return Collections.EMPTY_SET;
      }
      AcknowledgmentSet ackSet = (AcknowledgmentSet)channelMap.get(messageID);
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
      Map channelMap = (Map)map.get(channelID);
      if (channelMap == null)
      {
         return Collections.EMPTY_SET;
      }
      AcknowledgmentSet ackSet = (AcknowledgmentSet)channelMap.get(messageID);
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
