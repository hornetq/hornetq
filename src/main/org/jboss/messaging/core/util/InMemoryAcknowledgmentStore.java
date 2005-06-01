/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.util;

import org.jboss.messaging.core.Acknowledgment;
import org.jboss.messaging.core.State;
import org.jboss.messaging.core.MutableState;
import org.jboss.messaging.core.AcknowledgmentStore;
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

   public synchronized void update(Serializable channelID, Serializable messageID, State newState)
         throws Throwable
   {
      Map channelMap = ensureChannelMap(channelID);
      update(channelMap, messageID, newState);
   }

   public void acknowledge(Serializable channelID, 
                           Serializable messageID, 
                           Serializable receiverID,
                           String txID)
         throws Throwable
   {
      Map channelMap = ensureChannelMap(channelID);
      acknowledge(channelMap, messageID, receiverID, txID);
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
      remove(channelMap, messageID);
   }

   public Set getUnacknowledged(Serializable channelID)
   {
      Map channelMap = (Map)map.get(channelID);
      if (channelMap == null)
      {
         // TODO - throw an unchecked exception?
         return Collections.EMPTY_SET;
      }
      return getUnacknowledged(channelMap);
   }

   public boolean hasNACK(Serializable channelID, Serializable messageID)
   {
      Map channelMap = (Map)map.get(channelID);
      if (channelMap == null)
      {
         return false;
      }
      return hasNACK(channelMap, messageID);
   }


   public Set getNACK(Serializable channelID, Serializable messageID)
   {
      Map channelMap = (Map)map.get(channelID);
      if (channelMap == null)
      {
         return Collections.EMPTY_SET;
      }
      return getNACK(channelMap, messageID);
   }


   public Set getACK(Serializable channelID, Serializable messageID)
   {
      Map channelMap = (Map)map.get(channelID);
      if (channelMap == null)
      {
         return Collections.EMPTY_SET;
      }
      return getACK(channelMap, messageID);
   }

   public void commit(Serializable channelID, String txID)
   {
      Map channelMap = (Map)map.get(channelID);
      if (channelMap == null)
      {
         return;
      }
      commit(channelMap, txID);
   }

   public void rollback(Serializable channelID, String txID)
   {
      Map channelMap = (Map)map.get(channelID);
      if (channelMap == null)
      {
         return;
      }
      rollback(channelMap, txID);
   }


   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   /**
    * @param channelMap <messageID - State>
    */
   protected void update(Map channelMap, Serializable messageID, State newState)
   {
      MutableState state = ensureState(channelMap, messageID);
      state.update(newState);

      if (!state.isChannelNACK() && state.size() == 0)
      {
         // the message has been acknowledged by all receivers, delete it from the map
         channelMap.remove(messageID);
      }
   }

   /**
    * @param channelMap <messageID - State>
    */
   protected void acknowledge(Map channelMap, 
                              Serializable messageID, 
                              Serializable receiverID,
                              String txID)
   {
      MutableState state = ensureState(channelMap, messageID);
      state.acknowledge(receiverID, txID);
   }

   protected void remove(Map channelMap, Serializable messageID)
   {
      channelMap.remove(messageID);
   }

   protected Set getUnacknowledged(Map channelMap)
   {
      Set result = Collections.EMPTY_SET;
      for(Iterator i = channelMap.keySet().iterator(); i.hasNext(); )
      {
         Serializable messageID = (Serializable)i.next();
         State s = (State)channelMap.get(messageID);
         if (s.nackCount() > 0 || (s.isChannelNACK() && s.nonCommittedCount() == 0))
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

   protected boolean hasNACK(Map channelMap, Serializable messageID)
   {
      State s = (State)channelMap.get(messageID);
      if (s == null)
      {
         return false;
      }
      // there is either the Channel NACK or a set or receiver NACKS.
      return s.isChannelNACK() || s.nackCount() != 0;
   }

   protected Set getNACK(Map channelMap, Serializable messageID)
   {
      State state = (State)channelMap.get(messageID);
      if (state == null)
      {
         return Collections.EMPTY_SET;
      }

      if (state.isChannelNACK())
      {
         return null;
      }

      Set s = Collections.EMPTY_SET;
      for(Iterator i = state.getNACK().iterator(); i.hasNext(); )
      {
         if (s == Collections.EMPTY_SET)
         {
            s = new HashSet();
         }
         s.add(((Acknowledgment)i.next()).getReceiverID());
      }
      return s;
   }

   protected Set getACK(Map channelMap, Serializable messageID)
   {
      State state = (State)channelMap.get(messageID);
      if (state == null)
      {
         return Collections.EMPTY_SET;
      }

      Set s = Collections.EMPTY_SET;
      for(Iterator i = state.getACK().iterator(); i.hasNext(); )
      {
         if (s == Collections.EMPTY_SET)
         {
            s = new HashSet();
         }
         s.add(((Acknowledgment)i.next()).getReceiverID());
      }
      return s;
   }

   protected void commit(Map channelMap, String txID)
   {
      for(Iterator i = channelMap.keySet().iterator(); i.hasNext();)
      {
         Serializable messageID = (Serializable)i.next();
         MutableState state = (MutableState)channelMap.get(messageID);
         if (state == null)
         {
            continue;
         }
         state.commit(txID);
      }
   }

   protected void rollback(Map channelMap, String txID)
   {
      for(Iterator i = channelMap.keySet().iterator(); i.hasNext();)
      {
         Serializable messageID = (Serializable)i.next();
         MutableState state = (MutableState)channelMap.get(messageID);
         if (state == null)
         {
            continue;
         }
         state.rollback(txID);
         if (!state.isChannelNACK() && state.size() == 0)
         {
            i.remove();
         }
      }
   }

   // Private -------------------------------------------------------

   private Map ensureChannelMap(Serializable channelID)
   {
      Map channelMap = (Map)map.get(channelID);
      if (channelMap == null)
      {
         channelMap = new HashMap();
         map.put(channelID, channelMap);
      }
      return channelMap;
   }

   private MutableState ensureState(Map channelMap, Serializable messageID)
   {
      MutableState state = (MutableState)channelMap.get(messageID);
      if (state == null)
      {
         state = new StateImpl();
         channelMap.put(messageID, state);
      }
      return state;
   }

   // Inner classes -------------------------------------------------
}
