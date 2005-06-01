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

import java.util.Set;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Collections;
import java.io.Serializable;

/**
 * Generic implementation of a MutableState that can maintain Acknowlegments sent by
 * multiple receivers.
 *
 * TODO Do I need to synchronize access to a State instance?  Or have it implement Lockable?
 *
 * TODO What happens with positive acknowledge placed in the map and not canceled out?
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class StateImpl implements MutableState
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // <receiverID - Boolean>
   protected Map acknowledgments;

   // <receiverID - Set<String>>, where Strings are txIDs.
   protected Map nonCommittedACK;

   // <NonCommitted>
   protected Set nonCommitted;

   // true if no delivery was attempted for this message
   protected boolean channelNACK;

   // Constructors --------------------------------------------------

   public StateImpl()
   {
      acknowledgments = new HashMap();
      nonCommitted = new HashSet();
      nonCommittedACK = new HashMap();
      channelNACK = true;
   }

   public StateImpl(String nonCommittedTxID)
   {
      this();
      nonCommitted.add(new NonCommitted(nonCommittedTxID));
   }

   public StateImpl(Acknowledgment a)
   {
      this();
      if (a != null)
      {
         this.acknowledgments.put(a.getReceiverID(), a.isPositive() ? Boolean.TRUE : Boolean.FALSE);
         channelNACK = false;
      }
   }

   public StateImpl(Set acks)
   {
      this(acks, null);
   }

   public StateImpl(Set acks, Set nonCommitted)
   {
      this();
      if (acks != null)
      {
         for(Iterator i = acks.iterator(); i.hasNext();)
         {
            Acknowledgment a = (Acknowledgment)i.next();
            this.acknowledgments.put(a.getReceiverID(),
                                     a.isPositive() ? Boolean.TRUE : Boolean.FALSE);
            channelNACK = false;
         }
      }
      if (nonCommitted != null)
      {
         for(Iterator i = nonCommitted.iterator(); i.hasNext();)
         {
            NonCommitted nc = (NonCommitted)i.next();
            this.nonCommitted.add(nc);
         }
      }
   }


   // State implementation ------------------------------------------

   public boolean isChannelNACK()
   {
      return channelNACK;
   }

   public int nackCount()
   {
      int cnt = 0;
      for(Iterator i = acknowledgments.values().iterator(); i.hasNext(); )
      {
         if (((Boolean)i.next()).booleanValue() == false)
         {
            cnt++;
         }
      }
      return cnt;
   }

   public Set getNACK()
   {
      Set s = Collections.EMPTY_SET;
      for(Iterator i = acknowledgments.keySet().iterator(); i.hasNext(); )
      {
         Serializable receiverID = (Serializable)i.next();
         Boolean ack = (Boolean)acknowledgments.get(receiverID);
         if (ack.booleanValue() == false)
         {
            if (s == Collections.EMPTY_SET)
            {
               s = new HashSet();
            }
            s.add(new AcknowledgmentImpl(receiverID, false));
         }
      }
      return s;
   }

   public int ackCount()
   {
      int cnt = 0;
      for(Iterator i = acknowledgments.values().iterator(); i.hasNext(); )
      {
         if (((Boolean)i.next()).booleanValue() == true)
         {
            cnt++;
         }
      }
      return cnt;
   }

   public Set getACK()
   {
      Set s = Collections.EMPTY_SET;
      for(Iterator i = acknowledgments.keySet().iterator(); i.hasNext(); )
      {
         Serializable receiverID = (Serializable)i.next();
         Boolean ack = (Boolean)acknowledgments.get(receiverID);
         if (ack.booleanValue() == true)
         {
            if (s == Collections.EMPTY_SET)
            {
               s = new HashSet();
            }
            s.add(new AcknowledgmentImpl(receiverID, true));
         }
      }
      return s;
   }

   public int size()
   {
      return acknowledgments.keySet().size();
   }

   public int nonCommittedCount()
   {
      return nonCommitted.size();
   }

   public Set getNonCommitted()
   {
      if (nonCommitted.isEmpty())
      {
         return Collections.EMPTY_SET;
      }
      Set s = new HashSet();
      for(Iterator i = nonCommitted.iterator(); i.hasNext();)
      {
         s.add(i.next());
      }
      return s;
   }

   // MutableState implementation -----------------------------------

   public void acknowledge(Serializable receiverID, String txID)
   {
      if (txID != null)
      {
         // transacted acknowledgment

         Set s = (Set)nonCommittedACK.get(receiverID);
         if (s == null)
         {
            s = new HashSet();
            nonCommittedACK.put(receiverID, s);
         }
         s.add(txID);
         return;
      }

      // non-transacted acknowledgment

      channelNACK = false;

      Boolean ack = (Boolean)acknowledgments.get(receiverID);
      if (ack == null)
      {
         acknowledgments.put(receiverID, Boolean.TRUE);
      }
      else if (ack.booleanValue() == false)
      {
         acknowledgments.remove(receiverID);
      }
   }

   public void update(State state)
   {

      Set nacks = state.getNACK();
      for(Iterator i = nacks.iterator(); i.hasNext(); )
      {
         Acknowledgment nack = (Acknowledgment)i.next();
         Serializable receiverID = nack.getReceiverID();
         boolean positive = nack.isPositive();
         if (positive)
         {
            throw new IllegalStateException("Expecting a NEGATIVE ACK for " + receiverID);
         }

         channelNACK = false;

         // see if there is a positive acknowledgement waiting for us
         Boolean storedAck = (Boolean)acknowledgments.get(receiverID);
         if (storedAck != null && storedAck.booleanValue())
         {
            acknowledgments.remove(receiverID); // acks cancel each other
         }
         else if (storedAck == null)
         {
            acknowledgments.put(receiverID, Boolean.FALSE);
         }
      }

      Set acks = state.getACK();
      for(Iterator i = acks.iterator(); i.hasNext(); )
      {
         Acknowledgment ack = (Acknowledgment)i.next();
         Serializable receiverID = ack.getReceiverID();
         boolean positive = ack.isPositive();
         if (!positive)
         {
            throw new IllegalStateException("Expecting a POSITIVE ACK for " + receiverID);
         }

         // if there is already an ACK, this is a noop, otherwise cancel the NACK
         Boolean storedAck = (Boolean)acknowledgments.get(receiverID);
         if (storedAck != null && storedAck.booleanValue())
         {
            continue;
         }
         acknowledgments.remove(receiverID);
         channelNACK = false;
      }

      Set ncs = state.getNonCommitted();
      if (ncs.size() > 0 && !channelNACK)
      {
         throw new IllegalStateException("Cannot transact a message that " +
                                         "has already been delivered");
      }
      for(Iterator i = ncs.iterator(); i.hasNext(); )
      {

         NonCommitted nc = (NonCommitted)i.next();
         nonCommitted.add(nc);
      }
   }

   public void commit(String txID)
   {
      // "deliver" transacted messages
      for(Iterator i = nonCommitted.iterator(); i.hasNext(); )
      {
         NonCommitted nc = (NonCommitted)i.next();
         if (txID.equals(nc.getTxID()))
         {
            i.remove();
         }
      }
      // enable transacted acknowledgments
      for(Iterator i = nonCommittedACK.keySet().iterator(); i.hasNext(); )
      {
         Serializable receiverID = (Serializable)i.next();
         Set s = (Set)nonCommittedACK.get(receiverID);
         for(Iterator j = s.iterator(); j.hasNext(); )
         {
            String ncTxID = (String)j.next();
            if (ncTxID.equals(txID))
            {
               j.remove();
               acknowledge(receiverID, null);
            }
         }
      }
   }

   public void rollback(String txID)
   {
      // drop transacted messages
      boolean dropped = false;
      for(Iterator i = nonCommitted.iterator(); i.hasNext(); )
      {
         NonCommitted nc = (NonCommitted)i.next();
         if (txID.equals(nc.getTxID()))
         {
            i.remove();
            dropped = true;
         }
      }

      // make this set ready to be discarded at sweep
      // channelNACK = false and state.size() == 0
      if (nonCommitted.isEmpty() && dropped && channelNACK)
      {
         channelNACK = false;
      }

      // drop transacted acknowledgments
      for(Iterator i = nonCommittedACK.keySet().iterator(); i.hasNext(); )
      {
         Serializable receiverID = (Serializable)i.next();
         Set s = (Set)nonCommittedACK.get(receiverID);
         for(Iterator j = s.iterator(); j.hasNext(); )
         {
            String ncTxID = (String)j.next();
            if (ncTxID.equals(txID))
            {
               j.remove();
            }
         }
      }
   }

   public boolean hasNonCommitted()
   {
      return !nonCommitted.isEmpty();
   }
   


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}


