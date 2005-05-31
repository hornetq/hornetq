/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.util;


import org.jboss.messaging.core.Acknowledgment;

import java.util.Set;
import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Collections;
import java.io.Serializable;

/**
 * Contains a set of Acknowlegments that can be sent by multiple receivers.
 *
 * TODO Do I need to synchronize access to a AcknowledgmentSet instance?
 *
 * TODO What happens with positive acknowledge placed in the map and not canceled out?
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class MultipleReceiverAcknowledgmentSet implements AcknowledgmentSet
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // <receiverID - Boolean>
   protected Map acks;

   // <NonCommitted>
   protected Set nonCommitted;

   protected boolean deliveryAttempted;

   // Constructors --------------------------------------------------

   public MultipleReceiverAcknowledgmentSet()
   {
      acks = new HashMap();
      nonCommitted = new HashSet();
      deliveryAttempted = false;
   }

   // Public --------------------------------------------------------

   public void update(Set newAcks)
   {
      if (newAcks == null)
      {
         // Channel NACK 
         return;
      }

      for(Iterator i = newAcks.iterator(); i.hasNext(); )
      {

         Object o = i.next();

         if (o instanceof NonCommitted)
         {
            nonCommitted.add(o);
            continue;
         }

         deliveryAttempted = true;

         Acknowledgment ack = (Acknowledgment)o;
         Serializable receiverID = ack.getReceiverID();
         boolean positive = ack.isPositive();

         if (positive)
         {
            acks.remove(receiverID);
         }
         else
         {
            // negative acknowledge

            // see if there is a positive acknowledge waiting for us
            Boolean storedAck = (Boolean)acks.get(receiverID);
            if (storedAck != null && storedAck.booleanValue())
            {
               acks.remove(receiverID); // acks cancel each other
            }
            else if (storedAck == null)
            {
               acks.put(receiverID, Boolean.FALSE);
            }
         }
      }
   }

   public void acknowledge(Serializable receiverID)
   {
      deliveryAttempted = true;

      Boolean ack = (Boolean)acks.get(receiverID);
      if (ack == null)
      {
         acks.put(receiverID, Boolean.TRUE);
      }
      else if (ack.booleanValue() == false)
      {
         acks.remove(receiverID);
      }
   }

   public boolean isDeliveryAttempted()
   {
      return deliveryAttempted;
   }

   public int nackCount()
   {
      int cnt = 0;
      for(Iterator i = acks.values().iterator(); i.hasNext(); )
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
      for(Iterator i = acks.keySet().iterator(); i.hasNext(); )
      {
         Serializable receiverID = (Serializable)i.next();
         Boolean ack = (Boolean)acks.get(receiverID);
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
      for(Iterator i = acks.values().iterator(); i.hasNext(); )
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
      for(Iterator i = acks.keySet().iterator(); i.hasNext(); )
      {
         Serializable receiverID = (Serializable)i.next();
         Boolean ack = (Boolean)acks.get(receiverID);
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
      return acks.keySet().size();
   }

   public void enableNonCommitted(String txID)
   {
      for(Iterator i = nonCommitted.iterator(); i.hasNext(); )
      {
         NonCommitted nc = (NonCommitted)i.next();
         if (txID.equals(nc.getTxID()))
         {
            i.remove();
         }
      }
   }

   public void discardNonCommitted(String txID)
   {
      for(Iterator i = nonCommitted.iterator(); i.hasNext(); )
      {
         NonCommitted nc = (NonCommitted)i.next();
         if (txID.equals(nc.getTxID()))
         {
            i.remove();
         }
      }

      // make this set ready to be discarded at sweep
      // deliveryAttempted = true and ackSet.size() == 0
      if (nonCommitted.isEmpty() && !deliveryAttempted)
      {
         deliveryAttempted = true;
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


