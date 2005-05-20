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
 * TODO What happens with positive ACK placed in the map and not canceled out?
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class MultipleReceiverAcknowledgmentSet  implements AcknowledgmentSet
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // <receiverID - Boolean>
   protected Map acks;

   protected boolean deliveryAttempted;

   // Constructors --------------------------------------------------

   public MultipleReceiverAcknowledgmentSet()
   {
      acks = new HashMap();
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
         deliveryAttempted = true;

         Acknowledgment ack = (Acknowledgment)i.next();
         Serializable receiverID = ack.getReceiverID();
         boolean positive = ack.isPositive();

         if (positive)
         {
            acks.remove(receiverID);
         }
         else
         {
            // negative ACK

            // see if there is a positive ACK waiting for us
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

   public void ACK(Serializable receiverID)
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
      Set s = null;
      for(Iterator i = acks.keySet().iterator(); i.hasNext(); )
      {
         Serializable receiverID = (Serializable)i.next();
         Boolean ack = (Boolean)acks.get(receiverID);
         if (ack.booleanValue() == false)
         {
            if (s == null)
            {
               s = new HashSet();
            }
            s.add(receiverID);
         }
      }
      return s == null ? Collections.EMPTY_SET : s;
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
      Set s = null;
      for(Iterator i = acks.keySet().iterator(); i.hasNext(); )
      {
         Serializable receiverID = (Serializable)i.next();
         Boolean ack = (Boolean)acks.get(receiverID);
         if (ack.booleanValue() == true)
         {
            if (s == null)
            {
               s = new HashSet();
            }
            s.add(receiverID);
         }
      }
      return s == null ? Collections.EMPTY_SET : s;
   }


   public int size()
   {
      return acks.keySet().size();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}


