/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import org.jboss.logging.Logger;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.HashSet;
import java.io.Serializable;

/**
 * Does Receiver management but it stops short of enforcing any routing policy, leaving it to
 * subclasses.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public abstract class RouterSupport implements Router
{
   // Attributes ----------------------------------------------------

   /**
    * <ReceiverID - Receiver> map
    */
   protected Map receivers;
   protected Serializable id;
   protected Logger log;

   protected boolean passByReference;



   // Constructors --------------------------------------------------

   protected RouterSupport(Serializable id)
   {
      this.id = id;
      receivers = new HashMap();

      // by default a router passes by reference
      passByReference = true;
   }

   // Public --------------------------------------------------------

   // Router implementation -----------------------------------------

   public Serializable getRouterID()
   {
      return id;
   }

   public Set handle(DeliveryObserver o, Routable r)
   {
      return handle(o, r);
   }

   public boolean isPassByReference()
   {
      return passByReference;
   }

   // Distributor implementation ------------------------------------

   public boolean add(Receiver r)
   {
//      Serializable id = r.getReceiverID();
//      synchronized(this)
//      {
//         if (receivers.containsKey(id)) {
//            return false;
//         }
//         receivers.put(id, r);
//         return true;
//      }
      return false;
   }

   public Receiver get(Serializable receiverID)
   {
      synchronized(this)
      {
         return (Receiver)receivers.get(receiverID);
      }
   }

   public Receiver remove(Serializable receiverID)
   {
       synchronized(this)
       {
          Receiver removed = (Receiver)receivers.remove(receiverID);
          return removed;
       }
   }

   public boolean contains(Serializable receiverID)
   {
      synchronized(this)
      {
         return receivers.containsKey(receiverID);
      }
   }

   public Iterator iterator()
   {
      synchronized(this)
      {
         return receivers.keySet().iterator();
      }
   }

   public void clear()
   {
      synchronized(this)
      {
         receivers.clear();
      }
   }

   // Protected  ----------------------------------------------------

   /**
    * @return the intersection between the current receiverID set and the given receiverID set.
    */
   protected Iterator iterator(Set receiverIDs)
   {
      if (receiverIDs == null)
      {
         return iterator();
      }

      Set result = new HashSet();
      for(Iterator i = receiverIDs.iterator(); i.hasNext();)
      {
         Object o = i.next();
         if (receivers.containsKey(o))
         {
            result.add(o);
         }
         else
         {
            log.warn("receiver " + o + "has disappeared; the message for it will be dropped");
         }
      }
      return result.iterator();
   }



}


