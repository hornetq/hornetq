/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import org.jboss.messaging.interfaces.Router;
import org.jboss.messaging.interfaces.Routable;
import org.jboss.messaging.interfaces.Receiver;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;
import java.io.Serializable;

/**
 * Does Receiver management but it stops short of enforcing any routing policy, leaving its
 * implementation to subclasses.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public abstract class AbstractRouter implements Router
{
   // Attributes ----------------------------------------------------

   /**
    * <ReceiverID - Receiver> map
    */
   protected Map receivers;

   /**
    * <Receiver - Boolean> map, where the boolean instance contains the result of the latest
    * handle() invocation on that Receiver.
    */
   protected Map acknowledgments;

   protected Serializable id;

   protected boolean passByReference;

   // Constructors --------------------------------------------------

   protected AbstractRouter(Serializable id)
   {
      this.id = id;
      receivers = new HashMap();
      acknowledgments = new HashMap();

      // by default a router passes by reference
      passByReference = true;
   }

   // Public --------------------------------------------------------

   // Router implementation -----------------------------------------

   public boolean isPassByReference()
   {
      return passByReference;
   }

   // Distributor implementation ------------------------------------

   public boolean add(Receiver r)
   {
      Serializable id = r.getReceiverID();
      synchronized(this)
      {
         if (receivers.containsKey(id)) {
            return false;
         }
         receivers.put(id, r);
         acknowledgments.put(id, new Boolean(false));
         return true;
      }
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
          acknowledgments.remove(receiverID);
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
         acknowledgments.clear();
      }
   }

   public boolean acknowledged(Serializable receiverID)
   {
      Boolean successful;
      synchronized(this)
      {
         successful = (Boolean)acknowledgments.get(receiverID);
      }
      if (successful == null)
      {
         return false;
      }
      return successful.booleanValue();
   }

   // Receiver implementation ---------------------------------------

   public Serializable getReceiverID()
   {
      return id;
   }

   public abstract boolean handle(Routable m);

}


