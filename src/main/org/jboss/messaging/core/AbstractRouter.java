/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core;

import org.jboss.messaging.interfaces.Router;
import org.jboss.messaging.interfaces.Message;
import org.jboss.messaging.interfaces.Receiver;

import java.util.Iterator;
import java.util.Map;
import java.util.HashMap;

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
    * <Receiver - Boolean> map, where the boolean instance contains the result of the latest
    * handle() invocation on that Receiver.
    */
   protected Map receivers;

   protected boolean passByReference;

   // Constructors --------------------------------------------------

   protected AbstractRouter()
   {
      receivers = new HashMap();

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
      synchronized(receivers) {
         if (receivers.containsKey(r)) {
            return false;
         }
         receivers.put(r, new Boolean(false));
         return true;
      }
   }

   public boolean remove(Receiver r)
   {
       synchronized(receivers)
       {
          return receivers.remove(r) != null;
       }
   }

   public boolean contains(Receiver r)
   {
      synchronized(receivers)
      {
         return receivers.containsKey(r);
      }
   }

   public Iterator iterator()
   {
      synchronized(receivers)
      {
         return receivers.keySet().iterator();
      }
   }

   public void clear()
   {
      synchronized(receivers)
      {
         receivers.clear();
      }
   }

   public boolean acknowledged(Receiver r)
   {
      Boolean successful;
      synchronized(receivers)
      {
         successful = (Boolean)receivers.get(r);
      }
      if (successful == null)
      {
         return false;
      }
      return successful.booleanValue();
   }

   // Receiver implementation ---------------------------------------

   public abstract boolean handle(Message m);

   // DEBUG ---------------------------------------------------------

   public String dump()
   {
      StringBuffer sb = new StringBuffer("Receivers: ");
      for(Iterator i = iterator(); i.hasNext();)
      {
         sb.append(i.next());
         if (i.hasNext())
         {
            sb.append(", ");
         }
      }
      return sb.toString();
   }
}


