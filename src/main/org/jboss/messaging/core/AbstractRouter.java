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

import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Does Receiver management but it stop short of enforcing any routing policy, leaving its
 * implementation to subclasses.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public abstract class AbstractRouter implements Router
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Set receivers;

   // Constructors --------------------------------------------------

   protected AbstractRouter() {

      receivers = new HashSet();
   }

   // Public --------------------------------------------------------

   // Distributor implementation ------------------------------------

   public synchronized boolean add(Receiver r) {

       return receivers.add(r);
   }

   public synchronized boolean remove(Receiver r) {

       return receivers.remove(r);
   }

   public synchronized boolean contains(Receiver r) {

       return receivers.contains(r);
   }

   public Iterator iterator() {

       return receivers.iterator();
   }

   // Receiver implementation ---------------------------------------

   public abstract boolean handle(Message m);

   // DEBUG ---------------------------------------------------------

   public String dump() {

      StringBuffer sb = new StringBuffer("Receivers: ");
      for(Iterator i = receivers.iterator(); i.hasNext();)
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


