/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.tools;

import org.jboss.messaging.interfaces.MessageSet;
import org.jboss.messaging.interfaces.Routable;

import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

/**
 * Temporary implementation for a MessageSet.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class MessageSetImpl implements MessageSet
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   private Set messages = new HashSet();

   // Public --------------------------------------------------------

   // MessageSet implementation -------------------------------------

   public boolean add(Routable m)
   {
      return messages.add(m);
   }

   public Routable get()
   {

      if (messages.isEmpty()) {
         return null;
      }

      return (Routable)messages.iterator().next();
   }

   public boolean remove(Routable m)
   {
      return messages.remove(m);
   }

   public void lock()
   {
      // TODO

   }

   public void unlock()
   { 
      // TODO

   }

   // DEBUG ---------------------------------------------------------

   public String dump()
   {
       StringBuffer sb = new StringBuffer("{");
       for (Iterator i = messages.iterator(); i.hasNext();)
       {
           Routable m = (Routable)i.next();
           sb.append(m);
           if (i.hasNext())
           {
               sb.append(", ");
           }
       }
       return sb.append("}").toString();
   }
}
