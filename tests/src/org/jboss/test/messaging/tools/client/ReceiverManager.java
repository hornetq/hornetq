/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.tools.client;

import org.jboss.messaging.core.Receiver;
import org.jboss.test.messaging.core.SimpleReceiver;

import java.util.Set;
import java.util.HashSet;
import java.util.Iterator;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class ReceiverManager
{
   // Attributes ----------------------------------------------------

   private Set receivers = new HashSet();

   // Public --------------------------------------------------------

   public Receiver getReceiver(String name)
   {
      if (name == null)
      {
         return null;
      }
      for(Iterator i = receivers.iterator(); i.hasNext();)
      {
         SimpleReceiver r = (SimpleReceiver)i.next();
         if (name.equals(r.getName()))
         {
            return r;
         }
      }
      SimpleReceiver r = new SimpleReceiver(name);
      receivers.add(r);
      return r;
   }

   public String dump()
   {
      StringBuffer sb = new StringBuffer("{");
      for(Iterator i = receivers.iterator(); i.hasNext();)
      {
         SimpleReceiver r = (SimpleReceiver)i.next();
         sb.append(r.toString());
         if (i.hasNext())
         {
            sb.append(", ");
         }
      }
      return sb.append("}").toString();
   }
}

