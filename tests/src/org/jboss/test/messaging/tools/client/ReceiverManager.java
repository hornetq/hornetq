/*
* JBoss, Home of Professional Open Source
* Copyright 2005, JBoss Inc., and individual contributors as indicated
* by the @authors tag. See the copyright.txt in the distribution for a
* full listing of individual contributors.
*
* This is free software; you can redistribute it and/or modify it
* under the terms of the GNU Lesser General Public License as
* published by the Free Software Foundation; either version 2.1 of
* the License, or (at your option) any later version.
*
* This software is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
* Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public
* License along with this software; if not, write to the Free
* Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
* 02110-1301 USA, or see the FSF site: http://www.fsf.org.
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

