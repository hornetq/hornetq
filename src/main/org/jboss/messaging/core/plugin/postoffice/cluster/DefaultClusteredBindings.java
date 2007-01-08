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
package org.jboss.messaging.core.plugin.postoffice.cluster;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

import org.jboss.messaging.core.plugin.postoffice.Binding;
import org.jboss.messaging.core.plugin.postoffice.DefaultBindings;


/**
 * 
 * A DefaultClusteredBindings
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 *
 */
class DefaultClusteredBindings extends DefaultBindings implements ClusteredBindings
{

   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Map <name, ClusterRouter>
   private Map nameMap;
   private int thisNode;
   private int localDurableCount;

   // Constructors --------------------------------------------------

   DefaultClusteredBindings(int thisNode)
   {
      super();

      nameMap = new HashMap();

      this.thisNode = thisNode;
   }

   // DefaultBindings overrides -------------------------------------

   public void addBinding(Binding binding)
   {
      super.addBinding(binding);

      if (binding.getNodeID() == thisNode && binding.getQueue().isRecoverable())
      {
         localDurableCount++;
      }
   }

   public boolean removeBinding(Binding binding)
   {
      boolean removed = super.removeBinding(binding);

      if (!removed)
      {
         return false;
      }

      if (binding.getNodeID() == thisNode && binding.getQueue().isRecoverable())
      {
         localDurableCount--;
      }

      return true;
   }

   // ClusteredBindings implementation ------------------------------

   public Collection getRouters()
   {
      return nameMap.values();
   }

   public int getLocalDurableCount()
   {
      return localDurableCount;
   }

   public void addRouter(String queueName, ClusterRouter router)
   {
      nameMap.put(queueName, router);
   }

   public void removeRouter(String queueName)
   {
      nameMap.remove(queueName);
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      StringBuffer sb = new StringBuffer();
      sb.append(super.toString());
      sb.append(":ClusteredBindings[");
      for(Iterator i = nameMap.keySet().iterator(); i.hasNext();)
      {
         sb.append(i.next());
         if (i.hasNext())
         {
            sb.append(',');
         }
      }
      sb.append("]");
      return sb.toString();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
