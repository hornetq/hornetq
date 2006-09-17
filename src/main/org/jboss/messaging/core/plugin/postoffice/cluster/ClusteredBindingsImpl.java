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

import org.jboss.messaging.core.plugin.postoffice.Binding;
import org.jboss.messaging.core.plugin.postoffice.BindingsImpl;


/**
 * 
 * A ClusteredBindings
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
class ClusteredBindingsImpl extends BindingsImpl implements ClusteredBindings
{
   // Map <name, router>
   private Map nameMap;
   
   private String thisNode;
   
   private int localDurableCount;
   
   private ClusterRouterFactory rf;
   
   ClusteredBindingsImpl(String thisNode, ClusterRouterFactory rf)
   {
      super();
      
      nameMap = new HashMap();
      
      this.thisNode = thisNode;
      
      this.rf = rf;
   }
   
   public void addBinding(Binding binding)
   {
      super.addBinding(binding);
               
      ClusterRouter router = (ClusterRouter)nameMap.get(binding.getQueue().getName());
      
      if (router == null)
      {
         router = rf.createRouter();
         
         nameMap.put(binding.getQueue().getName(), router);
      }
      
      router.add(binding.getQueue());      
      
      if (binding.getNodeId().equals(thisNode) && binding.getQueue().isRecoverable())
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
           
      ClusterRouter router = (ClusterRouter)nameMap.get(binding.getQueue().getName());
      
      if (router == null)
      {
         throw new IllegalStateException("Cannot find router in name map");
      }
      
      removed = router.remove(binding.getQueue());
      
      if (!removed)
      {
         throw new IllegalStateException("Cannot find binding in list");
      }
      
      if (!router.iterator().hasNext())
      {
         nameMap.remove(binding.getQueue().getName());
      }
      
      if (binding.getNodeId().equals(thisNode) && binding.getQueue().isRecoverable())
      {
         localDurableCount--;
      }      

      return true;
   }
   
   public int getLocalDurableCount()
   {
      return localDurableCount;
   }
   
   public Collection getRouters()
   {
      return nameMap.values();
   }
}
