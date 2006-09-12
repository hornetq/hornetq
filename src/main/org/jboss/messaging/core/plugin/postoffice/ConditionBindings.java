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
package org.jboss.messaging.core.plugin.postoffice;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.jboss.messaging.core.plugin.contract.Binding;

/**
 * A ConditionBindings
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class ConditionBindings
{
   private List allBindings;
   
   private List durableBindings;
   
   private List nonDurableBindings;
   
   // Map <name, binding or list of bindings>
   private Map nameMap;
   
   private String thisNode;
   
   private int localDurableCount;
   
   public ConditionBindings(String thisNode)
   {
      allBindings = new ArrayList();
      
      durableBindings = new ArrayList();
      
      nonDurableBindings = new ArrayList();
      
      this.thisNode = thisNode;
   }
   
   public void addBinding(Binding binding)
   {
      if (allBindings.contains(binding))
      {
         throw new IllegalArgumentException("Bindings already contains binding: " + binding);
      }
               
      allBindings.add(binding);
      
      if (binding.isDurable())
      {
         durableBindings.add(binding);
      }
      else
      {
         nonDurableBindings.add(binding);
      }
      
      List bindings = (List)nameMap.get(binding.getQueueName());
      
      if (bindings == null)
      {
         bindings = new ArrayList();
         
         nameMap.put(binding.getQueueName(), bindings);
      }
      
      bindings.add(binding);      
      
      if (binding.isDurable() && binding.getNodeId().equals(thisNode))
      {
         localDurableCount++;
      }
   }
   
   public boolean removeBinding(Binding binding)
   {
      boolean removed = allBindings.remove(binding);
      
      if (!removed)
      {
         return false;
      }
      
      if (binding.isDurable())
      {
         durableBindings.remove(binding);
      }
      else
      {
         nonDurableBindings.remove(binding);
      }
      
      List bindings = (List)nameMap.get(binding.getQueueName());
      
      if (bindings == null)
      {
         throw new IllegalStateException("Cannot find bindins in name map");
      }
      
      removed = bindings.remove(binding);
      
      if (!removed)
      {
         throw new IllegalStateException("Cannot find binding in list");
      }
      
      if (bindings.isEmpty())
      {
         nameMap.remove(binding.getQueueName());
      }
      
      if (binding.isDurable() && binding.getNodeId().equals(thisNode))
      {
         localDurableCount--;
      }
      
      return true;
   }
   
   public Collection getBindingsByName()
   {
      return nameMap.values();
   }
   
   public boolean isEmpty()
   {
      return nameMap.isEmpty();
   }
   
   public List getAllBindings()
   {
      return allBindings;
   }
   
   public int getLocalDurableCount()
   {
      return this.localDurableCount;
   }
   
   public int getDurableCount()
   {
      return this.durableBindings.size();
   }
   
   public int getRemoteDurableCount()
   {
      return getDurableCount() - getLocalDurableCount();
   }
   
      
}
