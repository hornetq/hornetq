/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.messaging.core.postoffice.impl;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jboss.messaging.core.postoffice.AddressManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.util.ConcurrentHashSet;
import org.jboss.messaging.util.ConcurrentSet;
import org.jboss.messaging.util.SimpleString;

/**
 * A simple address manager that maintains the addresses and bindings.
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public  class SimpleAddressManager implements AddressManager
{
   private final ConcurrentMap<SimpleString, List<Binding>> mappings = new ConcurrentHashMap<SimpleString, List<Binding>>();

   private final ConcurrentSet<SimpleString> destinations = new ConcurrentHashSet<SimpleString>();

   private final ConcurrentMap<SimpleString, Binding> nameMap = new ConcurrentHashMap<SimpleString, Binding>();

   public void addBinding(Binding binding)
   {
      if (nameMap.putIfAbsent(binding.getQueue().getName(), binding) != null)
      {
         throw new IllegalStateException("Binding already exists " + binding);
      }
   }

   public boolean addMapping(final SimpleString address, final Binding binding)
   {
      List<Binding> bindings = new CopyOnWriteArrayList<Binding>();
      List<Binding> prevBindings = mappings.putIfAbsent(address, bindings);

      if (prevBindings != null)
      {
         bindings = prevBindings;
      }

      bindings.add(binding);
      return prevBindings != null;
   }
   
   public List<Binding> getBindings(final SimpleString address)
   {
      return mappings.get(address);
   }

   public boolean addDestination(final SimpleString address)
   {
      return destinations.addIfAbsent(address);
   }

   public boolean removeDestination(final SimpleString address)
   {
      return destinations.remove(address);
   }

   public boolean containsDestination(final SimpleString address)
   {
      return destinations.contains(address);
   }

   public Set<SimpleString> getDestinations()
   {
      return destinations;
   }

   public Binding getBinding(final SimpleString queueName)
   {
      return nameMap.get(queueName);
   }

   public Map<SimpleString, Binding> getBindings()
   {
      return nameMap;
   }

   public void clear()
   {
      destinations.clear();
      nameMap.clear();
      mappings.clear();
   }

   public Map<SimpleString, List<Binding>> getMappings()
   {
      return mappings;
   }

   public Binding removeBinding(final SimpleString queueName)
   {
      Binding binding = nameMap.remove(queueName);

      if (binding == null)
      {
         throw new IllegalStateException("Queue is not bound " + queueName);
      }
      return binding;
   }

   public boolean removeMapping(final SimpleString address, final SimpleString queueName)
   {
      List<Binding> bindings = mappings.get(address);
      
      Binding binding = removeMapping(queueName, bindings);

      if(bindings.isEmpty())
      {
         mappings.remove(binding.getAddress());
      }
      return bindings.isEmpty();
   }

   protected Binding removeMapping(final SimpleString queueName, final List<Binding> bindings)
   {
      Binding binding = null;
      
      for (Iterator<Binding> iter = bindings.iterator(); iter.hasNext();)
      {
         Binding b = iter.next();

         if (b.getQueue().getName().equals(queueName))
         {
            binding = b;

            break;
         }
      }

      if (binding == null)
      {
         throw new IllegalStateException("Cannot find binding " + queueName);
      }

      bindings.remove(binding);
      
      return binding;
   }
}
