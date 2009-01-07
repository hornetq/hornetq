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

import org.jboss.messaging.core.postoffice.AddressManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.Bindings;
import org.jboss.messaging.util.ConcurrentHashSet;
import org.jboss.messaging.util.ConcurrentSet;
import org.jboss.messaging.util.SimpleString;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * A simple address manager that maintains the addresses and bindings.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public  class SimpleAddressManager implements AddressManager
{
   private final ConcurrentMap<SimpleString, Bindings> mappings = new ConcurrentHashMap<SimpleString, Bindings>();

   private final ConcurrentSet<SimpleString> destinations = new ConcurrentHashSet<SimpleString>();

   private final ConcurrentMap<SimpleString, Binding> nameMap = new ConcurrentHashMap<SimpleString, Binding>();

   public void addBinding(final Binding binding)
   {
      if (nameMap.putIfAbsent(binding.getBindable().getName(), binding) != null)
      {
         throw new IllegalStateException("Binding already exists " + binding);
      }
   }

   public boolean addMapping(final SimpleString address, final Binding binding)
   {
      Bindings bindings = mappings.get(address);

      Bindings prevBindings = null;

      if (bindings == null)
      {
         bindings = new BindingsImpl();

         prevBindings = mappings.putIfAbsent(address, bindings);

         if (prevBindings != null)
         {
            bindings = prevBindings;
         }
      }

      bindings.addBinding(binding);

      return prevBindings != null;
   }

   public Bindings getBindings(final SimpleString address)
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

   public Binding getBinding(final SimpleString bindableName)
   {
      return nameMap.get(bindableName);
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

   public Binding removeBinding(final SimpleString bindableName)
   {
      Binding binding = nameMap.remove(bindableName);

      if (binding == null)
      {
         throw new IllegalStateException("Queue is not bound " + bindableName);
      }
      return binding;
   }

   public boolean removeMapping(final SimpleString address, final SimpleString bindableName)
   {
      Bindings bindings = mappings.get(address);

      if (bindings != null)
      {
         removeMapping(bindableName, bindings);

         if (bindings.getBindings().isEmpty())
         {
            mappings.remove(address);
         }

         return true;
      }

      return false;
   }

   protected Binding removeMapping(final SimpleString bindableName, final Bindings bindings)
   {
      Binding theBinding = null;

      for (Binding binding: bindings.getBindings())
      {
         if (binding.getBindable().getName().equals(bindableName))
         {
            theBinding = binding;

            break;
         }
      }

      if (theBinding == null)
      {
         throw new IllegalStateException("Cannot find binding " + bindableName);
      }

      bindings.removeBinding(theBinding);

      return theBinding;
   }
}
