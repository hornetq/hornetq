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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.postoffice.Address;
import org.jboss.messaging.core.postoffice.AddressManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.Bindings;
import org.jboss.messaging.utils.SimpleString;

/**
 * A simple address manager that maintains the addresses and bindings.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class SimpleAddressManager implements AddressManager
{
   private static final Logger log = Logger.getLogger(SimpleAddressManager.class);

   private final ConcurrentMap<SimpleString, Bindings> mappings = new ConcurrentHashMap<SimpleString, Bindings>();

   private final ConcurrentMap<SimpleString, Binding> nameMap = new ConcurrentHashMap<SimpleString, Binding>();

   public boolean addBinding(final Binding binding)
   {
      if (nameMap.putIfAbsent(binding.getUniqueName(), binding) != null)
      {
         throw new IllegalStateException("Binding already exists " + binding);         
      }
      
      return addMappingInternal(binding.getAddress(), binding);
   }

   public Binding removeBinding(final SimpleString uniqueName)
   {
      Binding binding = nameMap.remove(uniqueName);

      if (binding == null)
      {
         return null;
      }

      removeBindingInternal(binding.getAddress(), uniqueName);
      
      return binding;
   }

   public Bindings getBindingsForRoutingAddress(final SimpleString address)
   {
      return mappings.get(address);
   }

   public Binding getBinding(final SimpleString bindableName)
   {
      return nameMap.get(bindableName);
   }

   public Map<SimpleString, Binding> getBindings()
   {
      return nameMap;
   }
   
   public Bindings getMatchingBindings(final SimpleString address)
   {
      Address add = new AddressImpl(address);
      
      Bindings bindings = new BindingsImpl();
      
      for (Binding binding: nameMap.values())
      {
         Address addCheck = new AddressImpl(binding.getAddress());
         
         if (addCheck.matches(add))
         {
            bindings.addBinding(binding);
         }
      }
      
      return bindings;
   }
   
   
   public void clear()
   {
      nameMap.clear();
      mappings.clear();
   }

   protected void removeBindingInternal(final SimpleString address, final SimpleString bindableName)
   {
      Bindings bindings = mappings.get(address);

      if (bindings != null)
      {
         removeMapping(bindableName, bindings);

         if (bindings.getBindings().isEmpty())
         {
            mappings.remove(address);
         }
      }
   }

   protected Binding removeMapping(final SimpleString bindableName, final Bindings bindings)
   {
      Binding theBinding = null;

      for (Binding binding: bindings.getBindings())
      {
         if (binding.getUniqueName().equals(bindableName))
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

   protected boolean addMappingInternal(final SimpleString address, final Binding binding)
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
}
