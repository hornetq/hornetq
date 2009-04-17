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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.postoffice.Address;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.Bindings;
import org.jboss.messaging.utils.SimpleString;

/**
 * extends the simple manager to allow wildcard addresses to be used.
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class WildcardAddressManager extends SimpleAddressManager
{
   private static final Logger log = Logger.getLogger(WildcardAddressManager.class);

   static final char SINGLE_WORD = '*';

   static final char ANY_WORDS = '#';

   static final char DELIM = '.';

   static final SimpleString SINGLE_WORD_SIMPLESTRING = new SimpleString("*");

   static final SimpleString ANY_WORDS_SIMPLESTRING = new SimpleString("#");

   /**
    * These are all the addresses, we use this so we can link back from the actual address to its linked wilcard addresses
    * or vice versa
    */
   private final Map<SimpleString, Address> addresses = new HashMap<SimpleString, Address>();

   private final Map<SimpleString, Address> wildCardAddresses = new HashMap<SimpleString, Address>();

   public Bindings getBindingsForRoutingAddress(final SimpleString address)
   {
      Bindings bindings = super.getBindingsForRoutingAddress(address);
      
      //this should only happen if we're routing to an address that has no mappings when we're running checkAllowable
      if (bindings == null && !wildCardAddresses.isEmpty())
      {
         Address add = addAndUpdateAddressMap(address);
         if (!add.containsWildCard())
         {
            for (Address destAdd : add.getLinkedAddresses())
            {
               Bindings b = super.getBindingsForRoutingAddress(destAdd.getAddress());
               if (b != null)
               {
                  Collection<Binding> theBindings = b.getBindings();
                  for (Binding theBinding : theBindings)
                  {
                     super.addMappingInternal(address, theBinding);
                  }
               }
            }
         }
         bindings = super.getBindingsForRoutingAddress(address);
      }
      return bindings;
   }
      
   /**
    * If the address to add the binding to contains a wildcard then a copy of the binding (with the same underlying queue)
    * will be added to the actual mappings. Otherwise the binding is added as normal.
    *
    * @param binding the binding to add
    * @return true if the address was a new mapping
    */
   public boolean addBinding(final Binding binding)
   {
      boolean exists = super.addBinding(binding);
      if (!exists)
      {
         Address add = addAndUpdateAddressMap(binding.getAddress());
         if (add.containsWildCard())
         {
            for (Address destAdd : add.getLinkedAddresses())
            {
               super.addMappingInternal(destAdd.getAddress(), binding);
            }
         }
         else
         {
            for (Address destAdd : add.getLinkedAddresses())
            {
               Bindings bindings = super.getBindingsForRoutingAddress(destAdd.getAddress());
               for (Binding b : bindings.getBindings())
               {
                  super.addMappingInternal(binding.getAddress(), b);
               }
            }
         }
      }
      return exists;
   }


   /**
    * If the address is a wild card then the binding will be removed from the actual mappings for any linked address.
    * otherwise it will be removed as normal.
    *
    * @param uniqueName the name of the binding to remove
    * @return true if this was the last mapping for a specific address
    */
   public Binding removeBinding(final SimpleString uniqueName)
   {
      Binding binding = super.removeBinding(uniqueName);
      if (binding != null)
      {
         Address add = getAddress(binding.getAddress());
         if (!add.containsWildCard())
         {
            for (Address destination : add.getLinkedAddresses())
            {
               Bindings bindings = super.getBindingsForRoutingAddress(destination.getAddress());
               if (bindings != null)
               {
                  for (Binding b : bindings.getBindings())
                  {
                     super.removeBindingInternal(binding.getAddress(), b.getUniqueName());
                  }
               }
            }
         }
         else
         {
            for (Address destination : add.getLinkedAddresses())
            {
               super.removeBindingInternal(destination.getAddress(), uniqueName);
            }
         }
         removeAndUpdateAddressMap(add);
      }
      return binding;
   }

   public void clear()
   {
      super.clear();
      addresses.clear();
      wildCardAddresses.clear();
   }

   private Address getAddress(final SimpleString address)
   {
      Address add = new AddressImpl(address);
      Address actualAddress;
      if (add.containsWildCard())
      {
         actualAddress = wildCardAddresses.get(address);
      }
      else
      {
         actualAddress = addresses.get(address);
      }
      return actualAddress != null ? actualAddress : add;
   }

   private synchronized Address addAndUpdateAddressMap(final SimpleString address)
   {
      Address add = new AddressImpl(address);
      Address actualAddress;
      if (add.containsWildCard())
      {
         actualAddress = wildCardAddresses.get(address);
      }
      else
      {
         actualAddress = addresses.get(address);
      }
      if (actualAddress == null)
      {
         actualAddress = add;
         addAddress(address, actualAddress);
      }
      if (actualAddress.containsWildCard())
      {
         for (Address destAdd : addresses.values())
         {
            if (destAdd.matches(actualAddress))
            {
               destAdd.addLinkedAddress(actualAddress);
               actualAddress.addLinkedAddress(destAdd);
            }
         }
      }
      else
      {
         for (Address destAdd : wildCardAddresses.values())
         {
            if (actualAddress.matches(destAdd))
            {
               destAdd.addLinkedAddress(actualAddress);
               actualAddress.addLinkedAddress(destAdd);

            }
         }
      }
      return actualAddress;
   }

   private void addAddress(final SimpleString address, final Address actualAddress)
   {
      if (actualAddress.containsWildCard())
      {
         wildCardAddresses.put(address, actualAddress);
      }
      else
      {
         addresses.put(address, actualAddress);
      }
   }

   private synchronized void removeAndUpdateAddressMap(final Address address)
   {
      //we only remove if there are no bindings left
      Bindings bindings = super.getBindingsForRoutingAddress(address.getAddress());
      if (bindings == null || bindings.getBindings().size() == 0)
      {
         List<Address> addresses = address.getLinkedAddresses();
         for (Address address1 : addresses)
         {
            address1.removeLinkedAddress(address);
            Bindings linkedBindings = super.getBindingsForRoutingAddress(address1.getAddress());
            if (linkedBindings == null || linkedBindings.getBindings().size() == 0)
            {
               removeAddress(address1);
            }
         }
         removeAddress(address);
      }
   }

   private void removeAddress(final Address add)
   {
      if (add.containsWildCard())
      {
         wildCardAddresses.remove(add.getAddress());
      }
      else
      {
         addresses.remove(add.getAddress());
      }
   }
}
