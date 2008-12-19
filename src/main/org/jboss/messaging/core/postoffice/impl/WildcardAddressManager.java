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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.postoffice.Address;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.Bindings;
import org.jboss.messaging.util.SimpleString;

/**
 * extends the simple manager to allow wilcard addresses to be used.
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
    * This is all the addresses, we use this so we can link back from the actual address to its linked wilcard addresses
    * or vice versa
    */
   private final Map<SimpleString, Address> addresses = new HashMap<SimpleString, Address>();

   /**
    * If the address to add the binding to contains a wildcard then a copy of the binding (with the same underlying queue)
    * will be added to the actual mappings. Otherwise the binding is added as normal.
    *
    * @param address the address to add the mapping to
    * @param binding the binding to add
    * @return true if the address was a new mapping
    */
   public boolean addMapping(final SimpleString address, final Binding binding)
   {
      Address add = addAndUpdateAddressMap(address);
      if (!add.containsWildCard())
      {
         for (Address destination : add.getLinkedAddresses())
         {
            Bindings bindings = getBindings(destination.getAddress());
            if (bindings != null)
            {
               for (Binding b : bindings.getBindings())
               {
                  super.addMapping(address, b);
               }
            }
         }
         return super.addMapping(address, binding);
      }
      else
      {
         for (Address destination : add.getLinkedAddresses())
         {
            BindingImpl binding1 = new BindingImpl(destination.getAddress(), binding.getQueue(), binding.isExclusive());
            super.addMapping(destination.getAddress(), binding1);
         }
         return super.addMapping(address, binding);
      }
   }

   /**
    * If the address is a wild card then the binding will be removed from the actual mappings for any linked address.
    * otherwise it will be removed as normal.
    *
    * @param address   the address to remove the binding from
    * @param queueName the name of the queue for the binding to remove
    * @return true if this was the last mapping for a specific address
    */
   public boolean removeMapping(final SimpleString address, final SimpleString queueName)
   {
      Address add = removeAndUpdateAddressMap(address);
      if (!add.containsWildCard())
      {
         boolean removed = super.removeMapping(address, queueName);
         for (Address destination : add.getLinkedAddresses())
         {
            Bindings bindings = getBindings(destination.getAddress());
            if (bindings != null)
            {
               for (Binding b : bindings.getBindings())
               {
                  super.removeMapping(address, b.getQueue().getName());
               }
            }
         }
         return removed;
      }
      else
      {
         for (Address destination : add.getLinkedAddresses())
         {
            super.removeMapping(destination.getAddress(), queueName);
         }
         return super.removeMapping(address, queueName);
      }
   }

   public void clear()
   {
      super.clear();
      addresses.clear();
   }

   private synchronized Address addAndUpdateAddressMap(SimpleString address)
   {
      Address add = addresses.get(address);
      if (add == null)
      {
         add = new AddressImpl(address);
         addresses.put(address, add);
      }
      if (!add.containsWildCard())
      {
         List<SimpleString> adds = getAddresses(add);
         for (SimpleString simpleString : adds)
         {
            Address addressToAdd = addresses.get(simpleString);
            if (addressToAdd == null)
            {
               addressToAdd = new AddressImpl(simpleString);
               addresses.put(simpleString, addressToAdd);
            }
            addressToAdd.addLinkedAddress(add);
            add.addLinkedAddress(addressToAdd);
         }
      }
      return add;
   }

   private synchronized Address removeAndUpdateAddressMap(SimpleString address)
   {
      Address add = addresses.get(address);
      if (add == null)
      {
         return new AddressImpl(address);
      }
      if (!add.containsWildCard())
      {
         Bindings bindings1 = getBindings(address);
         if (bindings1 == null || bindings1.getBindings().size() == 0)
         {
            add = addresses.remove(address);
         }
         List<Address> addresses = add.getLinkedAddresses();
         for (Address address1 : addresses)
         {
            address1.removLinkedAddress(add);
            if (address1.getLinkedAddresses().size() == 0)
            {
               this.addresses.remove(address1.getAddress());
            }
         }
      }
      return add;
   }

   private List<SimpleString> getAddresses(final Address address)
   {
      List<SimpleString> addresses = new ArrayList<SimpleString>();
      SimpleString[] parts = address.getAddressParts();

      addresses.add(parts[0]);
      addresses.add(SINGLE_WORD_SIMPLESTRING);
      addresses.add(ANY_WORDS_SIMPLESTRING);
      if (address.getAddressParts().length > 1)
      {
         addresses = addPart(addresses, address, 1);
      }
      addresses.remove(address.getAddress());
      return addresses;
   }

   private List<SimpleString> addPart(final List<SimpleString> addresses, final Address address, final int pos)
   {
      List<SimpleString> newAddresses = new ArrayList<SimpleString>();
      for (SimpleString add : addresses)
      {
         newAddresses.add(add.concat(DELIM).concat(SINGLE_WORD));
         newAddresses.add(add.concat(DELIM).concat(ANY_WORDS));
         newAddresses.add(add.concat(DELIM).concat(address.getAddressParts()[pos]));
      }
      if (pos + 1 < address.getAddressParts().length)
      {
         return addPart(newAddresses, address, pos + 1);
      }
      else
      {
         return mergeAddresses(newAddresses);
      }
   }

   private List<SimpleString> mergeAddresses(final List<SimpleString> adds)
   {
      List<SimpleString> newAddresses = new ArrayList<SimpleString>();
      for (int j = 0; j < adds.size(); j++)
      {
         SimpleString add = adds.get(j);
         Address address = new AddressImpl(add);
         SimpleString prev = null;
         SimpleString next;
         for (int i = 0; i < address.getAddressParts().length; i++)
         {
            SimpleString current = address.getAddressParts()[i];
            if (i < address.getAddressParts().length - 1)
            {
               next = address.getAddressParts()[i + 1];
            }
            else
            {
               next = null;
            }
            if (current.equals(SINGLE_WORD_SIMPLESTRING) && (ANY_WORDS_SIMPLESTRING.equals(next)))
            {
               address.removeAddressPart(i);
               prev = null;
               i = -1;
            }
            else if (current.equals(ANY_WORDS_SIMPLESTRING) && (ANY_WORDS_SIMPLESTRING.equals(next)))
            {
               address.removeAddressPart(i);
               prev = null;
               i = -1;
            }
            else if (current.equals(ANY_WORDS_SIMPLESTRING) && (SINGLE_WORD_SIMPLESTRING.equals(next)))
            {
               address.removeAddressPart(i + 1);
               prev = null;
               i = -1;
            }
            else if (current.equals(ANY_WORDS_SIMPLESTRING) && (ANY_WORDS_SIMPLESTRING.equals(prev)))
            {
               address.removeAddressPart(i + 1);
               prev = null;
               i = -1;
            }
            else
            {
               prev = current;
            }
         }
         if (!newAddresses.contains(address.getAddress()))
         {
            newAddresses.add(address.getAddress());
         }
      }
      return newAddresses;
   }
}
