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
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

import org.jboss.messaging.core.postoffice.Address;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.util.SimpleString;

/**
 * extends the simple manager to allow wilcard addresses to be used.
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class WildcardAddressManager extends SimpleAddressManager
{
   static final char SINGLE_WORD = '*';

   static final char ANY_WORDS = '#';

   static final char DELIM = '.';

   static final SimpleString SINGLE_WORD_SIMPLESTRING = new SimpleString("*");

   static final SimpleString ANY_WORDS_SIMPLESTRING = new SimpleString("#");

   /**
    * This is the actual wild card binding, for every binding added here 1 or more actual bindings will be added.
    * i.e. A binding for A.* will bind the same queue to A.B and A.C if they are its linked addresses
    */
   private final ConcurrentMap<SimpleString, List<Binding>> wildcardBindings = new ConcurrentHashMap<SimpleString, List<Binding>>();

   /**
    * All the wild card destinations. So if A.B is added to the actual destinations we add A.*, *.B, *.* etc.
    */
   private final ConcurrentMap<SimpleString, Address> wildcardDestinations = new ConcurrentHashMap<SimpleString, Address>();

   /**
    * This is all the actual destinations, we use this so we can link back from the actual address to its linked wilcard addresses
    */
   private final ConcurrentMap<SimpleString, Address> actualDestinations = new ConcurrentHashMap<SimpleString, Address>();

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
      Address add = wildcardDestinations.get(address);
      // if this isnt a wildcard destination then just add normally
      if (add == null)
      {
         return super.addMapping(address, binding);
      }
      else
      {
         // add this as a wildcard binding and add a new binding to any linked addresses.
         for (Address destination : add.getLinkedAddresses())
         {
            BindingImpl binding1 = new BindingImpl(destination.getAddress(), binding.getQueue(), binding.isFanout());
            super.addMapping(destination.getAddress(), binding1);
         }
         List<Binding> bindings = new CopyOnWriteArrayList<Binding>();
         List<Binding> prevBindings = wildcardBindings.putIfAbsent(address, bindings);

         if (prevBindings != null)
         {
            bindings = prevBindings;
         }

         bindings.add(binding);
         return prevBindings != null;
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
      Address add = wildcardDestinations.get(address);
      // if this isnt a wildcard binding just remove normally
      if (add == null)
      {
         return super.removeMapping(address, queueName);
      }
      else
      {
         for (Address destination : add.getLinkedAddresses())
         {
            super.removeMapping(destination.getAddress(), queueName);
         }
         List<Binding> bindings = wildcardBindings.get(address);
         Binding binding = removeMapping(queueName, bindings);

         if (bindings.isEmpty())
         {
            wildcardBindings.remove(binding.getAddress());
         }
         return bindings.isEmpty();
      }
   }

   public void clear()
   {
      super.clear();
      wildcardBindings.clear();
      wildcardDestinations.clear();
   }

   /**
    * When we add a new destination we calculate all its corresponding wild card matches and add those. We also link the
    * wild card address added to the actual address.
    *
    * @param address the address to add
    * @return true if it didn't already exist
    */
   public boolean addDestination(final SimpleString address)
   {
      boolean added = super.addDestination(address);
      // if this is a new destination we compute any wilcard addresses that would match and add if necessary
      synchronized (actualDestinations)
      {
         if (added)
         {
            Address add = new AddressImpl(address);
            Address prevAddress = actualDestinations.putIfAbsent(address, add);
            if (prevAddress != null)
            {
               add = prevAddress;
            }
            List<SimpleString> adds = getAddresses(add);
            for (SimpleString simpleString : adds)
            {
               Address addressToAdd = new AddressImpl(simpleString);
               Address prev = wildcardDestinations.putIfAbsent(simpleString, addressToAdd);
               if (prev != null)
               {
                  addressToAdd = prev;
               }
               addressToAdd.addLinkedAddress(add);
               add.addLinkedAddress(addressToAdd);
            }
         }
      }
      return added;
   }

   /**
    * If the address is removed then we need to de-link it from its wildcard addresses.
    * If the wildcard address is then empty then we can remove it completely
    *
    * @param address the address to remove
    * @return if the address was removed
    */
   public boolean removeDestination(final SimpleString address)
   {
      boolean removed = super.removeDestination(address);
      synchronized (actualDestinations)
      {
         if (removed)
         {
            Address actualAddress = actualDestinations.remove(address);
            List<Address> addresses = actualAddress.getLinkedAddresses();
            for (Address address1 : addresses)
            {
               address1.removLinkedAddress(actualAddress);
               if (address1.getLinkedAddresses().size() == 0)
               {
                  wildcardDestinations.remove(address1.getAddress());
               }
            }
         }
      }
      return removed;
   }

   /**
    * @param address the address to check
    * @return true if the address exists or if a wildcard address exists
    */
   public boolean containsDestination(final SimpleString address)
   {
      return super.containsDestination(address) || wildcardDestinations.keySet().contains(address);
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
