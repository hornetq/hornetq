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

import org.jboss.messaging.core.postoffice.Address;
import static org.jboss.messaging.core.postoffice.impl.WildcardAddressManager.DELIM;
import static org.jboss.messaging.core.postoffice.impl.WildcardAddressManager.SINGLE_WORD;
import org.jboss.messaging.util.SimpleString;

import java.util.ArrayList;
import java.util.List;

/**splits an address string into its hierarchical parts split by '.'
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class AddressImpl implements Address
{
   private SimpleString address;

   private SimpleString[] addressParts;

   private boolean containsWildCard;
   private List<Address> linkedAddresses = new ArrayList<Address>();

   public AddressImpl(final SimpleString address)
   {
      this.address = address;
      this.addressParts = address.split(DELIM);
      containsWildCard = address.contains(SINGLE_WORD);
   }

   public SimpleString getAddress()
   {
      return address;
   }

   public SimpleString[] getAddressParts()
   {
      return addressParts;
   }

   public boolean containsWildCard()
   {
      return containsWildCard;
   }

   public List<Address> getLinkedAddresses()
   {
      return linkedAddresses;
   }

   public void addLinkedAddress(Address address)
   {
      linkedAddresses.add(address);
   }

   public void removLinkedAddress(Address actualAddress)
   {
      linkedAddresses.remove(actualAddress);
   }

   public void removeAddressPart(final int pos)
   {
      SimpleString newAddress = new SimpleString("");
      boolean started=false;
      for (int i = 0; i < addressParts.length; i++)
      {
         SimpleString addressPart = addressParts[i];
         if(i != pos)
         {
            if(started)
            {
               newAddress = newAddress.concat('.');
            }
            newAddress = newAddress.concat(addressPart);
            started=true;
         }
      }
      this.address = newAddress;
      this.addressParts = address.split(DELIM);
      containsWildCard = address.contains(SINGLE_WORD);
   }

   public boolean equals(Object o)
   {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      AddressImpl address1 = (AddressImpl) o;

      if (!address.equals(address1.address)) return false;

      return true;
   }

   public int hashCode()
   {
      return address.hashCode();
   }
}
