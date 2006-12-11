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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.Serializable;

import org.jboss.messaging.util.Streamable;
import org.jgroups.Address;
import org.jgroups.stack.IpAddress;

/**
 * 
 * A PostOfficeAddressInfo
 * 
 * Holds the addresses used by a clustered post office
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
class PostOfficeAddressInfo implements Streamable, Serializable
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 8462102430717730566L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private Address syncChannelAddress;
   private Address asyncChannelAddress;

   // Constructors --------------------------------------------------

   public PostOfficeAddressInfo()
   {
   }

   PostOfficeAddressInfo(Address syncChannelAddress, Address asyncChannelAddress)
   {
      this.syncChannelAddress = syncChannelAddress;
      this.asyncChannelAddress = asyncChannelAddress;
   }

   // Streamable implementation -------------------------------------

   public void read(DataInputStream in) throws Exception
   {
      syncChannelAddress = new IpAddress();

      syncChannelAddress.readFrom(in);

      asyncChannelAddress = new IpAddress();

      asyncChannelAddress.readFrom(in);

      byte[] byteInput = new byte[in.readInt()];

      in.read(byteInput);
   }

   public void write(DataOutputStream out) throws Exception
   {
      if (!(syncChannelAddress instanceof IpAddress))
      {
         throw new IllegalStateException("Address must be IpAddress");
      }

      if (!(asyncChannelAddress instanceof IpAddress))
      {
         throw new IllegalStateException("Address must be IpAddress");
      }

      syncChannelAddress.writeTo(out);

      asyncChannelAddress.writeTo(out);
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      StringBuffer sb = new StringBuffer("[");
      sb.append("synch addr ").append(syncChannelAddress);
      sb.append(", asynch addr ").append(asyncChannelAddress);
      sb.append("]");

      return sb.toString();
   }


   // Package protected ---------------------------------------------

   Address getSyncChannelAddress()
   {
      return syncChannelAddress;
   }

   Address getAsyncChannelAddress()
   {
      return asyncChannelAddress;
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
