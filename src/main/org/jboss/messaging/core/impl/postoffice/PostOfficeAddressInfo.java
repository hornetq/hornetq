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
package org.jboss.messaging.core.impl.postoffice;

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
 * @version <tt>$Revision: 1917 $</tt>
 *
 * $Id: PostOfficeAddressInfo.java 1917 2007-01-08 20:26:12Z clebert.suconic@jboss.com $
 *
 */
class PostOfficeAddressInfo implements Streamable, Serializable
{
   // Constants -----------------------------------------------------

   private static final long serialVersionUID = 8462102430717730566L;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private Address controlChannelAddress;
   private Address dataChannelAddress;

   // Constructors --------------------------------------------------

   PostOfficeAddressInfo()
   {
   }

   PostOfficeAddressInfo(Address controlChannelAddress, Address dataChannelAddress)
   {
      this.controlChannelAddress = controlChannelAddress;
      this.dataChannelAddress = dataChannelAddress;
   }

   // Streamable implementation -------------------------------------

   public void read(DataInputStream in) throws Exception
   {
      controlChannelAddress = new IpAddress();

      controlChannelAddress.readFrom(in);

      dataChannelAddress = new IpAddress();

      dataChannelAddress.readFrom(in);
   }

   public void write(DataOutputStream out) throws Exception
   {
      if (!(controlChannelAddress instanceof IpAddress))
      {
         throw new IllegalStateException("Address must be IpAddress");
      }

      if (!(dataChannelAddress instanceof IpAddress))
      {
         throw new IllegalStateException("Address must be IpAddress");
      }

      controlChannelAddress.writeTo(out);

      dataChannelAddress.writeTo(out);
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      StringBuffer sb = new StringBuffer("[");
      sb.append("synch addr ").append(controlChannelAddress);
      sb.append(", asynch addr ").append(dataChannelAddress);
      sb.append("]");

      return sb.toString();
   }


   // Package protected ---------------------------------------------

   Address getControlChannelAddress()
   {
      return controlChannelAddress;
   }

   Address getDataChannelAddress()
   {
      return dataChannelAddress;
   }

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
