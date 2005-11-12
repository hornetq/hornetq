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
package org.jboss.messaging.core.distributed;

import org.jgroups.Address;
import org.jgroups.stack.IpAddress;

import java.io.Serializable;
import java.io.Externalizable;
import java.io.ObjectOutput;
import java.io.IOException;
import java.io.ObjectInput;


/**
 * Incapsulates the identity of a peer (distributed component ID, peer ID and JGroups address).
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class PeerIdentity implements Externalizable
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   public static String identityToString(Serializable distributedID,
                                         Serializable peerID,
                                         Address address)
   {
      return distributedID + ":" + peerID + ":" + address;
   }

   // Attributes ----------------------------------------------------

   protected Serializable distributedID;
   protected Serializable peerID;
   protected Address address;

   // Constructors --------------------------------------------------

   /**
    * Used exclusively by externalization.
    */
   public PeerIdentity()
   {
   }

   public PeerIdentity(Serializable distributedID, Serializable peerID, Address address)
   {
      this.distributedID = distributedID;
      this.peerID = peerID;
      this.address = address;
   }

   // Externalizable implementation ---------------------------------

   public void writeExternal(ObjectOutput out) throws IOException
   {
      out.writeObject(distributedID);
      out.writeObject(peerID);
      address.writeExternal(out);
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {

      distributedID = (Serializable)in.readObject();
      peerID = (Serializable)in.readObject();
      address = new IpAddress(); // TODO ?
      address.readExternal(in);
   }

   // Public --------------------------------------------------------

   public Serializable getDistributedID()
   {
      return distributedID;
   }

   public Serializable getPeerID()
   {
      return peerID;
   }

   public Address getAddress()
   {
      return address;
   }

   public boolean equals(Object o)
   {
      if (this == o)
      {
         return true;
      }

      if (!(o instanceof PeerIdentity))
      {
         return false;
      }

      PeerIdentity that = (PeerIdentity)o;

      if (distributedID == null && that.distributedID != null)
      {
         return false;
      }

      if (peerID == null && that.peerID != null)
      {
         return false;
      }

      if (address == null && that.address != null)
      {
         return false;
      }

      return (distributedID == that.distributedID || distributedID.equals(that.distributedID)) &&
             (peerID == that.peerID || peerID.equals(that.peerID)) &&
             (address == that.address || address.equals(that.address));
   }

   private volatile int hashCode = 0;

   public int hasCode()
   {
      if (hashCode == 0)
      {
         int result = 17;
         result = 37 * result + (distributedID == null ? 0 : distributedID.hashCode());
         result = 37 * result + (peerID == null ? 0 : peerID.hashCode());
         result = 37 * result + (address == null ? 0 : address.hashCode());
         hashCode = result;
      }
      return hashCode;
   }

   public String toString()
   {
      return identityToString(distributedID, peerID, address);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
