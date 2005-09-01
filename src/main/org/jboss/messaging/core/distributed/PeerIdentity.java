/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
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

   public String toString()
   {
      return identityToString(distributedID, peerID, address);

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
