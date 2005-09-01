/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.messaging.core.distributed;

import org.jgroups.Address;
import org.jgroups.stack.IpAddress;

import java.io.Serializable;
import java.io.ObjectOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.Externalizable;

/**
 * The acknowledgment returned by a peer accepting a new peer to join a distributed structure.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class Acknowledgment implements Externalizable
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Address address;
   protected Serializable pipeID;

   // Constructors --------------------------------------------------

   /**
    * For externalization.
    */
   public Acknowledgment()
   {
   }

   /**
    * @param address - the address of acknowledging peer.
    * @param pipeID - the id of the distributed pipe the acknowledging peer can be contacted at.
    */
   public Acknowledgment(Address address, Serializable pipeID)
   {
      this.address = address;
      this.pipeID = pipeID;
   }

   // Externalizable implementation ---------------------------------

   public void writeExternal(ObjectOutput out) throws IOException
   {
      address.writeExternal(out);
      out.writeObject(pipeID);
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
      address = new IpAddress(); // TODO ?
      address.readExternal(in);
      pipeID = (Serializable)in.readObject();
   }


   // Public --------------------------------------------------------

   public Address getAddress()
   {
      return address;
   }

   public Serializable getPipeID()
   {
      return pipeID;
   }


   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
