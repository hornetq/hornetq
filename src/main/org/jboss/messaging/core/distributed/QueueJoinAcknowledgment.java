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
 * An acknowledgement send by a peer that accepts a new peer into the distributed queue.
 *
 * <p>The acknowledgment contains the peer's JGroups address and the distributed pipe ID used by
 * the peer to receive incoming messages from the other peers.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class QueueJoinAcknowledgment implements Externalizable
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
   public QueueJoinAcknowledgment()
   {
   }

   public QueueJoinAcknowledgment(Address a, Serializable pid)
   {
      address = a;
      pipeID = pid;
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
