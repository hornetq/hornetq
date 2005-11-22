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
package org.jboss.messaging.core.distributed.queue;

import org.jboss.messaging.core.distributed.RemotePeerInfo;
import org.jboss.messaging.core.distributed.PeerIdentity;

import java.io.Serializable;
import java.io.ObjectOutput;
import java.io.IOException;
import java.io.ObjectInput;

/**
 * The acknowledgment returned by a peer accepting a new peer to join a distributed structure.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class QueuePeerInfo extends RemotePeerInfo
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Serializable pipeID;

   // Constructors --------------------------------------------------

   /**
    * For externalization.
    */
   public QueuePeerInfo()
   {
   }

   /**
    * @param peerIdentity - the identity of acknowledging peer.
    * @param pipeID - the id of the distributed pipe the acknowledging peer can be contacted at.
    */
   public QueuePeerInfo(PeerIdentity peerIdentity, Serializable pipeID)
   {
      this.peerIdentity = peerIdentity;
      this.pipeID = pipeID;
   }

   // Externalizable implementation ---------------------------------

   public void writeExternal(ObjectOutput out) throws IOException
   {
      super.writeExternal(out);
      out.writeObject(pipeID);
   }

   public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException
   {
      super.readExternal(in);
      pipeID = (Serializable)in.readObject();
   }

   // Public --------------------------------------------------------

   public Serializable getPipeID()
   {
      return pipeID;
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
