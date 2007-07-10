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

import org.jboss.messaging.util.StreamUtils;


/**
 * 
 * A PutReplicantRequest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1917 $</tt>
 *
 * $Id: PutReplicantRequest.java 1917 2007-01-08 20:26:12Z clebert.suconic@jboss.com $
 *
 */
class PutReplicantRequest extends ClusterRequest
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private int nodeId;
   
   private Serializable key;
   
   private Serializable replicant;

   // Constructors --------------------------------------------------

   PutReplicantRequest()
   {
   }

   PutReplicantRequest(int nodeId, Serializable key, Serializable replicant)
   {
      this.nodeId = nodeId;

      this.key = key;

      this.replicant = replicant;
   }

   // Streamable implementation -------------------------------------

   public void read(DataInputStream in) throws Exception
   {
      nodeId = in.readInt();

      key = (Serializable)StreamUtils.readObject(in, true);

      replicant = (Serializable)StreamUtils.readObject(in, true);
   }

   public void write(DataOutputStream out) throws Exception
   {
      out.writeInt(nodeId);

      StreamUtils.writeObject(out, key, true, true);

      StreamUtils.writeObject(out, replicant, true, true);
   }

   // ClusterRequest overrides --------------------------------------

   Object execute(RequestTarget office) throws Exception
   {
      office.putReplicantLocally(nodeId, key, replicant);
      return null;
   }

   byte getType()
   {
      return ClusterRequest.PUT_REPLICANT_REQUEST;
   }

   // Public --------------------------------------------------------

   public String toString()
   {
      return "PutRequest[" + key + "][" + replicant + "]";
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
