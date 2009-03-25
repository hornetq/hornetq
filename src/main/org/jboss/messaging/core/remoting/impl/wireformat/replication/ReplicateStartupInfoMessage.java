/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.remoting.impl.wireformat.replication;

import static org.jboss.messaging.utils.UUID.TYPE_TIME_BASED;

import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.utils.DataConstants;
import org.jboss.messaging.utils.UUID;

/**
 * 
 * A ReplicateAcknowledgeMessage
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 4 Mar 2009 18:36:30
 *
 *
 */
public class ReplicateStartupInfoMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private UUID nodeID;

   private long currentMessageID;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicateStartupInfoMessage(final UUID nodeID, final long currentMessageID)
   {
      super(REPLICATE_STARTUP_INFO);

      this.nodeID = nodeID;

      this.currentMessageID = currentMessageID;
   }

   // Public --------------------------------------------------------

   public ReplicateStartupInfoMessage()
   {
      super(REPLICATE_STARTUP_INFO);
   }

   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE + 
             nodeID.asBytes().length + // buffer.writeBytes(nodeID.asBytes());
             DataConstants.SIZE_LONG; // buffer.writeLong(currentMessageID);
   }

   @Override
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.writeBytes(nodeID.asBytes());
      buffer.writeLong(currentMessageID);
   }

   @Override
   public void decodeBody(final MessagingBuffer buffer)
   {
      byte[] bytes = new byte[16];
      buffer.readBytes(bytes);
      nodeID = new UUID(TYPE_TIME_BASED, bytes);
      currentMessageID = buffer.readLong();
   }

   public UUID getNodeID()
   {
      return nodeID;
   }

   public long getCurrentMessageID()
   {
      return currentMessageID;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
