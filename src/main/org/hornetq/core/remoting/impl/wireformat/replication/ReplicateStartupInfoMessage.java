/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.remoting.impl.wireformat.replication;

import static org.hornetq.utils.UUID.TYPE_TIME_BASED;

import org.hornetq.core.remoting.impl.wireformat.PacketImpl;
import org.hornetq.core.remoting.spi.MessagingBuffer;
import org.hornetq.utils.DataConstants;
import org.hornetq.utils.UUID;

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
