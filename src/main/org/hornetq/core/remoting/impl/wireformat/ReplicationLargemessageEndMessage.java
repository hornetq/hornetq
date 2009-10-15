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

package org.hornetq.core.remoting.impl.wireformat;

import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.utils.DataConstants;

/**
 * A ReplicationLargemessageEndMessage
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicationLargemessageEndMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   long messageId;

   boolean isDelete;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ReplicationLargemessageEndMessage()
   {
      super(REPLICATION_LARGE_MESSAGE_END);
   }

   public ReplicationLargemessageEndMessage(final long messageId, final boolean isDelete)
   {
      this();
      this.messageId = messageId;
      this.isDelete = isDelete;
   }

   // Public --------------------------------------------------------

   @Override
   public int getRequiredBufferSize()
   {
      return BASIC_PACKET_SIZE + DataConstants.SIZE_LONG + DataConstants.SIZE_BOOLEAN;
   }

   @Override
   public void encodeBody(final HornetQBuffer buffer)
   {
      buffer.writeLong(messageId);
      buffer.writeBoolean(isDelete);
   }

   @Override
   public void decodeBody(final HornetQBuffer buffer)
   {
      messageId = buffer.readLong();
      isDelete = buffer.readBoolean();
   }

   /**
    * @return the messageId
    */
   public long getMessageId()
   {
      return messageId;
   }

   /**
    * @return the isDelete
    */
   public boolean isDelete()
   {
      return isDelete;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
