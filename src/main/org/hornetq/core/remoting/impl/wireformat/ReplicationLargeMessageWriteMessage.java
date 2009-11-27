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

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.utils.DataConstants;

/**
 * A ReplicationLargeMessageWriteMessage
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicationLargeMessageWriteMessage extends PacketImpl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long messageId;

   private byte body[];

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------
   public ReplicationLargeMessageWriteMessage()
   {
      super(REPLICATION_LARGE_MESSAGE_WRITE);
   }

   /**
    * @param messageId
    * @param body
    */
   public ReplicationLargeMessageWriteMessage(final long messageId, final byte[] body)
   {
      this();

      this.messageId = messageId;
      this.body = body;
   }

   // Public --------------------------------------------------------

   @Override
   public void encodeRest(final HornetQBuffer buffer)
   {
      buffer.writeLong(messageId);
      buffer.writeInt(body.length);
      buffer.writeBytes(body);
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      messageId = buffer.readLong();
      int size = buffer.readInt();
      body = new byte[size];
      buffer.readBytes(body);
   }

   /**
    * @return the messageId
    */
   public long getMessageId()
   {
      return messageId;
   }

   /**
    * @return the body
    */
   public byte[] getBody()
   {
      return body;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
