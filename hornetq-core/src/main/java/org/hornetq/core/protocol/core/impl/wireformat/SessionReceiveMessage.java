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

package org.hornetq.core.protocol.core.impl.wireformat;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageInternal;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.utils.DataConstants;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionReceiveMessage extends MessagePacket
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SessionReceiveMessage.class);

   // Attributes ----------------------------------------------------

   private long consumerID;

   private int deliveryCount;

   public SessionReceiveMessage(final long consumerID, final MessageInternal message, final int deliveryCount)
   {
      super(PacketImpl.SESS_RECEIVE_MSG, message);

      this.consumerID = consumerID;

      this.deliveryCount = deliveryCount;
   }

   public SessionReceiveMessage()
   {
      super(PacketImpl.SESS_RECEIVE_MSG, new ClientMessageImpl());
   }

   // Public --------------------------------------------------------

   public long getConsumerID()
   {
      return consumerID;
   }

   public int getDeliveryCount()
   {
      return deliveryCount;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   public HornetQBuffer encode(final RemotingConnection connection)
   {
      HornetQBuffer buffer = message.getEncodedBuffer();

      // Sanity check
      if (buffer.writerIndex() != message.getEndOfMessagePosition())
      {
         throw new IllegalStateException("Wrong encode position");
      }

      buffer.writeLong(consumerID);
      buffer.writeInt(deliveryCount);

      size = buffer.writerIndex();

      // Write standard headers

      int len = size - DataConstants.SIZE_INT;
      buffer.setInt(0, len);
      buffer.setByte(DataConstants.SIZE_INT, getType());
      buffer.setLong(DataConstants.SIZE_INT + DataConstants.SIZE_BYTE, channelID);

      // Position reader for reading by Netty
      buffer.setIndex(0, size);

      return buffer;
   }

   @Override
   public void decode(final HornetQBuffer buffer)
   {
      channelID = buffer.readLong();

      message.decodeFromBuffer(buffer);

      consumerID = buffer.readLong();

      deliveryCount = buffer.readInt();

      size = buffer.readerIndex();

      // Need to position buffer for reading

      buffer.setIndex(PacketImpl.PACKET_HEADERS_SIZE + DataConstants.SIZE_INT, message.getEndOfBodyPosition());
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
