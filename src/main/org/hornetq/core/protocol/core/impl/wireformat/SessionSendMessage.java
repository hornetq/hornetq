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
import org.hornetq.api.core.Message;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageInternal;
import org.hornetq.core.protocol.core.impl.PacketImpl;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.utils.DataConstants;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * @version <tt>$Revision$</tt>
 */
public class SessionSendMessage extends MessagePacket
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SessionSendMessage.class);

   // Attributes ----------------------------------------------------

   private boolean requiresResponse;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionSendMessage(final MessageInternal message, final boolean requiresResponse)
   {
      super(PacketImpl.SESS_SEND, message);

      this.requiresResponse = requiresResponse;
   }

   public SessionSendMessage()
   {
      super(PacketImpl.SESS_SEND, new ServerMessageImpl());
   }

   // Public --------------------------------------------------------

   public boolean isRequiresResponse()
   {
      return requiresResponse;
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

      buffer.writeBoolean(requiresResponse);

      size = buffer.writerIndex();

      // Write standard headers

      int len = size - DataConstants.SIZE_INT;
      buffer.setInt(0, len);
      buffer.setByte(DataConstants.SIZE_INT, type);
      buffer.setLong(DataConstants.SIZE_INT + DataConstants.SIZE_BYTE, channelID);

      // Position reader for reading by Netty
      buffer.readerIndex(0);

      message.resetCopied();

      return buffer;
   }

   @Override
   public void decodeRest(final HornetQBuffer buffer)
   {
      // Buffer comes in after having read standard headers and positioned at Beginning of body part

      message.decodeFromBuffer(buffer);

      int ri = buffer.readerIndex();

      requiresResponse = buffer.readBoolean();

      buffer.readerIndex(ri);

   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
