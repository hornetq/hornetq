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
import org.hornetq.core.message.impl.MessageInternal;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.utils.DataConstants;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SessionSendMessage extends MessagePacket
{

   private boolean requiresResponse;

   public SessionSendMessage(final MessageInternal message, final boolean requiresResponse)
   {
      super(SESS_SEND, message);

      this.requiresResponse = requiresResponse;
   }

   public SessionSendMessage(final MessageInternal message)
   {
      super(SESS_SEND, message);
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
      buffer.setByte(DataConstants.SIZE_INT, getType());
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

   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + (requiresResponse ? 1231 : 1237);
      return result;
   }

   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (!super.equals(obj))
         return false;
      if (!(obj instanceof SessionSendMessage))
         return false;
      SessionSendMessage other = (SessionSendMessage)obj;
      if (requiresResponse != other.requiresResponse)
         return false;
      return true;
   }

}
