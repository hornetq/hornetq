/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.remoting.impl.wireformat;

import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.util.DataConstants;

/**
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionSendChunkMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private long targetID;

   private byte[] header;

   private byte[] body;

   private boolean continues;

   private long messageID = 0;

   private boolean requiresResponse;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionSendChunkMessage(final long targetID,
                                  final byte[] header,
                                  final byte[] body,
                                  final boolean continues,
                                  final boolean requiresResponse)
   {
      super(SESS_CHUNK_SEND);
      this.targetID = targetID;
      this.header = header;
      this.body = body;
      this.continues = continues;
      this.requiresResponse = requiresResponse;
   }

   public SessionSendChunkMessage()
   {
      super(SESS_CHUNK_SEND);
   }

   // Public --------------------------------------------------------

   public long getTargetID()
   {
      return targetID;
   }

   public boolean isRequiresResponse()
   {
      return requiresResponse;
   }

   public byte[] getHeader()
   {
      return header;
   }

   public byte[] getBody()
   {
      return body;
   }

   public long getMessageID()
   {
      return messageID;
   }

   public void setMessageID(final long messageId)
   {
      messageID = messageId;
   }

   public boolean isContinues()
   {
      return continues;
   }

   @Override
   public int getRequiredBufferSize()
   {
      return DEFAULT_PACKET_SIZE + DataConstants.SIZE_LONG /* TargetID */+
             DataConstants.SIZE_INT /* HeaderLength */+
             (header != null ? header.length : 0) /* Header bytes */+
             DataConstants.SIZE_INT /* BodyLength */+
             body.length /* Body bytes */+
             DataConstants.SIZE_BOOLEAN /* hasContinuations */+
             DataConstants.SIZE_BOOLEAN /* requiresResponse */+
             DataConstants.SIZE_BOOLEAN /* has MessageId */+
             (messageID > 0 ? DataConstants.SIZE_LONG : 0);
   }

   @Override
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putLong(targetID);

      if (header != null)
      {
         buffer.putInt(header.length);
         buffer.putBytes(header);
      }
      else
      {
         buffer.putInt(0);
      }

      buffer.putInt(body.length);
      buffer.putBytes(body);

      buffer.putBoolean(continues);

      buffer.putBoolean(requiresResponse);

      buffer.putBoolean(messageID > 0);

      if (messageID > 0)
      {
         buffer.putLong(messageID);
      }
      
   }

   @Override
   public void decodeBody(final MessagingBuffer buffer)
   {
      targetID = buffer.getLong();

      final int headerLength = buffer.getInt();

      if (headerLength > 0)
      {
         header = new byte[headerLength];
         buffer.getBytes(header);
      }
      else
      {
         header = null;
      }

      final int bodyLength = buffer.getInt();

      body = new byte[bodyLength];
      buffer.getBytes(body);

      continues = buffer.getBoolean();

      requiresResponse = buffer.getBoolean();

      final boolean hasMessageID = buffer.getBoolean();

      if (hasMessageID)
      {
         messageID = buffer.getLong();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
