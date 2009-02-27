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

import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;
import org.jboss.messaging.utils.DataConstants;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:csuconic@redhat.com">Clebert Suconic</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionSendMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private Message clientMessage;

   private boolean largeMessage;

   /** Used only if largeMessage */
   private byte[] largeMessageHeader;
   
   /** We need to set the MessageID when replicating this on the server */
   private long largeMessageId = -1;

   private ServerMessage serverMessage;

   private boolean requiresResponse;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionSendMessage(final Message message, final boolean requiresResponse)
   {
      super(SESS_SEND);

      clientMessage = message;

      this.requiresResponse = requiresResponse;

      largeMessage = false;
   }

   public SessionSendMessage(final byte[] largeMessageHeader, final boolean requiresResponse)
   {
      super(SESS_SEND);

      this.largeMessageHeader = largeMessageHeader;

      this.requiresResponse = requiresResponse;

      largeMessage = true;
   }

   public SessionSendMessage()
   {
      super(SESS_SEND);
   }

   // Public --------------------------------------------------------

   
   public boolean isLargeMessage()
   {
      return largeMessage;
   }
   
   public Message getClientMessage()
   {
      return clientMessage;
   }

   public ServerMessage getServerMessage()
   {
      return serverMessage;
   }

   public byte[] getLargeMessageHeader()
   {
      return largeMessageHeader;
   }

   public boolean isRequiresResponse()
   {
      return requiresResponse;
   }
   
   /**
    * @return the largeMessageId
    */
   public long getMessageID()
   {
      if (largeMessage)
      {
         return largeMessageId;
      }
      else
      {
         return serverMessage.getMessageID();
      }
   }

   /**
    * @param largeMessageId the largeMessageId to set
    */
   public void setMessageID(long id)
   {
      if (largeMessage)
      {
         this.largeMessageId = id;
      }
      else
      {
         serverMessage.setMessageID(id);
      }
   }

   @Override
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.writeBoolean(largeMessage);

      if (largeMessage)
      {
         buffer.writeInt(largeMessageHeader.length);
         buffer.writeBytes(largeMessageHeader);
         
         if (largeMessageId > 0)
         {
            buffer.writeBoolean(true);
            buffer.writeLong(largeMessageId);
         }
         else
         {
            buffer.writeBoolean(false);
         }
      }
      else if (clientMessage != null)
      {
         clientMessage.encode(buffer);
      }
      else
      {
         // If we're replicating a buffer to a backup node then we encode the serverMessage not the clientMessage
         serverMessage.encode(buffer);
      }

      buffer.writeBoolean(requiresResponse);
   }

   @Override
   public void decodeBody(final MessagingBuffer buffer)
   {
      largeMessage = buffer.readBoolean();

      if (largeMessage)
      {
         int largeMessageLength = buffer.readInt();

         largeMessageHeader = new byte[largeMessageLength];

         buffer.readBytes(largeMessageHeader);
         
         final boolean largeMessageIDFilled = buffer.readBoolean();
         
         if (largeMessageIDFilled)
         {
            this.largeMessageId = buffer.readLong();
         }
         else
         {
            this.largeMessageId = -1;
         }
      }
      else
      {
         // TODO can be optimised

         serverMessage = new ServerMessageImpl();
         
         clientMessage = serverMessage;

         serverMessage.decode(buffer);

         serverMessage.getBody().resetReaderIndex();

         requiresResponse = buffer.readBoolean();
      }
   }

   @Override
   public int getRequiredBufferSize()
   {
      if (largeMessage)
      {
         return BASIC_PACKET_SIZE +
                // IsLargeMessage
                DataConstants.SIZE_BOOLEAN +
                // BufferSize
                DataConstants.SIZE_INT +
                // Bytes sent
                largeMessageHeader.length +
                // LargeMessageID (if > 0) and a boolean statying if the largeMessageID is set
                DataConstants.SIZE_BOOLEAN + (largeMessageId >= 0 ? DataConstants.SIZE_LONG : 0) + 
                DataConstants.SIZE_BOOLEAN;
      }
      else
      {
         return DataConstants.SIZE_BOOLEAN + BASIC_PACKET_SIZE +
                clientMessage.getEncodeSize() +
                DataConstants.SIZE_BOOLEAN;
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
