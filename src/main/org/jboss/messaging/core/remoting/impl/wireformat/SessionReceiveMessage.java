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

import org.jboss.messaging.core.client.impl.ClientMessageImpl;
import org.jboss.messaging.core.client.impl.ClientMessageInternal;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.utils.DataConstants;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionReceiveMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   public static final int SESSION_RECEIVE_MESSAGE_LARGE_MESSAGE_SIZE = BASIC_PACKET_SIZE + DataConstants.SIZE_LONG +
                                                                       DataConstants.SIZE_LONG +
                                                                       DataConstants.SIZE_INT +
                                                                       DataConstants.SIZE_BOOLEAN +
                                                                       DataConstants.SIZE_INT;

   private static final Logger log = Logger.getLogger(SessionReceiveMessage.class);

   // Attributes ----------------------------------------------------

   private long consumerID;

   private boolean largeMessage;

   private byte[] largeMessageHeader;

   private ClientMessageInternal clientMessage;

   private ServerMessage serverMessage;

   private int deliveryCount;
   
   /** Since we receive the message before the entire message was received, */
   private long largeMessageSize;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionReceiveMessage(final long consumerID, final byte[] largeMessageHeader, final long largeMessageSize, final int deliveryCount)
   {
      super(SESS_RECEIVE_MSG);

      this.consumerID = consumerID;

      this.largeMessageHeader = largeMessageHeader;

      this.deliveryCount = deliveryCount;

      this.largeMessage = true;
      
      this.largeMessageSize = largeMessageSize;
   }

   public SessionReceiveMessage(final long consumerID, final ServerMessage message, final int deliveryCount)
   {
      super(SESS_RECEIVE_MSG);

      this.consumerID = consumerID;

      this.serverMessage = message;

      this.clientMessage = null;

      this.deliveryCount = deliveryCount;

      this.largeMessage = false;
   }

   public SessionReceiveMessage()
   {
      super(SESS_RECEIVE_MSG);
   }

   // Public --------------------------------------------------------

   public long getConsumerID()
   {
      return consumerID;
   }

   public ClientMessageInternal getClientMessage()
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

   /**
    * @return the largeMessage
    */
   public boolean isLargeMessage()
   {
      return largeMessage;
   }

   public int getDeliveryCount()
   {
      return deliveryCount;
   }

   /**
    * @return the largeMessageSize
    */
   public long getLargeMessageSize()
   {
      return largeMessageSize;
   }

   public int getRequiredBufferSize()
   {
      if (largeMessage)
      {
         return SESSION_RECEIVE_MESSAGE_LARGE_MESSAGE_SIZE + largeMessageHeader.length;
      }
      else
      {
         return BASIC_PACKET_SIZE + 
                DataConstants.SIZE_LONG +
                DataConstants.SIZE_INT +
                DataConstants.SIZE_BOOLEAN +
                (serverMessage != null ? serverMessage.getEncodeSize() : clientMessage.getEncodeSize());
      }
   }
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.writeLong(consumerID);
      buffer.writeInt(deliveryCount);
      buffer.writeBoolean(largeMessage);
      if (largeMessage)
      {
         buffer.writeLong(largeMessageSize);
         buffer.writeInt(largeMessageHeader.length);
         buffer.writeBytes(largeMessageHeader);
      }
      else
      {
         serverMessage.encode(buffer);
      }
   }

   public void decodeBody(final MessagingBuffer buffer)
   {
      // TODO can be optimised

      consumerID = buffer.readLong();

      deliveryCount = buffer.readInt();

      largeMessage = buffer.readBoolean();

      if (largeMessage)
      {
         largeMessageSize = buffer.readLong();
         int size = buffer.readInt();
         largeMessageHeader = new byte[size];
         buffer.readBytes(largeMessageHeader);
      }
      else
      {
         clientMessage = new ClientMessageImpl(deliveryCount);
         clientMessage.decode(buffer);
         clientMessage.getBody().resetReaderIndex();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
