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

import org.hornetq.core.client.impl.ClientMessageImpl;
import org.hornetq.core.client.impl.ClientMessageInternal;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.spi.HornetQBuffer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.utils.DataConstants;

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
   public void encodeBody(final HornetQBuffer buffer)
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

   public void decodeBody(final HornetQBuffer buffer)
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
