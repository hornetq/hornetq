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

import org.hornetq.core.message.Message;
import org.hornetq.core.remoting.spi.MessagingBuffer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.utils.DataConstants;

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

   private ServerMessage serverMessage;

   private boolean requiresResponse;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionSendMessage(final Message message, final boolean requiresResponse)
   {
      super(SESS_SEND);

      clientMessage = message;

      this.requiresResponse = requiresResponse;
   }

   public SessionSendMessage()
   {
      super(SESS_SEND);
   }

   // Public --------------------------------------------------------

   public Message getClientMessage()
   {
      return clientMessage;
   }

   public ServerMessage getServerMessage()
   {
      return serverMessage;
   }

   public boolean isRequiresResponse()
   {
      return requiresResponse;
   }

   @Override
   public void encodeBody(final MessagingBuffer buffer)
   {
      if (clientMessage != null)
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
      // TODO can be optimised

      serverMessage = new ServerMessageImpl();

      clientMessage = serverMessage;

      serverMessage.decode(buffer);

      serverMessage.getBody().resetReaderIndex();

      requiresResponse = buffer.readBoolean();
   }

   public int getRequiredBufferSize()
   {
      int size = BASIC_PACKET_SIZE + clientMessage.getEncodeSize() + DataConstants.SIZE_BOOLEAN;

      return size;
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
