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

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionSendMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SessionSendMessage.class);
   
   // Attributes ----------------------------------------------------

   private long producerID;
   
   private ClientMessage clientMessage;
   
   private ServerMessage serverMessage;
   
   private boolean requiresResponse;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionSendMessage(final long producerID, final ClientMessage message, final boolean requiresResponse)
   {
      super(SESS_SEND);

      this.producerID = producerID;
      
      this.clientMessage = message;
      
      this.requiresResponse = requiresResponse;
   }
      
   public SessionSendMessage()
   {
      super(SESS_SEND);
   }

   protected SessionSendMessage(final byte type, final long producerID, final ClientMessage message, final boolean requiresResponse)
   {
      super(type);

      this.producerID = producerID;

      this.clientMessage = message;

      this.requiresResponse = requiresResponse;
   }

   protected SessionSendMessage(byte type)
   {
      super(type);
   }

   // Public --------------------------------------------------------

   public boolean isReHandleResponseOnFailure()
   {
      return true;
   }
   
   public long getProducerID()
   {
      return producerID;
   }
   
   public ClientMessage getClientMessage()
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
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      buffer.putLong(producerID);      
      
      if (clientMessage != null)
      {
         clientMessage.encode(buffer);
      }
      else
      {
         //If we're replicating a buffer to a backup node then we encode the serverMessage not the clientMessage
         serverMessage.encode(buffer);
      }
      
      buffer.putBoolean(requiresResponse);
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {
      //TODO can be optimised
      
      producerID = buffer.getLong();
                  
      serverMessage = new ServerMessageImpl();
      
      serverMessage.decode(buffer);
      
      serverMessage.getBody().flip();
      
      requiresResponse = buffer.getBoolean();
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
