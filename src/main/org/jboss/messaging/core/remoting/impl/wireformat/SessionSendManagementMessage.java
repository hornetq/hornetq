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
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * @version <tt>$Revision$</tt>
 */
public class SessionSendManagementMessage extends PacketImpl
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SessionSendManagementMessage.class);
   
   // Attributes ----------------------------------------------------

   private ClientMessage clientMessage;
   
   private ServerMessage serverMessage;
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public SessionSendManagementMessage(final ClientMessage message)
   {
      super(SESS_MANAGEMENT_SEND);

      this.clientMessage = message;
   }
      
   public SessionSendManagementMessage()
   {
      super(SESS_MANAGEMENT_SEND);
   }

   // Public --------------------------------------------------------

   public ClientMessage getClientMessage()
   {
      return clientMessage;
   }
   
   public ServerMessage getServerMessage()
   {
      return serverMessage;
   }
   
   public void encodeBody(final MessagingBuffer buffer)
   {
      if (clientMessage != null)
      {
         clientMessage.encode(buffer);
      }
      else
      {
         //If we're replicating a buffer to a backup node then we encode the serverMessage not the clientMessage
         serverMessage.encode(buffer);
      }
   }
   
   public void decodeBody(final MessagingBuffer buffer)
   {          
      serverMessage = new ServerMessageImpl();
      
      serverMessage.decode(buffer);
      
      serverMessage.getBody().flip();
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
