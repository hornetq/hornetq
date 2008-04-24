/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.messaging.core.server.impl;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.NULL;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.impl.wireformat.ConnectionCreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketType;
import org.jboss.messaging.core.server.ServerConnection;

/**
 * 
 * A ServerConnectionPacketHandler
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ServerConnectionPacketHandler extends ServerPacketHandlerSupport
{
	private final ServerConnection connection;
	
   public ServerConnectionPacketHandler(final ServerConnection connection)
   {
   	this.connection = connection;
   }

   public long getID()
   {
      return connection.getID();
   }

   public Packet doHandle(final Packet packet, final PacketSender sender) throws Exception
   {
      Packet response = null;

      PacketType type = packet.getType();
      
      switch (type)
      {
      case CONN_CREATESESSION:
         ConnectionCreateSessionMessage request = (ConnectionCreateSessionMessage) packet;   
         response = connection.createSession(request.isXA(), request.isAutoCommitSends(), request.isAutoCommitAcks(), sender);
         break;
      case CONN_START:
         connection.start();
         break;
      case CONN_STOP:
         connection.stop();
         break;
      case CLOSE:
         connection.close();
         break;
      default:
         throw new MessagingException(MessagingException.UNSUPPORTED_PACKET,
               "Unsupported packet " + type);
      }

      // reply if necessary
      if (response == null && packet.isOneWay() == false)
      {
         response = new PacketImpl(NULL);               
      }
      
      return response;
   }
}
