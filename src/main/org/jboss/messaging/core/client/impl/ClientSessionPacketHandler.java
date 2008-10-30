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

package org.jboss.messaging.core.client.impl;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.EXCEPTION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.SESS_RECEIVE_MSG;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.ChannelHandler;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.SessionReceiveMessage;

/**
 *
 * A ClientSessionPacketHandler
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ClientSessionPacketHandler implements ChannelHandler
{
   private static final Logger log = Logger.getLogger(ClientSessionPacketHandler.class);

   private final ClientSessionInternal clientSession;

   public ClientSessionPacketHandler(final ClientSessionInternal clientSesssion)
   {     
      this.clientSession = clientSesssion;
   }
      
   public void handlePacket(final Packet packet)
   {
      byte type = packet.getType();
       
      try
      {
         switch (type)
         {
            case SESS_RECEIVE_MSG:
            {
               SessionReceiveMessage message = (SessionReceiveMessage) packet;
      
               clientSession.handleReceiveMessage(message.getConsumerID(), message.getClientMessage());
               
               break;
            }
            case EXCEPTION:
            {
               //TODO - we can provide a means for async exceptions to get back to to client
               //For now we just log it
               MessagingExceptionMessage mem = (MessagingExceptionMessage)packet;
               
               log.error("Received exception asynchronously from server", mem.getException());
               
               break;
            }
            default:
            {
               throw new IllegalStateException("Invalid packet: " + type);
            }
         }
      }
      catch (Exception e)
      {
         log.error("Failed to handle packet", e);
      }
   }
}