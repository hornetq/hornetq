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

package org.jboss.messaging.core.server.impl;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.NO_ID_SET;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketReturner;
import org.jboss.messaging.core.remoting.impl.wireformat.ConsumerFlowCreditMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.server.ServerConsumer;

/**
 * 
 * A ServerConsumerPacketHandler
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ServerConsumerPacketHandler extends ServerPacketHandlerSupport
{
   private static final Logger log = Logger.getLogger(ServerConsumerPacketHandler.class);

	private final ServerConsumer consumer;
	
	public ServerConsumerPacketHandler(final ServerConsumer consumer)
	{
		this.consumer = consumer;
	}

   public long getID()
   {
      return consumer.getID();
   }

   public Packet doHandle(final Packet packet, final PacketReturner sender) throws Exception
   {
      Packet response = null;

      byte type = packet.getType();
      switch (type)
      {
      case PacketImpl.CONS_FLOWTOKEN:
         ConsumerFlowCreditMessage message = (ConsumerFlowCreditMessage) packet;
         consumer.receiveCredits(message.getTokens());
         break;
      case PacketImpl.CLOSE:
         consumer.close();
         break;
      default:
         throw new MessagingException(MessagingException.UNSUPPORTED_PACKET,
               "Unsupported packet " + type);
      }

      // reply if necessary
      if (response == null && packet.getResponseTargetID() != NO_ID_SET)
      {
         response = new PacketImpl(PacketImpl.NULL);               
      }
      
      return response;
   }
}
