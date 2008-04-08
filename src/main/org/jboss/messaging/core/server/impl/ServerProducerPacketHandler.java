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

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.CLOSE;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.NULL;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketType.PROD_SEND;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.Packet;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerSendMessage;
import org.jboss.messaging.core.server.ServerProducer;

/**
 * 
 * A ServerProducerPacketHandler
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ServerProducerPacketHandler extends ServerPacketHandlerSupport
{
	private final ServerProducer producer;
	
	public ServerProducerPacketHandler(final ServerProducer producer)
	{
		this.producer = producer;
	}

   public String getID()
   {
      return producer.getID();
   }

   public Packet doHandle(final Packet packet, final PacketSender sender) throws Exception
   {
      Packet response = null;

      byte type = packet.getType();
      switch (type)
      {
      case PROD_SEND:
         ProducerSendMessage message = (ProducerSendMessage) packet;
         producer.send(message.getAddress(), message.getMessage());
         break;
      case CLOSE:
         producer.close();
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

   @Override
   public String toString()
   {
      return "ServerConsumerEndpointPacketHandler[id=" + producer.getID() + "]";
   }
}
