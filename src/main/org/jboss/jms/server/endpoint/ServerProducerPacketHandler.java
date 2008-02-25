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
package org.jboss.jms.server.endpoint;

import static org.jboss.messaging.core.remoting.wireformat.PacketType.CLOSE;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.CONS_FLOWTOKEN;
import static org.jboss.messaging.core.remoting.wireformat.PacketType.PROD_SEND;

import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.ConsumerFlowTokenMessage;
import org.jboss.messaging.core.remoting.wireformat.NullPacket;
import org.jboss.messaging.core.remoting.wireformat.Packet;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.core.remoting.wireformat.ProducerSendMessage;
import org.jboss.messaging.util.MessagingException;


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

      PacketType type = packet.getType();
      
      if (type == PROD_SEND)
      {
         ProducerSendMessage message = (ProducerSendMessage) packet;
         
         producer.send(message.getAddress(), message.getMessage());
      }
      else if (type == CLOSE)
      {
         producer.close();
      }
      else
      {
         throw new MessagingException(MessagingException.UNSUPPORTED_PACKET,
               "Unsupported packet " + type);
      }

      // reply if necessary
      if (response == null && packet.isOneWay() == false)
      {
         response = new NullPacket();               
      }
      
      return response;
   }

   @Override
   public String toString()
   {
      return "ServerConsumerEndpointPacketHandler[id=" + producer.getID() + "]";
   }
}
