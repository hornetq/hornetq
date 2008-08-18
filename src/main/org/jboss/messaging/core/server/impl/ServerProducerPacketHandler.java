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

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.ProducerSendMessage;
import org.jboss.messaging.core.server.ServerProducer;

/**
 *
 * A ServerProducerPacketHandler
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ServerProducerPacketHandler implements PacketHandler
{
   private static final Logger log = Logger.getLogger(ServerProducerPacketHandler.class);

	private final ServerProducer producer;

	private final RemotingConnection remotingConnection;

	public ServerProducerPacketHandler(final ServerProducer producer, final RemotingConnection remotingConnection)
	{
		this.producer = producer;

		this.remotingConnection = remotingConnection;
	}

   public long getID()
   {
      return producer.getID();
   }

   public void handle(final Object remotingConnectionID, final Packet packet)
   {
      Packet response = null;

      byte type = packet.getType();

      try
      {
         switch (type)
         {
         case PacketImpl.PROD_SEND:
            ProducerSendMessage message = (ProducerSendMessage) packet;
            producer.send(message.getServerMessage());
            if (packet.getResponseTargetID() != PacketImpl.NO_ID_SET)
            {
               response = new PacketImpl(PacketImpl.NULL);
            }
            break;
         case PacketImpl.CLOSE:
            producer.close();
            response = new PacketImpl(PacketImpl.NULL);
            break;
         default:
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.UNSUPPORTED_PACKET,
                                                     "Unsupported packet " + type));
         }
      }
      catch (Throwable t)
      {
         MessagingException me;

         log.error("Caught unexpected exception", t);

         if (t instanceof MessagingException)
         {
            me = (MessagingException)t;
         }
         else
         {
            me = new MessagingException(MessagingException.INTERNAL_ERROR);
         }

         response = new MessagingExceptionMessage(me);
      }

      if (response != null)
      {
         response.normalize(packet);

         remotingConnection.sendOneWay(response);
      }
   }
}
