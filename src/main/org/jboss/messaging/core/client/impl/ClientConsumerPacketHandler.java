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

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.remoting.impl.wireformat.ReceiveMessage;
import org.jboss.messaging.core.server.CommandManager;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * @version <tt>$Revision$</tt>
 *
 */
public class ClientConsumerPacketHandler implements PacketHandler
{
   private static final Logger log = Logger.getLogger(ClientConsumerPacketHandler.class);

   //private final ClientSessionInternal session;
   
   private final ClientConsumerInternal clientConsumer;

   private final long consumerID;
   
   private final CommandManager commandManager;
   
   public ClientConsumerPacketHandler(final ClientConsumerInternal clientConsumer,
                                      final long consumerID,
                                      final CommandManager commandManager)
                                      
   {
      this.clientConsumer = clientConsumer;
         
      this.consumerID = consumerID;
      
      this.commandManager = commandManager;
   }

   public long getID()
   {
      return consumerID;
   }

   public void handle(final Object connectionID, final Packet packet)
   {
      byte type = packet.getType();

      if (type == PacketImpl.RECEIVE_MSG)
      {
         ReceiveMessage message = (ReceiveMessage) packet;

         try
         {
            clientConsumer.handleMessage(message.getClientMessage());
         }
         catch (Exception e)
         {
            log.error("Failed to handle packet " + packet);
         }
         
         commandManager.packetProcessed(packet);
      }
      else
      {
      	throw new IllegalStateException("Invalid packet: " + type);
      }
   }

   @Override
   public String toString()
   {
      return "ClientConsumerPacketHandler[id=" + consumerID + "]";
   }

   @Override
   public boolean equals(Object other)
   {
      if (other instanceof ClientConsumerPacketHandler == false)
      {
         return false;
      }

      ClientConsumerPacketHandler r = (ClientConsumerPacketHandler)other;

      return r.consumerID == consumerID;
   }
}