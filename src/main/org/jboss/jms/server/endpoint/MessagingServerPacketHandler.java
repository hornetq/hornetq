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

import static org.jboss.messaging.core.remoting.wireformat.PacketType.CREATECONNECTION;

import org.jboss.jms.client.impl.ClientConnectionFactoryImpl;
import org.jboss.logging.Logger;
import org.jboss.messaging.core.MessagingServer;
import org.jboss.messaging.core.remoting.PacketSender;
import org.jboss.messaging.core.remoting.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.wireformat.Packet;
import org.jboss.messaging.core.remoting.wireformat.PacketType;
import org.jboss.messaging.util.MessagingException;

/**
 * A packet handler for all packets that need to be handled at the server level
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class MessagingServerPacketHandler extends ServerPacketHandlerSupport
{
   private static final Logger log = Logger.getLogger(MessagingServerPacketHandler.class);
   
   private final MessagingServer server;

   public MessagingServerPacketHandler(final MessagingServer server)
   {
      this.server = server;
   }
   
   /*
   * The advantage to use String as ID is that we can leverage Java 5 UUID to
   * generate these IDs. However theses IDs are 128 bite long and it increases
   * the size of a packet (compared to integer or long).
   *
   * By switching to Long, we could reduce the size of the packet and maybe
   * increase the performance (to check after some performance tests)
   */
   public String getID()
   {
      return ClientConnectionFactoryImpl.id;
   }

   public Packet doHandle(final Packet packet, final PacketSender sender) throws Exception
   {
      Packet response = null;
     
      PacketType type = packet.getType();
      
      if (type == CREATECONNECTION)
      {
         CreateConnectionRequest request = (CreateConnectionRequest) packet;
         
         response = server.createConnection(request.getUsername(), request.getPassword(),
         		                             request.getRemotingSessionID(),
                                            request.getClientVMID(), request.getPrefetchSize(),
                                            sender.getRemoteAddress());
      }     
      else
      {
         throw new MessagingException(MessagingException.UNSUPPORTED_PACKET,
                                      "Unsupported packet " + type);
      }
      
      return response;
   }
  
}