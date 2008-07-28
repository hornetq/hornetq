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

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.CREATECONNECTION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.PING;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.PONG;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.PacketHandler;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.RemotingService;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateConnectionRequest;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl;
import org.jboss.messaging.core.server.MessagingServer;

/**
 * A packet handler for all packets that need to be handled at the server level
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class MessagingServerPacketHandler implements PacketHandler
{
   private static final Logger log = Logger.getLogger(MessagingServerPacketHandler.class);

   private final MessagingServer server;
   
   private final RemotingService remotingService;

   public MessagingServerPacketHandler(final MessagingServer server, final RemotingService remotingService)
   {
      this.server = server;
      
      this.remotingService = remotingService;
   }

   public long getID()
   {
      //0 is reserved for this handler
      return 0;
   }
   
   public void handle(final long connectionID, final Packet packet)
   {
      Packet response = null;
            
      RemotingConnection connection = remotingService.getConnection(connectionID);

      byte type = packet.getType();
      
      try
      {
         if (type == PING)
         {
            response = new PacketImpl(PONG);
         }
         else if (type == CREATECONNECTION)
         {            
            CreateConnectionRequest request = (CreateConnectionRequest) packet;
   
            response =
               server.createConnection(request.getUsername(), request.getPassword(),                             
                                       request.getVersion(),
                                       connection);               
         }
         else
         {
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
      
      response.normalize(packet);
      
      connection.sendOneWay(response);
   }

}