/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.server.impl;

import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.CREATESESSION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.REATTACH_SESSION;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.ChannelHandler;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ReattachSessionMessage;
import org.jboss.messaging.core.server.MessagingServer;

/**
 * A packet handler for all packets that need to be handled at the server level
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class MessagingServerPacketHandler implements ChannelHandler
{
   private static final Logger log = Logger.getLogger(MessagingServerPacketHandler.class);

   private final MessagingServer server;

   private final Channel channel1;

   private final RemotingConnection connection;

   public MessagingServerPacketHandler(final MessagingServer server,
                                       final Channel channel1,
                                       final RemotingConnection connection)
   {
      this.server = server;

      this.channel1 = channel1;

      this.connection = connection;
   }

   public void handlePacket(final Packet packet)
   {            
      Packet response = null;

      byte type = packet.getType();

      // All these operations need to be idempotent since they are outside of the session
      // reliability replay functionality
      try
      {
         channel1.replicatePacket(packet);
                  
         switch (type)
         {
            case CREATESESSION:
            {
               CreateSessionMessage request = (CreateSessionMessage)packet;
   
               response = server.createSession(request.getName(),
                                               request.getSessionChannelID(),
                                               request.getUsername(),
                                               request.getPassword(),
                                               request.getVersion(),
                                               connection,
                                               request.isAutoCommitSends(),
                                               request.isAutoCommitAcks(),
                                               request.isXA());
               break;
            }
            case REATTACH_SESSION:
            {
               ReattachSessionMessage request = (ReattachSessionMessage)packet;
   
               response = server.reattachSession(connection, request.getName(), request.getLastReceivedCommandID());
               
               break;
            }
            default:
            {
               response = new MessagingExceptionMessage(new MessagingException(MessagingException.UNSUPPORTED_PACKET,
                                                                               "Unsupported packet " + type));
            }
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

      channel1.send(response);
   }
}