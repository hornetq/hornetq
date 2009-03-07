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
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.REPLICATE_CREATESESSION;
import static org.jboss.messaging.core.remoting.impl.wireformat.PacketImpl.REPLICATE_UPDATE_CONNECTORS;

import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.ChannelHandler;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.wireformat.CreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.MessagingExceptionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ReattachSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.ReplicateCreateSessionMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.replication.ReplicateClusterConnectionUpdate;
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
      byte type = packet.getType();

      // All these operations need to be idempotent since they are outside of the session
      // reliability replay functionality
      switch (type)
      {
         case CREATESESSION:
         {
            CreateSessionMessage request = (CreateSessionMessage)packet;

            handleCreateSession(request);

            break;
         }
         case REPLICATE_CREATESESSION:
         {
            ReplicateCreateSessionMessage request = (ReplicateCreateSessionMessage)packet;

            handleReplicateCreateSession(request);

            break;
         }
         case REATTACH_SESSION:
         {
            ReattachSessionMessage request = (ReattachSessionMessage)packet;

            handleReattachSession(request);

            break;
         }
         case REPLICATE_UPDATE_CONNECTORS:
         {
            ReplicateClusterConnectionUpdate request = (ReplicateClusterConnectionUpdate)packet;
            
            handleClusterConnectionUpdate(request);
            
            break;
            
         }
         default:
         {
            log.error("Invalid packet " + packet);
         }
      }
   }
   
   private void doHandleCreateSession(final CreateSessionMessage request, final long oppositeChannelID)
   {
      Packet response;
      try
      {
         response = server.createSession(request.getName(),
                                         request.getSessionChannelID(),
                                         oppositeChannelID,
                                         request.getUsername(),
                                         request.getPassword(),
                                         request.getMinLargeMessageSize(),
                                         request.getVersion(),
                                         connection,
                                         request.isAutoCommitSends(),
                                         request.isAutoCommitAcks(),
                                         request.isPreAcknowledge(),
                                         request.isXA(),
                                         request.getWindowSize());
      }
      catch (Exception e)
      {
         log.error("Failed to create session", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }
      
      channel1.send(response);
   }

   private void handleCreateSession(final CreateSessionMessage request)
   {
      Channel replicatingChannel = server.getReplicatingChannel();

      if (replicatingChannel == null)
      {
         doHandleCreateSession(request, -1);
      }
      else
      {
         final long replicatedChannelID = replicatingChannel.getConnection().generateChannelID();

         Packet replPacket = new ReplicateCreateSessionMessage(request.getName(),
                                                               replicatedChannelID,
                                                               request.getSessionChannelID(),
                                                               request.getVersion(),
                                                               request.getUsername(),
                                                               request.getPassword(),
                                                               request.getMinLargeMessageSize(),
                                                               request.isXA(),
                                                               request.isAutoCommitSends(),
                                                               request.isAutoCommitAcks(),
                                                               request.isPreAcknowledge(),
                                                               request.getWindowSize());

         replicatingChannel.replicatePacket(replPacket, 1, new Runnable()
         {
            public void run()
            {
               doHandleCreateSession(request, replicatedChannelID);
            }
         });
      }
   }

   private void handleReplicateCreateSession(final ReplicateCreateSessionMessage request)
   {
      Packet response;

      try
      {
         response = server.replicateCreateSession(request.getName(),
                                                  request.getReplicatedSessionChannelID(),
                                                  request.getOriginalSessionChannelID(),
                                                  request.getUsername(),
                                                  request.getPassword(),
                                                  request.getMinLargeMessageSize(),
                                                  request.getVersion(),
                                                  connection,
                                                  request.isAutoCommitSends(),
                                                  request.isAutoCommitAcks(),
                                                  request.isPreAcknowledge(),
                                                  request.isXA(),
                                                  request.getWindowSize());
      }
      catch (Exception e)
      {
         log.error("Failed to handle replicate create session", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }
      
      channel1.send(response);
   }
   
   private void handleReattachSession(final ReattachSessionMessage request)
   {
      Packet response;

      try
      {
         response = server.reattachSession(connection, request.getName(), request.getLastReceivedCommandID());
      }
      catch (Exception e)
      {
         log.error("Failed to reattach session", e);

         if (e instanceof MessagingException)
         {
            response = new MessagingExceptionMessage((MessagingException)e);
         }
         else
         {
            response = new MessagingExceptionMessage(new MessagingException(MessagingException.INTERNAL_ERROR));
         }
      }
      
      channel1.send(response);
   }
   
   private void handleClusterConnectionUpdate(final ReplicateClusterConnectionUpdate request)
   {
      try
      {
         server.updateClusterConnectionConnectors(request.getClusterConnectionName(), request.getConnectors());
      }
      catch (Exception e)
      {
         log.error("Failed to handle cluster connection update", e);
      }
   }

}