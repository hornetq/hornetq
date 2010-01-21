/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.core.protocol.core;

import static org.hornetq.core.protocol.core.PacketImpl.CREATESESSION;
import static org.hornetq.core.protocol.core.PacketImpl.CREATE_QUEUE;
import static org.hornetq.core.protocol.core.PacketImpl.CREATE_REPLICATION;
import static org.hornetq.core.protocol.core.PacketImpl.REATTACH_SESSION;

import org.hornetq.api.core.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.core.wireformat.CreateQueueMessage;
import org.hornetq.core.protocol.core.wireformat.CreateReplicationSessionMessage;
import org.hornetq.core.protocol.core.wireformat.CreateSessionMessage;
import org.hornetq.core.protocol.core.wireformat.CreateSessionResponseMessage;
import org.hornetq.core.protocol.core.wireformat.HornetQExceptionMessage;
import org.hornetq.core.protocol.core.wireformat.NullResponseMessage;
import org.hornetq.core.protocol.core.wireformat.ReattachSessionMessage;
import org.hornetq.core.protocol.core.wireformat.ReattachSessionResponseMessage;
import org.hornetq.core.replication.ReplicationEndpoint;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.ServerSession;
import org.hornetq.core.version.Version;

/**
 * A packet handler for all packets that need to be handled at the server level
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class HornetQPacketHandler implements ChannelHandler
{
   private static final Logger log = Logger.getLogger(HornetQPacketHandler.class);

   private final HornetQServer server;

   private final Channel channel1;

   private final CoreRemotingConnection connection;

   private final CoreProtocolManager protocolManager;

   public HornetQPacketHandler(final CoreProtocolManager protocolManager,
                               final HornetQServer server,
                               final Channel channel1,
                               final CoreRemotingConnection connection)
   {
      this.protocolManager = protocolManager;

      this.server = server;

      this.channel1 = channel1;

      this.connection = connection;
   }

   public void handlePacket(final Packet packet)
   {
      byte type = packet.getType();

      switch (type)
      {
         case CREATESESSION:
         {
            CreateSessionMessage request = (CreateSessionMessage)packet;

            handleCreateSession(request);

            break;
         }
         case REATTACH_SESSION:
         {
            ReattachSessionMessage request = (ReattachSessionMessage)packet;

            handleReattachSession(request);

            break;
         }
         case CREATE_QUEUE:
         {
            // Create queue can also be fielded here in the case of a replicated store and forward queue creation

            CreateQueueMessage request = (CreateQueueMessage)packet;

            handleCreateQueue(request);

            break;
         }
         case CREATE_REPLICATION:
         {
            // Create queue can also be fielded here in the case of a replicated store and forward queue creation

            CreateReplicationSessionMessage request = (CreateReplicationSessionMessage)packet;

            handleCreateReplication(request);

            break;
         }
         default:
         {
            HornetQPacketHandler.log.error("Invalid packet " + packet);
         }
      }
   }

   private void handleCreateSession(final CreateSessionMessage request)
   {
      boolean incompatibleVersion = false;
      Packet response;
      try
      {
         Version version = server.getVersion();

         if (version.getIncrementingVersion() != request.getVersion())
         {
            log.warn("Client with version " + request.getVersion() +
                     " and address " +
                     connection.getRemoteAddress() +
                     " is not compatible with server version " +
                     version.getFullVersion() +
                     ". " +
                     "Please ensure all clients and servers are upgraded to the same version for them to " +
                     "interoperate properly");
            throw new HornetQException(HornetQException.INCOMPATIBLE_CLIENT_SERVER_VERSIONS,
                                       "Server and client versions incompatible");
         }

         if (!server.isStarted())
         {
            throw new HornetQException(HornetQException.SESSION_CREATION_REJECTED, "Server not started");
         }

         if (!server.checkActivate())
         {
            throw new HornetQException(HornetQException.SESSION_CREATION_REJECTED,
                                       "Server will not accept create session requests");
         }

         Channel channel = connection.getChannel(request.getSessionChannelID(), request.getWindowSize());

         ServerSession session = server.createSession(request.getName(),                                                      
                                                      request.getUsername(),
                                                      request.getPassword(),
                                                      request.getMinLargeMessageSize(),                                                    
                                                      connection,
                                                      request.isAutoCommitSends(),
                                                      request.isAutoCommitAcks(),
                                                      request.isPreAcknowledge(),
                                                      request.isXA());

         ServerSessionPacketHandler handler = new ServerSessionPacketHandler(protocolManager,
                                                                             session,
                                                                             server.getStorageManager()
                                                                                   .newContext(server.getExecutorFactory()
                                                                                                     .getExecutor()),
                                                                             server.getStorageManager(),
                                                                             channel);

         session.setCallback(handler);

         channel.setHandler(handler);

         // TODO - where is this removed?
         protocolManager.addSessionHandler(request.getName(), handler);

         response = new CreateSessionResponseMessage(server.getVersion().getIncrementingVersion());
      }
      catch (HornetQException e)
      {
         response = new HornetQExceptionMessage((HornetQException)e);

         if (e.getCode() == HornetQException.INCOMPATIBLE_CLIENT_SERVER_VERSIONS)
         {
            incompatibleVersion = true;
         }
      }
      catch (Exception e)
      {
         HornetQPacketHandler.log.error("Failed to create session", e);
         
         response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
      }

      // send the exception to the client and destroy
      // the connection if the client and server versions
      // are not compatible
      if (incompatibleVersion)
      {
         channel1.sendAndFlush(response);
      }
      else
      {
         channel1.send(response);
      }
   }

   private void handleReattachSession(final ReattachSessionMessage request)
   {
      Packet response = null;
      
      try
      {
   
         if (!server.isStarted())
         {
            response = new ReattachSessionResponseMessage(-1, false);
         }
   
         ServerSessionPacketHandler sessionHandler = protocolManager.getSessionHandler(request.getName());
         
         if (!server.checkActivate())
         {
            response = new ReattachSessionResponseMessage(-1, false);
         }
   
         if (sessionHandler == null)
         {
            response = new ReattachSessionResponseMessage(-1, false);
         }
         else
         {
            if (sessionHandler.getChannel().getConfirmationWindowSize() == -1)
            {
               // Even though session exists, we can't reattach since confi window size == -1,
               // i.e. we don't have a resend cache for commands, so we just close the old session
               // and let the client recreate
   
               sessionHandler.close();
   
               response = new ReattachSessionResponseMessage(-1, false);
            }
            else
            {
               // Reconnect the channel to the new connection
               int serverLastConfirmedCommandID = sessionHandler.transferConnection(connection,
                                                                                    request.getLastConfirmedCommandID());
   
               response = new ReattachSessionResponseMessage(serverLastConfirmedCommandID, true);
            }
         }
      }
      catch (Exception e)
      {
         HornetQPacketHandler.log.error("Failed to reattach session", e);
         
         response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
      }

      channel1.send(response);
   }

   private void handleCreateQueue(final CreateQueueMessage request)
   {
      try
      {
         server.createQueue(request.getAddress(),
                            request.getQueueName(),
                            request.getFilterString(),
                            request.isDurable(),
                            request.isTemporary());
      }
      catch (Exception e)
      {
         HornetQPacketHandler.log.error("Failed to handle create queue", e);
      }
   }

   private void handleCreateReplication(final CreateReplicationSessionMessage request)
   {
      Packet response;

      try
      {
         Channel channel = connection.getChannel(request.getSessionChannelID(), -1);

         ReplicationEndpoint endpoint = server.connectToReplicationEndpoint(channel);

         channel.setHandler(endpoint);

         response = new NullResponseMessage();
      }
      catch (Exception e)
      {
         if (e instanceof HornetQException)
         {
            response = new HornetQExceptionMessage((HornetQException)e);
         }
         else
         {
            HornetQPacketHandler.log.warn(e.getMessage(), e);
            response = new HornetQExceptionMessage(new HornetQException(HornetQException.INTERNAL_ERROR));
         }
      }

      channel1.send(response);
   }

}