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

package org.hornetq.core.protocol.core.impl;

import static org.hornetq.core.protocol.core.impl.PacketImpl.CREATESESSION;
import static org.hornetq.core.protocol.core.impl.PacketImpl.CREATE_QUEUE;
import static org.hornetq.core.protocol.core.impl.PacketImpl.CREATE_REPLICATION;
import static org.hornetq.core.protocol.core.impl.PacketImpl.REATTACH_SESSION;

import org.hornetq.api.core.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.ChannelHandler;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.ServerSessionPacketHandler;
import org.hornetq.core.protocol.core.impl.wireformat.CreateQueueMessage;
import org.hornetq.core.protocol.core.impl.wireformat.CreateReplicationSessionMessage;
import org.hornetq.core.protocol.core.impl.wireformat.CreateSessionMessage;
import org.hornetq.core.protocol.core.impl.wireformat.CreateSessionResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.HornetQExceptionMessage;
import org.hornetq.core.protocol.core.impl.wireformat.NullResponseMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReattachSessionMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ReattachSessionResponseMessage;
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
         int[] compatibleList = version.getCompatibleVersionList();
         boolean isCompatibleClient = false;
         for (int i = 0; i < compatibleList.length; i++)
         {
            if (compatibleList[i] == request.getVersion())
            {
               isCompatibleClient = true;
               break;
            }
         }

         if (!isCompatibleClient)
         {
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

         if (connection.getClientVersion() == 0)
         {
            connection.setClientVersion(request.getVersion());
         }
         else if (connection.getClientVersion() != request.getVersion())
         {
            log.debug("Client is not being consistent on the request versioning. " +
                     "It just sent a version id=" + request.getVersion() +
                     " while it informed " + connection.getClientVersion() + " previously");
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
                                                      request.isXA(),
                                                      request.getDefaultAddress(),
                                                      new CoreSessionCallback(request.getName(),
                                                                              protocolManager,
                                                                              channel));

         session.setSessionContext(server.getStorageManager().newContext(server.getExecutorFactory().getExecutor()));

         ServerSessionPacketHandler handler = new ServerSessionPacketHandler(session,
                                                                             server.getStorageManager(),
                                                                             channel);
         channel.setHandler(handler);

         // TODO - where is this removed?
         protocolManager.addSessionHandler(request.getName(), handler);

         response = new CreateSessionResponseMessage(server.getVersion().getIncrementingVersion());
      }
      catch (HornetQException e)
      {

         if (e.getCode() == HornetQException.INCOMPATIBLE_CLIENT_SERVER_VERSIONS)
         {
            log.debug("Sending HornetQException after Incompatible client", e);
            incompatibleVersion = true;
         }
         else
         {
            log.error("Failed to create session ", e);
         }

         response = new HornetQExceptionMessage((HornetQException)e);
      }
      catch (Exception e)
      {
         log.error("Failed to create session ", e);

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

         log.debug("Reattaching request from " +  connection.getRemoteAddress());


         ServerSessionPacketHandler sessionHandler = protocolManager.getSessionHandler(request.getName());

         if (!server.checkActivate() || sessionHandler == null)
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

               log.warn("Reattach request from " + connection.getRemoteAddress() + " failed as there is no confirmationWindowSize configured, which may be ok for your system");

               sessionHandler.closeListeners();
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