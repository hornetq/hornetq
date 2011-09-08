/*
 * Copyright 2010 Red Hat, Inc.
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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.ChannelHandler;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.ServerSessionPacketHandler;
import org.hornetq.core.protocol.core.impl.ChannelImpl.CHANNEL_ID;
import org.hornetq.core.protocol.core.impl.wireformat.BackupRegistrationMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ClusterTopologyChangeMessage;
import org.hornetq.core.protocol.core.impl.wireformat.NodeAnnounceMessage;
import org.hornetq.core.protocol.core.impl.wireformat.Ping;
import org.hornetq.core.protocol.core.impl.wireformat.SubscribeClusterTopologyUpdatesMessage;
import org.hornetq.core.remoting.CloseListener;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.spi.core.protocol.ConnectionEntry;
import org.hornetq.spi.core.protocol.ProtocolManager;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.Connection;

/**
 * A CoreProtocolManager
 *
 * @author Tim Fox
 *
 *
 */
public class CoreProtocolManager implements ProtocolManager
{
   private final HornetQServer server;

   private final List<Interceptor> interceptors;

   private final Map<String, ServerSessionPacketHandler> sessionHandlers =
            new ConcurrentHashMap<String, ServerSessionPacketHandler>();

   public CoreProtocolManager(final HornetQServer server, final List<Interceptor> interceptors)
   {
      this.server = server;

      this.interceptors = interceptors;
   }

   public ConnectionEntry createConnectionEntry(final Connection connection)
   {
      final Configuration config = server.getConfiguration();

      final CoreRemotingConnection rc = new RemotingConnectionImpl(connection,
                                                                   interceptors,
                                                                   config.isAsyncConnectionExecutionEnabled() ? server.getExecutorFactory()
                                                                                                                      .getExecutor()
                                                                                                             : null,
                                                                                                             server.getNodeID());

      Channel channel1 = rc.getChannel(CHANNEL_ID.SESSION.id, -1);

      ChannelHandler handler = new HornetQPacketHandler(this, server, channel1, rc);

      channel1.setHandler(handler);

      long ttl = HornetQClient.DEFAULT_CONNECTION_TTL;

      if (config.getConnectionTTLOverride() != -1)
      {
         ttl = config.getConnectionTTLOverride();
      }

      final ConnectionEntry entry = new ConnectionEntry(rc, System.currentTimeMillis(), ttl);

      final Channel channel0 = rc.getChannel(CHANNEL_ID.PING.id, -1);

      channel0.setHandler(new ChannelHandler()
      {
         public void handlePacket(final Packet packet)
         {
            if (packet.getType() == PacketImpl.PING)
            {
               Ping ping = (Ping)packet;

               if (config.getConnectionTTLOverride() == -1)
               {
                  // Allow clients to specify connection ttl
                  entry.ttl = ping.getConnectionTTL();
               }

               // Just send a ping back
               channel0.send(packet);
            }
            else if (packet.getType() == PacketImpl.SUBSCRIBE_TOPOLOGY)
            {
               SubscribeClusterTopologyUpdatesMessage msg = (SubscribeClusterTopologyUpdatesMessage)packet;

               final ClusterTopologyListener listener = new ClusterTopologyListener()
               {
                  public void nodeUP(String nodeID, Pair<TransportConfiguration, TransportConfiguration> connectorPair, boolean last)
                  {
                     channel0.send(new ClusterTopologyChangeMessage(nodeID, connectorPair, last));
                  }

                  public void nodeDown(String nodeID)
                  {
                     channel0.send(new ClusterTopologyChangeMessage(nodeID));
                  }
               };

               final boolean isCC = msg.isClusterConnection();

               server.getClusterManager().addClusterTopologyListener(listener, isCC);

               rc.addCloseListener(new CloseListener()
               {
                  public void connectionClosed()
                  {
                     server.getClusterManager().removeClusterTopologyListener(listener, isCC);
                  }
               });
            }
            else if (packet.getType() == PacketImpl.NODE_ANNOUNCE)
            {
               NodeAnnounceMessage msg = (NodeAnnounceMessage)packet;
               final boolean backup = msg.isBackup();
               server.getClusterManager().notifyNodeUp(msg.getNodeID(), getPair(msg.getConnector(), backup), backup,
                                                       true);
            }
            else if (packet.getType() == PacketImpl.BACKUP_REGISTRATION)
            {
               BackupRegistrationMessage msg = (BackupRegistrationMessage)packet;
               if (server.startReplication(rc))
               {
                  server.getClusterManager().notifyNodeUp(msg.getNodeID(), getPair(msg.getConnector(), true), true,
                                                          true);
               }
            }
         }

         private Pair<TransportConfiguration, TransportConfiguration> getPair(TransportConfiguration conn,
                                                                              boolean isBackup)
         {
            if (isBackup)
            {
               return new Pair<TransportConfiguration, TransportConfiguration>(null, conn);
            }
            return new Pair<TransportConfiguration, TransportConfiguration>(conn, null);
         }
      });



      return entry;
   }

   ServerSessionPacketHandler getSessionHandler(final String sessionName)
   {
      return sessionHandlers.get(sessionName);
   }

   void addSessionHandler(final String name, final ServerSessionPacketHandler handler)
   {
      sessionHandlers.put(name, handler);
   }

   public void removeHandler(final String name)
   {
      sessionHandlers.remove(name);
   }

   public void handleBuffer(RemotingConnection connection, HornetQBuffer buffer)
   {
   }

   // This is never called using the core protocol, since we override the HornetQFrameDecoder with our core
   // optimised version HornetQFrameDecoder2, which never calls this
   public int isReadyToHandle(HornetQBuffer buffer)
   {
      return -1;
   }

   @Override
   public String toString()
   {
      return "CoreProtocolManager(server=" + server + ")";
   }
}
