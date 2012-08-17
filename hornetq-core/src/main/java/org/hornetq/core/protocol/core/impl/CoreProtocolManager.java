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
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;

import org.hornetq.api.core.HornetQAlreadyReplicatingException;
import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQException;
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
import org.hornetq.core.protocol.core.impl.wireformat.BackupReplicationStartFailedMessage;
import org.hornetq.core.protocol.core.impl.wireformat.BackupRegistrationMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ClusterTopologyChangeMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ClusterTopologyChangeMessage_V2;
import org.hornetq.core.protocol.core.impl.wireformat.NodeAnnounceMessage;
import org.hornetq.core.protocol.core.impl.wireformat.Ping;
import org.hornetq.core.protocol.core.impl.wireformat.SubscribeClusterTopologyUpdatesMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SubscribeClusterTopologyUpdatesMessageV2;
import org.hornetq.core.remoting.CloseListener;
import org.hornetq.core.server.HornetQLogger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.spi.core.protocol.ConnectionEntry;
import org.hornetq.spi.core.protocol.ProtocolManager;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.spi.core.remoting.Acceptor;
import org.hornetq.spi.core.remoting.Connection;

/**
 * A CoreProtocolManager
 *
 * @author Tim Fox
 *
 *
 */
class CoreProtocolManager implements ProtocolManager
{
   private static final boolean isTrace = HornetQLogger.LOGGER.isTraceEnabled();

   private final HornetQServer server;

   private final List<Interceptor> interceptors;

   CoreProtocolManager(final HornetQServer server, final List<Interceptor> interceptors)
   {
      this.server = server;

      this.interceptors = interceptors;
   }

   public ConnectionEntry createConnectionEntry(final Acceptor acceptorUsed, final Connection connection)
   {
      final Configuration config = server.getConfiguration();

      Executor connectionExecutor = server.getExecutorFactory().getExecutor();

      final CoreRemotingConnection rc = new RemotingConnectionImpl(connection,
                                                                   interceptors,
                                                                   config.isAsyncConnectionExecutionEnabled() ? connectionExecutor
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

      final ConnectionEntry entry = new ConnectionEntry(rc, connectionExecutor, System.currentTimeMillis(), ttl);

      final Channel channel0 = rc.getChannel(0, -1);

      channel0.setHandler(new LocalChannelHandler(config, entry, channel0, acceptorUsed, rc));

      return entry;
   }

   private final Map<String, ServerSessionPacketHandler> sessionHandlers = new ConcurrentHashMap<String, ServerSessionPacketHandler>();

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

   private class LocalChannelHandler implements ChannelHandler
   {
      private final Configuration config;
      private final ConnectionEntry entry;
      private final Channel channel0;
      private final Acceptor acceptorUsed;
      private final CoreRemotingConnection rc;

      public LocalChannelHandler(final Configuration config, final ConnectionEntry entry,
             final Channel channel0, final Acceptor acceptorUsed, final CoreRemotingConnection rc)
      {
         this.config = config;
         this.entry = entry;
         this.channel0 = channel0;
         this.acceptorUsed = acceptorUsed;
         this.rc = rc;
      }

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
         else if (packet.getType() == PacketImpl.SUBSCRIBE_TOPOLOGY || packet.getType() == PacketImpl.SUBSCRIBE_TOPOLOGY_V2)
         {
            SubscribeClusterTopologyUpdatesMessage msg = (SubscribeClusterTopologyUpdatesMessage)packet;

            if (packet.getType() == PacketImpl.SUBSCRIBE_TOPOLOGY_V2)
            {
               channel0.getConnection().setClientVersion(((SubscribeClusterTopologyUpdatesMessageV2)msg).getClientVersion());
            }

            final ClusterTopologyListener listener = new ClusterTopologyListener()
            {
               public void nodeUP(final long uniqueEventID,
                                  final String nodeID,
                                  final String nodeName,
                                  final Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                                  final boolean last)
               {
                  try
                  {
                     // Using an executor as most of the notifications on the Topology
                     // may come from a channel itself
                     // What could cause deadlocks
                     entry.connectionExecutor.execute(new Runnable()
                     {
                        public void run()
                        {
                           if (channel0.supports(PacketImpl.CLUSTER_TOPOLOGY_V2))
                           {
                              channel0.send(new ClusterTopologyChangeMessage_V2(uniqueEventID, nodeID, nodeName, connectorPair, last));
                           }
                           else
                           {
                              channel0.send(new ClusterTopologyChangeMessage(nodeID, connectorPair, last));
                           }
                        }
                     });
                  }
                  catch (RejectedExecutionException ignored)
                  {
                     // this could happen during a shutdown and we don't care, if we lost a nodeDown during a shutdown
                     // what can we do anyways?
                  }

               }

               public void nodeDown(final long uniqueEventID, final String nodeID)
               {
                  // Using an executor as most of the notifications on the Topology
                  // may come from a channel itself
                  // What could cause deadlocks
                  try
                  {
                     entry.connectionExecutor.execute(new Runnable()
                     {
                        public void run()
                        {
                           if (channel0.supports(PacketImpl.CLUSTER_TOPOLOGY_V2))
                           {
                              channel0.send(new ClusterTopologyChangeMessage_V2(uniqueEventID, nodeID));
                           }
                           else
                           {
                              channel0.send(new ClusterTopologyChangeMessage(nodeID));
                           }
                        }
                     });
                  }
                  catch (RejectedExecutionException ignored)
                  {
                     // this could happen during a shutdown and we don't care, if we lost a nodeDown during a shutdown
                     // what can we do anyways?
                  }
               }

               @Override
               public String toString()
               {
                  return "Remote Proxy on channel " + Integer.toHexString(System.identityHashCode(this));
               }
            };

            if (acceptorUsed.getClusterConnection() != null)
            {
               acceptorUsed.getClusterConnection().addClusterTopologyListener(listener);

               rc.addCloseListener(new CloseListener()
               {
                  public void connectionClosed()
                  {
                     acceptorUsed.getClusterConnection().removeClusterTopologyListener(listener);
                  }
               });
            }
            else
            {
               // if not clustered, we send a single notification to the client containing the node-id where the server is connected to
               // This is done this way so Recovery discovery could also use the node-id for non-clustered setups
               entry.connectionExecutor.execute(new Runnable()
               {
                  public void run()
                  {
                     String nodeId = server.getNodeID().toString();
                     Pair<TransportConfiguration, TransportConfiguration> emptyConfig = new Pair<TransportConfiguration, TransportConfiguration>(null, null);
                     if (channel0.supports(PacketImpl.CLUSTER_TOPOLOGY_V2))
                     {
                        channel0.send(new ClusterTopologyChangeMessage_V2(System.currentTimeMillis(), nodeId, server.getConfiguration().getName(), emptyConfig, true));
                     }
                     else
                     {
                        channel0.send(new ClusterTopologyChangeMessage(nodeId, emptyConfig, true));
                     }
                  }
               });

            }
         }
         else if (packet.getType() == PacketImpl.NODE_ANNOUNCE)
         {
            NodeAnnounceMessage msg = (NodeAnnounceMessage)packet;

            Pair<TransportConfiguration, TransportConfiguration> pair;
            if (msg.isBackup())
            {
               pair = new Pair<TransportConfiguration, TransportConfiguration>(null, msg.getConnector());
            }
            else
            {
               pair = new Pair<TransportConfiguration, TransportConfiguration>(msg.getConnector(), msg.getBackupConnector());
            }
            if (isTrace)
            {
               HornetQLogger.LOGGER.trace("Server " + server + " receiving nodeUp from NodeID=" + msg.getNodeID() + ", pair=" + pair);
            }

            if (acceptorUsed != null)
            {
               ClusterConnection clusterConn = acceptorUsed.getClusterConnection();
               if (clusterConn != null)
               {
                  clusterConn.nodeAnnounced(msg.getCurrentEventID(), msg.getNodeID(), msg.getNodeName(), pair, msg.isBackup());
               }
               else
               {
                  HornetQLogger.LOGGER.debug("Cluster connection is null on acceptor = " + acceptorUsed);
               }
            }
            else
            {
               HornetQLogger.LOGGER.debug("there is no acceptor used configured at the CoreProtocolManager " + this);
            }
         } else if (packet.getType() == PacketImpl.BACKUP_REGISTRATION)
         {
            BackupRegistrationMessage msg = (BackupRegistrationMessage)packet;
            ClusterConnection clusterConnection = acceptorUsed.getClusterConnection();

            if (clusterConnection.verify(msg.getClusterUser(), msg.getClusterPassword()))
            {
               try
               {
                  server.startReplication(rc, clusterConnection, getPair(msg.getConnector(), true),
                                          msg.isFailBackRequest());
               }
               catch(HornetQAlreadyReplicatingException are)
               {
                  channel0.send(new BackupReplicationStartFailedMessage(BackupReplicationStartFailedMessage.BackupRegistrationProblem.ALREADY_REPLICATING));
               }
               catch (HornetQException e)
               {
                  channel0.send(new BackupReplicationStartFailedMessage(BackupReplicationStartFailedMessage.BackupRegistrationProblem.EXCEPTION));
               }
            }
            else
            {
               channel0.send(new BackupReplicationStartFailedMessage(BackupReplicationStartFailedMessage.BackupRegistrationProblem.AUTHENTICATION));
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
   }
}
