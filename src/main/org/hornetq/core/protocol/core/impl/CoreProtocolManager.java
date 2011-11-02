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

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.ChannelHandler;
import org.hornetq.core.protocol.core.CoreRemotingConnection;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.ServerSessionPacketHandler;
import org.hornetq.core.protocol.core.impl.wireformat.ClusterTopologyChangeMessage;
import org.hornetq.core.protocol.core.impl.wireformat.ClusterTopologyChangeMessage_V2;
import org.hornetq.core.protocol.core.impl.wireformat.NodeAnnounceMessage;
import org.hornetq.core.protocol.core.impl.wireformat.Ping;
import org.hornetq.core.protocol.core.impl.wireformat.SubscribeClusterTopologyUpdatesMessage;
import org.hornetq.core.protocol.core.impl.wireformat.SubscribeClusterTopologyUpdatesMessageV2;
import org.hornetq.core.remoting.CloseListener;
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
public class CoreProtocolManager implements ProtocolManager
{
   private static final Logger log = Logger.getLogger(CoreProtocolManager.class);
   
   private static final boolean isTrace = log.isTraceEnabled();
   
   private final HornetQServer server;

   private final List<Interceptor> interceptors;

   public CoreProtocolManager(final HornetQServer server, final List<Interceptor> interceptors)
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

      Channel channel1 = rc.getChannel(1, -1);

      ChannelHandler handler = new HornetQPacketHandler(this, server, channel1, rc);

      channel1.setHandler(handler);

      long ttl = HornetQClient.DEFAULT_CONNECTION_TTL;

      if (config.getConnectionTTLOverride() != -1)
      {
         ttl = config.getConnectionTTLOverride();
      }

      final ConnectionEntry entry = new ConnectionEntry(rc, connectionExecutor, System.currentTimeMillis(), ttl);

      final Channel channel0 = rc.getChannel(0, -1);

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
                                     final Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                                     final boolean last)
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
                              channel0.send(new ClusterTopologyChangeMessage_V2(uniqueEventID, nodeID, connectorPair, last));
                           }
                           else
                           {
                              channel0.send(new ClusterTopologyChangeMessage(nodeID, connectorPair, last));
                           }
                        }
                     });
                   }

                  public void nodeDown(final long uniqueEventID, final String nodeID)
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
                              channel0.send(new ClusterTopologyChangeMessage_V2(uniqueEventID, nodeID));
                           }
                           else
                           {
                              channel0.send(new ClusterTopologyChangeMessage(nodeID));
                           }
                        }
                     });
                  }
                  
                  public String toString()
                  {
                     return "Remote Proxy on channel " + Integer.toHexString(System.identityHashCode(this));
                  }
               };
               
               final boolean isCC = msg.isClusterConnection();
               if (acceptorUsed.getClusterConnection() != null)
               {
                  acceptorUsed.getClusterConnection().addClusterTopologyListener(listener, isCC);
                  
                  rc.addCloseListener(new CloseListener()
                  {
                     public void connectionClosed()
                     {
                        acceptorUsed.getClusterConnection().removeClusterTopologyListener(listener, isCC);
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
                  log.trace("Server " + server + " receiving nodeUp from NodeID=" + msg.getNodeID() + ", pair=" + pair);
               }
               
               if (acceptorUsed != null)
               {
                  ClusterConnection clusterConn = acceptorUsed.getClusterConnection();
                  if (clusterConn != null)
                  {
                     clusterConn.nodeAnnounced(msg.getCurrentEventID(), msg.getNodeID(), pair, msg.isBackup());
                  }
                  else
                  {
                     log.debug("Cluster connection is null on acceptor = " + acceptorUsed);
                  }
               }
               else
               {
                  log.debug("there is no acceptor used configured at the CoreProtocolManager " + this);
               }
            }
         }
      });

      return entry;
   }

   private Map<String, ServerSessionPacketHandler> sessionHandlers = new ConcurrentHashMap<String, ServerSessionPacketHandler>();

   public ServerSessionPacketHandler getSessionHandler(final String sessionName)
   {
      return sessionHandlers.get(sessionName);
   }

   public void addSessionHandler(final String name, final ServerSessionPacketHandler handler)
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
   // optimised version HornetQFrameDecoder2, which nevers calls this
   public int isReadyToHandle(HornetQBuffer buffer)
   {
      return -1;
   }
}
