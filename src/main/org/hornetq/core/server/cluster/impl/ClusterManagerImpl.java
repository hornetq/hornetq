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

package org.hornetq.core.server.cluster.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.lang.reflect.Array;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.client.impl.Topology;
import org.hornetq.core.client.impl.TopologyMember;
import org.hornetq.core.config.*;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.protocol.core.impl.wireformat.NodeAnnounceMessage;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.cluster.Bridge;
import org.hornetq.core.server.cluster.BroadcastGroup;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.cluster.ClusterManager;
import org.hornetq.core.server.cluster.Transformer;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.UUID;

/**
 * A ClusterManagerImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 18 Nov 2008 09:23:49
 *
 *
 */
public class ClusterManagerImpl implements ClusterManager
{
   private static final Logger log = Logger.getLogger(ClusterManagerImpl.class);

   private final Map<String, BroadcastGroup> broadcastGroups = new HashMap<String, BroadcastGroup>();

   private final Map<String, Bridge> bridges = new HashMap<String, Bridge>();

   private final ExecutorFactory executorFactory;

   private final HornetQServer server;

   private final PostOffice postOffice;

   private final ScheduledExecutorService scheduledExecutor;

   private final ManagementService managementService;

   private final Configuration configuration;

   private final UUID nodeUUID;

   private volatile boolean started;

   private boolean backup;

   private final boolean clustered;

   // the cluster connections which links this node to other cluster nodes
   private final Map<String, ClusterConnection> clusterConnections = new HashMap<String, ClusterConnection>();

   // regular client listeners to be notified of cluster topology changes.
   // they correspond to regular clients using a HA ServerLocator
   private Set<ClusterTopologyListener> clientListeners = new ConcurrentHashSet<ClusterTopologyListener>();

   // cluster connections listeners to be notified of cluster topology changes
   // they correspond to cluster connections on *other nodes connected to this one*
   private Set<ClusterTopologyListener> clusterConnectionListeners = new ConcurrentHashSet<ClusterTopologyListener>();

   private Topology topology = new Topology();

   private volatile ServerLocatorInternal backupServerLocator;

   private final List<ServerLocatorInternal> clusterLocators = new ArrayList<ServerLocatorInternal>();

   private Executor executor;

   public ClusterManagerImpl(final ExecutorFactory executorFactory,
                             final HornetQServer server,
                             final PostOffice postOffice,
                             final ScheduledExecutorService scheduledExecutor,
                             final ManagementService managementService,
                             final Configuration configuration,
                             final UUID nodeUUID,
                             final boolean backup,
                             final boolean clustered)
   {
      if (nodeUUID == null)
      {
         throw new IllegalArgumentException("Node uuid is null");
      }

      this.executorFactory = executorFactory;

      executor = executorFactory.getExecutor();

      this.server = server;

      this.postOffice = postOffice;

      this.scheduledExecutor = scheduledExecutor;

      this.managementService = managementService;

      this.configuration = configuration;

      this.nodeUUID = nodeUUID;

      this.backup = backup;

      this.clustered = clustered;
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      if (clustered)
      {
         for (BroadcastGroupConfiguration config : configuration.getBroadcastGroupConfigurations())
         {
            deployBroadcastGroup(config);
         }

         for (ClusterConnectionConfiguration config : configuration.getClusterConfigurations())
         {
            deployClusterConnection(config);
         }

      }

      for (BridgeConfiguration config : configuration.getBridgeConfigurations())
      {
         deployBridge(config);
      }

      // Now announce presence

      if (clusterConnections.size() > 0)
      {
         announceNode();
      }

      started = true;
   }

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;
      }

      if (clustered)
      {
         for (BroadcastGroup group : broadcastGroups.values())
         {
            group.stop();
            managementService.unregisterBroadcastGroup(group.getName());
         }

         broadcastGroups.clear();

         for (ClusterConnection clusterConnection : clusterConnections.values())
         {
            clusterConnection.stop();
            managementService.unregisterCluster(clusterConnection.getName().toString());
         }

         clusterConnectionListeners.clear();
         clientListeners.clear();
         clusterConnections.clear();
         topology.clear();

      }

      for (Bridge bridge : bridges.values())
      {
         bridge.stop();
         managementService.unregisterBridge(bridge.getName().toString());
      }

      bridges.clear();

      if (backupServerLocator != null)
      {
         backupServerLocator.close();
         backupServerLocator = null;
      }

      for (ServerLocatorInternal clusterLocator : clusterLocators)
      {
         clusterLocator.close();
      }
      clusterLocators.clear();
      started = false;
   }

   public void notifyNodeDown(String nodeID)
   {
      if (nodeID.equals(nodeUUID.toString()))
      {
         return;
      }

      boolean removed = topology.removeMember(nodeID);

      if (removed)
      {

         for (ClusterTopologyListener listener : clientListeners)
         {
            listener.nodeDown(nodeID);
         }

         for (ClusterTopologyListener listener : clusterConnectionListeners)
         {
            listener.nodeDown(nodeID);
         }
      }
   }

   public void notifyNodeUp(final String nodeID,
                            final Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                            final boolean last,
                            final boolean nodeAnnounce)
   {
      TopologyMember member = new TopologyMember(connectorPair);
      boolean updated = topology.addMember(nodeID, member);

      if (!updated)
      {
         return;
      }

      for (ClusterTopologyListener listener : clientListeners)
      {
         listener.nodeUP(nodeID, member.getConnector(), last);
      }

      for (ClusterTopologyListener listener : clusterConnectionListeners)
      {
         listener.nodeUP(nodeID, member.getConnector(), last);
      }

      // if this is a node being announced we are hearing it direct from the nodes CM so need to inform our cluster
      // connections.
      if (nodeAnnounce)
      {
         for (ClusterConnection clusterConnection : clusterConnections.values())
         {
            clusterConnection.nodeAnnounced(nodeID, connectorPair);
         }
      }
   }

   public boolean isStarted()
   {
      return started;
   }

   public Map<String, Bridge> getBridges()
   {
      return new HashMap<String, Bridge>(bridges);
   }

   public Set<ClusterConnection> getClusterConnections()
   {
      return new HashSet<ClusterConnection>(clusterConnections.values());
   }

   public Set<BroadcastGroup> getBroadcastGroups()
   {
      return new HashSet<BroadcastGroup>(broadcastGroups.values());
   }

   public ClusterConnection getClusterConnection(final SimpleString name)
   {
      return clusterConnections.get(name.toString());
   }

   public void addClusterTopologyListener(final ClusterTopologyListener listener, final boolean clusterConnection)
   {
      synchronized (this)
      {
         if (clusterConnection)
         {
            this.clusterConnectionListeners.add(listener);
         }
         else
         {
            this.clientListeners.add(listener);
         }
      }

      // We now need to send the current topology to the client
      topology.sendTopology(listener);
   }

   public synchronized void removeClusterTopologyListener(final ClusterTopologyListener listener,
                                                          final boolean clusterConnection)
   {
      if (clusterConnection)
      {
         this.clusterConnectionListeners.remove(listener);
      }
      else
      {
         this.clientListeners.remove(listener);
      }
   }

   public Topology getTopology()
   {
      return topology;
   }

   // backup node becomes live
   public synchronized void activate()
   {
      if (backup)
      {
         backup = false;

         String nodeID = server.getNodeID().toString();

         TopologyMember member = topology.getMember(nodeID);
         // we swap the topology backup now = live
         if (member != null)
         {
            member.getConnector().a = member.getConnector().b;

            member.getConnector().b = null;
         }

         if (backupServerLocator != null)
         {
            // todo we could use the topology of this to preempt it arriving from the cc
            try
            {
               backupServerLocator.close();
            }
            catch (Exception e)
            {
               log.warn("problem closing backup session factory", e);
            }
            backupServerLocator = null;
         }

         for (BroadcastGroup broadcastGroup : broadcastGroups.values())
         {
            try
            {
               broadcastGroup.start();
               broadcastGroup.activate();
            }
            catch (Exception e)
            {
               log.warn("unable to start broadcast group " + broadcastGroup.getName(), e);
            }
         }

         for (ClusterConnection clusterConnection : clusterConnections.values())
         {
            try
            {
               clusterConnection.activate();
            }
            catch (Exception e)
            {
               log.warn("unable to start cluster connection " + clusterConnection.getName(), e);
            }
         }

         for (Bridge bridge : bridges.values())
         {
            try
            {
               bridge.start();
            }
            catch (Exception e)
            {
               log.warn("unable to start bridge " + bridge.getName(), e);
            }
         }

         for (ClusterTopologyListener listener : clientListeners)
         {
            listener.nodeUP(nodeID, member.getConnector(), false);
         }

         for (ClusterTopologyListener listener : clusterConnectionListeners)
         {
            listener.nodeUP(nodeID, member.getConnector(), false);
         }
      }
   }

   public void announceBackup() throws Exception
   {
      List<ClusterConnectionConfiguration> configs = this.configuration.getClusterConfigurations();
      if (!configs.isEmpty())
      {
         ClusterConnectionConfiguration config = configs.get(0);

         TransportConfiguration connector = configuration.getConnectorConfigurations().get(config.getConnectorName());

         if (connector == null)
         {
            log.warn("No connecor with name '" + config.getConnectorName() + "'. backup cannot be announced.");
            return;
         }
         announceBackup(config, connector);
      }
      else
      {
         log.warn("no cluster connections defined, unable to announce backup");
      }
   }

   private synchronized void announceNode()
   {
      // TODO does this really work with more than one cluster connection? I think not

      // Just take the first one for now
      ClusterConnection cc = clusterConnections.values().iterator().next();

      String nodeID = server.getNodeID().toString();

      TopologyMember member = topology.getMember(nodeID);

      if (member == null)
      {
         if (backup)
         {
            member = new TopologyMember(new Pair<TransportConfiguration, TransportConfiguration>(null,
                                                                                                 cc.getConnector()));
         }
         else
         {
            member = new TopologyMember(new Pair<TransportConfiguration, TransportConfiguration>(cc.getConnector(),
                                                                                                 null));
         }

         topology.addMember(nodeID, member);
      }
      else
      {
         if (backup)
         {
            // pair.b = cc.getConnector();
         }
         else
         {
            // pair.a = cc.getConnector();
         }
      }

      // Propagate the announcement

      for (ClusterTopologyListener listener : clientListeners)
      {
         listener.nodeUP(nodeID, member.getConnector(), false);
      }

      for (ClusterTopologyListener listener : clusterConnectionListeners)
      {
         listener.nodeUP(nodeID, member.getConnector(), false);
      }

   }

   private synchronized void deployBroadcastGroup(final BroadcastGroupConfiguration config) throws Exception
   {
      if (broadcastGroups.containsKey(config.getName()))
      {
         ClusterManagerImpl.log.warn("There is already a broadcast-group with name " + config.getName() +
                                     " deployed. This one will not be deployed.");

         return;
      }

      InetAddress localAddress = null;
      if (config.getLocalBindAddress() != null)
      {
         localAddress = InetAddress.getByName(config.getLocalBindAddress());
      }

      InetAddress groupAddress = InetAddress.getByName(config.getGroupAddress());

      BroadcastGroupImpl group = new BroadcastGroupImpl(nodeUUID.toString(),
                                                        config.getName(),
                                                        localAddress,
                                                        config.getLocalBindPort(),
                                                        groupAddress,
                                                        config.getGroupPort(),
                                                        !backup);

      for (String connectorInfo : config.getConnectorInfos())
      {
         TransportConfiguration connector = configuration.getConnectorConfigurations().get(connectorInfo);

         if (connector == null)
         {
            logWarnNoConnector(config.getName(), connectorInfo);

            return;
         }

         group.addConnector(connector);
      }

      ScheduledFuture<?> future = scheduledExecutor.scheduleWithFixedDelay(group,
                                                                           0L,
                                                                           config.getBroadcastPeriod(),
                                                                           MILLISECONDS);

      group.setScheduledFuture(future);

      broadcastGroups.put(config.getName(), group);

      managementService.registerBroadcastGroup(group, config);

      if (!backup)
      {
         group.start();
      }
   }

   private void logWarnNoConnector(final String connectorName, final String bgName)
   {
      ClusterManagerImpl.log.warn("There is no connector deployed with name '" + connectorName +
                                  "'. The broadcast group with name '" +
                                  bgName +
                                  "' will not be deployed.");
   }

   private TransportConfiguration[] connectorNameListToArray(final List<String> connectorNames)
   {
      TransportConfiguration[] tcConfigs = (TransportConfiguration[])Array.newInstance(TransportConfiguration.class,
                                                                                       connectorNames.size());
      int count = 0;
      for (String connectorName : connectorNames)
      {
         TransportConfiguration connector = configuration.getConnectorConfigurations().get(connectorName);

         if (connector == null)
         {
            ClusterManagerImpl.log.warn("No connector defined with name '" + connectorName +
                                        "'. The bridge will not be deployed.");

            return null;
         }

         tcConfigs[count++] = connector;
      }

      return tcConfigs;
   }

   public synchronized void deployBridge(final BridgeConfiguration config) throws Exception
   {
      if (config.getName() == null)
      {
         ClusterManagerImpl.log.warn("Must specify a unique name for each bridge. This one will not be deployed.");

         return;
      }

      if (config.getQueueName() == null)
      {
         ClusterManagerImpl.log.warn("Must specify a queue name for each bridge. This one will not be deployed.");

         return;
      }

      if (config.getForwardingAddress() == null)
      {
         ClusterManagerImpl.log.debug("Forward address is not specified. Will use original message address instead");
      }

      if (bridges.containsKey(config.getName()))
      {
         ClusterManagerImpl.log.warn("There is already a bridge with name " + config.getName() +
                                     " deployed. This one will not be deployed.");

         return;
      }

      Transformer transformer = instantiateTransformer(config.getTransformerClassName());

      Binding binding = postOffice.getBinding(new SimpleString(config.getQueueName()));

      if (binding == null)
      {
         ClusterManagerImpl.log.warn("No queue found with name " + config.getQueueName() +
                                     " bridge will not be deployed.");

         return;
      }

      Queue queue = (Queue)binding.getBindable();

      ServerLocatorInternal serverLocator;

      if (config.getDiscoveryGroupName() != null)
      {
         DiscoveryGroupConfiguration discoveryGroupConfiguration = configuration.getDiscoveryGroupConfigurations()
                                                                                .get(config.getDiscoveryGroupName());
         if (discoveryGroupConfiguration == null)
         {
            ClusterManagerImpl.log.warn("No discovery group configured with name '" + config.getDiscoveryGroupName() +
                                        "'. The bridge will not be deployed.");

            return;
         }

         if (config.isHA())
         {
            serverLocator = (ServerLocatorInternal)HornetQClient.createServerLocatorWithHA(discoveryGroupConfiguration);
         }
         else
         {
            serverLocator = (ServerLocatorInternal)HornetQClient.createServerLocatorWithoutHA(discoveryGroupConfiguration);
         }

      }
      else
      {
         TransportConfiguration[] tcConfigs = connectorNameListToArray(config.getStaticConnectors());

         if (tcConfigs == null)
         {
            return;
         }

         if (config.isHA())
         {
            serverLocator = (ServerLocatorInternal)HornetQClient.createServerLocatorWithHA(tcConfigs);
         }
         else
         {
            serverLocator = (ServerLocatorInternal)HornetQClient.createServerLocatorWithoutHA(tcConfigs);
         }

      }

      serverLocator.setConfirmationWindowSize(config.getConfirmationWindowSize());
      serverLocator.setReconnectAttempts(config.getReconnectAttempts());
      serverLocator.setRetryInterval(config.getRetryInterval());
      serverLocator.setRetryIntervalMultiplier(config.getRetryIntervalMultiplier());
      serverLocator.setClientFailureCheckPeriod(config.getClientFailureCheckPeriod());
      serverLocator.setInitialConnectAttempts(config.getReconnectAttempts());
      clusterLocators.add(serverLocator);
      Bridge bridge = new BridgeImpl(serverLocator,
                                     nodeUUID,
                                     new SimpleString(config.getName()),
                                     queue,
                                     executorFactory.getExecutor(),
                                     SimpleString.toSimpleString(config.getFilterString()),
                                     SimpleString.toSimpleString(config.getForwardingAddress()),
                                     scheduledExecutor,
                                     transformer,
                                     config.isUseDuplicateDetection(),
                                     config.getUser(),
                                     config.getPassword(),
                                     !backup,
                                     server.getStorageManager());

      bridges.put(config.getName(), bridge);

      managementService.registerBridge(bridge, config);

      if (!backup)
      {
         bridge.start();
      }
   }

   public synchronized void destroyBridge(final String name) throws Exception
   {
      Bridge bridge = bridges.remove(name);
      if (bridge != null)
      {
         bridge.stop();
         managementService.unregisterBridge(name);
      }
   }

   private synchronized void deployClusterConnection(final ClusterConnectionConfiguration config) throws Exception
   {
      if (config.getName() == null)
      {
         ClusterManagerImpl.log.warn("Must specify a unique name for each cluster. This one will not be deployed.");

         return;
      }

      if (config.getAddress() == null)
      {
         ClusterManagerImpl.log.warn("Must specify an address for each cluster connection. This one will not be deployed.");

         return;
      }

      TransportConfiguration connector = configuration.getConnectorConfigurations().get(config.getConnectorName());

      if (connector == null)
      {
         log.warn("No connecor with name '" + config.getConnectorName() +
                  "'. The cluster connection will not be deployed.");
         return;
      }

      if (clusterConnections.containsKey(config.getName()))
      {
         log.warn("Cluster Configuration  '" + config.getConnectorName() +
                  "' already exists. The cluster connection will not be deployed.", new Exception("trace"));
         return;
      }

      ClusterConnectionImpl clusterConnection;

      if (config.getDiscoveryGroupName() != null)
      {
         DiscoveryGroupConfiguration dg = configuration.getDiscoveryGroupConfigurations()
                                                       .get(config.getDiscoveryGroupName());

         if (dg == null)
         {
            ClusterManagerImpl.log.warn("No discovery group with name '" + config.getDiscoveryGroupName() +
                                        "'. The cluster connection will not be deployed.");
         }

         clusterConnection = new ClusterConnectionImpl(dg,
                                                       connector,
                                                       new SimpleString(config.getName()),
                                                       new SimpleString(config.getAddress()),
                                                       config.getRetryInterval(),
                                                       config.isDuplicateDetection(),
                                                       config.isForwardWhenNoConsumers(),
                                                       config.getConfirmationWindowSize(),
                                                       executorFactory,
                                                       server,
                                                       postOffice,
                                                       managementService,
                                                       scheduledExecutor,
                                                       config.getMaxHops(),
                                                       nodeUUID,
                                                       backup,
                                                       server.getConfiguration().getClusterUser(),
                                                       server.getConfiguration().getClusterPassword(),
                                                       config.isAllowDirectConnectionsOnly());
      }
      else
      {
         TransportConfiguration[] tcConfigs = config.getStaticConnectors() != null ? connectorNameListToArray(config.getStaticConnectors())
                                                                                  : null;

         clusterConnection = new ClusterConnectionImpl(tcConfigs,
                                                       connector,
                                                       new SimpleString(config.getName()),
                                                       new SimpleString(config.getAddress()),
                                                       config.getRetryInterval(),
                                                       config.isDuplicateDetection(),
                                                       config.isForwardWhenNoConsumers(),
                                                       config.getConfirmationWindowSize(),
                                                       executorFactory,
                                                       server,
                                                       postOffice,
                                                       managementService,
                                                       scheduledExecutor,
                                                       config.getMaxHops(),
                                                       nodeUUID,
                                                       backup,
                                                       server.getConfiguration().getClusterUser(),
                                                       server.getConfiguration().getClusterPassword(),
                                                       config.isAllowDirectConnectionsOnly());
      }

      managementService.registerCluster(clusterConnection, config);

      clusterConnections.put(config.getName(), clusterConnection);

      clusterConnection.start();

      if (backup)
      {
         announceBackup(config, connector);
      }
   }

   private void announceBackup(final ClusterConnectionConfiguration config, final TransportConfiguration connector) throws Exception
   {
      if (config.getStaticConnectors() != null)
      {
         TransportConfiguration[] tcConfigs = connectorNameListToArray(config.getStaticConnectors());

         backupServerLocator = (ServerLocatorInternal)HornetQClient.createServerLocatorWithoutHA(tcConfigs);
         backupServerLocator.setReconnectAttempts(-1);
      }
      else if (config.getDiscoveryGroupName() != null)
      {
         DiscoveryGroupConfiguration dg = configuration.getDiscoveryGroupConfigurations()
                                                       .get(config.getDiscoveryGroupName());

         if (dg == null)
         {
            ClusterManagerImpl.log.warn("No discovery group with name '" + config.getDiscoveryGroupName() +
                                        "'. The cluster connection will not be deployed.");
         }

         backupServerLocator = (ServerLocatorInternal)HornetQClient.createServerLocatorWithoutHA(dg);
         backupServerLocator.setReconnectAttempts(-1);
      }
      else
      {
         return;
      }
      log.info("announcing backup");
      executor.execute(new Runnable()
      {
         public void run()
         {
            try
            {
               ClientSessionFactory backupSessionFactory = backupServerLocator.connect();
               if (backupSessionFactory != null)
               {
                  backupSessionFactory.getConnection()
                                      .getChannel(0, -1)
                                      .send(new NodeAnnounceMessage(nodeUUID.toString(), true, connector));
                  log.info("backup announced");
               }
            }
            catch (Exception e)
            {
               log.warn("Unable to announce backup", e);
            }
         }
      });
   }

   private Transformer instantiateTransformer(final String transformerClassName)
   {
      Transformer transformer = null;

      if (transformerClassName != null)
      {
         ClassLoader loader = Thread.currentThread().getContextClassLoader();
         try
         {
            Class<?> clz = loader.loadClass(transformerClassName);
            transformer = (Transformer)clz.newInstance();
         }
         catch (Exception e)
         {
            throw new IllegalArgumentException("Error instantiating transformer class \"" + transformerClassName + "\"",
                                               e);
         }
      }
      return transformer;
   }

   // for testing
   public void clear()
   {
      bridges.clear();
      for (ClusterConnection clusterConnection : clusterConnections.values())
      {
         try
         {
            clusterConnection.stop();
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }
      clusterConnections.clear();
   }
}
