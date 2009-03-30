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

package org.jboss.messaging.core.server.cluster.impl;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.jboss.messaging.core.cluster.DiscoveryGroup;
import org.jboss.messaging.core.cluster.impl.DiscoveryGroupImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.BridgeConfiguration;
import org.jboss.messaging.core.config.cluster.BroadcastGroupConfiguration;
import org.jboss.messaging.core.config.cluster.ClusterConnectionConfiguration;
import org.jboss.messaging.core.config.cluster.DiscoveryGroupConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.cluster.Bridge;
import org.jboss.messaging.core.server.cluster.BroadcastGroup;
import org.jboss.messaging.core.server.cluster.ClusterConnection;
import org.jboss.messaging.core.server.cluster.ClusterManager;
import org.jboss.messaging.core.server.cluster.MessageFlowRecord;
import org.jboss.messaging.core.server.cluster.Transformer;
import org.jboss.messaging.utils.Pair;
import org.jboss.messaging.utils.SimpleString;
import org.jboss.messaging.utils.UUID;

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

   private final Map<String, DiscoveryGroup> discoveryGroups = new HashMap<String, DiscoveryGroup>();

   private final Map<String, Bridge> bridges = new HashMap<String, Bridge>();

   private final Map<String, ClusterConnection> clusters = new HashMap<String, ClusterConnection>();

   private final org.jboss.messaging.utils.ExecutorFactory executorFactory;
   
   private final MessagingServer server;

   private final PostOffice postOffice;

   private final ScheduledExecutorService scheduledExecutor;

   private final ManagementService managementService;

   private final Configuration configuration;

   private final UUID nodeUUID;
   
   private Channel replicatingChannel;

   private volatile boolean started;
   
   private boolean backup;

   public ClusterManagerImpl(final org.jboss.messaging.utils.ExecutorFactory executorFactory,
                             final MessagingServer server,
                             final PostOffice postOffice,
                             final ScheduledExecutorService scheduledExecutor,
                             final ManagementService managementService,
                             final Configuration configuration,
                             final UUID nodeUUID,
                             final Channel replicatingChannel,
                             final boolean backup)
   {
      if (nodeUUID == null)
      {
         throw new IllegalArgumentException("Node uuid is null");
      }
      
      this.executorFactory = executorFactory;
      
      this.server = server;

      this.postOffice = postOffice;

      this.scheduledExecutor = scheduledExecutor;

      this.managementService = managementService;

      this.configuration = configuration;

      this.nodeUUID = nodeUUID;
      
      this.replicatingChannel = replicatingChannel;
      
      this.backup = backup;
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      for (BroadcastGroupConfiguration config : configuration.getBroadcastGroupConfigurations())
      {
         deployBroadcastGroup(config);
      }

      for (DiscoveryGroupConfiguration config : configuration.getDiscoveryGroupConfigurations().values())
      {
         deployDiscoveryGroup(config);
      }

      for (BridgeConfiguration config : configuration.getBridgeConfigurations())
      {
         deployBridge(config);
      }

      for (ClusterConnectionConfiguration config : configuration.getClusterConfigurations())
      {
         deployClusterConnection(config);
      }

      started = true;
   }

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;
      }

      for (BroadcastGroup group : broadcastGroups.values())
      {
         group.stop();
         managementService.unregisterBroadcastGroup(group.getName());
      }

      for (DiscoveryGroup group : discoveryGroups.values())
      {
         group.stop();
         managementService.unregisterDiscoveryGroup(group.getName());
      }

      for (Bridge bridge : bridges.values())
      {
         bridge.stop();
         managementService.unregisterBridge(bridge.getName().toString());
      }

      for (ClusterConnection clusterConnection : clusters.values())
      {
         clusterConnection.stop();
         managementService.unregisterCluster(clusterConnection.getName().toString());
      }

      broadcastGroups.clear();

      discoveryGroups.clear();

      bridges.clear();

      started = false;
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
      return new HashSet<ClusterConnection>(clusters.values());
   }
   
   public ClusterConnection getClusterConnection(final SimpleString name)
   {
      return clusters.get(name.toString()); 
   }
   
   public synchronized void activate()
   {      
      for (BroadcastGroup bg: broadcastGroups.values())
      {
         bg.activate();
      }
      
      for (Bridge bridge: bridges.values())
      {
         bridge.activate();
      }
      
      for (ClusterConnection cc: clusters.values())
      {
         cc.activate();
      }
      
      replicatingChannel = null;
      
      backup = false;
   }

   private synchronized void deployBroadcastGroup(final BroadcastGroupConfiguration config) throws Exception
   {
      if (broadcastGroups.containsKey(config.getName()))
      {
         log.warn("There is already a broadcast-group with name " + config.getName() +
                  " deployed. This one will not be deployed.");

         return;
      }

      InetAddress groupAddress = InetAddress.getByName(config.getGroupAddress());

      BroadcastGroupImpl group = new BroadcastGroupImpl(nodeUUID.toString(),
                                                        config.getName(),
                                                        config.getLocalBindPort(),
                                                        groupAddress,
                                                        config.getGroupPort(),
                                                        !backup);

      for (Pair<String, String> connectorInfo : config.getConnectorInfos())
      {
         TransportConfiguration connector = configuration.getConnectorConfigurations().get(connectorInfo.a);

         if (connector == null)
         {
            logWarnNoConnector(config.getName(), connectorInfo.a);

            return;
         }

         TransportConfiguration backupConnector = null;

         if (connectorInfo.b != null)
         {
            backupConnector = configuration.getConnectorConfigurations().get(connectorInfo.b);

            if (connector == null)
            {
               logWarnNoConnector(config.getName(), connectorInfo.b);

               return;
            }
         }

         group.addConnectorPair(new Pair<TransportConfiguration, TransportConfiguration>(connector, backupConnector));
      }

      ScheduledFuture<?> future = scheduledExecutor.scheduleWithFixedDelay(group,
                                                                           0L,
                                                                           config.getBroadcastPeriod(),
                                                                           MILLISECONDS);

      group.setScheduledFuture(future);

      broadcastGroups.put(config.getName(), group);

      managementService.registerBroadcastGroup(group, config);

      group.start();
   }

   private void logWarnNoConnector(final String connectorName, final String bgName)
   {
      log.warn("There is no connector deployed with name '" + connectorName +
               "'. The broadcast group with name '" +
               bgName +
               "' will not be deployed.");
   }

   private synchronized void deployDiscoveryGroup(final DiscoveryGroupConfiguration config) throws Exception
   {
      if (discoveryGroups.containsKey(config.getName()))
      {
         log.warn("There is already a discovery-group with name " + config.getName() +
                  " deployed. This one will not be deployed.");

         return;
      }

      InetAddress groupAddress = InetAddress.getByName(config.getGroupAddress());

      DiscoveryGroup group = new DiscoveryGroupImpl(nodeUUID.toString(),
                                                    config.getName(),
                                                    groupAddress,
                                                    config.getGroupPort(),
                                                    config.getRefreshTimeout());

      discoveryGroups.put(config.getName(), group);

      managementService.registerDiscoveryGroup(group, config);

      group.start();
   }

   private synchronized void deployBridge(final BridgeConfiguration config) throws Exception
   {
      if (config.getName() == null)
      {
         log.warn("Must specify a unique name for each bridge. This one will not be deployed.");

         return;
      }

      if (config.getQueueName() == null)
      {
         log.warn("Must specify a queue name for each bridge. This one will not be deployed.");

         return;
      }

      if (config.getForwardingAddress() == null)
      {
         log.warn("Must specify an forwarding address each bridge. This one will not be deployed.");

         return;
      }

      if (bridges.containsKey(config.getName()))
      {
         log.warn("There is already a bridge with name " + config.getName() +
                  " deployed. This one will not be deployed.");

         return;
      }

      Transformer transformer = instantiateTransformer(config.getTransformerClassName());

      Pair<String, String> connectorNamePair = config.getConnectorPair();

      Binding binding = postOffice.getBinding(new SimpleString(config.getQueueName()));

      if (binding == null)
      {
         log.warn("No queue found with name " + config.getQueueName() + " bridge will not be deployed.");

         return;
      }

      Queue queue = (Queue)binding.getBindable();

      Bridge bridge;

      if (connectorNamePair != null)
      {
         TransportConfiguration connector = configuration.getConnectorConfigurations().get(connectorNamePair.a);

         if (connector == null)
         {
            log.warn("No connector defined with name '" + connectorNamePair.a + "'. The bridge will not be deployed.");

            return;
         }

         TransportConfiguration backupConnector = null;

         if (connectorNamePair.b != null)
         {
            backupConnector = configuration.getConnectorConfigurations().get(connectorNamePair.b);

            if (backupConnector == null)
            {
               log.warn("No connector defined with name '" + connectorNamePair.b +
                        "'. The bridge will not be deployed.");

               return;
            }
         }

         Pair<TransportConfiguration, TransportConfiguration> pair = new Pair<TransportConfiguration, TransportConfiguration>(connector,
                                                                                                                              backupConnector);
         bridge = new BridgeImpl(nodeUUID,
                                 new SimpleString(config.getName()),
                                 queue,
                                 pair,
                                 executorFactory.getExecutor(),
                                 config.getFilterString() == null ? null : new SimpleString(config.getFilterString()),
                                 new SimpleString(config.getForwardingAddress()),
                                 scheduledExecutor,
                                 transformer,
                                 config.getRetryInterval(),
                                 config.getRetryIntervalMultiplier(),
                                 config.getReconnectAttempts(),
                                 config.isFailoverOnServerShutdown(),
                                 config.isUseDuplicateDetection(),
                                 managementService.getManagementAddress(),
                                 managementService.getManagementNotificationAddress(),
                                 managementService.getClusterPassword(),
                                 replicatingChannel,
                                 !backup,
                                 server.getStorageManager());

         bridges.put(config.getName(), bridge);

         managementService.registerBridge(bridge, config);

         bridge.start();
      }
   }

   private synchronized void deployClusterConnection(final ClusterConnectionConfiguration config) throws Exception
   {
      if (config.getName() == null)
      {
         log.warn("Must specify a unique name for each cluster. This one will not be deployed.");

         return;
      }

      if (config.getAddress() == null)
      {
         log.warn("Must specify an address for each cluster. This one will not be deployed.");

         return;
      }

      ClusterConnection clusterConnection;

      List<Pair<TransportConfiguration, TransportConfiguration>> connectors = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();

      if (config.getStaticConnectorNamePairs() != null)
      {
         for (Pair<String, String> connectorNamePair : config.getStaticConnectorNamePairs())
         {
            TransportConfiguration connector = configuration.getConnectorConfigurations().get(connectorNamePair.a);

            if (connector == null)
            {
               log.warn("No connector defined with name '" + connectorNamePair.a +
                        "'. The cluster connection will not be deployed.");

               return;
            }

            TransportConfiguration backupConnector = null;

            if (connectorNamePair.b != null)
            {
               backupConnector = configuration.getConnectorConfigurations().get(connectorNamePair.b);

               if (backupConnector == null)
               {
                  log.warn("No connector defined with name '" + connectorNamePair.b +
                           "'. The cluster connection will not be deployed.");

                  return;
               }
            }

            Pair<TransportConfiguration, TransportConfiguration> pair = new Pair<TransportConfiguration, TransportConfiguration>(connector,
                                                                                                                                 backupConnector);

            connectors.add(pair);
         }

         clusterConnection = new ClusterConnectionImpl(new SimpleString(config.getName()),
                                                       new SimpleString(config.getAddress()),
                                                       config.getRetryInterval(),                                                       
                                                       config.isDuplicateDetection(),
                                                       config.isForwardWhenNoConsumers(),
                                                       executorFactory,
                                                       server,                                         
                                                       postOffice,
                                                       managementService,
                                                       scheduledExecutor,                                            
                                                       connectors,
                                                       config.getMaxHops(),
                                                       nodeUUID,
                                                       replicatingChannel,
                                                       backup);
      }
      else
      {
         DiscoveryGroup dg = discoveryGroups.get(config.getDiscoveryGroupName());

         if (dg == null)
         {
            log.warn("No discovery group with name '" + config.getDiscoveryGroupName() +
                     "'. The cluster connection will not be deployed.");
         }

         clusterConnection = new ClusterConnectionImpl(new SimpleString(config.getName()),
                                                       new SimpleString(config.getAddress()),
                                                       config.getRetryInterval(),                                                      
                                                       config.isDuplicateDetection(),
                                                       config.isForwardWhenNoConsumers(),
                                                       executorFactory,
                                                       server,                                             
                                                       postOffice,
                                                       managementService,
                                                       scheduledExecutor,                                               
                                                       dg,
                                                       config.getMaxHops(),
                                                       nodeUUID,
                                                       replicatingChannel,
                                                       backup);
      }

      managementService.registerCluster(clusterConnection, config);

      clusters.put(config.getName(), clusterConnection);

      clusterConnection.start();
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

}
