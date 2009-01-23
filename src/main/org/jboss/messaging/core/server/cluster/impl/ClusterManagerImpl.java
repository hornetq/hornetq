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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.jboss.messaging.core.cluster.DiscoveryGroup;
import org.jboss.messaging.core.cluster.impl.DiscoveryGroupImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.BridgeConfiguration;
import org.jboss.messaging.core.config.cluster.BroadcastGroupConfiguration;
import org.jboss.messaging.core.config.cluster.ClusterConfiguration;
import org.jboss.messaging.core.config.cluster.DiscoveryGroupConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.cluster.Bridge;
import org.jboss.messaging.core.server.cluster.BroadcastGroup;
import org.jboss.messaging.core.server.cluster.Cluster;
import org.jboss.messaging.core.server.cluster.ClusterManager;
import org.jboss.messaging.core.server.cluster.Transformer;
import org.jboss.messaging.util.ExecutorFactory;
import org.jboss.messaging.util.Pair;
import org.jboss.messaging.util.SimpleString;

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
   
   private final Map<String, Cluster> clusters = new HashMap<String, Cluster>();

   private final ExecutorFactory executorFactory;

   private final StorageManager storageManager;

   private final PostOffice postOffice;

   private final ScheduledExecutorService scheduledExecutor;

   private final ManagementService managementService;

   private final Configuration configuration;
   
   private final QueueFactory queueFactory;

   private volatile boolean started;

   public ClusterManagerImpl(final ExecutorFactory executorFactory,
                             final StorageManager storageManager,
                             final PostOffice postOffice,
                             final ScheduledExecutorService scheduledExecutor,
                             final ManagementService managementService,
                             final Configuration configuration,
                             final QueueFactory queueFactory)
   {
      this.executorFactory = executorFactory;

      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.scheduledExecutor = scheduledExecutor;

      this.managementService = managementService;

      this.configuration = configuration;
      
      this.queueFactory = queueFactory;
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
      
      for (ClusterConfiguration config: configuration.getClusterConfigurations())
      {
         deployCluster(config);
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
      
      for (Cluster cluster: clusters.values())
      {
         cluster.stop();
         managementService.unregisterCluster(cluster.getName().toString());
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

   private synchronized void deployBroadcastGroup(final BroadcastGroupConfiguration config) throws Exception
   {
      if (broadcastGroups.containsKey(config.getName()))
      {
         log.warn("There is already a broadcast-group with name " + config.getName() +
                  " deployed. This one will not be deployed.");

         return;
      }

      InetAddress localBindAddress = InetAddress.getByName(config.getLocalBindAddress());

      InetAddress groupAddress = InetAddress.getByName(config.getGroupAddress());

      BroadcastGroupImpl group = new BroadcastGroupImpl(config.getName(),
                                                        localBindAddress,
                                                        config.getLocalBindPort(),
                                                        groupAddress,
                                                        config.getGroupPort());

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

      DiscoveryGroup group = new DiscoveryGroupImpl(config.getName(),
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

      if (config.getMaxBatchTime() == 0 || config.getMaxBatchTime() < -1)
      {
         log.warn("Invalid value for max-batch-time. Valid values are -1 or > 0");

         return;
      }

      if (config.getMaxBatchSize() < 1)
      {
         log.warn("Invalid value for max-batch-size. Valid values are > 0");

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

         bridge = new BridgeImpl(new SimpleString(config.getName()),
                                 queue,
                                 pair,
                                 executorFactory.getExecutor(),
                                 config.getMaxBatchSize(),
                                 config.getMaxBatchTime(),
                                 config.getFilterString() == null ? null : new SimpleString(config.getFilterString()),
                                 new SimpleString(config.getForwardingAddress()),
                                 storageManager,
                                 scheduledExecutor,
                                 transformer,
                                 config.getRetryInterval(),
                                 config.getRetryIntervalMultiplier(),
                                 config.getMaxRetriesBeforeFailover(),
                                 config.getMaxRetriesAfterFailover(),
                                 config.isUseDuplicateDetection(),
                                 null,
                                 null);

         bridges.put(config.getName(), bridge);

         managementService.registerBridge(bridge, config);

         bridge.start();
      }
   }
   
   private synchronized void deployCluster(final ClusterConfiguration config) throws Exception
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
      
      Cluster cluster;
     
      List<Pair<TransportConfiguration, TransportConfiguration>> connectors = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();
      
      if (config.getStaticConnectorNamePairs() != null)
      {
         for (Pair<String, String> connectorNamePair: config.getStaticConnectorNamePairs())
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
            
            connectors.add(pair);
         }
         
         cluster = new ClusterImpl(new SimpleString(config.getName()),
                                           new SimpleString(config.getAddress()),
                                           config.getBridgeConfig(),
                                           config.isDuplicateDetection(),
                                           executorFactory,
                                           storageManager,
                                           postOffice,
                                           scheduledExecutor,
                                           queueFactory,
                                           connectors);
      }
      else
      {
         DiscoveryGroup dg = discoveryGroups.get(config.getDiscoveryGroupName());
         
         if (dg == null)
         {
            log.warn("No discovery group with name '" + config.getDiscoveryGroupName() + "'. The bridge will not be deployed.");
         }
         
         cluster = new ClusterImpl(new SimpleString(config.getName()),
                                           new SimpleString(config.getAddress()),
                                           config.getBridgeConfig(),
                                           config.isDuplicateDetection(),
                                           executorFactory,
                                           storageManager,
                                           postOffice,
                                           scheduledExecutor,
                                           queueFactory,
                                           dg);
      }

      managementService.registerCluster(cluster, config);

      clusters.put(config.getName(), cluster);
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
