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

package org.hornetq.core.server.cluster;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.cluster.BroadcastEndpointFactory;
import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.core.config.BroadcastGroupConfiguration;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.ConfigurationUtils;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.protocol.core.Channel;
import org.hornetq.core.protocol.core.impl.wireformat.BackupRegistrationMessage;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.HornetQLogger;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.cluster.impl.BridgeImpl;
import org.hornetq.core.server.cluster.impl.BroadcastGroupImpl;
import org.hornetq.core.server.cluster.impl.ClusterConnectionBridge;
import org.hornetq.core.server.cluster.impl.ClusterConnectionImpl;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.utils.ConcurrentHashSet;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.FutureLatch;

/**
 * A ClusterManager manages {@link ClusterConnection}s, {@link BroadcastGroup}s and {@link Bridge}s.
 * <p>
 * Note that {@link ClusterConnectionBridge}s extend Bridges but are controlled over through
 * {@link ClusterConnectionImpl}. As a node is discovered a new {@link ClusterConnectionBridge} is
 * deployed.
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author Clebert Suconic
 */
public class ClusterManager implements HornetQComponent
{
   private final Map<String, BroadcastGroup> broadcastGroups = new HashMap<String, BroadcastGroup>();

   private final Map<String, Bridge> bridges = new HashMap<String, Bridge>();

   private final ExecutorFactory executorFactory;

   private final HornetQServer server;

   private final PostOffice postOffice;

   private final ScheduledExecutorService scheduledExecutor;

   private ClusterConnection defaultClusterConnection;

   private final ManagementService managementService;

   private final Configuration configuration;

   private volatile boolean started;

   private volatile boolean backup;

   // the cluster connections which links this node to other cluster nodes
   private final Map<String, ClusterConnection> clusterConnections = new HashMap<String, ClusterConnection>();

   private final Set<ServerLocatorInternal> clusterLocators = new ConcurrentHashSet<ServerLocatorInternal>();

   private final Executor executor;

   private final NodeManager nodeManager;

   public ClusterManager(final ExecutorFactory executorFactory,
                             final HornetQServer server,
                             final PostOffice postOffice,
                             final ScheduledExecutorService scheduledExecutor,
                             final ManagementService managementService,
                             final Configuration configuration,
                         final NodeManager nodeManager, final boolean backup)
   {
      if (nodeManager.getNodeId() == null)
      {
         throw HornetQMessageBundle.BUNDLE.nodeIdNull();
      }

      this.executorFactory = executorFactory;

      executor = executorFactory.getExecutor();;

      this.server = server;

      this.postOffice = postOffice;

      this.scheduledExecutor = scheduledExecutor;

      this.managementService = managementService;

      this.configuration = configuration;

      this.nodeManager = nodeManager;

      this.backup = backup;
   }

   public String describe()
   {
      StringWriter str = new StringWriter();
      PrintWriter out = new PrintWriter(str);

      out.println("Information on " + this);
      out.println("*******************************************************");

      for (ClusterConnection conn : cloneClusterConnections())
      {
         out.println(conn.describe());
      }

      out.println("*******************************************************");

      return str.toString();
   }

   /**
    * Return the default ClusterConnection to be used case it's not defined by the acceptor
    * @return default connection
    */
   public ClusterConnection getDefaultConnection(TransportConfiguration acceptorConfig)
   {
      if (acceptorConfig == null)
      {
         // if the parameter is null, we just return whatever is defined on defaultClusterConnection
         return defaultClusterConnection;
      }
      else if (defaultClusterConnection != null && defaultClusterConnection.getConnector().isEquivalent(acceptorConfig))
      {
         return defaultClusterConnection;
      }
      else
      {
         for (ClusterConnection conn : cloneClusterConnections())
         {
            if (conn.getConnector().isEquivalent(acceptorConfig))
            {
               return conn;
            }
         }
         return null;
      }
   }

   @Override
   public String toString()
   {
      return "ClusterManagerImpl[server=" + server + "]@" + System.identityHashCode(this);
   }

   public String getNodeId()
   {
      return nodeManager.getNodeId().toString();
   }

   public String getNodeGroupName()
   {
      return configuration.getNodeGroupName();
   }

   public synchronized void deploy() throws Exception
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

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      for (BroadcastGroup group: broadcastGroups.values())
      {
         if (!backup)
         {
            group.start();
         }
      }

      for (ClusterConnection conn : clusterConnections.values())
      {
         conn.start();
         if (backup && configuration.isSharedStore())
         {
            conn.informTopology();
            conn.announceBackup();
         }
      }

      for (BridgeConfiguration config : configuration.getBridgeConfigurations())
      {
         deployBridge(config, !backup);
      }


      started = true;
   }

   public void stop() throws Exception
   {
      synchronized (this)
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

            broadcastGroups.clear();

            for (ClusterConnection clusterConnection : clusterConnections.values())
            {
               clusterConnection.stop();
               managementService.unregisterCluster(clusterConnection.getName().toString());
            }

         for (Bridge bridge : bridges.values())
         {
            bridge.stop();
            managementService.unregisterBridge(bridge.getName().toString());
         }

         bridges.clear();
      }

      for (ServerLocatorInternal clusterLocator : clusterLocators)
      {
         try
         {
            clusterLocator.close();
         }
         catch (Exception e)
         {
            HornetQLogger.LOGGER.errorClosingServerLocator(e, clusterLocator);
         }
      }
      clusterLocators.clear();
      started = false;

      clearClusterConnections();
   }

   public void flushExecutor()
   {
      FutureLatch future = new FutureLatch();
      executor.execute(future);
      if (!future.await(10000))
      {
         server.threadDump("Couldn't flush ClusterManager executor (" + this +
                           ") in 10 seconds, verify your thread pool size");
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

   public ClusterConnection getClusterConnection(final String name)
   {
      return clusterConnections.get(name);
   }

   // backup node becomes live
   public synchronized void activate()
   {
      if (backup)
      {
         backup = false;

         for (BroadcastGroup broadcastGroup : broadcastGroups.values())
         {
            try
            {
               broadcastGroup.start();
            }
            catch (Exception e)
            {
               HornetQLogger.LOGGER.unableToStartBroadcastGroup(e, broadcastGroup.getName());
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
               HornetQLogger.LOGGER.unableToStartClusterConnection(e, clusterConnection.getName());
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
               HornetQLogger.LOGGER.unableToStartBridge(e, bridge.getName());
            }
         }
      }
   }

   public void announceBackup()
   {
      for (ClusterConnection conn : cloneClusterConnections())
      {
         conn.announceBackup();
      }
   }

   /**
    * XXX HORNETQ-720
    * @param liveChannel channel for opening connection with live
    * @param attemptingFailBack if {@code true} then this server wants to trigger a fail-back when
    *           up-to-date, that is it wants to take over the role of 'live' from the current 'live'
    *           server.
    * @throws HornetQException
    */
   public
            void
            announceReplicatingBackupToLive(final Channel liveChannel, final boolean attemptingFailBack)
                                                                                                        throws HornetQException
   {
      ClusterConnectionConfiguration config = ConfigurationUtils.getReplicationClusterConfiguration(configuration);
      if (config == null)
      {
         HornetQLogger.LOGGER.announceBackupNoClusterConnections();
         throw new HornetQException("lacking cluster connection");

      }
      TransportConfiguration connector = configuration.getConnectorConfigurations().get(config.getConnectorName());

      if (connector == null)
      {
         HornetQLogger.LOGGER.announceBackupNoConnector(config.getConnectorName());
         throw new HornetQException("lacking cluster connection");
      }
      liveChannel.send(new BackupRegistrationMessage(connector, configuration.getClusterUser(),
                                                     configuration.getClusterPassword(), attemptingFailBack));
   }

   public void addClusterLocator(final ServerLocatorInternal serverLocator)
   {
      this.clusterLocators.add(serverLocator);
   }

   public void removeClusterLocator(final ServerLocatorInternal serverLocator)
   {
      this.clusterLocators.remove(serverLocator);
   }

   public synchronized void deployBridge(final BridgeConfiguration config, final boolean start) throws Exception
   {
      if (config.getName() == null)
      {
         HornetQLogger.LOGGER.bridgeNotUnique();

         return;
      }

      if (config.getQueueName() == null)
      {
         HornetQLogger.LOGGER.bridgeNoQueue();

         return;
      }

      if (config.getForwardingAddress() == null)
      {
         HornetQLogger.LOGGER.bridgeNoForwardAddress();
      }

      if (bridges.containsKey(config.getName()))
      {
         HornetQLogger.LOGGER.bridgeAlreadyDeployed(config.getName());

         return;
      }

      Transformer transformer = instantiateTransformer(config.getTransformerClassName());

      Binding binding = postOffice.getBinding(new SimpleString(config.getQueueName()));

      if (binding == null)
      {
         HornetQLogger.LOGGER.bridgeNoQueue(config.getQueueName());

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
            HornetQLogger.LOGGER.bridgeNoDiscoveryGroup(config.getDiscoveryGroupName());

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

      // We are going to manually retry on the bridge in case of failure
      serverLocator.setReconnectAttempts(0);
      serverLocator.setInitialConnectAttempts(-1);
      serverLocator.setRetryInterval(config.getRetryInterval());
      serverLocator.setMaxRetryInterval(config.getMaxRetryInterval());
      serverLocator.setRetryIntervalMultiplier(config.getRetryIntervalMultiplier());
      serverLocator.setClientFailureCheckPeriod(config.getClientFailureCheckPeriod());
      serverLocator.setBlockOnDurableSend(!config.isUseDuplicateDetection());
      serverLocator.setBlockOnNonDurableSend(!config.isUseDuplicateDetection());
      serverLocator.setMinLargeMessageSize(config.getMinLargeMessageSize());
      //disable flow control
      serverLocator.setProducerWindowSize(-1);

      // This will be set to 30s unless it's changed from embedded / testing
      // there is no reason to exception the config for this timeout
      // since the Bridge is supposed to be non-blocking and fast
      // We may expose this if we find a good use case
      serverLocator.setCallTimeout(config.getCallTimeout());
      if (!config.isUseDuplicateDetection())
      {
         HornetQLogger.LOGGER.debug("Bridge " + config.getName() +
                   " is configured to not use duplicate detecion, it will send messages synchronously");
      }

      clusterLocators.add(serverLocator);

      Bridge bridge = new BridgeImpl(serverLocator,
                                     config.getReconnectAttempts(),
                                     config.getRetryInterval(),
                                     config.getRetryIntervalMultiplier(),
                                     config.getMaxRetryInterval(),
                                     nodeManager.getUUID(),
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

      if (start)
      {
         bridge.start();
      }

   }

   public void destroyBridge(final String name) throws Exception
   {
      Bridge bridge;

      synchronized (this)
      {
         bridge = bridges.remove(name);
         if (bridge != null)
         {
            bridge.stop();
            managementService.unregisterBridge(name);
         }
      }
      if (bridge != null)
      {
         bridge.flushExecutor();
      }
   }

   // for testing
   public void clear()
   {
      for (Bridge bridge : bridges.values())
      {
         try
         {
            bridge.stop();
         }
         catch (Exception e)
         {
            HornetQLogger.LOGGER.warn(e.getMessage(), e);
         }
      }
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
      clearClusterConnections();
   }

   // Private methods ----------------------------------------------------------------------------------------------------


   private void clearClusterConnections()
   {
      clusterConnections.clear();
      this.defaultClusterConnection = null;
   }

   private void deployClusterConnection(final ClusterConnectionConfiguration config) throws Exception
   {
      if (config.getName() == null)
      {
         HornetQLogger.LOGGER.clusterConnectionNotUnique();

         return;
      }

      if (config.getAddress() == null)
      {
         HornetQLogger.LOGGER.clusterConnectionNoForwardAddress();

         return;
      }

      TransportConfiguration connector = configuration.getConnectorConfigurations().get(config.getConnectorName());

      if (connector == null)
      {
         HornetQLogger.LOGGER.clusterConnectionNoConnector(config.getConnectorName());
         return;
      }

      if (clusterConnections.containsKey(config.getName()))
      {
         HornetQLogger.LOGGER.clusterConnectionAlreadyExists(config.getConnectorName());
         return;
      }

      ClusterConnectionImpl clusterConnection;
      DiscoveryGroupConfiguration dg;

      if (config.getDiscoveryGroupName() != null)
      {
         dg = configuration.getDiscoveryGroupConfigurations()
                                                       .get(config.getDiscoveryGroupName());

         if (dg == null)
         {
            HornetQLogger.LOGGER.clusterConnectionNoDiscoveryGroup(config.getDiscoveryGroupName());
            return;
         }

         if (HornetQLogger.LOGGER.isDebugEnabled())
         {
            HornetQLogger.LOGGER.debug(this + " Starting a Discovery Group Cluster Connection, name=" +
                      config.getDiscoveryGroupName() +
                      ", dg=" +
                      dg);
         }

         clusterConnection = new ClusterConnectionImpl(this,
                                                       dg,
                                                       connector,
                                                       new SimpleString(config.getName()),
                                                       new SimpleString(config.getAddress()),
                                                       config.getMinLargeMessageSize(),
                                                       config.getClientFailureCheckPeriod(),
                                                       config.getConnectionTTL(),
                                                       config.getRetryInterval(),
                                                       config.getRetryIntervalMultiplier(),
                                                       config.getMaxRetryInterval(),
                                                       config.getReconnectAttempts(),
                                                       config.getCallTimeout(),
                                                       config.getCallFailoverTimeout(),
                                                       config.isDuplicateDetection(),
                                                       config.isForwardWhenNoConsumers(),
                                                       config.getConfirmationWindowSize(),
                                                       executorFactory,
                                                       server,
                                                       postOffice,
                                                       managementService,
                                                       scheduledExecutor,
                                                       config.getMaxHops(),
                                                       nodeManager,
                                                       backup,
                                                       server.getConfiguration().getClusterUser(),
                                                       server.getConfiguration().getClusterPassword(),
                                                       config.isAllowDirectConnectionsOnly());
      }
      else
      {
         TransportConfiguration[] tcConfigs = config.getStaticConnectors() != null ? connectorNameListToArray(config.getStaticConnectors())
                                                                                  : null;

         if (HornetQLogger.LOGGER.isDebugEnabled())
         {
            HornetQLogger.LOGGER.debug(this + " defining cluster connection towards " + Arrays.toString(tcConfigs));
         }

         clusterConnection = new ClusterConnectionImpl(this,
                                                       tcConfigs,
                                                       connector,
                                                       new SimpleString(config.getName()),
                                                       new SimpleString(config.getAddress()),
                                                       config.getMinLargeMessageSize(),
                                                       config.getClientFailureCheckPeriod(),
                                                       config.getConnectionTTL(),
                                                       config.getRetryInterval(),
                                                       config.getRetryIntervalMultiplier(),
                                                       config.getMaxRetryInterval(),
                                                       config.getReconnectAttempts(),
                                                       config.getCallTimeout(),
                                                       config.getCallFailoverTimeout(),
                                                       config.isDuplicateDetection(),
                                                       config.isForwardWhenNoConsumers(),
                                                       config.getConfirmationWindowSize(),
                                                       executorFactory,
                                                       server,
                                                       postOffice,
                                                       managementService,
                                                       scheduledExecutor,
                                                       config.getMaxHops(),
                                                       nodeManager,
                                                       backup,
                                                       server.getConfiguration().getClusterUser(),
                                                       server.getConfiguration().getClusterPassword(),
                                                       config.isAllowDirectConnectionsOnly());
      }

      if (defaultClusterConnection == null)
      {
         defaultClusterConnection = clusterConnection;
      }

      managementService.registerCluster(clusterConnection, config);

      clusterConnections.put(config.getName(), clusterConnection);

      if (HornetQLogger.LOGGER.isDebugEnabled())
      {
         HornetQLogger.LOGGER.debug("ClusterConnection.start at " + clusterConnection, new Exception("trace"));
      }
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
            throw HornetQMessageBundle.BUNDLE.errorCreatingTransformerClass(e, transformerClassName);
         }
      }
      return transformer;
   }


   private synchronized void deployBroadcastGroup(final BroadcastGroupConfiguration config) throws Exception
   {
      if (broadcastGroups.containsKey(config.getName()))
      {
         HornetQLogger.LOGGER.broadcastGroupAlreadyExists(config.getName());

         return;
      }

      BroadcastGroup group = createBroadcastGroup(config);

      managementService.registerBroadcastGroup(group, config);
   }

   private BroadcastGroup createBroadcastGroup(BroadcastGroupConfiguration config) throws Exception
   {
       BroadcastGroup group = broadcastGroups.get(config.getName());

       if (group == null)
       {
          group = new BroadcastGroupImpl(nodeManager, config.getName(),
                                        config.getBroadcastPeriod(), scheduledExecutor, config.getEndpointFactoryConfiguration().createBroadcastEndpointFactory());

          for (String connectorInfo : config.getConnectorInfos())
          {
             TransportConfiguration connector = configuration.getConnectorConfigurations().get(connectorInfo);

             if (connector == null)
             {
                logWarnNoConnector(config.getName(), connectorInfo);

                return null;
             }

             group.addConnector(connector);
          }
       }

       if (group.size() == 0)
       {
          logWarnNoConnector(config.getConnectorInfos().toString(), group.getName());
          return null;
       }

       broadcastGroups.put(config.getName(), group);

       return group;
   }

   private void logWarnNoConnector(final String connectorName, final String bgName)
   {
      HornetQLogger.LOGGER.broadcastGroupNoConnector(connectorName, bgName);
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
            HornetQLogger.LOGGER.bridgeNoConnector(connectorName);

            return null;
         }

         tcConfigs[count++] = connector;
      }

      return tcConfigs;
   }

   private synchronized Collection<ClusterConnection> cloneClusterConnections()
   {
      ArrayList<ClusterConnection> list = new ArrayList<ClusterConnection>(clusterConnections.size());
      list.addAll(clusterConnections.values());
      return list;
   }


}
