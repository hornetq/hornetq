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
import org.jboss.messaging.core.config.cluster.BroadcastGroupConfiguration;
import org.jboss.messaging.core.config.cluster.DiscoveryGroupConfiguration;
import org.jboss.messaging.core.config.cluster.MessageFlowConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.cluster.BroadcastGroup;
import org.jboss.messaging.core.server.cluster.ClusterManager;
import org.jboss.messaging.core.server.cluster.MessageFlow;
import org.jboss.messaging.core.server.cluster.Transformer;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
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

   private final Map<String, MessageFlow> messageFlows = new HashMap<String, MessageFlow>();

   private final ExecutorFactory executorFactory;

   private final StorageManager storageManager;

   private final PostOffice postOffice;

   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;

   private final ScheduledExecutorService scheduledExecutor;

   private final Configuration configuration;

   private volatile boolean started;
      
   public ClusterManagerImpl(final ExecutorFactory executorFactory,
                             final StorageManager storageManager,
                             final PostOffice postOffice,
                             final HierarchicalRepository<QueueSettings> queueSettingsRepository,
                             final ScheduledExecutorService scheduledExecutor,
                             final Configuration configuration)
   {
      this.executorFactory = executorFactory;

      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.queueSettingsRepository = queueSettingsRepository;

      this.scheduledExecutor = scheduledExecutor;

      this.configuration = configuration;
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

      for (MessageFlowConfiguration config : configuration.getMessageFlowConfigurations())
      {
         deployMessageFlow(config);
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
      }

      for (DiscoveryGroup group : discoveryGroups.values())
      {
         group.stop();
      }

      for (MessageFlow flow : this.messageFlows.values())
      {
         flow.stop();
      }

      broadcastGroups.clear();

      discoveryGroups.clear();

      messageFlows.clear();

      started = false;
   }

   public boolean isStarted()
   {
      return started;
   }
   
   public Map<String, MessageFlow> getMessageFlows()
   {
      return new HashMap<String, MessageFlow>(messageFlows);
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

      BroadcastGroupImpl group = new BroadcastGroupImpl(localBindAddress,
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

      DiscoveryGroup group = new DiscoveryGroupImpl(groupAddress, config.getGroupPort(), config.getRefreshTimeout());

      discoveryGroups.put(config.getName(), group);

      group.start();
   }

   private synchronized void deployMessageFlow(final MessageFlowConfiguration config) throws Exception
   {
      if (config.getName() == null)
      {
         log.warn("Must specify a unique name for each message flow. This one will not be deployed.");

         return;
      }

      if (config.getAddress() == null)
      {
         log.warn("Must specify an address each message flow. This one will not be deployed.");

         return;
      }

      if (messageFlows.containsKey(config.getName()))
      {
         log.warn("There is already a message-flow with name " + config.getName() +
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

      Transformer transformer = null;

      if (config.getTransformerClassName() != null)
      {
         ClassLoader loader = Thread.currentThread().getContextClassLoader();
         try
         {
            Class<?> clz = loader.loadClass(config.getTransformerClassName());
            transformer = (Transformer)clz.newInstance();
         }
         catch (Exception e)
         {
            throw new IllegalArgumentException("Error instantiating transformer class \"" + config.getTransformerClassName() +
                                                        "\"",
                                               e);
         }
      }

      MessageFlow flow;

      if (config.getDiscoveryGroupName() == null)
      {
         // Create message flow with list of static connectors

         List<Pair<TransportConfiguration, TransportConfiguration>> conns = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();

         for (Pair<String, String> connectorNamePair : config.getConnectorNamePairs())
         {
            TransportConfiguration connector = configuration.getConnectorConfigurations().get(connectorNamePair.a);

            if (connector == null)
            {
               log.warn("No connector defined with name '" + connectorNamePair.a +
                        "'. The message flow will not be deployed.");

               return;
            }

            TransportConfiguration backupConnector = null;

            if (connectorNamePair.b != null)
            {
               backupConnector = configuration.getConnectorConfigurations().get(connectorNamePair.b);

               if (backupConnector == null)
               {
                  log.warn("No connector defined with name '" + connectorNamePair.b +
                           "'. The message flow will not be deployed.");

                  return;
               }
            }

            conns.add(new Pair<TransportConfiguration, TransportConfiguration>(connector, backupConnector));
         }

         flow = new MessageFlowImpl(new SimpleString(config.getName()),
                                    new SimpleString(config.getAddress()),
                                    config.getMaxBatchSize(),
                                    config.getMaxBatchTime(),
                                    config.getFilterString() == null ? null
                                                                    : new SimpleString(config.getFilterString()),
                                    config.isFanout(),
                                    executorFactory,
                                    storageManager,
                                    postOffice,
                                    queueSettingsRepository,
                                    scheduledExecutor,
                                    transformer,
                                    config.getRetryInterval(),
                                    config.getRetryIntervalMultiplier(),
                                    config.getMaxRetriesBeforeFailover(),
                                    config.getMaxRetriesAfterFailover(),
                                    conns);
      }
      else
      {
         // Create message flow with connectors from discovery group

         DiscoveryGroup group = discoveryGroups.get(config.getDiscoveryGroupName());

         if (group == null)
         {
            log.warn("There is no discovery-group with name " + config.getDiscoveryGroupName() +
                     " deployed. This one will not be deployed.");

            return;
         }

         flow = new MessageFlowImpl(new SimpleString(config.getName()),
                                    new SimpleString(config.getAddress()),
                                    config.getMaxBatchSize(),
                                    config.getMaxBatchTime(),
                                    config.getFilterString() == null ? null
                                                                    : new SimpleString(config.getFilterString()),
                                    config.isFanout(),
                                    this.executorFactory,
                                    storageManager,
                                    postOffice,
                                    queueSettingsRepository,
                                    scheduledExecutor,
                                    transformer,
                                    config.getRetryInterval(),
                                    config.getRetryIntervalMultiplier(),
                                    config.getMaxRetriesBeforeFailover(),
                                    config.getMaxRetriesAfterFailover(),
                                    group);
      }

      messageFlows.put(config.getName(), flow);

      flow.start();
   }
}
