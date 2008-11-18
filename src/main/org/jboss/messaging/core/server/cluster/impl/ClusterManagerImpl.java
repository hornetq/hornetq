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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;

import org.jboss.messaging.core.config.cluster.BroadcastGroupConfiguration;
import org.jboss.messaging.core.config.cluster.DiscoveryGroupConfiguration;
import org.jboss.messaging.core.config.cluster.MessageFlowConfiguration;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.cluster.BroadcastGroup;
import org.jboss.messaging.core.server.cluster.ClusterManager;
import org.jboss.messaging.core.server.cluster.DiscoveryGroup;
import org.jboss.messaging.core.server.cluster.MessageFlow;
import org.jboss.messaging.core.server.cluster.Transformer;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.util.ExecutorFactory;
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
   private final Map<String, BroadcastGroup> broadcastGroups = new HashMap<String, BroadcastGroup>();

   private final Map<String, DiscoveryGroup> discoveryGroups = new HashMap<String, DiscoveryGroup>();

   private final Map<String, MessageFlow> messageFlows = new HashMap<String, MessageFlow>();

   private final ExecutorFactory executorFactory;

   private final StorageManager storageManager;

   private final PostOffice postOffice;

   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;

   private final ScheduledExecutorService scheduledExecutor;

   private volatile boolean started;

   public ClusterManagerImpl(final ExecutorFactory executorFactory,
                             final StorageManager storageManager,
                             final PostOffice postOffice,
                             final HierarchicalRepository<QueueSettings> queueSettingsRepository,
                             final ScheduledExecutorService scheduledExecutor)
   {
      this.executorFactory = executorFactory;

      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.queueSettingsRepository = queueSettingsRepository;

      this.scheduledExecutor = scheduledExecutor;
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }
      
      for (BroadcastGroup group: broadcastGroups.values())
      {
         group.start();
      }
      
      for (DiscoveryGroup group: discoveryGroups.values())
      {
         group.start();
      }
      
      for (MessageFlow flow: this.messageFlows.values())
      {
         flow.start();
      }

      started = true;
   }

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;
      }
      
      for (BroadcastGroup group: broadcastGroups.values())
      {
         group.stop();
      }
      
      for (DiscoveryGroup group: discoveryGroups.values())
      {
         group.stop();
      }
      
      for (MessageFlow flow: this.messageFlows.values())
      {
         flow.stop();
      }

      started = false;
   }

   public boolean isStarted()
   {
      return started;
   }

   public synchronized void deployBroadcastGroup(final BroadcastGroupConfiguration config) throws Exception
   {
      if (broadcastGroups.containsKey(config.getName()))
      {
         throw new IllegalArgumentException("There is already a broadcast-group with name " + config.getName() +
                                            " deployed");
      }

      InetAddress localBindAddress = InetAddress.getByName(config.getLocalBindAddress());

      InetAddress groupAddress = InetAddress.getByName(config.getGroupAddress());

      BroadcastGroupImpl group = new BroadcastGroupImpl(localBindAddress,
                                                        config.getLocalBindPort(),
                                                        groupAddress,
                                                        config.getGroupPort());

      ScheduledFuture<?> future = scheduledExecutor.scheduleWithFixedDelay(group,
                                                                           0L,
                                                                           config.getBroadcastPeriod(),
                                                                           MILLISECONDS);

      group.setScheduledFuture(future);

      broadcastGroups.put(config.getName(), group);

      group.start();
   }

   public synchronized void deployDiscoveryGroup(final DiscoveryGroupConfiguration config) throws Exception
   {
      if (discoveryGroups.containsKey(config.getName()))
      {
         throw new IllegalArgumentException("There is already a discovery-group with name " + config.getName() +
                                            " deployed");
      }

      InetAddress groupAddress = InetAddress.getByName(config.getGroupAddress());

      DiscoveryGroup group = new DiscoveryGroupImpl(groupAddress, config.getGroupPort(), config.getRefreshTimeout());

      discoveryGroups.put(config.getName(), group);

      group.start();
   }

   public synchronized void deployMessageFlow(final MessageFlowConfiguration config) throws Exception
   {
      if (messageFlows.containsKey(config.getName()))
      {
         throw new IllegalArgumentException("There is already a message-flow with name " + config.getName() +
                                            " deployed");
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
                                    transformer,
                                    config.getConnectors());
      }
      else
      {
         // Create message flow with connectors from discovery group

         DiscoveryGroup group = discoveryGroups.get(config.getDiscoveryGroupName());

         if (group == null)
         {
            throw new IllegalArgumentException("There is no discovery-group with name " + config.getDiscoveryGroupName() +
                                               " deployed");
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
                                    transformer,
                                    group);
      }

      messageFlows.put(config.getName(), flow);

      flow.start();
   }

   public synchronized void undeployBroadcastGroup(final String name) throws Exception
   {
      BroadcastGroup group = broadcastGroups.get(name);

      if (group != null)
      {
         group.stop();
      }
   }

   public synchronized void undeployDiscoveryGroup(final String name) throws Exception
   {
      DiscoveryGroup group = discoveryGroups.get(name);

      if (group != null)
      {
         group.stop();
      }
   }

   public synchronized void undeployMessageFlow(final String name) throws Exception
   {
      MessageFlow flow = messageFlows.get(name);

      if (flow != null)
      {
         flow.stop();
      }
   }

}
