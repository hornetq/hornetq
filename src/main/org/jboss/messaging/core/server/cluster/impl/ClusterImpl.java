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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import org.jboss.messaging.core.cluster.DiscoveryGroup;
import org.jboss.messaging.core.cluster.DiscoveryListener;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.BridgeConfiguration;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.postoffice.impl.BindingImpl;
import org.jboss.messaging.core.postoffice.impl.FlowBinding;
import org.jboss.messaging.core.server.Bindable;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.cluster.Bridge;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.util.ExecutorFactory;
import org.jboss.messaging.util.Pair;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A ClusterImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 21 Jan 2009 14:43:05
 *
 *
 */
public class ClusterImpl implements DiscoveryListener
{
   private static final Logger log = Logger.getLogger(ClusterImpl.class);

   private final ExecutorFactory executorFactory;

   private final StorageManager storageManager;

   private final PostOffice postOffice;

   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;

   private final SimpleString name;

   private final SimpleString address;

   private final BridgeConfiguration bridgeConfig;

   private final boolean useDuplicateDetection;

   private final int maxHops;

   private Map<Pair<TransportConfiguration, TransportConfiguration>, Bridge> bridges = new HashMap<Pair<TransportConfiguration, TransportConfiguration>, Bridge>();

   private final DiscoveryGroup discoveryGroup;

   private final ScheduledExecutorService scheduledExecutor;

   private final QueueFactory queueFactory;

   private volatile boolean started;

   /*
    * Constructor using static list of connectors
    */
   public ClusterImpl(final SimpleString name,
                      final SimpleString address,
                      final BridgeConfiguration bridgeConfig,
                      final boolean useDuplicateDetection,
                      final int maxHops,
                      final ExecutorFactory executorFactory,
                      final StorageManager storageManager,
                      final PostOffice postOffice,
                      final HierarchicalRepository<QueueSettings> queueSettingsRepository,
                      final ScheduledExecutorService scheduledExecutor,
                      final QueueFactory queueFactory,
                      final List<Pair<TransportConfiguration, TransportConfiguration>> connectors) throws Exception
   {
      this.name = name;

      this.address = address;

      this.bridgeConfig = bridgeConfig;

      this.useDuplicateDetection = useDuplicateDetection;

      this.maxHops = maxHops;

      this.executorFactory = executorFactory;

      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.queueSettingsRepository = queueSettingsRepository;

      this.discoveryGroup = null;

      this.scheduledExecutor = scheduledExecutor;

      this.queueFactory = queueFactory;

      this.updateConnectors(connectors);
   }

   /*
    * Constructor using discovery to get connectors
    */
   public ClusterImpl(final SimpleString name,
                      final SimpleString address,
                      final BridgeConfiguration bridgeConfig,
                      final boolean useDuplicateDetection,
                      final int maxHops,
                      final ExecutorFactory executorFactory,
                      final StorageManager storageManager,
                      final PostOffice postOffice,
                      final HierarchicalRepository<QueueSettings> queueSettingsRepository,
                      final ScheduledExecutorService scheduledExecutor,
                      final QueueFactory queueFactory,
                      final DiscoveryGroup discoveryGroup) throws Exception
   {
      this.name = name;

      this.address = address;
      
      this.bridgeConfig = bridgeConfig;

      this.executorFactory = executorFactory;

      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.queueSettingsRepository = queueSettingsRepository;

      this.scheduledExecutor = scheduledExecutor;
      
      this.queueFactory = queueFactory;

      this.discoveryGroup = discoveryGroup;

      this.useDuplicateDetection = useDuplicateDetection;

      this.maxHops = maxHops;
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      if (discoveryGroup != null)
      {
         updateConnectors(discoveryGroup.getConnectors());

         discoveryGroup.registerListener(this);
      }

      started = true;
   }

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;
      }

      if (discoveryGroup != null)
      {
         discoveryGroup.unregisterListener(this);
      }

      for (Bridge bridge : bridges.values())
      {
         bridge.stop();
      }

      started = false;
   }

   public boolean isStarted()
   {
      return started;
   }

   public SimpleString getName()
   {
      return name;
   }

   // DiscoveryListener implementation ------------------------------------------------------------------

   public void connectorsChanged()
   {
      try
      {
         List<Pair<TransportConfiguration, TransportConfiguration>> connectors = discoveryGroup.getConnectors();

         updateConnectors(connectors);
      }
      catch (Exception e)
      {
         log.error("Failed to update connectors", e);
      }
   }

   private void updateConnectors(final List<Pair<TransportConfiguration, TransportConfiguration>> connectors) throws Exception
   {
      Set<Pair<TransportConfiguration, TransportConfiguration>> connectorSet = new HashSet<Pair<TransportConfiguration, TransportConfiguration>>();

      connectorSet.addAll(connectors);

      Iterator<Map.Entry<Pair<TransportConfiguration, TransportConfiguration>, Bridge>> iter = bridges.entrySet()
                                                                                                      .iterator();

      while (iter.hasNext())
      {
         Map.Entry<Pair<TransportConfiguration, TransportConfiguration>, Bridge> entry = iter.next();

         if (!connectorSet.contains(entry.getKey()))
         {
            // Connector no longer there - we should remove and close it - we don't delete the queue though - it may have messages - this is up to the admininstrator to do this

            entry.getValue().stop();

            iter.remove();
         }
      }

      for (Pair<TransportConfiguration, TransportConfiguration> connectorPair : connectors)
      {
         if (!bridges.containsKey(connectorPair))
         {
            SimpleString queueName = new SimpleString("cluster." + name +
                                                      "." +
                                                      generateConnectorString(connectorPair.a) +
                                                      "-" +
                                                      (connectorPair.b == null ? "null"
                                                                              : generateConnectorString(connectorPair.b)));

            Binding queueBinding = postOffice.getBinding(queueName);

            Queue queue;

            if (queueBinding != null)
            {
               queue = (Queue)queueBinding.getBindable();
            }
            else
            {
               queue = queueFactory.createQueue(-1, name, null, true, false);

               // Add binding in storage so the queue will get reloaded on startup and we can find it - it's never actually routed to at that address though

               Binding storeBinding = new BindingImpl(queue.getName(), queue.getName(), queue.getName(), queue, false, true);

               storageManager.addQueueBinding(storeBinding);
            }

            Bridge bridge = new BridgeImpl(queueName,
                                           queue,
                                           connectorPair,
                                           executorFactory.getExecutor(),
                                           bridgeConfig.getMaxBatchSize(),
                                           bridgeConfig.getMaxBatchTime(),
                                           new SimpleString(bridgeConfig.getFilterString()),
                                           null,
                                           storageManager,
                                           scheduledExecutor,
                                           null,
                                           bridgeConfig.getRetryInterval(),
                                           bridgeConfig.getRetryIntervalMultiplier(),
                                           bridgeConfig.getMaxRetriesBeforeFailover(),
                                           bridgeConfig.getMaxRetriesAfterFailover(),
                                           false);

            bridges.put(connectorPair, bridge);

            bridge.start();
         }
      }
   }
       
   private void updateQueueInfo(final Pair<TransportConfiguration, TransportConfiguration> connectorPair, final QueueInfo info) throws Exception
   {
      Bridge bridge = this.bridges.get(connectorPair);
      
      if (bridge == null)
      {
         throw new IllegalArgumentException("Cannot find bridge for " + connectorPair);
      }
      
      SimpleString uniqueName = null;  // ?????
      
      FlowBinding flowBinding = (FlowBinding)postOffice.getBinding(uniqueName);
      
      if (flowBinding == null)
      {
         //TODO - can be optimised by storing the queue with the bridge in this class
         Binding binding = postOffice.getBinding(bridge.getName());
         
         if (binding == null)
         {
            throw new IllegalStateException("Cannot find queue with name " + bridge.getName());
         }
         
         Queue queue = (Queue)binding.getBindable();
           
         FlowBindingFilter filter = new FlowBindingFilter(info);
         
         flowBinding = new FlowBinding(new SimpleString(info.getAddress()), uniqueName, new SimpleString(info.getQueueName()), queue, filter);
         
         postOffice.addBinding(flowBinding);
      }
      else
      {
         FlowBindingFilter filter = flowBinding.getFilter();
         
         filter.updateInfo(info);
      }
   }
   
   private void removeQueueInfo()
   {
      //TODO
   }
   
   private String replaceWildcardChars(final String str)
   {
      return str.replace('.', '-');
   }

   private SimpleString generateConnectorString(final TransportConfiguration config) throws Exception
   {
      StringBuilder str = new StringBuilder(replaceWildcardChars(config.getFactoryClassName()));

      if (!config.getParams().isEmpty())
      {
         str.append("?");
      }

      boolean first = true;
      for (Map.Entry<String, Object> entry : config.getParams().entrySet())
      {
         if (!first)
         {
            str.append("&");
         }
         String encodedKey = replaceWildcardChars(entry.getKey());

         String val = entry.getValue().toString();
         String encodedVal = replaceWildcardChars(val);

         str.append(encodedKey).append('=').append(encodedVal);

         first = false;
      }

      return new SimpleString(str.toString());
   }

}
