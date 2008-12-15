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
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.filter.impl.FilterImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.server.cluster.Forwarder;
import org.jboss.messaging.core.server.cluster.MessageFlow;
import org.jboss.messaging.core.server.cluster.Transformer;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.util.ExecutorFactory;
import org.jboss.messaging.util.Pair;
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.UUIDGenerator;

/**
 * A MessageFlowImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 17 Nov 2008 15:55:49
 *
 *
 */
public class MessageFlowImpl implements DiscoveryListener, MessageFlow
{
   private static final Logger log = Logger.getLogger(MessageFlowImpl.class);

   private final SimpleString name;

   private final SimpleString address;

   private final SimpleString filterString;

   private final boolean fanout;

   private final int maxBatchSize;

   private final long maxBatchTime;

   private final ExecutorFactory executorFactory;

   private final StorageManager storageManager;

   private final PostOffice postOffice;

   private final HierarchicalRepository<QueueSettings> queueSettingsRepository;

   private final Transformer transformer;

   private Map<Pair<TransportConfiguration, TransportConfiguration>, Forwarder> forwarders = new HashMap<Pair<TransportConfiguration, TransportConfiguration>, Forwarder>();

   private final DiscoveryGroup discoveryGroup;

   private final ScheduledExecutorService scheduledExecutor;

   private volatile boolean started;

   private final long retryInterval;

   private final double retryIntervalMultiplier;

   private final int maxRetriesBeforeFailover;

   private final int maxRetriesAfterFailover;
   
   private final boolean useDuplicateDetection;

   /*
    * Constructor using static list of connectors
    */
   public MessageFlowImpl(final SimpleString name,
                          final SimpleString address,
                          final int maxBatchSize,
                          final long maxBatchTime,
                          final SimpleString filterString,
                          final boolean fanout,
                          final ExecutorFactory executorFactory,
                          final StorageManager storageManager,
                          final PostOffice postOffice,
                          final HierarchicalRepository<QueueSettings> queueSettingsRepository,
                          final ScheduledExecutorService scheduledExecutor,
                          final Transformer transformer,
                          final long retryInterval,
                          final double retryIntervalMultiplier,
                          final int maxRetriesBeforeFailover,
                          final int maxRetriesAfterFailover,
                          final boolean useDuplicateDetection,
                          final List<Pair<TransportConfiguration, TransportConfiguration>> connectors) throws Exception
   {
      this.name = name;

      this.address = address;

      this.maxBatchSize = maxBatchSize;

      this.maxBatchTime = maxBatchTime;

      this.filterString = filterString;

      this.fanout = fanout;

      this.executorFactory = executorFactory;

      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.queueSettingsRepository = queueSettingsRepository;

      this.transformer = transformer;

      this.discoveryGroup = null;

      this.scheduledExecutor = scheduledExecutor;

      this.retryInterval = retryInterval;

      this.retryIntervalMultiplier = retryIntervalMultiplier;

      this.maxRetriesBeforeFailover = maxRetriesBeforeFailover;

      this.maxRetriesAfterFailover = maxRetriesAfterFailover;
      
      this.useDuplicateDetection = useDuplicateDetection;

      this.updateConnectors(connectors);
   }

   /*
    * Constructor using discovery to get connectors
    */
   public MessageFlowImpl(final SimpleString name,
                          final SimpleString address,
                          final int maxBatchSize,
                          final long maxBatchTime,
                          final SimpleString filterString,
                          final boolean fanout,
                          final ExecutorFactory executorFactory,
                          final StorageManager storageManager,
                          final PostOffice postOffice,
                          final HierarchicalRepository<QueueSettings> queueSettingsRepository,
                          final ScheduledExecutorService scheduledExecutor,
                          final Transformer transformer,
                          final long retryInterval,
                          final double retryIntervalMultiplier,
                          final int maxRetriesBeforeFailover,
                          final int maxRetriesAfterFailover,
                          final boolean useDuplicateDetection,
                          final DiscoveryGroup discoveryGroup) throws Exception
   {
      this.name = name;

      this.address = address;

      this.maxBatchSize = maxBatchSize;

      this.maxBatchTime = maxBatchTime;

      this.filterString = filterString;

      this.fanout = fanout;

      this.executorFactory = executorFactory;

      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.queueSettingsRepository = queueSettingsRepository;

      this.scheduledExecutor = scheduledExecutor;

      this.transformer = transformer;

      this.discoveryGroup = discoveryGroup;

      this.retryInterval = retryInterval;

      this.retryIntervalMultiplier = retryIntervalMultiplier;

      this.maxRetriesBeforeFailover = maxRetriesBeforeFailover;

      this.maxRetriesAfterFailover = maxRetriesAfterFailover;
      
      this.useDuplicateDetection = useDuplicateDetection;
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

      for (Forwarder forwarder : forwarders.values())
      {
         forwarder.stop();
      }

      started = false;
   }

   public boolean isStarted()
   {
      return started;
   }
   
   // MessageFlow implementation ------------------------------------
   
   public SimpleString getName()
   {
      return name;
   }
   
   //For testing only
   public Set<Forwarder> getForwarders()
   {
      return new HashSet<Forwarder>(forwarders.values());
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

      Iterator<Map.Entry<Pair<TransportConfiguration, TransportConfiguration>, Forwarder>> iter = forwarders.entrySet()
                                                                                                            .iterator();

      while (iter.hasNext())
      {
         Map.Entry<Pair<TransportConfiguration, TransportConfiguration>, Forwarder> entry = iter.next();

         if (!connectorSet.contains(entry.getKey()))
         {
            // Connector no longer there - we should remove and close it

            entry.getValue().stop();

            iter.remove();
         }
      }

      for (Pair<TransportConfiguration, TransportConfiguration> connectorPair : connectors)
      {
         if (!forwarders.containsKey(connectorPair))
         {
            SimpleString queueName = new SimpleString("outflow." + name +
                                                      "." +
                                                      UUIDGenerator.getInstance().generateSimpleStringUUID());

            Binding binding = postOffice.getBinding(queueName);

            // TODO need to delete store and forward queues that are no longer in the config
            // and also allow ability to change filterstring etc. while keeping the same name
            if (binding == null)
            {
               Filter filter = filterString == null ? null : new FilterImpl(filterString);

               binding = postOffice.addBinding(address, queueName, filter, true, false, fanout);
            }

            Forwarder forwarder = new ForwarderImpl(binding.getQueue(),
                                                    connectorPair,
                                                    executorFactory.getExecutor(),
                                                    maxBatchSize,
                                                    maxBatchTime,
                                                    storageManager,
                                                    postOffice,
                                                    queueSettingsRepository,
                                                    scheduledExecutor,
                                                    transformer,
                                                    retryInterval,
                                                    retryIntervalMultiplier,
                                                    maxRetriesBeforeFailover,
                                                    maxRetriesAfterFailover);

            forwarders.put(connectorPair, forwarder);

            binding.getQueue().addConsumer(forwarder);

            forwarder.start();
         }
      }
   }

}
