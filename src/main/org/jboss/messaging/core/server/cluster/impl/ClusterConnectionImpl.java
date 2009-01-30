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

import static org.jboss.messaging.core.postoffice.impl.PostOfficeImpl.HDR_RESET_QUEUE_DATA;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.client.management.impl.ManagementHelper;
import org.jboss.messaging.core.cluster.DiscoveryGroup;
import org.jboss.messaging.core.cluster.DiscoveryListener;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.BridgeConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.NotificationType;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.postoffice.impl.LocalQueueBinding;
import org.jboss.messaging.core.postoffice.impl.PostOfficeImpl;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.cluster.Bridge;
import org.jboss.messaging.core.server.cluster.ClusterConnection;
import org.jboss.messaging.core.server.cluster.RemoteQueueBinding;
import org.jboss.messaging.util.ExecutorFactory;
import org.jboss.messaging.util.Pair;
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.UUIDGenerator;

/**
 * 
 * A ClusterConnectionImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 21 Jan 2009 14:43:05
 *
 *
 */
public class ClusterConnectionImpl implements ClusterConnection, DiscoveryListener
{
   private static final Logger log = Logger.getLogger(ClusterConnectionImpl.class);

   private final ExecutorFactory executorFactory;

   private final StorageManager storageManager;

   private final PostOffice postOffice;

   private final SimpleString name;

   private final SimpleString address;

   private final BridgeConfiguration bridgeConfig;

   private final boolean useDuplicateDetection;

   private final boolean forwardWhenNoMatchingConsumers;

   private Map<Pair<TransportConfiguration, TransportConfiguration>, MessageFlowRecord> records = new HashMap<Pair<TransportConfiguration, TransportConfiguration>, MessageFlowRecord>();

   private final DiscoveryGroup discoveryGroup;

   private final ScheduledExecutorService scheduledExecutor;

   private final QueueFactory queueFactory;

   private volatile boolean started;

   /*
    * Constructor using static list of connectors
    */
   public ClusterConnectionImpl(final SimpleString name,
                                final SimpleString address,
                                final BridgeConfiguration bridgeConfig,
                                final boolean useDuplicateDetection,
                                final boolean forwardWhenNoMatchingConsumers,
                                final ExecutorFactory executorFactory,
                                final StorageManager storageManager,
                                final PostOffice postOffice,
                                final ScheduledExecutorService scheduledExecutor,
                                final QueueFactory queueFactory,
                                final List<Pair<TransportConfiguration, TransportConfiguration>> connectors) throws Exception
   {
      this.name = name;

      this.address = address;

      this.bridgeConfig = bridgeConfig;

      this.useDuplicateDetection = useDuplicateDetection;

      this.forwardWhenNoMatchingConsumers = forwardWhenNoMatchingConsumers;

      this.executorFactory = executorFactory;

      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.discoveryGroup = null;

      this.scheduledExecutor = scheduledExecutor;

      this.queueFactory = queueFactory;

      this.updateConnectors(connectors);
   }

   /*
    * Constructor using discovery to get connectors
    */
   public ClusterConnectionImpl(final SimpleString name,
                                final SimpleString address,
                                final BridgeConfiguration bridgeConfig,
                                final boolean useDuplicateDetection,
                                final boolean forwardWhenNoMatchingConsumers,
                                final ExecutorFactory executorFactory,
                                final StorageManager storageManager,
                                final PostOffice postOffice,
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

      this.scheduledExecutor = scheduledExecutor;

      this.queueFactory = queueFactory;

      this.discoveryGroup = discoveryGroup;

      this.useDuplicateDetection = useDuplicateDetection;

      this.forwardWhenNoMatchingConsumers = forwardWhenNoMatchingConsumers;
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

      for (MessageFlowRecord record : records.values())
      {
         record.close();
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

      Iterator<Map.Entry<Pair<TransportConfiguration, TransportConfiguration>, MessageFlowRecord>> iter = records.entrySet()
                                                                                                                 .iterator();

      while (iter.hasNext())
      {
         Map.Entry<Pair<TransportConfiguration, TransportConfiguration>, MessageFlowRecord> entry = iter.next();

         if (!connectorSet.contains(entry.getKey()))
         {
            // Connector no longer there - we should remove and close it - we don't delete the queue though - it may
            // have messages - this is up to the admininstrator to do this

            entry.getValue().close();

            iter.remove();
         }
      }

      for (Pair<TransportConfiguration, TransportConfiguration> connectorPair : connectors)
      {
         if (!records.containsKey(connectorPair))
         {
            SimpleString queueName = generateQueueName(name, connectorPair);

            Binding queueBinding = postOffice.getBinding(queueName);

            Queue queue;

            if (queueBinding != null)
            {
               queue = (Queue)queueBinding.getBindable();
            }
            else
            {
               queue = queueFactory.createQueue(-1, name, null, true, false);

               // Add binding in storage so the queue will get reloaded on startup and we can find it - it's never
               // actually routed to at that address though

               Binding storeBinding = new LocalQueueBinding(queue.getName(), queue);

               storageManager.addQueueBinding(storeBinding);
            }

            MessageFlowRecord record = new MessageFlowRecord(queue);

            Bridge bridge = new BridgeImpl(queueName,
                                           queue,
                                           connectorPair,
                                           executorFactory.getExecutor(),
                                           bridgeConfig.getMaxBatchSize(),
                                           bridgeConfig.getMaxBatchTime(),
                                           bridgeConfig.getFilterString() == null ? null
                                                                                 : new SimpleString(bridgeConfig.getFilterString()),
                                           null,
                                           storageManager,
                                           scheduledExecutor,
                                           null,
                                           bridgeConfig.getRetryInterval(),
                                           bridgeConfig.getRetryIntervalMultiplier(),
                                           bridgeConfig.getMaxRetriesBeforeFailover(),
                                           bridgeConfig.getMaxRetriesAfterFailover(),
                                           false, // Duplicate detection is handled in the RemoteQueueBindingImpl
                                           record,
                                           address.toString(),
                                           true);

            record.setBridge(bridge);

            records.put(connectorPair, record);

            bridge.start();
         }
      }
   }

   private SimpleString generateQueueName(final SimpleString clusterName,
                                          final Pair<TransportConfiguration, TransportConfiguration> connectorPair) throws Exception
   {
      return new SimpleString("cluster." + name +
                              "." +
                              generateConnectorString(connectorPair.a) +
                              "-" +
                              (connectorPair.b == null ? "null" : generateConnectorString(connectorPair.b)));
   }

   private String replaceWildcardChars(final String str)
   {
      return str.replace('.', '-');
   }

   private SimpleString generateConnectorString(final TransportConfiguration config) throws Exception
   {
      StringBuilder str = new StringBuilder(replaceWildcardChars(config.getFactoryClassName()));

      if (config.getParams() != null)
      {
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
      }

      return new SimpleString(str.toString());
   }

   // Inner classes -----------------------------------------------------------------------------------

   private class MessageFlowRecord implements MessageHandler
   {
      private Bridge bridge;

      private final Queue queue;

      private final Map<SimpleString, RemoteQueueBinding> bindings = new HashMap<SimpleString, RemoteQueueBinding>();

      private boolean firstReset = false;

      public MessageFlowRecord(final Queue queue)
      {
         this.queue = queue;
      }

      public void close() throws Exception
      {
         bridge.stop();

         for (RemoteQueueBinding binding : bindings.values())
         {
            postOffice.removeBinding(binding.getUniqueName());
         }
      }

      public void setBridge(final Bridge bridge)
      {
         this.bridge = bridge;
      }

      public void onMessage(final ClientMessage message)
      {
         try
         {
            // Reset the bindings
            if (message.getProperty(HDR_RESET_QUEUE_DATA) != null)
            {
               for (RemoteQueueBinding binding : bindings.values())
               {
                  postOffice.removeBinding(binding.getUniqueName());
               }

               bindings.clear();

               firstReset = true;

               return;
            }

            if (!firstReset)
            {
               return;
            }

            NotificationType type = NotificationType.valueOf(message.getProperty(ManagementHelper.HDR_NOTIFICATION_TYPE)
                                                                    .toString());
 
       
            if (type == NotificationType.BINDING_ADDED)
            {               
               SimpleString uniqueName = UUIDGenerator.getInstance().generateSimpleStringUUID();

               SimpleString queueAddress = (SimpleString)message.getProperty(ManagementHelper.HDR_ADDRESS);

               SimpleString queueName = (SimpleString)message.getProperty(ManagementHelper.HDR_QUEUE_NAME);
               
               SimpleString filterString = (SimpleString)message.getProperty(ManagementHelper.HDR_FILTERSTRING);
               
               Integer queueID = (Integer)message.getProperty(ManagementHelper.HDR_BINDING_ID);

               RemoteQueueBinding binding = new RemoteQueueBindingImpl(queueAddress,
                                                                       uniqueName,
                                                                       queueName,
                                                                       queueID,
                                                                       filterString,
                                                                       queue,
                                                                       useDuplicateDetection,
                                                                       forwardWhenNoMatchingConsumers,
                                                                       bridge.getName());

               bindings.put(queueName, binding);

               postOffice.addBinding(binding);
            }
            else if (type == NotificationType.BINDING_REMOVED)
            {
               SimpleString queueName = (SimpleString)message.getProperty(ManagementHelper.HDR_QUEUE_NAME);

               RemoteQueueBinding binding = bindings.remove(queueName);

               postOffice.removeBinding(binding.getUniqueName());
            }
            else if (type == NotificationType.CONSUMER_CREATED)
            {
               SimpleString queueName = (SimpleString)message.getProperty(ManagementHelper.HDR_QUEUE_NAME);

               SimpleString filterString = (SimpleString)message.getProperty(ManagementHelper.HDR_FILTERSTRING);

               RemoteQueueBinding binding = bindings.get(queueName);

               binding.addConsumer(filterString);
            }
            else if (type == NotificationType.CONSUMER_CLOSED)
            {
               SimpleString queueName = (SimpleString)message.getProperty(ManagementHelper.HDR_QUEUE_NAME);

               SimpleString filterString = (SimpleString)message.getProperty(ManagementHelper.HDR_FILTERSTRING);

               RemoteQueueBinding binding = bindings.get(queueName);

               binding.removeConsumer(filterString);
            }
         }
         catch (Exception e)
         {
            log.error("Failed to handle message", e);
         }
      }

   }

}
