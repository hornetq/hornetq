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

import static org.hornetq.core.management.NotificationType.CONSUMER_CLOSED;
import static org.hornetq.core.management.NotificationType.CONSUMER_CREATED;
import static org.hornetq.core.postoffice.impl.PostOfficeImpl.HDR_RESET_QUEUE_DATA;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.management.impl.ManagementHelper;
import org.hornetq.core.cluster.DiscoveryEntry;
import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.cluster.DiscoveryListener;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.ManagementService;
import org.hornetq.core.management.Notification;
import org.hornetq.core.management.NotificationType;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.cluster.Bridge;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.cluster.MessageFlowRecord;
import org.hornetq.core.server.cluster.RemoteQueueBinding;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.Pair;
import org.hornetq.utils.SimpleString;
import org.hornetq.utils.UUID;

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

   private final org.hornetq.utils.ExecutorFactory executorFactory;

   private final HornetQServer server;

   private final PostOffice postOffice;

   private final ManagementService managementService;

   private final SimpleString name;

   private final SimpleString address;

   private final long retryInterval;

   private final boolean useDuplicateDetection;

   private final boolean routeWhenNoConsumers;

   private Map<String, MessageFlowRecord> records = new HashMap<String, MessageFlowRecord>();

   private final DiscoveryGroup discoveryGroup;

   private final ScheduledExecutorService scheduledExecutor;

   private final int maxHops;

   private final UUID nodeUUID;

   private final List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors;

   private boolean backup;

   private volatile boolean started;

   /*
    * Constructor using static list of connectors
    */
   public ClusterConnectionImpl(final SimpleString name,
                                final SimpleString address,
                                final long retryInterval,
                                final boolean useDuplicateDetection,
                                final boolean routeWhenNoConsumers,
                                final org.hornetq.utils.ExecutorFactory executorFactory,
                                final HornetQServer server,
                                final PostOffice postOffice,
                                final ManagementService managementService,
                                final ScheduledExecutorService scheduledExecutor,
                                final List<Pair<TransportConfiguration, TransportConfiguration>> connectors,
                                final int maxHops,
                                final UUID nodeUUID,
                                final boolean backup) throws Exception
   {
      this.name = name;

      this.address = address;

      this.retryInterval = retryInterval;

      this.useDuplicateDetection = useDuplicateDetection;

      this.routeWhenNoConsumers = routeWhenNoConsumers;

      this.executorFactory = executorFactory;

      this.server = server;

      this.postOffice = postOffice;

      this.managementService = managementService;

      this.discoveryGroup = null;

      this.scheduledExecutor = scheduledExecutor;

      this.maxHops = maxHops;

      if (nodeUUID == null)
      {
         throw new IllegalArgumentException("node id is null");
      }

      this.nodeUUID = nodeUUID;

      this.backup = backup;

      this.staticConnectors = connectors;

      if (!backup)
      {
         this.updateFromStaticConnectors(connectors);
      }
   }

   /*
    * Constructor using discovery to get connectors
    */
   public ClusterConnectionImpl(final SimpleString name,
                                final SimpleString address,
                                final long retryInterval,
                                final boolean useDuplicateDetection,
                                final boolean routeWhenNoConsumers,
                                final ExecutorFactory executorFactory,
                                final HornetQServer server,
                                final PostOffice postOffice,
                                final ManagementService managementService,
                                final ScheduledExecutorService scheduledExecutor,
                                final DiscoveryGroup discoveryGroup,
                                final int maxHops,
                                final UUID nodeUUID,
                                final boolean backup) throws Exception
   {
      this.name = name;

      this.address = address;

      this.retryInterval = retryInterval;

      this.executorFactory = executorFactory;

      this.server = server;

      this.postOffice = postOffice;

      this.managementService = managementService;

      this.scheduledExecutor = scheduledExecutor;

      this.discoveryGroup = discoveryGroup;

      this.useDuplicateDetection = useDuplicateDetection;

      this.routeWhenNoConsumers = routeWhenNoConsumers;

      this.maxHops = maxHops;

      this.nodeUUID = nodeUUID;

      this.backup = backup;

      this.staticConnectors = null;
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      if (discoveryGroup != null)
      {
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
         try
         {
            record.close();
         }
         catch (Exception ignore)
         {
         }
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

   public synchronized void activate()
   {
      if (!started)
      {
         return;
      }

      backup = false;

      if (discoveryGroup != null)
      {
         connectorsChanged();
      }
      else
      {
         try
         {
            updateFromStaticConnectors(staticConnectors);
         }
         catch (Exception e)
         {
            log.error("Failed to update connectors", e);
         }
      }
   }

   // DiscoveryListener implementation ------------------------------------------------------------------

   public synchronized void connectorsChanged()
   {
      if (backup)
      {
         return;
      }

      try
      {
         Map<String, DiscoveryEntry> connectors = discoveryGroup.getDiscoveryEntryMap();

         updateConnectors(connectors);
      }
      catch (Exception e)
      {
         log.error("Failed to update connectors", e);
      }
   }

   private void updateFromStaticConnectors(final List<Pair<TransportConfiguration, TransportConfiguration>> connectors) throws Exception
   {
      Map<String, DiscoveryEntry> map = new HashMap<String, DiscoveryEntry>();

      // TODO - we fudge the node id - it's never updated anyway
      int i = 0;
      for (Pair<TransportConfiguration, TransportConfiguration> connectorPair : connectors)
      {
         map.put(String.valueOf(i++), new DiscoveryEntry(connectorPair, 0));
      }

      updateConnectors(map);
   }

   private void updateConnectors(final Map<String, DiscoveryEntry> connectors) throws Exception
   {
      Iterator<Map.Entry<String, MessageFlowRecord>> iter = records.entrySet().iterator();

      while (iter.hasNext())
      {
         Map.Entry<String, MessageFlowRecord> entry = iter.next();

         if (!connectors.containsKey(entry.getKey()))
         {
            // Connector no longer there - we should remove and close it - we don't delete the queue though - it may
            // have messages - this is up to the administrator to do this

            entry.getValue().close();

            iter.remove();
         }
      }

      for (final Map.Entry<String, DiscoveryEntry> entry : connectors.entrySet())
      {
         if (!records.containsKey(entry.getKey()))
         {
            Pair<TransportConfiguration, TransportConfiguration> connectorPair = entry.getValue().getConnectorPair();

            final SimpleString queueName = new SimpleString("sf." + name + "." + entry.getKey());

            Binding queueBinding = postOffice.getBinding(queueName);

            Queue queue;

            if (queueBinding != null)
            {
               queue = (Queue)queueBinding.getBindable();

               createNewRecord(entry.getKey(), connectorPair, queueName, queue, true);
            }
            else
            {
               // Add binding in storage so the queue will get reloaded on startup and we can find it - it's never
               // actually routed to at that address though

               queue = server.createQueue(queueName, queueName, null, true, false);

               createNewRecord(entry.getKey(), connectorPair, queueName, queue, true);
            }
         }
      }
   }

   private void createNewRecord(final String nodeID,
                                final Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                                final SimpleString queueName,
                                final Queue queue,
                                final boolean start) throws Exception
   {
      MessageFlowRecordImpl record = new MessageFlowRecordImpl(queue);

      Bridge bridge = new BridgeImpl(nodeUUID,
                                     queueName,
                                     queue,
                                     null,
                                     -1,
                                     connectorPair,
                                     executorFactory.getExecutor(),
                                     null,
                                     null,
                                     scheduledExecutor,
                                     null,
                                     retryInterval,
                                     1d,
                                     -1,
                                     true,
                                     useDuplicateDetection,
                                     managementService.getManagementAddress(),
                                     managementService.getManagementNotificationAddress(),
                                     managementService.getClusterUser(),
                                     managementService.getClusterPassword(),
                                     record,
                                     !backup,
                                     server.getStorageManager());

      record.setBridge(bridge);

      records.put(nodeID, record);

      if (start)
      {
         bridge.start();
      }
   }

   // Inner classes -----------------------------------------------------------------------------------

   private class MessageFlowRecordImpl implements MessageFlowRecord
   {
      private Bridge bridge;

      private Queue queue;

      private final Map<SimpleString, RemoteQueueBinding> bindings = new HashMap<SimpleString, RemoteQueueBinding>();

      private volatile boolean firstReset = false;

      public MessageFlowRecordImpl(final Queue queue)
      {
         this.queue = queue;
      }

      public String getAddress()
      {
         return address.toString();
      }

      public int getMaxHops()
      {
         return maxHops;
      }

      public void close() throws Exception
      {
         bridge.stop();

         clearBindings();
      }

      public void activate(final Queue queue) throws Exception
      {
         this.queue = queue;

         bridge.setQueue(queue);

         bridge.start();
      }

      public void setBridge(final Bridge bridge)
      {
         this.bridge = bridge;
      }

      public synchronized void onMessage(final ClientMessage message)
      {
         try
         {
            // Reset the bindings
            if (message.getProperty(HDR_RESET_QUEUE_DATA) != null)
            {
               clearBindings();

               firstReset = true;

               return;
            }

            if (!firstReset)
            {
               return;
            }

            // TODO - optimised this by just passing int in header - but filter needs to be extended to support IN with
            // a list of integers
            SimpleString type = (SimpleString)message.getProperty(ManagementHelper.HDR_NOTIFICATION_TYPE);

            NotificationType ntype = NotificationType.valueOf(type.toString());

            switch (ntype)
            {
               case BINDING_ADDED:
               {
                  doBindingAdded(message);

                  break;
               }
               case BINDING_REMOVED:
               {
                  doBindingRemoved(message);

                  break;
               }
               case CONSUMER_CREATED:
               {
                  doConsumerCreated(message);

                  break;
               }
               case CONSUMER_CLOSED:
               {
                  doConsumerClosed(message);

                  break;
               }
               case SECURITY_AUTHENTICATION_VIOLATION:
               case SECURITY_PERMISSION_VIOLATION:
                  break;
               default:
               {
                  throw new IllegalArgumentException("Invalid type " + ntype);
               }
            }
         }
         catch (Exception e)
         {
            log.error("Failed to handle message", e);
         }
      }

      private synchronized void clearBindings() throws Exception
      {
         for (RemoteQueueBinding binding : new HashSet<RemoteQueueBinding>(bindings.values()))
         {
            removeBinding(binding.getClusterName());
         }
      }

      private synchronized void doBindingAdded(final ClientMessage message) throws Exception
      {
         Integer distance = (Integer)message.getProperty(ManagementHelper.HDR_DISTANCE);

         if (distance == null)
         {
            throw new IllegalStateException("distance is null");
         }

         SimpleString queueAddress = (SimpleString)message.getProperty(ManagementHelper.HDR_ADDRESS);

         if (queueAddress == null)
         {
            throw new IllegalStateException("queueAddress is null");
         }

         SimpleString clusterName = (SimpleString)message.getProperty(ManagementHelper.HDR_CLUSTER_NAME);

         if (clusterName == null)
         {
            throw new IllegalStateException("clusterName is null");
         }

         SimpleString routingName = (SimpleString)message.getProperty(ManagementHelper.HDR_ROUTING_NAME);

         if (routingName == null)
         {
            throw new IllegalStateException("routingName is null");
         }

         SimpleString filterString = (SimpleString)message.getProperty(ManagementHelper.HDR_FILTERSTRING);

         Integer queueID = (Integer)message.getProperty(ManagementHelper.HDR_BINDING_ID);

         if (queueID == null)
         {
            throw new IllegalStateException("queueID is null");
         }

         RemoteQueueBinding binding = new RemoteQueueBindingImpl(queueAddress,
                                                                 clusterName,
                                                                 routingName,
                                                                 queueID,
                                                                 filterString,
                                                                 queue,
                                                                 bridge.getName(),
                                                                 distance + 1);

         bindings.put(clusterName, binding);

         if (postOffice.getBinding(clusterName) != null)
         {
            // Sanity check - this means the binding has already been added via another bridge, probably max
            // hops is too high
            // or there are multiple cluster connections for the same address

            log.warn("Remote queue binding " + clusterName +
                     " has already been bound in the post office. Most likely cause for this is you have a loop " +
                     "in your cluster due to cluster max-hops being too large or you have multiple cluster connections to the same nodes using overlapping addresses");

            return;
         }

         try
         {
            postOffice.addBinding(binding);
         }
         catch (Exception ignore)
         {
         }

         Bindings theBindings = postOffice.getBindingsForAddress(queueAddress);

         theBindings.setRouteWhenNoConsumers(routeWhenNoConsumers);

      }

      private void doBindingRemoved(final ClientMessage message) throws Exception
      {
         SimpleString clusterName = (SimpleString)message.getProperty(ManagementHelper.HDR_CLUSTER_NAME);

         if (clusterName == null)
         {
            throw new IllegalStateException("clusterName is null");
         }

         removeBinding(clusterName);
      }

      private synchronized void removeBinding(final SimpleString clusterName) throws Exception
      {
         RemoteQueueBinding binding = bindings.remove(clusterName);

         if (binding == null)
         {
            throw new IllegalStateException("Cannot find binding for queue " + clusterName);
         }

         postOffice.removeBinding(binding.getUniqueName());
      }

      private synchronized void doConsumerCreated(final ClientMessage message) throws Exception
      {
         Integer distance = (Integer)message.getProperty(ManagementHelper.HDR_DISTANCE);

         if (distance == null)
         {
            throw new IllegalStateException("distance is null");
         }

         SimpleString clusterName = (SimpleString)message.getProperty(ManagementHelper.HDR_CLUSTER_NAME);

         if (clusterName == null)
         {
            throw new IllegalStateException("clusterName is null");
         }

         message.putIntProperty(ManagementHelper.HDR_DISTANCE, distance + 1);

         SimpleString filterString = (SimpleString)message.getProperty(ManagementHelper.HDR_FILTERSTRING);

         RemoteQueueBinding binding = bindings.get(clusterName);

         if (binding == null)
         {
            throw new IllegalStateException("Cannot find binding for " + clusterName);
         }

         binding.addConsumer(filterString);

         // Need to propagate the consumer add
         Notification notification = new Notification(null, CONSUMER_CREATED, message.getProperties());

         managementService.sendNotification(notification);
      }

      private synchronized void doConsumerClosed(final ClientMessage message) throws Exception
      {
         Integer distance = (Integer)message.getProperty(ManagementHelper.HDR_DISTANCE);

         if (distance == null)
         {
            throw new IllegalStateException("distance is null");
         }

         SimpleString clusterName = (SimpleString)message.getProperty(ManagementHelper.HDR_CLUSTER_NAME);

         if (clusterName == null)
         {
            throw new IllegalStateException("clusterName is null");
         }

         message.putIntProperty(ManagementHelper.HDR_DISTANCE, distance + 1);

         SimpleString filterString = (SimpleString)message.getProperty(ManagementHelper.HDR_FILTERSTRING);

         RemoteQueueBinding binding = bindings.get(clusterName);

         if (binding == null)
         {
            throw new IllegalStateException("Cannot find binding for " + clusterName);
         }

         binding.removeConsumer(filterString);

         // Need to propagate the consumer close
         Notification notification = new Notification(null, CONSUMER_CLOSED, message.getProperties());

         managementService.sendNotification(notification);         
      }

   }

   public void handleReplicatedAddBinding(final SimpleString address,
                                          final SimpleString uniqueName,
                                          final SimpleString routingName,
                                          final int queueID,
                                          final SimpleString filterString,
                                          final SimpleString queueName,
                                          final int distance) throws Exception
   {
      Binding queueBinding = postOffice.getBinding(queueName);

      if (queueBinding == null)
      {
         throw new IllegalStateException("Cannot find s & f queue " + queueName);
      }

      Queue queue = (Queue)queueBinding.getBindable();

      RemoteQueueBinding binding = new RemoteQueueBindingImpl(address,
                                                              uniqueName,
                                                              routingName,
                                                              queueID,
                                                              filterString,
                                                              queue,
                                                              queueName,
                                                              distance);

      if (postOffice.getBinding(uniqueName) != null)
      {
         log.warn("Remoting queue binding " + uniqueName +
                  " has already been bound in the post office. Most likely cause for this is you have a loop " +
                  "in your cluster due to cluster max-hops being too large or you have multiple cluster connections to the same nodes using overlapping addresses");

         return;
      }

      postOffice.addBinding(binding);

      Bindings theBindings = postOffice.getBindingsForAddress(address);

      theBindings.setRouteWhenNoConsumers(routeWhenNoConsumers);
   }

}
