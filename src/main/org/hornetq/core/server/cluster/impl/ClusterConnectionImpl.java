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

import static org.hornetq.api.core.management.NotificationType.CONSUMER_CLOSED;
import static org.hornetq.api.core.management.NotificationType.CONSUMER_CREATED;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.cluster.DiscoveryEntry;
import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.cluster.DiscoveryListener;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.impl.PostOfficeImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.cluster.Bridge;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.cluster.MessageFlowRecord;
import org.hornetq.core.server.cluster.RemoteQueueBinding;
import org.hornetq.core.server.group.impl.Proposal;
import org.hornetq.core.server.group.impl.Response;
import org.hornetq.core.server.management.ManagementService;
import org.hornetq.core.server.management.Notification;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.TypedProperties;
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

   private final int confirmationWindowSize;

   private final boolean routeWhenNoConsumers;

   private final Map<String, MessageFlowRecord> records = new HashMap<String, MessageFlowRecord>();

   private final DiscoveryGroup discoveryGroup;

   private final ScheduledExecutorService scheduledExecutor;

   private final int maxHops;

   private final UUID nodeUUID;

   private final List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors;

   private boolean backup;

   private volatile boolean started;
   
   private final String clusterUser;
   
   private final String clusterPassword;

   /*
    * Constructor using static list of connectors
    */
   public ClusterConnectionImpl(final SimpleString name,
                                final SimpleString address,
                                final long retryInterval,
                                final boolean useDuplicateDetection,
                                final boolean routeWhenNoConsumers,
                                final int confirmationWindowSize,
                                final org.hornetq.utils.ExecutorFactory executorFactory,
                                final HornetQServer server,
                                final PostOffice postOffice,
                                final ManagementService managementService,
                                final ScheduledExecutorService scheduledExecutor,
                                final List<Pair<TransportConfiguration, TransportConfiguration>> connectors,
                                final int maxHops,
                                final UUID nodeUUID,
                                final boolean backup,
                                final String clusterUser,
                                final String clusterPassword) throws Exception
   {
      this.name = name;

      this.address = address;

      this.retryInterval = retryInterval;

      this.useDuplicateDetection = useDuplicateDetection;

      this.routeWhenNoConsumers = routeWhenNoConsumers;

      this.confirmationWindowSize = confirmationWindowSize;

      this.executorFactory = executorFactory;

      this.server = server;

      this.postOffice = postOffice;

      this.managementService = managementService;

      discoveryGroup = null;

      this.scheduledExecutor = scheduledExecutor;

      this.maxHops = maxHops;

      if (nodeUUID == null)
      {
         throw new IllegalArgumentException("node id is null");
      }

      this.nodeUUID = nodeUUID;

      this.backup = backup;

      staticConnectors = connectors;
      
      this.clusterUser = clusterUser;
      
      this.clusterPassword = clusterPassword;

      if (!backup)
      {
         updateFromStaticConnectors(connectors);
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
                                final int confirmationWindowSize,
                                final ExecutorFactory executorFactory,
                                final HornetQServer server,
                                final PostOffice postOffice,
                                final ManagementService managementService,
                                final ScheduledExecutorService scheduledExecutor,
                                final DiscoveryGroup discoveryGroup,
                                final int maxHops,
                                final UUID nodeUUID,
                                final boolean backup,
                                final String clusterUser,
                                final String clusterPassword) throws Exception
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

      this.confirmationWindowSize = confirmationWindowSize;

      this.maxHops = maxHops;

      this.nodeUUID = nodeUUID;

      this.backup = backup;
      
      this.clusterUser = clusterUser;
      
      this.clusterPassword = clusterPassword;

      staticConnectors = null;
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

      if (managementService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), name);
         Notification notification = new Notification(nodeUUID.toString(),
                                                      NotificationType.CLUSTER_CONNECTION_STARTED,
                                                      props);
         managementService.sendNotification(notification);
      }
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

      if (managementService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), name);
         Notification notification = new Notification(nodeUUID.toString(),
                                                      NotificationType.CLUSTER_CONNECTION_STOPPED,
                                                      props);
         managementService.sendNotification(notification);
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

   public String getNodeID()
   {
      return nodeUUID.toString();
   }

   public synchronized Map<String, String> getNodes()
   {
      Map<String, String> nodes = new HashMap<String, String>();
      for (Entry<String, MessageFlowRecord> record : records.entrySet())
      {
         if (record.getValue().getBridge().getForwardingConnection() != null)
         {
            nodes.put(record.getKey(), record.getValue().getBridge().getForwardingConnection().getRemoteAddress());
         }
      }
      return nodes;
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
            ClusterConnectionImpl.log.error("Failed to update connectors", e);
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
         ClusterConnectionImpl.log.error("Failed to update connectors", e);
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
                                     false,
                                     useDuplicateDetection,
                                     confirmationWindowSize,
                                     managementService.getManagementAddress(),
                                     managementService.getManagementNotificationAddress(),
                                     clusterUser,
                                     clusterPassword,
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

      private final Queue queue;

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

      public void reset() throws Exception
      {
         clearBindings();
      }

      public void setBridge(final Bridge bridge)
      {
         this.bridge = bridge;
      }

      public Bridge getBridge()
      {
         return bridge;
      }

      public synchronized void onMessage(final ClientMessage message)
      {
         try
         {
            // Reset the bindings
            if (message.containsProperty(PostOfficeImpl.HDR_RESET_QUEUE_DATA))
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
            SimpleString type = message.getSimpleStringProperty(ManagementHelper.HDR_NOTIFICATION_TYPE);

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
               case PROPOSAL:
                  doProposalReceived(message);
                  break;
               case PROPOSAL_RESPONSE:
                  doProposalResponseReceived(message);
                  break;
               default:
               {
                  throw new IllegalArgumentException("Invalid type " + ntype);
               }
            }
         }
         catch (Exception e)
         {
            ClusterConnectionImpl.log.error("Failed to handle message", e);
         }
      }

      /*
      * Inform the grouping handler of a proposal
      * */
      private synchronized void doProposalReceived(final ClientMessage message) throws Exception
      {
         if (!message.containsProperty(ManagementHelper.HDR_PROPOSAL_GROUP_ID))
         {
            throw new IllegalStateException("proposal type is null");
         }

         SimpleString type = message.getSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_GROUP_ID);

         SimpleString val = message.getSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_VALUE);

         Integer hops = message.getIntProperty(ManagementHelper.HDR_DISTANCE);

         Response response = server.getGroupingHandler().receive(new Proposal(type, val), hops + 1);

         if (response != null)
         {
            server.getGroupingHandler().send(response, 0);
         }
      }

      /*
      * Inform the grouping handler of a response from a proposal
      *
      * */
      private synchronized void doProposalResponseReceived(final ClientMessage message) throws Exception
      {
         if (!message.containsProperty(ManagementHelper.HDR_PROPOSAL_GROUP_ID))
         {
            throw new IllegalStateException("proposal type is null");
         }

         SimpleString type = message.getSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_GROUP_ID);
         SimpleString val = message.getSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_VALUE);
         SimpleString alt = message.getSimpleStringProperty(ManagementHelper.HDR_PROPOSAL_ALT_VALUE);
         Integer hops = message.getIntProperty(ManagementHelper.HDR_DISTANCE);
         Response response = new Response(type, val, alt);
         server.getGroupingHandler().proposed(response);
         server.getGroupingHandler().send(response, hops + 1);
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
         if (!message.containsProperty(ManagementHelper.HDR_DISTANCE))
         {
            throw new IllegalStateException("distance is null");
         }

         if (!message.containsProperty(ManagementHelper.HDR_ADDRESS))
         {
            throw new IllegalStateException("queueAddress is null");
         }

         if (!message.containsProperty(ManagementHelper.HDR_CLUSTER_NAME))
         {
            throw new IllegalStateException("clusterName is null");
         }

         if (!message.containsProperty(ManagementHelper.HDR_ROUTING_NAME))
         {
            throw new IllegalStateException("routingName is null");
         }

         if (!message.containsProperty(ManagementHelper.HDR_BINDING_ID))
         {
            throw new IllegalStateException("queueID is null");
         }

         Integer distance = message.getIntProperty(ManagementHelper.HDR_DISTANCE);

         SimpleString queueAddress = message.getSimpleStringProperty(ManagementHelper.HDR_ADDRESS);

         SimpleString clusterName = message.getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);

         SimpleString routingName = message.getSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME);

         SimpleString filterString = message.getSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING);

         Long queueID = message.getLongProperty(ManagementHelper.HDR_BINDING_ID);

         RemoteQueueBinding binding = new RemoteQueueBindingImpl(server.getStorageManager().generateUniqueID(),
                                                                 queueAddress,
                                                                 clusterName,
                                                                 routingName,
                                                                 queueID,
                                                                 filterString,
                                                                 queue,
                                                                 bridge.getName(),
                                                                 distance + 1);

         if (postOffice.getBinding(clusterName) != null)
         {
            // Sanity check - this means the binding has already been added via another bridge, probably max
            // hops is too high
            // or there are multiple cluster connections for the same address

            ClusterConnectionImpl.log.warn("Remote queue binding " + clusterName +
                                           " has already been bound in the post office. Most likely cause for this is you have a loop " +
                                           "in your cluster due to cluster max-hops being too large or you have multiple cluster connections to the same nodes using overlapping addresses");

            return;
         }

         bindings.put(clusterName, binding);

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
         if (!message.containsProperty(ManagementHelper.HDR_CLUSTER_NAME))
         {
            throw new IllegalStateException("clusterName is null");
         }

         SimpleString clusterName = message.getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);

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
         if (!message.containsProperty(ManagementHelper.HDR_DISTANCE))
         {
            throw new IllegalStateException("distance is null");
         }

         if (!message.containsProperty(ManagementHelper.HDR_CLUSTER_NAME))
         {
            throw new IllegalStateException("clusterName is null");
         }

         Integer distance = message.getIntProperty(ManagementHelper.HDR_DISTANCE);

         SimpleString clusterName = message.getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);

         message.putIntProperty(ManagementHelper.HDR_DISTANCE, distance + 1);

         SimpleString filterString = message.getSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING);

         RemoteQueueBinding binding = bindings.get(clusterName);

         if (binding == null)
         {
            throw new IllegalStateException("Cannot find binding for " + clusterName);
         }

         binding.addConsumer(filterString);

         // Need to propagate the consumer add
         TypedProperties props = new TypedProperties();

         props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, binding.getAddress());

         props.putSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME, clusterName);

         props.putSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME, binding.getRoutingName());

         props.putIntProperty(ManagementHelper.HDR_DISTANCE, distance + 1);

         Queue theQueue = (Queue)binding.getBindable();

         props.putIntProperty(ManagementHelper.HDR_CONSUMER_COUNT, theQueue.getConsumerCount());

         if (filterString != null)
         {
            props.putSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING, filterString);
         }

         Notification notification = new Notification(null, CONSUMER_CREATED, props);

         managementService.sendNotification(notification);
      }

      private synchronized void doConsumerClosed(final ClientMessage message) throws Exception
      {
         if (!message.containsProperty(ManagementHelper.HDR_DISTANCE))
         {
            throw new IllegalStateException("distance is null");
         }

         if (!message.containsProperty(ManagementHelper.HDR_CLUSTER_NAME))
         {
            throw new IllegalStateException("clusterName is null");
         }

         Integer distance = message.getIntProperty(ManagementHelper.HDR_DISTANCE);

         SimpleString clusterName = message.getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);

         message.putIntProperty(ManagementHelper.HDR_DISTANCE, distance + 1);

         SimpleString filterString = message.getSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING);

         RemoteQueueBinding binding = bindings.get(clusterName);

         if (binding == null)
         {
            throw new IllegalStateException("Cannot find binding for " + clusterName);
         }

         binding.removeConsumer(filterString);

         // Need to propagate the consumer close
         TypedProperties props = new TypedProperties();

         props.putSimpleStringProperty(ManagementHelper.HDR_ADDRESS, binding.getAddress());

         props.putSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME, clusterName);

         props.putSimpleStringProperty(ManagementHelper.HDR_ROUTING_NAME, binding.getRoutingName());

         props.putIntProperty(ManagementHelper.HDR_DISTANCE, distance + 1);

         Queue theQueue = (Queue)binding.getBindable();

         props.putIntProperty(ManagementHelper.HDR_CONSUMER_COUNT, theQueue.getConsumerCount());

         if (filterString != null)
         {
            props.putSimpleStringProperty(ManagementHelper.HDR_FILTERSTRING, filterString);
         }
         Notification notification = new Notification(null, CONSUMER_CLOSED, props);

         managementService.sendNotification(notification);
      }

   }

   public void handleReplicatedAddBinding(final SimpleString address,
                                          final SimpleString uniqueName,
                                          final SimpleString routingName,
                                          final long queueID,
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

      RemoteQueueBinding binding = new RemoteQueueBindingImpl(server.getStorageManager().generateUniqueID(),
                                                              address,
                                                              uniqueName,
                                                              routingName,
                                                              queueID,
                                                              filterString,
                                                              queue,
                                                              queueName,
                                                              distance);

      if (postOffice.getBinding(uniqueName) != null)
      {
         ClusterConnectionImpl.log.warn("Remoting queue binding " + uniqueName +
                                        " has already been bound in the post office. Most likely cause for this is you have a loop " +
                                        "in your cluster due to cluster max-hops being too large or you have multiple cluster connections to the same nodes using overlapping addresses");

         return;
      }

      postOffice.addBinding(binding);

      Bindings theBindings = postOffice.getBindingsForAddress(address);

      theBindings.setRouteWhenNoConsumers(routeWhenNoConsumers);
   }

   // for testing only
   public Map<String, MessageFlowRecord> getRecords()
   {
      return records;
   }
}
