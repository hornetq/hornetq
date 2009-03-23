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

import static org.jboss.messaging.core.management.NotificationType.CONSUMER_CLOSED;
import static org.jboss.messaging.core.management.NotificationType.CONSUMER_CREATED;
import static org.jboss.messaging.core.management.NotificationType.SECURITY_AUTHENTICATION_VIOLATION;
import static org.jboss.messaging.core.management.NotificationType.SECURITY_PERMISSION_VIOLATION;
import static org.jboss.messaging.core.postoffice.impl.PostOfficeImpl.HDR_RESET_QUEUE_DATA;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;

import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.management.impl.ManagementHelper;
import org.jboss.messaging.core.cluster.DiscoveryGroup;
import org.jboss.messaging.core.cluster.DiscoveryListener;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.ManagementService;
import org.jboss.messaging.core.management.Notification;
import org.jboss.messaging.core.management.NotificationType;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.Bindings;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.postoffice.impl.LocalQueueBinding;
import org.jboss.messaging.core.remoting.Channel;
import org.jboss.messaging.core.remoting.Packet;
import org.jboss.messaging.core.remoting.impl.wireformat.replication.ReplicateRemoteBindingAddedMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.replication.ReplicateRemoteBindingRemovedMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.replication.ReplicateRemoteConsumerAddedMessage;
import org.jboss.messaging.core.remoting.impl.wireformat.replication.ReplicateRemoteConsumerRemovedMessage;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.QueueFactory;
import org.jboss.messaging.core.server.cluster.Bridge;
import org.jboss.messaging.core.server.cluster.ClusterConnection;
import org.jboss.messaging.core.server.cluster.MessageFlowRecord;
import org.jboss.messaging.core.server.cluster.RemoteQueueBinding;
import org.jboss.messaging.utils.ExecutorFactory;
import org.jboss.messaging.utils.Pair;
import org.jboss.messaging.utils.SimpleString;
import org.jboss.messaging.utils.UUID;

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

   private final org.jboss.messaging.utils.ExecutorFactory executorFactory;

   private final StorageManager storageManager;

   private final PostOffice postOffice;

   private final ManagementService managementService;

   private final SimpleString name;

   private final SimpleString address;

   private final long retryInterval;

   private final double retryIntervalMultiplier;

   private final int maxRetriesBeforeFailover;

   private final int maxRetriesAfterFailover;

   private final boolean useDuplicateDetection;

   private final boolean routeWhenNoConsumers;

   private Map<Pair<TransportConfiguration, TransportConfiguration>, MessageFlowRecord> records = new HashMap<Pair<TransportConfiguration, TransportConfiguration>, MessageFlowRecord>();

   private final DiscoveryGroup discoveryGroup;

   private final ScheduledExecutorService scheduledExecutor;

   private final QueueFactory queueFactory;

   private final int maxHops;

   private final UUID nodeUUID;

   private final Channel replicatingChannel;
   
   private final List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors;

   private boolean backup;

   private volatile boolean started;
   
   /*
    * Constructor using static list of connectors
    */
   public ClusterConnectionImpl(final SimpleString name,
                                final SimpleString address,
                                final long retryInterval,
                                final double retryIntervalMultiplier,
                                final int maxRetriesBeforeFailover,
                                final int maxRetriesAfterFailover,
                                final boolean useDuplicateDetection,
                                final boolean routeWhenNoConsumers,
                                final org.jboss.messaging.utils.ExecutorFactory executorFactory,
                                final StorageManager storageManager,
                                final PostOffice postOffice,
                                final ManagementService managementService,
                                final ScheduledExecutorService scheduledExecutor,
                                final QueueFactory queueFactory,
                                final List<Pair<TransportConfiguration, TransportConfiguration>> connectors,
                                final int maxHops,
                                final UUID nodeUUID,
                                final Channel replicatingChannel,
                                final boolean backup) throws Exception
   {
      this.name = name;

      this.address = address;

      this.retryInterval = retryInterval;

      this.retryIntervalMultiplier = retryIntervalMultiplier;

      this.maxRetriesBeforeFailover = maxRetriesBeforeFailover;

      this.maxRetriesAfterFailover = maxRetriesAfterFailover;

      this.useDuplicateDetection = useDuplicateDetection;

      this.routeWhenNoConsumers = routeWhenNoConsumers;

      this.executorFactory = executorFactory;

      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.managementService = managementService;

      this.discoveryGroup = null;

      this.scheduledExecutor = scheduledExecutor;

      this.queueFactory = queueFactory;

      this.maxHops = maxHops;

      this.nodeUUID = nodeUUID;

      this.replicatingChannel = replicatingChannel;

      this.backup = backup;
      
      this.staticConnectors = connectors;

      this.updateConnectors(connectors);
   }

   /*
    * Constructor using discovery to get connectors
    */
   public ClusterConnectionImpl(final SimpleString name,
                                final SimpleString address,
                                final long retryInterval,
                                final double retryIntervalMultiplier,
                                final int maxRetriesBeforeFailover,
                                final int maxRetriesAfterFailover,
                                final boolean useDuplicateDetection,
                                final boolean routeWhenNoConsumers,
                                final ExecutorFactory executorFactory,
                                final StorageManager storageManager,
                                final PostOffice postOffice,
                                final ManagementService managementService,
                                final ScheduledExecutorService scheduledExecutor,
                                final QueueFactory queueFactory,
                                final DiscoveryGroup discoveryGroup,
                                final int maxHops,
                                final UUID nodeUUID,
                                final Channel replicatingChannel,
                                final boolean backup) throws Exception
   {
      this.name = name;

      this.address = address;

      this.retryInterval = retryInterval;

      this.retryIntervalMultiplier = retryIntervalMultiplier;

      this.maxRetriesBeforeFailover = maxRetriesBeforeFailover;

      this.maxRetriesAfterFailover = maxRetriesAfterFailover;

      this.executorFactory = executorFactory;

      this.storageManager = storageManager;

      this.postOffice = postOffice;

      this.managementService = managementService;

      this.scheduledExecutor = scheduledExecutor;

      this.queueFactory = queueFactory;

      this.discoveryGroup = discoveryGroup;

      this.useDuplicateDetection = useDuplicateDetection;

      this.routeWhenNoConsumers = routeWhenNoConsumers;

      this.maxHops = maxHops;

      this.nodeUUID = nodeUUID;

      this.replicatingChannel = replicatingChannel;

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

   public synchronized void activate()
   {
      backup = false;
      
      if (discoveryGroup != null)
      {
         connectorsChanged();
      }
      else
      {
         try
         {
            updateConnectors(staticConnectors);
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
      doUpdateConnectors(connectors);
   }

   private void doUpdateConnectors(final List<Pair<TransportConfiguration, TransportConfiguration>> connectors) throws Exception
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
               queue = queueFactory.createQueue(-1, queueName, queueName, null, true, false);

               // Add binding in storage so the queue will get reloaded on startup and we can find it - it's never
               // actually routed to at that address though

               Binding storeBinding = new LocalQueueBinding(queue.getName(),
                                                            queue,
                                                            new SimpleString(nodeUUID.toString()));

               storageManager.addQueueBinding(storeBinding);
            }

            MessageFlowRecordImpl record = new MessageFlowRecordImpl(queue);

            Bridge bridge = new BridgeImpl(nodeUUID,
                                           queueName,
                                           queue,
                                           connectorPair,
                                           executorFactory.getExecutor(),
                                           null,
                                           null,
                                           scheduledExecutor,
                                           null,
                                           retryInterval,
                                           retryIntervalMultiplier,
                                           maxRetriesBeforeFailover,
                                           maxRetriesAfterFailover,
                                           false, // Duplicate detection is handled in the RemoteQueueBindingImpl,
                                           managementService.getManagementAddress(),
                                           managementService.getManagementNotificationAddress(),
                                           managementService.getClusterPassword(),
                                           record,
                                           replicatingChannel,
                                           !backup);

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

      public void setBridge(final Bridge bridge)
      {
         this.bridge = bridge;
      }

      public void reset() throws Exception
      {
         clearBindings();
      }

      
      public void onMessage(final ClientMessage message)
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
                  doBindingAdded(message, replicatingChannel);

                  break;
               }
               case BINDING_REMOVED:
               {
                  doBindingRemoved(message, replicatingChannel);

                  break;
               }
               case CONSUMER_CREATED:
               {
                  doConsumerCreated(message, replicatingChannel);

                  break;
               }
               case CONSUMER_CLOSED:
               {
                  doConsumerClosed(message, replicatingChannel);

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

      private void clearBindings() throws Exception
      {
         for (RemoteQueueBinding binding : bindings.values())
         {
            postOffice.removeBinding(binding.getUniqueName());
         }

         bindings.clear();
      }
      
      private void doBindingAdded(final ClientMessage message, final Channel replChannel) throws Exception
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
         
         if (replChannel != null)
         {
            Packet packet = new ReplicateRemoteBindingAddedMessage(name, queueAddress, clusterName, routingName, queueID, filterString, queue.getName(), distance + 1);
            
            replChannel.replicatePacket(packet, 2, new Runnable()
            {
               public void run()
               {
                  try
                  {
                     doBindingAdded(message, null);
                  }
                  catch (Exception e)
                  {
                     log.error("Failed to add remote queue binding", e);
                  }
               }
            });
         }
         else
         {   
            RemoteQueueBinding binding = new RemoteQueueBindingImpl(queueAddress,
                                                                    clusterName,
                                                                    routingName,
                                                                    queueID,
                                                                    filterString,
                                                                    queue,
                                                                    useDuplicateDetection,
                                                                    bridge.getName(),
                                                                    distance + 1);
   
            bindings.put(clusterName, binding);
   
            if (postOffice.getBinding(clusterName) != null)
            {
               // Sanity check - this means the binding has already been added via another bridge, probably max
               // hops is too high
               // or there are multiple cluster connections for the same address
   
               log.warn("Remoting queue binding " + clusterName +
                        " has already been bound in the post office. Most likely cause for this is you have a loop " +
                        "in your cluster due to cluster max-hops being too large or you have multiple cluster connections to the same nodes using overlapping addresses");
   
               return;
            }
   
            postOffice.addBinding(binding);
   
            Bindings theBindings = postOffice.getBindingsForAddress(queueAddress);
   
            theBindings.setRouteWhenNoConsumers(routeWhenNoConsumers);
         }
      }

      private void doBindingRemoved(final ClientMessage message, final Channel replChannel) throws Exception
      {
         SimpleString clusterName = (SimpleString)message.getProperty(ManagementHelper.HDR_CLUSTER_NAME);

         if (clusterName == null)
         {
            throw new IllegalStateException("clusterName is null");
         }
                  
         if (replChannel != null)
         {
            Packet packet = new ReplicateRemoteBindingRemovedMessage(clusterName);
            
            replChannel.replicatePacket(packet, 2, new Runnable()
            {
               public void run()
               {
                  try
                  {
                     doBindingRemoved(message, null);
                  }
                  catch (Exception e)
                  {
                     log.error("Failed to remove remote queue binding", e);
                  }
               }
            });
         }
         else
         {            
            RemoteQueueBinding binding = bindings.remove(clusterName);
   
            if (binding == null)
            {
               throw new IllegalStateException("Cannot find binding for queue " + clusterName);
            }
   
            postOffice.removeBinding(binding.getUniqueName());
         }
      }

      private void doConsumerCreated(final ClientMessage message, final Channel replChannel) throws Exception
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

         SimpleString filterString = (SimpleString)message.getProperty(ManagementHelper.HDR_FILTERSTRING);
         
         if (replChannel != null)
         {
            Packet packet = new ReplicateRemoteConsumerAddedMessage(clusterName, filterString);
            
            replChannel.replicatePacket(packet, 2, new Runnable()
            {
               public void run()
               {
                  try
                  {
                     doConsumerCreated(message, null);
                  }
                  catch (Exception e)
                  {
                     log.error("Failed to add remote consumer", e);
                  }
               }
            });
         }
         else
         {           
            RemoteQueueBinding binding = bindings.get(clusterName);
   
            if (binding == null)
            {
               throw new IllegalStateException("Cannot find binding for " + clusterName);
            }
   
            binding.addConsumer(filterString);
   
            message.putIntProperty(ManagementHelper.HDR_DISTANCE, distance + 1);
   
            // Need to propagate the consumer add
            Notification notification = new Notification(CONSUMER_CREATED, message.getProperties());
   
            managementService.sendNotification(notification);
         }
      }

      private void doConsumerClosed(final ClientMessage message, final Channel replChannel) throws Exception
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

         SimpleString filterString = (SimpleString)message.getProperty(ManagementHelper.HDR_FILTERSTRING);
         
         if (replChannel != null)
         {
            Packet packet = new ReplicateRemoteConsumerRemovedMessage(clusterName, filterString);
            
            replChannel.replicatePacket(packet, 2, new Runnable()
            {
               public void run()
               {
                  try
                  {
                     doConsumerClosed(message, null);
                  }
                  catch (Exception e)
                  {
                     log.error("Failed to remove remote consumer", e);
                  }
               }
            });
         }
         else
         {           
            RemoteQueueBinding binding = bindings.get(clusterName);
   
            if (binding == null)
            {
               throw new IllegalStateException("Cannot find binding for " + clusterName);
            }
   
            binding.removeConsumer(filterString);
   
            message.putIntProperty(ManagementHelper.HDR_DISTANCE, distance + 1);
   
            // Need to propagate the consumer close
            Notification notification = new Notification(CONSUMER_CLOSED, message.getProperties());
   
            managementService.sendNotification(notification);
         }
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
                                                              useDuplicateDetection,
                                                              queueName,
                                                              distance);

      postOffice.addBinding(binding);

      Bindings theBindings = postOffice.getBindingsForAddress(address);

      theBindings.setRouteWhenNoConsumers(routeWhenNoConsumers);
   }

}
