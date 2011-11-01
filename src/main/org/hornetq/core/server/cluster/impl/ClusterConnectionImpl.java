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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ScheduledExecutorService;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.management.ManagementHelper;
import org.hornetq.api.core.management.NotificationType;
import org.hornetq.core.client.impl.AfterConnectInternalListener;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.client.impl.Topology;
import org.hornetq.core.client.impl.TopologyMember;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.Bindings;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.postoffice.impl.PostOfficeImpl;
import org.hornetq.core.protocol.core.impl.wireformat.NodeAnnounceMessage;
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
import org.hornetq.utils.Future;
import org.hornetq.utils.TypedProperties;
import org.hornetq.utils.UUID;

/**
 * 
 * A ClusterConnectionImpl
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author Clebert Suconic
 * 
 * Created 21 Jan 2009 14:43:05
 *
 *
 */
public class ClusterConnectionImpl implements ClusterConnection, AfterConnectInternalListener
{
   private static final Logger log = Logger.getLogger(ClusterConnectionImpl.class);

   private static final boolean isTrace = log.isTraceEnabled();

   private final ExecutorFactory executorFactory;

   private final Executor executor;

   private final HornetQServer server;

   private final PostOffice postOffice;

   private final ManagementService managementService;

   private final SimpleString name;

   private final SimpleString address;

   private final long clientFailureCheckPeriod;

   private final long connectionTTL;

   private final long retryInterval;

   private final long callTimeout;

   private final double retryIntervalMultiplier;

   private final long maxRetryInterval;

   private final int reconnectAttempts;

   private final boolean useDuplicateDetection;

   private final boolean routeWhenNoConsumers;

   private final int confirmationWindowSize;

   private final Map<String, MessageFlowRecord> records = new ConcurrentHashMap<String, MessageFlowRecord>();

   private final ScheduledExecutorService scheduledExecutor;

   private final int maxHops;

   private final UUID nodeUUID;

   private boolean backup;

   private volatile boolean started;

   private final String clusterUser;

   private final String clusterPassword;

   private final ClusterConnector clusterConnector;

   private ServerLocatorInternal serverLocator;

   private final TransportConfiguration connector;

   private final boolean allowDirectConnectionsOnly;

   private final Set<TransportConfiguration> allowableConnections = new HashSet<TransportConfiguration>();

   private final ClusterManagerInternal manager;
   
   private final int minLargeMessageSize;


   // Stuff that used to be on the ClusterManager

   private final Topology topology = new Topology(this);

   private volatile ServerLocatorInternal backupServerLocator;

   private boolean stopping = false;

   public ClusterConnectionImpl(final ClusterManagerInternal manager,
                                final TransportConfiguration[] tcConfigs,
                                final TransportConfiguration connector,
                                final SimpleString name,
                                final SimpleString address,
                                final int minLargeMessageSize,
                                final long clientFailureCheckPeriod,
                                final long connectionTTL,
                                final long retryInterval,
                                final double retryIntervalMultiplier,
                                final long maxRetryInterval,
                                final int reconnectAttempts,
                                final long callTimeout,
                                final boolean useDuplicateDetection,
                                final boolean routeWhenNoConsumers,
                                final int confirmationWindowSize,
                                final ExecutorFactory executorFactory,
                                final HornetQServer server,
                                final PostOffice postOffice,
                                final ManagementService managementService,
                                final ScheduledExecutorService scheduledExecutor,
                                final int maxHops,
                                final UUID nodeUUID,
                                final boolean backup,
                                final String clusterUser,
                                final String clusterPassword,
                                final boolean allowDirectConnectionsOnly) throws Exception
   {

      if (nodeUUID == null)
      {
         throw new IllegalArgumentException("node id is null");
      }

      this.nodeUUID = nodeUUID;

      this.connector = connector;

      this.name = name;

      this.address = address;

      this.clientFailureCheckPeriod = clientFailureCheckPeriod;

      this.connectionTTL = connectionTTL;

      this.retryInterval = retryInterval;

      this.retryIntervalMultiplier = retryIntervalMultiplier;

      this.maxRetryInterval = maxRetryInterval;

      this.reconnectAttempts = reconnectAttempts;

      this.useDuplicateDetection = useDuplicateDetection;

      this.routeWhenNoConsumers = routeWhenNoConsumers;

      this.confirmationWindowSize = confirmationWindowSize;

      this.executorFactory = executorFactory;

      this.executor = executorFactory.getExecutor();

      this.topology.setExecutor(executor);

      this.server = server;

      this.postOffice = postOffice;

      this.managementService = managementService;

      this.scheduledExecutor = scheduledExecutor;

      this.maxHops = maxHops;

      this.backup = backup;

      this.clusterUser = clusterUser;

      this.clusterPassword = clusterPassword;

      this.allowDirectConnectionsOnly = allowDirectConnectionsOnly;

      this.manager = manager;

      this.callTimeout = callTimeout;
      
      this.minLargeMessageSize = minLargeMessageSize;

      clusterConnector = new StaticClusterConnector(tcConfigs);

      backupServerLocator = clusterConnector.createServerLocator(false);

      if (backupServerLocator != null)
      {
         backupServerLocator.setReconnectAttempts(-1);
         backupServerLocator.setInitialConnectAttempts(-1);
      }

      if (tcConfigs != null && tcConfigs.length > 0)
      {
         // a cluster connection will connect to other nodes only if they are directly connected
         // through a static list of connectors or broadcasting using UDP.
         if (allowDirectConnectionsOnly)
         {
            allowableConnections.addAll(Arrays.asList(tcConfigs));
         }
      }

   }

   public ClusterConnectionImpl(final ClusterManagerImpl manager,
                                DiscoveryGroupConfiguration dg,
                                final TransportConfiguration connector,
                                final SimpleString name,
                                final SimpleString address,
                                final int minLargeMessageSize,
                                final long clientFailureCheckPeriod,
                                final long connectionTTL,
                                final long retryInterval,
                                final double retryIntervalMultiplier,
                                final long maxRetryInterval,
                                final int reconnectAttempts,
                                final long callTimeout,
                                final boolean useDuplicateDetection,
                                final boolean routeWhenNoConsumers,
                                final int confirmationWindowSize,
                                final ExecutorFactory executorFactory,
                                final HornetQServer server,
                                final PostOffice postOffice,
                                final ManagementService managementService,
                                final ScheduledExecutorService scheduledExecutor,
                                final int maxHops,
                                final UUID nodeUUID,
                                final boolean backup,
                                final String clusterUser,
                                final String clusterPassword,
                                final boolean allowDirectConnectionsOnly) throws Exception
   {

      if (nodeUUID == null)
      {
         throw new IllegalArgumentException("node id is null");
      }

      this.nodeUUID = nodeUUID;

      this.connector = connector;

      this.name = name;

      this.address = address;

      this.clientFailureCheckPeriod = clientFailureCheckPeriod;

      this.connectionTTL = connectionTTL;

      this.retryInterval = retryInterval;

      this.retryIntervalMultiplier = retryIntervalMultiplier;

      this.maxRetryInterval = maxRetryInterval;
      
      this.minLargeMessageSize = minLargeMessageSize;

      this.reconnectAttempts = reconnectAttempts;

      this.callTimeout = callTimeout;

      this.useDuplicateDetection = useDuplicateDetection;

      this.routeWhenNoConsumers = routeWhenNoConsumers;

      this.confirmationWindowSize = confirmationWindowSize;

      this.executorFactory = executorFactory;

      this.executor = executorFactory.getExecutor();

      this.topology.setExecutor(executor);

      this.server = server;

      this.postOffice = postOffice;

      this.managementService = managementService;

      this.scheduledExecutor = scheduledExecutor;

      this.maxHops = maxHops;

      this.backup = backup;

      this.clusterUser = clusterUser;

      this.clusterPassword = clusterPassword;

      this.allowDirectConnectionsOnly = allowDirectConnectionsOnly;

      clusterConnector = new DiscoveryClusterConnector(dg);

      backupServerLocator = clusterConnector.createServerLocator(true);

      if (backupServerLocator != null)
      {
         backupServerLocator.setReconnectAttempts(-1);
         backupServerLocator.setInitialConnectAttempts(-1);
      }

      this.manager = manager;
   }

   public void start() throws Exception
   {
      synchronized (this)
      {
         if (started)
         {
            return;
         }

         stopping = false;
         started = true;

         if (!backup)
         {
            activate();
         }
      }

   }

   public void flushExecutor()
   {
      Future future = new Future();
      executor.execute(future);
      if (!future.await(10000))
      {
         server.threadDump("Couldn't finish executor on " + this);
      }
   }

   public void stop() throws Exception
   {
      if (!started)
      {
         return;
      }
      stopping = true;
      if (log.isDebugEnabled())
      {
         log.debug(this + "::stopping ClusterConnection");
      }

      if (serverLocator != null)
      {
         serverLocator.removeClusterTopologyListener(this);
      }

      log.debug("Cluster connection being stopped for node" + nodeUUID +
                ", server = " +
                this.server +
                " serverLocator = " +
                serverLocator);

      synchronized (this)
      {
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

      executor.execute(new Runnable()
      {
         public void run()
         {
            synchronized (ClusterConnectionImpl.this)
            {
               if (backupServerLocator != null)
               {
                  backupServerLocator.close();
                  backupServerLocator = null;
               }

               if (serverLocator != null)
               {
                  serverLocator.close();
                  serverLocator = null;
               }
            }

         }
      });

      started = false;
   }

   public void announceBackup()
   {
      executor.execute(new Runnable()
      {
         public void run()
         {
            try
            {
               if (log.isDebugEnabled())
               {
                  log.debug(ClusterConnectionImpl.this + ":: announcing " + connector + " to " + backupServerLocator);
               }
               ClientSessionFactory backupSessionFactory = backupServerLocator.connect();
               if (backupSessionFactory != null)
               {
                  backupSessionFactory.getConnection()
                                      .getChannel(0, -1)
                                      .send(new NodeAnnounceMessage(System.currentTimeMillis(),
                                                                    nodeUUID.toString(),
                                                                    true,
                                                                    connector,
                                                                    null));
                  log.info("backup announced");
               }
            }
            catch (Exception e)
            {
               log.warn("Unable to announce backup, retrying", e);
            }
         }
      });
   }

   private TopologyMember getLocalMember()
   {
      return topology.getMember(manager.getNodeId());
   }

   public void addClusterTopologyListener(final ClusterTopologyListener listener, final boolean clusterConnection)
   {
      topology.addClusterTopologyListener(listener);

      // no need to use an executor here since the Topology is already using one
      topology.sendTopology(listener);
   }

   public void removeClusterTopologyListener(final ClusterTopologyListener listener, final boolean clusterConnection)
   {
      topology.removeClusterTopologyListener(listener);
   }

   public Topology getTopology()
   {
      return topology;
   }

   public void nodeAnnounced(final long uniqueEventID,
                             final String nodeID,
                             final Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                             final boolean backup)
   {
      if (log.isDebugEnabled())
      {
         log.debug(this + "::NodeAnnounced, backup=" + backup + nodeID + connectorPair);
      }

      TopologyMember newMember = new TopologyMember(connectorPair.getA(), connectorPair.getB());
      newMember.setUniqueEventID(uniqueEventID);
      if (backup)
      {
         topology.updateBackup(nodeID, new TopologyMember(connectorPair.getA(), connectorPair.getB()));
      }
      else
      {
         topology.updateMember(uniqueEventID, nodeID, newMember);
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.client.impl.AfterConnectInternalListener#onConnection(org.hornetq.core.client.impl.ClientSessionFactoryInternal)
    */
   public void onConnection(ClientSessionFactoryInternal sf)
   {
      TopologyMember localMember = getLocalMember();
      sf.sendNodeAnnounce(localMember.getUniqueEventID(),
                          manager.getNodeId(),
                          false,
                          localMember.getConnector().getA(),
                          localMember.getConnector().getB());

      // sf.sendNodeAnnounce(System.currentTimeMillis(),
      // manager.getNodeId(),
      // false,
      // localMember.getConnector().a,
      // localMember.getConnector().b);
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

   public HornetQServer getServer()
   {
      return server;
   }

   public boolean isNodeActive(String nodeId)
   {
      MessageFlowRecord rec = records.get(nodeId);
      if (rec == null)
      {
         return false;
      }
      return rec.getBridge().isConnected();
   }

   public Map<String, String> getNodes()
   {
      synchronized (records)
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
   }

   public synchronized void activate() throws Exception
   {
      if (!started)
      {
         return;
      }

      if (log.isDebugEnabled())
      {
         log.debug("Activating cluster connection nodeID=" + nodeUUID + " for server=" + this.server);
      }

      backup = false;

      topology.updateAsLive(manager.getNodeId(), new TopologyMember(connector, null));

      if (backupServerLocator != null)
      {
         // todo we could use the topology of this to preempt it arriving from the cc
         try
         {
            backupServerLocator.close();
         }
         catch (Exception e)
         {
            log.warn("problem closing backup session factory", e);
         }
         backupServerLocator = null;
      }

      serverLocator = clusterConnector.createServerLocator(true);

      if (serverLocator != null)
      {

         if (!useDuplicateDetection)
         {
            log.debug("DuplicateDetection is disabled, sending clustered messages blocked");
         }

         final TopologyMember currentMember = topology.getMember(manager.getNodeId());

         if (currentMember == null)
         {
            // sanity check only
            throw new IllegalStateException("InternalError! The ClusterConnection doesn't know about its own node = " + this);
         }

         serverLocator.setNodeID(nodeUUID.toString());
         serverLocator.setIdentity("(main-ClusterConnection::" + server.toString() + ")");
         serverLocator.setReconnectAttempts(0);
         serverLocator.setClusterConnection(true);
         serverLocator.setClusterTransportConfiguration(connector);
         serverLocator.setBackup(server.getConfiguration().isBackup());
         serverLocator.setInitialConnectAttempts(-1);
         serverLocator.setClientFailureCheckPeriod(clientFailureCheckPeriod);
         serverLocator.setConnectionTTL(connectionTTL);
         serverLocator.setConfirmationWindowSize(confirmationWindowSize);
         // if not using duplicate detection, we will send blocked
         serverLocator.setBlockOnDurableSend(!useDuplicateDetection);
         serverLocator.setBlockOnNonDurableSend(!useDuplicateDetection);
         serverLocator.setCallTimeout(callTimeout);

         if (retryInterval > 0)
         {
            this.serverLocator.setRetryInterval(retryInterval);
         }

         serverLocator.addClusterTopologyListener(this);

         serverLocator.setAfterConnectionInternalListener(this);

         serverLocator.start(server.getExecutorFactory().getExecutor());
      }

      if (managementService != null)
      {
         TypedProperties props = new TypedProperties();
         props.putSimpleStringProperty(new SimpleString("name"), name);
         Notification notification = new Notification(nodeUUID.toString(),
                                                      NotificationType.CLUSTER_CONNECTION_STARTED,
                                                      props);
         log.debug("sending notification: " + notification);
         managementService.sendNotification(notification);
      }

   }

   public TransportConfiguration getConnector()
   {
      return connector;
   }

   // ClusterTopologyListener implementation ------------------------------------------------------------------

   public void nodeDown(final long eventUID, final String nodeID)
   {
      if (stopping)
      {
         return;
      }
      if (log.isDebugEnabled())
      {
         log.debug(this + " receiving nodeDown for nodeID=" + nodeID, new Exception("trace"));
      }
      if (nodeID.equals(nodeUUID.toString()))
      {
         return;
      }

      // Remove the flow record for that node

      MessageFlowRecord record = records.remove(nodeID);

      if (record != null)
      {
         try
         {
            if (isTrace)
            {
               log.trace("Closing clustering record " + record);
            }
            record.close();
         }
         catch (Exception e)
         {
            log.error("Failed to close flow record", e);
         }
      }
   }

   public void nodeUP(final long eventUID,
                      final String nodeID,
                      final Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                      final boolean last)
   {
      if (stopping)
      {
         return;
      }
      if (log.isDebugEnabled())
      {
         String ClusterTestBase = "receiving nodeUP for nodeID=";
         log.debug(this + ClusterTestBase + nodeID + " connectionPair=" + connectorPair);
      }
      // discard notifications about ourselves unless its from our backup

      if (nodeID.equals(nodeUUID.toString()))
      {
         if (log.isTraceEnabled())
         {
            log.trace(this + "::informing about backup to itself, nodeUUID=" +
                      nodeUUID +
                      ", connectorPair=" +
                      connectorPair +
                      " this = " +
                      this);
         }
         return;
      }

      // if the node is more than 1 hop away, we do not create a bridge for direct cluster connection
      if (allowDirectConnectionsOnly && !allowableConnections.contains(connectorPair.getA()))
      {
         return;
      }

      // FIXME required to prevent cluster connections w/o discovery group
      // and empty static connectors to create bridges... ulgy!
      if (serverLocator == null)
      {
         return;
      }
      /*we dont create bridges to backups*/
      if (connectorPair.getA() == null)
      {
         if (isTrace)
         {
            log.trace(this + " ignoring call with nodeID=" +
                      nodeID +
                      ", connectorPair=" +
                      connectorPair +
                      ", last=" +
                      last);
         }
         return;
      }

      synchronized (records)
      {
         try
         {
            MessageFlowRecord record = records.get(nodeID);

            if (record == null)
            {
               if (log.isDebugEnabled())
               {
                  log.debug(this + "::Creating record for nodeID=" + nodeID + ", connectorPair=" + connectorPair);
               }

               // New node - create a new flow record

               final SimpleString queueName = new SimpleString("sf." + name + "." + nodeID);

               Binding queueBinding = postOffice.getBinding(queueName);

               Queue queue;

               if (queueBinding != null)
               {
                  queue = (Queue)queueBinding.getBindable();
               }
               else
               {
                  // Add binding in storage so the queue will get reloaded on startup and we can find it - it's never
                  // actually routed to at that address though
                  queue = server.createQueue(queueName, queueName, null, true, false);
               }

               createNewRecord(eventUID, nodeID, connectorPair.getA(), queueName, queue, true);
            }
            else
            {
               if (isTrace)
               {
                  log.trace(this + " ignored nodeUp record for " +
                            connectorPair +
                            " on nodeID=" +
                            nodeID +
                            " as the record already existed");
               }
            }
         }
         catch (Exception e)
         {
            log.error("Failed to update topology", e);
         }
      }
   }

   public synchronized void informTopology()
   {
      String nodeID = server.getNodeID().toString();

      TopologyMember localMember;

      if (backup)
      {
         localMember = new TopologyMember(null, connector);
      }
      else
      {
         localMember = new TopologyMember(connector, null);
      }

      topology.updateAsLive(nodeID, localMember);
   }

   private void createNewRecord(final long eventUID,
                                final String targetNodeID,
                                final TransportConfiguration connector,
                                final SimpleString queueName,
                                final Queue queue,
                                final boolean start) throws Exception
   {
      final ServerLocatorInternal targetLocator = new ServerLocatorImpl(topology, true, connector);

      String nodeId;

      synchronized (this)
      {
         if (!started)
         {
            return;
         }

         if (serverLocator == null)
         {
            return;
         }

         nodeId = serverLocator.getNodeID();
      }

      targetLocator.setReconnectAttempts(0);

      targetLocator.setInitialConnectAttempts(0);
      targetLocator.setClientFailureCheckPeriod(clientFailureCheckPeriod);
      targetLocator.setConnectionTTL(connectionTTL);
      targetLocator.setInitialConnectAttempts(0);

      targetLocator.setConfirmationWindowSize(confirmationWindowSize);
      targetLocator.setBlockOnDurableSend(!useDuplicateDetection);
      targetLocator.setBlockOnNonDurableSend(!useDuplicateDetection);

      targetLocator.setRetryInterval(retryInterval);
      targetLocator.setMaxRetryInterval(maxRetryInterval);
      targetLocator.setRetryIntervalMultiplier(retryIntervalMultiplier);
      targetLocator.setMinLargeMessageSize(minLargeMessageSize);

      targetLocator.setAfterConnectionInternalListener(this);

      targetLocator.setNodeID(nodeId);

      targetLocator.setClusterTransportConfiguration(serverLocator.getClusterTransportConfiguration());

      if (retryInterval > 0)
      {
         targetLocator.setRetryInterval(retryInterval);
      }

      targetLocator.disableFinalizeCheck();

      MessageFlowRecordImpl record = new MessageFlowRecordImpl(targetLocator,
                                                               eventUID,
                                                               targetNodeID,
                                                               connector,
                                                               queueName,
                                                               queue);

      ClusterConnectionBridge bridge = new ClusterConnectionBridge(this,
                                                                   manager,
                                                                   targetLocator,
                                                                   serverLocator,
                                                                   reconnectAttempts,
                                                                   retryInterval,
                                                                   retryIntervalMultiplier,
                                                                   maxRetryInterval,
                                                                   nodeUUID,
                                                                   record.getEventUID(),
                                                                   record.getTargetNodeID(),
                                                                   record.getQueueName(),
                                                                   record.getQueue(),
                                                                   executorFactory.getExecutor(),
                                                                   null,
                                                                   null,
                                                                   scheduledExecutor,
                                                                   null,
                                                                   useDuplicateDetection,
                                                                   clusterUser,
                                                                   clusterPassword,
                                                                   !backup,
                                                                   server.getStorageManager(),
                                                                   managementService.getManagementAddress(),
                                                                   managementService.getManagementNotificationAddress(),
                                                                   record,
                                                                   record.getConnector());

      targetLocator.setIdentity("(Cluster-connection-bridge::" + bridge.toString() + "::" + this.toString() + ")");

      if (log.isDebugEnabled())
      {
         log.debug("creating record between " + this.connector + " and " + connector + bridge);
      }

      record.setBridge(bridge);

      records.put(targetNodeID, record);

      if (start)
      {
         bridge.start();
      }
   }

   // Inner classes -----------------------------------------------------------------------------------

   private class MessageFlowRecordImpl implements MessageFlowRecord
   {
      private BridgeImpl bridge;

      private final long eventUID;

      private final String targetNodeID;

      private final TransportConfiguration connector;

      private final ServerLocatorInternal targetLocator;

      private final SimpleString queueName;

      private boolean disconnected = false;

      private final Queue queue;

      private final Map<SimpleString, RemoteQueueBinding> bindings = new HashMap<SimpleString, RemoteQueueBinding>();

      private volatile boolean isClosed = false;

      private volatile boolean firstReset = false;

      public MessageFlowRecordImpl(final ServerLocatorInternal targetLocator,
                                   final long eventUID,
                                   final String targetNodeID,
                                   final TransportConfiguration connector,
                                   final SimpleString queueName,
                                   final Queue queue)
      {
         this.targetLocator = targetLocator;
         this.queue = queue;
         this.targetNodeID = targetNodeID;
         this.connector = connector;
         this.queueName = queueName;
         this.eventUID = eventUID;
      }

      /* (non-Javadoc)
       * @see java.lang.Object#toString()
       */
      @Override
      public String toString()
      {
         return "MessageFlowRecordImpl [nodeID=" + targetNodeID +
                ", connector=" +
                connector +
                ", queueName=" +
                queueName +
                ", queue=" +
                queue +
                ", isClosed=" +
                isClosed +
                ", firstReset=" +
                firstReset +
                "]";
      }

      public void serverDisconnected()
      {
         this.disconnected = true;
      }

      public String getAddress()
      {
         return address.toString();
      }

      /**
       * @return the eventUID
       */
      public long getEventUID()
      {
         return eventUID;
      }

      /**
       * @return the nodeID
       */
      public String getTargetNodeID()
      {
         return targetNodeID;
      }

      /**
       * @return the connector
       */
      public TransportConfiguration getConnector()
      {
         return connector;
      }

      /**
       * @return the queueName
       */
      public SimpleString getQueueName()
      {
         return queueName;
      }

      /**
       * @return the queue
       */
      public Queue getQueue()
      {
         return queue;
      }

      public int getMaxHops()
      {
         return maxHops;
      }

      public void close() throws Exception
      {
         if (isTrace)
         {
            log.trace("Stopping bridge " + bridge);
         }

         isClosed = true;
         clearBindings();

         if (disconnected)
         {
            bridge.disconnect();
         }

         bridge.stop();

         bridge.getExecutor().execute(new Runnable()
         {
            public void run()
            {
               try
               {
                  if (disconnected)
                  {
                     targetLocator.cleanup();
                  }
                  else
                  {
                     targetLocator.close();
                  }
               }
               catch (Exception ignored)
               {
                  log.debug(ignored.getMessage(), ignored);
               }
            }
         });
      }

      public boolean isClosed()
      {
         return isClosed;
      }

      public void reset() throws Exception
      {
         clearBindings();
      }

      public void setBridge(final BridgeImpl bridge)
      {
         this.bridge = bridge;
      }

      public Bridge getBridge()
      {
         return bridge;
      }

      public synchronized void onMessage(final ClientMessage message)
      {
         if (log.isDebugEnabled())
         {
            log.debug("ClusterCommunication::Flow record on " + clusterConnector + " Receiving message " + message);
         }
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
               {
                  doProposalReceived(message);

                  break;
               }
               case PROPOSAL_RESPONSE:
               {
                  doProposalResponseReceived(message);

                  break;
               }
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
         log.debug(ClusterConnectionImpl.this + " clearing bindings");
         for (RemoteQueueBinding binding : new HashSet<RemoteQueueBinding>(bindings.values()))
         {
            removeBinding(binding.getClusterName());
         }
      }

      private synchronized void doBindingAdded(final ClientMessage message) throws Exception
      {
         if (log.isTraceEnabled())
         {
            log.trace(ClusterConnectionImpl.this + " Adding binding " + message);
         }
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

            ClusterConnectionImpl.log.warn(this + "::Remote queue binding " +
                                           clusterName +
                                           " has already been bound in the post office. Most likely cause for this is you have a loop " +
                                           "in your cluster due to cluster max-hops being too large or you have multiple cluster connections to the same nodes using overlapping addresses");

            return;
         }

         if (isTrace)
         {
            log.trace("Adding binding " + clusterName + " into " + ClusterConnectionImpl.this);
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
         if (log.isTraceEnabled())
         {
            log.trace(ClusterConnectionImpl.this + " Removing binding " + message);
         }
         if (!message.containsProperty(ManagementHelper.HDR_CLUSTER_NAME))
         {
            throw new IllegalStateException("clusterName is null");
         }

         SimpleString clusterName = message.getSimpleStringProperty(ManagementHelper.HDR_CLUSTER_NAME);

         System.out.println("Removing clusterName=" + clusterName + " on " + ClusterConnectionImpl.this);

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
         if (log.isTraceEnabled())
         {
            log.trace(ClusterConnectionImpl.this + " Consumer created " + message);
         }
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
            throw new IllegalStateException("Cannot find binding for " + clusterName +
                                            " on " +
                                            ClusterConnectionImpl.this);
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
         if (log.isTraceEnabled())
         {
            log.trace(ClusterConnectionImpl.this + " Consumer closed " + message);
         }
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

   // for testing only
   public Map<String, MessageFlowRecord> getRecords()
   {
      return records;
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      return "ClusterConnectionImpl@" + System.identityHashCode(this) +
             "[nodeUUID=" +
             nodeUUID +
             ", connector=" +
             connector +
             ", address=" +
             address +
             ", server=" +
             server +
             "]";
   }

   public String describe()
   {
      StringWriter str = new StringWriter();
      PrintWriter out = new PrintWriter(str);

      out.println(this);
      out.println("***************************************");
      out.println(name + " connected to");
      for (Entry<String, MessageFlowRecord> messageFlow : records.entrySet())
      {
         out.println("\t Bridge = " + messageFlow.getValue().getBridge());
         out.println("\t Flow Record = " + messageFlow.getValue());
      }
      out.println("***************************************");

      return str.toString();
   }

   interface ClusterConnector
   {
      ServerLocatorInternal createServerLocator(boolean includeTopology);
   }

   private class StaticClusterConnector implements ClusterConnector
   {
      private final TransportConfiguration[] tcConfigs;

      public StaticClusterConnector(TransportConfiguration[] tcConfigs)
      {
         this.tcConfigs = tcConfigs;
      }

      public ServerLocatorInternal createServerLocator(boolean includeTopology)
      {
         if (tcConfigs != null && tcConfigs.length > 0)
         {
            if (log.isDebugEnabled())
            {
               log.debug(ClusterConnectionImpl.this + "Creating a serverLocator for " + Arrays.toString(tcConfigs));
            }
            ServerLocatorImpl locator = new ServerLocatorImpl(includeTopology ? topology : null, true, tcConfigs);
            locator.setClusterConnection(true);
            return locator;
         }
         else
         {
            return null;
         }
      }

      /* (non-Javadoc)
       * @see java.lang.Object#toString()
       */
      @Override
      public String toString()
      {
         return "StaticClusterConnector [tcConfigs=" + Arrays.toString(tcConfigs) + "]";
      }

   }

   private class DiscoveryClusterConnector implements ClusterConnector
   {
      private final DiscoveryGroupConfiguration dg;

      public DiscoveryClusterConnector(DiscoveryGroupConfiguration dg)
      {
         this.dg = dg;
      }

      public ServerLocatorInternal createServerLocator(boolean includeTopology)
      {
         ServerLocatorImpl locator = new ServerLocatorImpl(includeTopology ? topology : null, true, dg);
         return locator;

      }
   }
}
