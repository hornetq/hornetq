/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.core.client.impl;

import java.io.Serializable;
import java.lang.reflect.Array;
import java.net.InetAddress;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.*;
import java.util.concurrent.*;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.loadbalance.ConnectionLoadBalancingPolicy;
import org.hornetq.core.cluster.DiscoveryEntry;
import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.cluster.DiscoveryListener;
import org.hornetq.core.cluster.impl.DiscoveryGroupImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.utils.HornetQThreadFactory;
import org.hornetq.utils.UUIDGenerator;

/**
 * A ServerLocatorImpl
 *
 * @author Tim Fox
 */
public class ServerLocatorImpl implements ServerLocatorInternal, DiscoveryListener, Serializable
{
   private static final long serialVersionUID = -1615857864410205260L;

   private static final Logger log = Logger.getLogger(ServerLocatorImpl.class);

   private final boolean ha;

   private boolean clusterConnection;

   private final String discoveryAddress;

   private final int discoveryPort;

   private final Set<ClusterTopologyListener> topologyListeners = new HashSet<ClusterTopologyListener>();

   private Set<ClientSessionFactory> factories = new HashSet<ClientSessionFactory>();

   private TransportConfiguration[] initialConnectors;

   private StaticConnector staticConnector = new StaticConnector();

   private Topology topology = new Topology();

   private Pair<TransportConfiguration, TransportConfiguration>[] topologyArray;

   private boolean receivedTopology;

   private ExecutorService threadPool;

   private ScheduledExecutorService scheduledThreadPool;

   private DiscoveryGroup discoveryGroup;

   private ConnectionLoadBalancingPolicy loadBalancingPolicy;

   private boolean readOnly;

   // Settable attributes:

   private boolean cacheLargeMessagesClient;

   private String localBindAddress;

   private long discoveryRefreshTimeout;

   private long discoveryInitialWaitTimeout;

   private long clientFailureCheckPeriod;

   private long connectionTTL;

   private long callTimeout;

   private int minLargeMessageSize;

   private int consumerWindowSize;

   private int consumerMaxRate;

   private int confirmationWindowSize;

   private int producerWindowSize;

   private int producerMaxRate;

   private boolean blockOnAcknowledge;

   private boolean blockOnDurableSend;

   private boolean blockOnNonDurableSend;

   private boolean autoGroup;

   private boolean preAcknowledge;

   private String connectionLoadBalancingPolicyClassName;

   private int ackBatchSize;

   private boolean useGlobalPools;

   private int scheduledThreadPoolMaxSize;

   private int threadPoolMaxSize;

   private long retryInterval;

   private double retryIntervalMultiplier;

   private long maxRetryInterval;

   private int reconnectAttempts;

   private int initialConnectAttempts;

   private boolean failoverOnInitialConnection;

   private int initialMessagePacketSize;

   private volatile boolean closed;

   private final List<Interceptor> interceptors = new CopyOnWriteArrayList<Interceptor>();

   private static ExecutorService globalThreadPool;

   private static ScheduledExecutorService globalScheduledThreadPool;

   private String groupID;

   private String nodeID;

   private TransportConfiguration clusterTransportConfiguration;

   private boolean backup;

   private final Exception e = new Exception();

   private static synchronized ExecutorService getGlobalThreadPool()
   {
      if (globalThreadPool == null)
      {
         ThreadFactory factory = new HornetQThreadFactory("HornetQ-client-global-threads", true, getThisClassLoader());

         globalThreadPool = Executors.newCachedThreadPool(factory);
      }

      return globalThreadPool;
   }

   public static synchronized ScheduledExecutorService getGlobalScheduledThreadPool()
   {
      if (globalScheduledThreadPool == null)
      {
         ThreadFactory factory = new HornetQThreadFactory("HornetQ-client-global-scheduled-threads",
               true,
               getThisClassLoader());

         globalScheduledThreadPool = Executors.newScheduledThreadPool(HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,

               factory);
      }

      return globalScheduledThreadPool;
   }

   private void setThreadPools()
   {
      if (useGlobalPools)
      {
         threadPool = getGlobalThreadPool();

         scheduledThreadPool = getGlobalScheduledThreadPool();
      }
      else
      {
         ThreadFactory factory = new HornetQThreadFactory("HornetQ-client-factory-threads-" + System.identityHashCode(this),
               true,
               getThisClassLoader());

         if (threadPoolMaxSize == -1)
         {
            threadPool = Executors.newCachedThreadPool(factory);
         }
         else
         {
            threadPool = Executors.newFixedThreadPool(threadPoolMaxSize, factory);
         }

         factory = new HornetQThreadFactory("HornetQ-client-factory-pinger-threads-" + System.identityHashCode(this),
               true,
               getThisClassLoader());

         scheduledThreadPool = Executors.newScheduledThreadPool(scheduledThreadPoolMaxSize, factory);
      }
   }

   private static ClassLoader getThisClassLoader()
   {
      return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>()
      {
         public ClassLoader run()
         {
            return ClientSessionFactoryImpl.class.getClassLoader();
         }
      });

   }

   private void instantiateLoadBalancingPolicy()
   {
      if (connectionLoadBalancingPolicyClassName == null)
      {
         throw new IllegalStateException("Please specify a load balancing policy class name on the session factory");
      }

      AccessController.doPrivileged(new PrivilegedAction<Object>()
      {
         public Object run()
         {
            ClassLoader loader = Thread.currentThread().getContextClassLoader();
            try
            {
               Class<?> clazz = loader.loadClass(connectionLoadBalancingPolicyClassName);
               loadBalancingPolicy = (ConnectionLoadBalancingPolicy) clazz.newInstance();
               return null;
            }
            catch (Exception e)
            {
               throw new IllegalArgumentException("Unable to instantiate load balancing policy \"" + connectionLoadBalancingPolicyClassName +
                     "\"",
                     e);
            }
         }
      });
   }

   private synchronized void initialise() throws Exception
   {
      if (!readOnly)
      {
         setThreadPools();

         instantiateLoadBalancingPolicy();

         if (discoveryAddress != null)
         {
            InetAddress groupAddress = InetAddress.getByName(discoveryAddress);

            InetAddress lbAddress;

            if (localBindAddress != null)
            {
               lbAddress = InetAddress.getByName(localBindAddress);
            }
            else
            {
               lbAddress = null;
            }

            discoveryGroup = new DiscoveryGroupImpl(nodeID,
                  discoveryAddress,
                  lbAddress,
                  groupAddress,
                  discoveryPort,
                  discoveryRefreshTimeout);

            discoveryGroup.registerListener(this);

            discoveryGroup.start();
         }

         readOnly = true;
      }
   }

   private ServerLocatorImpl(final boolean useHA,
                             final String discoveryAddress,
                             final int discoveryPort,
                             final TransportConfiguration[] transportConfigs)
   {
      e.fillInStackTrace();
      this.ha = useHA;

      this.discoveryAddress = discoveryAddress;

      this.discoveryPort = discoveryPort;

      this.initialConnectors = transportConfigs;

      this.nodeID = UUIDGenerator.getInstance().generateStringUUID();

      clientFailureCheckPeriod = HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD;

      connectionTTL = HornetQClient.DEFAULT_CONNECTION_TTL;

      callTimeout = HornetQClient.DEFAULT_CALL_TIMEOUT;

      minLargeMessageSize = HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE;

      consumerWindowSize = HornetQClient.DEFAULT_CONSUMER_WINDOW_SIZE;

      consumerMaxRate = HornetQClient.DEFAULT_CONSUMER_MAX_RATE;

      confirmationWindowSize = HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE;

      producerWindowSize = HornetQClient.DEFAULT_PRODUCER_WINDOW_SIZE;

      producerMaxRate = HornetQClient.DEFAULT_PRODUCER_MAX_RATE;

      blockOnAcknowledge = HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE;

      blockOnDurableSend = HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND;

      blockOnNonDurableSend = HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND;

      autoGroup = HornetQClient.DEFAULT_AUTO_GROUP;

      preAcknowledge = HornetQClient.DEFAULT_PRE_ACKNOWLEDGE;

      ackBatchSize = HornetQClient.DEFAULT_ACK_BATCH_SIZE;

      connectionLoadBalancingPolicyClassName = HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;

      discoveryInitialWaitTimeout = HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT;

      useGlobalPools = HornetQClient.DEFAULT_USE_GLOBAL_POOLS;

      scheduledThreadPoolMaxSize = HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;

      threadPoolMaxSize = HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE;

      retryInterval = HornetQClient.DEFAULT_RETRY_INTERVAL;

      retryIntervalMultiplier = HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER;

      maxRetryInterval = HornetQClient.DEFAULT_MAX_RETRY_INTERVAL;

      reconnectAttempts = HornetQClient.DEFAULT_RECONNECT_ATTEMPTS;

      initialConnectAttempts = HornetQClient.INITIAL_CONNECT_ATTEMPTS;

      failoverOnInitialConnection = HornetQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION;

      cacheLargeMessagesClient = HornetQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT;

      initialMessagePacketSize = HornetQClient.DEFAULT_INITIAL_MESSAGE_PACKET_SIZE;

      cacheLargeMessagesClient = HornetQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT;

      clusterConnection = false;
   }

   /**
    * Create a ServerLocatorImpl using UDP discovery to lookup cluster
    *
    * @param discoveryAddress
    * @param discoveryPort
    */
   public ServerLocatorImpl(final boolean useHA, final String discoveryAddress, final int discoveryPort)
   {
      this(useHA, discoveryAddress, discoveryPort, null);
   }

   /**
    * Create a ServerLocatorImpl using a static list of live servers
    *
    * @param transportConfigs
    */
   public ServerLocatorImpl(final boolean useHA, final TransportConfiguration... transportConfigs)
   {
      this(useHA, null, -1, transportConfigs);
   }

   private TransportConfiguration selectConnector()
   {
      if (receivedTopology)
      {
         int pos = loadBalancingPolicy.select(topologyArray.length);

         Pair<TransportConfiguration, TransportConfiguration> pair = topologyArray[pos];

         return pair.a;
      }
      else
      {
         // Get from initialconnectors

         int pos = loadBalancingPolicy.select(initialConnectors.length);

         return initialConnectors[pos];
      }
   }

   public void start() throws Exception
   {
      initialise();
   }

   public ClientSessionFactory connect() throws Exception
   {
      ClientSessionFactoryInternal sf;
      // static list of initial connectors
      if (initialConnectors != null && discoveryGroup == null)
      {
         sf = (ClientSessionFactoryInternal) staticConnector.connect();
      }
      // wait for discovery group to get the list of initial connectors
      else
      {
         sf = (ClientSessionFactoryInternal) createSessionFactory();
      }
      addFactory(sf);
      return sf;
   }

   public ClientSessionFactory createSessionFactory(final TransportConfiguration transportConfiguration) throws Exception
   {
      if (closed)
      {
         throw new IllegalStateException("Cannot create session factory, server locator is closed (maybe it has been garbage collected)");
      }

      try
      {
         initialise();
      }
      catch (Exception e)
      {
         throw new HornetQException(HornetQException.INTERNAL_ERROR, "Failed to initialise session factory", e);
      }

      ClientSessionFactoryInternal factory = new ClientSessionFactoryImpl(this,
            transportConfiguration,
            callTimeout,
            clientFailureCheckPeriod,
            connectionTTL,
            retryInterval,
            retryIntervalMultiplier,
            maxRetryInterval,
            reconnectAttempts,
            threadPool,
            scheduledThreadPool,
            interceptors);

      factory.connect(reconnectAttempts, failoverOnInitialConnection);

      addFactory(factory);

      return factory;
   }

   public ClientSessionFactory createSessionFactory() throws Exception
   {
      if (closed)
      {
         throw new IllegalStateException("Cannot create session factory, server locator is closed (maybe it has been garbage collected)");
      }

      try
      {
         initialise();
      }
      catch (Exception e)
      {
         throw new HornetQException(HornetQException.INTERNAL_ERROR, "Failed to initialise session factory", e);
      }

      if (initialConnectors == null && discoveryGroup != null)
      {
         // Wait for an initial broadcast to give us at least one node in the cluster
         long timeout = clusterConnection?0:discoveryInitialWaitTimeout;
         boolean ok = discoveryGroup.waitForBroadcast(timeout);

         if (!ok)
         {
            throw new HornetQException(HornetQException.CONNECTION_TIMEDOUT,
                  "Timed out waiting to receive initial broadcast from cluster");
         }
      }

      ClientSessionFactoryInternal factory = null;

      synchronized (this)
      {
         boolean retry;
         int attempts = 0;
         do
         {
            retry = false;

            TransportConfiguration tc = selectConnector();

            // try each factory in the list until we find one which works

            try
            {
               factory = new ClientSessionFactoryImpl(this,
                     tc,
                     callTimeout,
                     clientFailureCheckPeriod,
                     connectionTTL,
                     retryInterval,
                     retryIntervalMultiplier,
                     maxRetryInterval,
                     reconnectAttempts,
                     threadPool,
                     scheduledThreadPool,
                     interceptors);
               factory.connect(initialConnectAttempts, failoverOnInitialConnection);
            }
            catch (HornetQException e)
            {
               factory.close();
               factory = null;
               if (e.getCode() == HornetQException.NOT_CONNECTED)
               {
                  attempts++;

                  if (topologyArray != null && attempts == topologyArray.length)
                  {
                     throw new HornetQException(HornetQException.NOT_CONNECTED,
                           "Cannot connect to server(s). Tried with all available servers.");
                  }
                  if (topologyArray == null && initialConnectors != null && attempts == initialConnectors.length)
                  {
                     throw new HornetQException(HornetQException.NOT_CONNECTED,
                           "Cannot connect to server(s). Tried with all available servers.");
                  }
                  retry = true;
               }
               else
               {
                  throw e;
               }
            }
         }
         while (retry);

         if (ha)
         {
            long toWait = 30000;
            long start = System.currentTimeMillis();
            while (!receivedTopology && toWait > 0)
            {
               // Now wait for the topology

               try
               {
                  wait(toWait);
               }
               catch (InterruptedException ignore)
               {
               }

               long now = System.currentTimeMillis();

               toWait -= now - start;

               start = now;
            }

            if (toWait <= 0)
            {
               throw new HornetQException(HornetQException.CONNECTION_TIMEDOUT,
                     "Timed out waiting to receive cluster topology");
            }
         }

         addFactory(factory);

         return factory;
      }
   }

   public synchronized boolean isHA()
   {
      return ha;
   }

   public synchronized boolean isCacheLargeMessagesClient()
   {
      return cacheLargeMessagesClient;
   }

   public synchronized void setCacheLargeMessagesClient(final boolean cached)
   {
      cacheLargeMessagesClient = cached;
   }

   public synchronized long getClientFailureCheckPeriod()
   {
      return clientFailureCheckPeriod;
   }

   public synchronized void setClientFailureCheckPeriod(final long clientFailureCheckPeriod)
   {
      checkWrite();
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
   }

   public synchronized long getConnectionTTL()
   {
      return connectionTTL;
   }

   public synchronized void setConnectionTTL(final long connectionTTL)
   {
      checkWrite();
      this.connectionTTL = connectionTTL;
   }

   public synchronized long getCallTimeout()
   {
      return callTimeout;
   }

   public synchronized void setCallTimeout(final long callTimeout)
   {
      checkWrite();
      this.callTimeout = callTimeout;
   }

   public synchronized int getMinLargeMessageSize()
   {
      return minLargeMessageSize;
   }

   public synchronized void setMinLargeMessageSize(final int minLargeMessageSize)
   {
      checkWrite();
      this.minLargeMessageSize = minLargeMessageSize;
   }

   public synchronized int getConsumerWindowSize()
   {
      return consumerWindowSize;
   }

   public synchronized void setConsumerWindowSize(final int consumerWindowSize)
   {
      checkWrite();
      this.consumerWindowSize = consumerWindowSize;
   }

   public synchronized int getConsumerMaxRate()
   {
      return consumerMaxRate;
   }

   public synchronized void setConsumerMaxRate(final int consumerMaxRate)
   {
      checkWrite();
      this.consumerMaxRate = consumerMaxRate;
   }

   public synchronized int getConfirmationWindowSize()
   {
      return confirmationWindowSize;
   }

   public synchronized void setConfirmationWindowSize(final int confirmationWindowSize)
   {
      checkWrite();
      this.confirmationWindowSize = confirmationWindowSize;
   }

   public synchronized int getProducerWindowSize()
   {
      return producerWindowSize;
   }

   public synchronized void setProducerWindowSize(final int producerWindowSize)
   {
      checkWrite();
      this.producerWindowSize = producerWindowSize;
   }

   public synchronized int getProducerMaxRate()
   {
      return producerMaxRate;
   }

   public synchronized void setProducerMaxRate(final int producerMaxRate)
   {
      checkWrite();
      this.producerMaxRate = producerMaxRate;
   }

   public synchronized boolean isBlockOnAcknowledge()
   {
      return blockOnAcknowledge;
   }

   public synchronized void setBlockOnAcknowledge(final boolean blockOnAcknowledge)
   {
      checkWrite();
      this.blockOnAcknowledge = blockOnAcknowledge;
   }

   public synchronized boolean isBlockOnDurableSend()
   {
      return blockOnDurableSend;
   }

   public synchronized void setBlockOnDurableSend(final boolean blockOnDurableSend)
   {
      checkWrite();
      this.blockOnDurableSend = blockOnDurableSend;
   }

   public synchronized boolean isBlockOnNonDurableSend()
   {
      return blockOnNonDurableSend;
   }

   public synchronized void setBlockOnNonDurableSend(final boolean blockOnNonDurableSend)
   {
      checkWrite();
      this.blockOnNonDurableSend = blockOnNonDurableSend;
   }

   public synchronized boolean isAutoGroup()
   {
      return autoGroup;
   }

   public synchronized void setAutoGroup(final boolean autoGroup)
   {
      checkWrite();
      this.autoGroup = autoGroup;
   }

   public synchronized boolean isPreAcknowledge()
   {
      return preAcknowledge;
   }

   public synchronized void setPreAcknowledge(final boolean preAcknowledge)
   {
      checkWrite();
      this.preAcknowledge = preAcknowledge;
   }

   public synchronized int getAckBatchSize()
   {
      return ackBatchSize;
   }

   public synchronized void setAckBatchSize(final int ackBatchSize)
   {
      checkWrite();
      this.ackBatchSize = ackBatchSize;
   }

   public synchronized long getDiscoveryInitialWaitTimeout()
   {
      return discoveryInitialWaitTimeout;
   }

   public synchronized void setDiscoveryInitialWaitTimeout(final long initialWaitTimeout)
   {
      checkWrite();
      discoveryInitialWaitTimeout = initialWaitTimeout;
   }

   public synchronized boolean isUseGlobalPools()
   {
      return useGlobalPools;
   }

   public synchronized void setUseGlobalPools(final boolean useGlobalPools)
   {
      checkWrite();
      this.useGlobalPools = useGlobalPools;
   }

   public synchronized int getScheduledThreadPoolMaxSize()
   {
      return scheduledThreadPoolMaxSize;
   }

   public synchronized void setScheduledThreadPoolMaxSize(final int scheduledThreadPoolMaxSize)
   {
      checkWrite();
      this.scheduledThreadPoolMaxSize = scheduledThreadPoolMaxSize;
   }

   public synchronized int getThreadPoolMaxSize()
   {
      return threadPoolMaxSize;
   }

   public synchronized void setThreadPoolMaxSize(final int threadPoolMaxSize)
   {
      checkWrite();
      this.threadPoolMaxSize = threadPoolMaxSize;
   }

   public synchronized long getRetryInterval()
   {
      return retryInterval;
   }

   public synchronized void setRetryInterval(final long retryInterval)
   {
      checkWrite();
      this.retryInterval = retryInterval;
   }

   public synchronized long getMaxRetryInterval()
   {
      return maxRetryInterval;
   }

   public synchronized void setMaxRetryInterval(final long retryInterval)
   {
      checkWrite();
      maxRetryInterval = retryInterval;
   }

   public synchronized double getRetryIntervalMultiplier()
   {
      return retryIntervalMultiplier;
   }

   public synchronized void setRetryIntervalMultiplier(final double retryIntervalMultiplier)
   {
      checkWrite();
      this.retryIntervalMultiplier = retryIntervalMultiplier;
   }

   public synchronized int getReconnectAttempts()
   {
      return reconnectAttempts;
   }

   public synchronized void setReconnectAttempts(final int reconnectAttempts)
   {
      checkWrite();
      this.reconnectAttempts = reconnectAttempts;
   }

   public void setInitialConnectAttempts(int initialConnectAttempts)
   {
      checkWrite();
      this.initialConnectAttempts = initialConnectAttempts;
   }

   public int getInitialConnectAttempts()
   {
      return initialConnectAttempts;
   }

   public synchronized boolean isFailoverOnInitialConnection()
   {
      return this.failoverOnInitialConnection;
   }

   public synchronized void setFailoverOnInitialConnection(final boolean failover)
   {
      checkWrite();
      this.failoverOnInitialConnection = failover;
   }

   public synchronized String getConnectionLoadBalancingPolicyClassName()
   {
      return connectionLoadBalancingPolicyClassName;
   }

   public synchronized void setConnectionLoadBalancingPolicyClassName(final String loadBalancingPolicyClassName)
   {
      checkWrite();
      connectionLoadBalancingPolicyClassName = loadBalancingPolicyClassName;
   }

   public synchronized String getLocalBindAddress()
   {
      return localBindAddress;
   }

   public synchronized void setLocalBindAddress(final String localBindAddress)
   {
      checkWrite();
      this.localBindAddress = localBindAddress;
   }

   public synchronized String getDiscoveryAddress()
   {
      return discoveryAddress;
   }

   public synchronized int getDiscoveryPort()
   {
      return discoveryPort;
   }

   public TransportConfiguration[] getStaticTransportConfigurations()
   {
      return this.initialConnectors;
   }

   public synchronized long getDiscoveryRefreshTimeout()
   {
      return discoveryRefreshTimeout;
   }

   public void addInterceptor(final Interceptor interceptor)
   {
      interceptors.add(interceptor);
   }

   public boolean removeInterceptor(final Interceptor interceptor)
   {
      return interceptors.remove(interceptor);
   }

   public synchronized void setDiscoveryRefreshTimeout(final long discoveryRefreshTimeout)
   {
      checkWrite();
      this.discoveryRefreshTimeout = discoveryRefreshTimeout;
   }

   public synchronized int getInitialMessagePacketSize()
   {
      return initialMessagePacketSize;
   }

   public synchronized void setInitialMessagePacketSize(final int size)
   {
      checkWrite();
      initialMessagePacketSize = size;
   }

   public void setGroupID(final String groupID)
   {
      checkWrite();
      this.groupID = groupID;
   }

   public String getGroupID()
   {
      return groupID;
   }

   private void checkWrite()
   {
      if (readOnly)
      {
         throw new IllegalStateException("Cannot set attribute on SessionFactory after it has been used");
      }
   }

   public void setNodeID(String nodeID)
   {
      this.nodeID = nodeID;
   }

   public String getNodeID()
   {
      return nodeID;
   }

   public void setClusterConnection(boolean clusterConnection)
   {
      this.clusterConnection = clusterConnection;
   }

   public boolean isClusterConnection()
   {
      return clusterConnection;
   }

   public TransportConfiguration getClusterTransportConfiguration()
   {
      return clusterTransportConfiguration;
   }

   public void setClusterTransportConfiguration(TransportConfiguration tc)
   {
      this.clusterTransportConfiguration = tc;
   }

   public boolean isBackup()
   {
      return backup;
   }

   public void setBackup(boolean backup)
   {
      this.backup = backup;
   }

   @Override
   protected void finalize() throws Throwable
   {
      close();

      super.finalize();
   }

   public void close()
   {
      if (closed)
      {
         return;
      }

      if (discoveryGroup != null)
      {
         try
         {
            discoveryGroup.stop();
         }
         catch (Exception e)
         {
            log.error("Failed to stop discovery group", e);
         }
      }
      else
      {
         staticConnector.disconnect();
      }

      for (ClientSessionFactory factory : factories)
      {
         factory.close();
      }

      factories.clear();

      if (!useGlobalPools)
      {
         if (threadPool != null)
         {
            threadPool.shutdown();

            try
            {
               if (!threadPool.awaitTermination(10000, TimeUnit.MILLISECONDS))
               {
                  log.warn("Timed out waiting for pool to terminate");
               }
            }
            catch (InterruptedException ignore)
            {
            }
         }

         if (scheduledThreadPool != null)
         {
            scheduledThreadPool.shutdown();

            try
            {
               if (!scheduledThreadPool.awaitTermination(10000, TimeUnit.MILLISECONDS))
               {
                  log.warn("Timed out waiting for scheduled pool to terminate");
               }
            }
            catch (InterruptedException ignore)
            {
            }
         }
      }

      closed = true;
   }

   public synchronized void notifyNodeDown(final String nodeID)
   {
      boolean removed = false;

      if (!ha)
      {
         return;
      }

      removed = topology.removeMember(nodeID);

      if (!topology.isEmpty())
      {
         updateArraysAndPairs();

         if (topology.nodes() == 1 && topology.getMember(this.nodeID) != null)
         {
            receivedTopology = false;
         }
      }
      else
      {
         topologyArray = null;

         receivedTopology = false;
      }

      if (removed)
      {
         for (ClusterTopologyListener listener : topologyListeners)
         {
            listener.nodeDown(nodeID);
         }
      }
   }

   public synchronized void notifyNodeUp(final String nodeID,
                                         final Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                                         final boolean last,
                                         final int distance)
   {
      if (!ha)
      {
         return;
      }

      topology.addMember(nodeID, new TopologyMember(connectorPair, distance));

      TopologyMember actMember = topology.getMember(nodeID);

      if (actMember.getConnector().a != null && actMember.getConnector().b != null)
      {
         for (ClientSessionFactory factory : factories)
         {
            ((ClientSessionFactoryInternal) factory).setBackupConnector(actMember.getConnector().a, actMember.getConnector().b);
         }
      }

      if (connectorPair.a != null)
      {
         updateArraysAndPairs();
      }

      if (last)
      {
         receivedTopology = true;
      }

      for (ClusterTopologyListener listener : topologyListeners)
      {
         listener.nodeUP(nodeID, connectorPair, last, distance);
      }

      // Notify if waiting on getting topology
      notify();
   }

   private void updateArraysAndPairs()
   {
      topologyArray = (Pair<TransportConfiguration, TransportConfiguration>[]) Array.newInstance(Pair.class,
            topology.members());

      int count = 0;
      for (TopologyMember pair : topology.getMembers())
      {
         topologyArray[count++] = pair.getConnector();
      }
   }

   public synchronized void connectorsChanged()
   {
      List<DiscoveryEntry> newConnectors = discoveryGroup.getDiscoveryEntries();

      this.initialConnectors = (TransportConfiguration[]) Array.newInstance(TransportConfiguration.class, newConnectors.size());

      int count = 0;
      for (DiscoveryEntry entry : newConnectors)
      {
         this.initialConnectors[count++] = entry.getConnector();
      }

      if (ha && clusterConnection && !receivedTopology && initialConnectors.length > 0)
      {
         // FIXME the node is alone in the cluster. We create a connection to the new node
         // to trigger the node notification to form the cluster.
         try
         {
            connect();
         }
         catch (Exception e)
         {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
         }
      }
   }

   public synchronized void factoryClosed(final ClientSessionFactory factory)
   {
      factories.remove(factory);

      if (factories.isEmpty())
      {
         // Go back to using the broadcast or static list

         receivedTopology = false;

         topology = null;

      }
   }

   public Topology getTopology()
   {
      return topology;
   }

   public void addClusterTopologyListener(final ClusterTopologyListener listener)
   {
      topologyListeners.add(listener);
   }

   public void removeClusterTopologyListener(final ClusterTopologyListener listener)
   {
      topologyListeners.remove(listener);
   }

   public synchronized void addFactory(ClientSessionFactoryInternal factory)
   {
      if (factory != null)
      {
         TransportConfiguration backup = topology.getBackupForConnector(factory.getConnectorConfiguration());
         factory.setBackupConnector(factory.getConnectorConfiguration(), backup);
         factories.add(factory);
      }
   }
   public static void shutdown()
   {
      if (globalScheduledThreadPool != null)
      {
         globalScheduledThreadPool.shutdown();
         globalScheduledThreadPool = null;
      }
      if (globalThreadPool != null)
      {
         globalThreadPool.shutdown();
         globalThreadPool = null;
      }
   }

   class StaticConnector implements Serializable
   {
      private List<Connector> connectors;

      public ClientSessionFactory connect() throws HornetQException
      {
         if (closed)
         {
            throw new IllegalStateException("Cannot create session factory, server locator is closed (maybe it has been garbage collected)");
         }

         try
         {
            initialise();
         }
         catch (Exception e)
         {
            throw new HornetQException(HornetQException.INTERNAL_ERROR, "Failed to initialise session factory", e);
         }

         ClientSessionFactory csf = null;

         createConnectors();

         try
         {
            List<Future<ClientSessionFactory>> futures = threadPool.invokeAll(connectors);
            for (int i = 0, futuresSize = futures.size(); i < futuresSize; i++)
            {
               Future<ClientSessionFactory> future = futures.get(i);
               try
               {
                  csf = future.get();
                  if(csf != null)
                     break;
               }
               catch (Exception e)
               {
                  log.debug("unable to connect with static connector " + connectors.get(i).initialConnector);
               }
            }
            if (csf == null && !closed)
            {
               throw new HornetQException(HornetQException.NOT_CONNECTED, "Failed to connect to any static connectors");
            }
         }
         catch (InterruptedException e)
         {
            throw new HornetQException(HornetQException.NOT_CONNECTED, "Failed to connect to any static connectors", e);
         }

         if (csf == null && !closed)
         {
            throw new HornetQException(HornetQException.NOT_CONNECTED, "Failed to connect to any static connectors");
         }
         return csf;
      }

      private synchronized void createConnectors()
      {
         connectors = new ArrayList<Connector>();
         for (TransportConfiguration initialConnector : initialConnectors)
         {
            ClientSessionFactoryInternal factory = new ClientSessionFactoryImpl(ServerLocatorImpl.this,
                     initialConnector,
                     callTimeout,
                     clientFailureCheckPeriod,
                     connectionTTL,
                     retryInterval,
                     retryIntervalMultiplier,
                     maxRetryInterval,
                     reconnectAttempts,
                     threadPool,
                     scheduledThreadPool,
                     interceptors);
            connectors.add(new Connector(initialConnector, factory));
         }
      }


      public synchronized void disconnect()
      {
         if (connectors != null)
         {
            for (Connector connector : connectors)
            {
               connector.disconnect();
            }
         }
      }

       public void finalize() throws Throwable
      {
         if (!closed)
         {
            log.warn("I'm closing a core ServerLocator you left open. Please make sure you close all ServerLocators explicitly " + "before letting them go out of scope! " +
                                       System.identityHashCode(this));

            log.warn("The ServerLocator you didn't close was created here:", e);

            close();
         }

         super.finalize();
      }

      class Connector implements Callable<ClientSessionFactory>
      {
         private TransportConfiguration initialConnector;
         private volatile ClientSessionFactoryInternal factory;
         private boolean isConnected = false;
         private boolean interrupted = false;
         private Exception e;

         public Connector(TransportConfiguration initialConnector, ClientSessionFactoryInternal factory)
         {
            this.initialConnector = initialConnector;
            this.factory = factory;
         }

         public ClientSessionFactory call() throws HornetQException
         {
            try
            {
               factory.connect(reconnectAttempts, failoverOnInitialConnection);
            }
            catch (HornetQException e)
            {
               if (!interrupted)
               {
                  this.e = e;
                  throw e;
               }
               /*if(factory != null)
               {
                  factory.close();
                  factory = null;
               }*/
               return null;
            }
            isConnected = true;
            for (Connector connector : connectors)
            {
               if (!connector.isConnected())
               {
                  connector.disconnect();
               }
            }
            return factory;
         }

         public boolean isConnected()
         {
            return isConnected;
         }

         public void disconnect()
         {
            interrupted = true;

            if (factory != null)
            {
               factory.causeExit();
               factory.close();
               factory = null;
            }
         }
      }
   }
}
