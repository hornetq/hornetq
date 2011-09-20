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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
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
import org.hornetq.core.remoting.FailureListener;
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

   private boolean finalizeCheck = true;

   private boolean clusterConnection;

   private transient String identity;

   private Set<ClientSessionFactoryInternal> factories = new HashSet<ClientSessionFactoryInternal>();

   private TransportConfiguration[] initialConnectors;

   private DiscoveryGroupConfiguration discoveryGroupConfiguration;

   private StaticConnector staticConnector = new StaticConnector();

   private final Topology topology;

   private Pair<TransportConfiguration, TransportConfiguration>[] topologyArray;

   private boolean receivedTopology;

   private boolean compressLargeMessage;

   // if the system should shutdown the pool when shutting down
   private transient boolean shutdownPool;

   private ExecutorService threadPool;

   private ScheduledExecutorService scheduledThreadPool;

   private DiscoveryGroup discoveryGroup;

   private ConnectionLoadBalancingPolicy loadBalancingPolicy;

   private boolean readOnly;

   // Settable attributes:

   private boolean cacheLargeMessagesClient;

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

   private volatile boolean closing;

   private final List<Interceptor> interceptors = new CopyOnWriteArrayList<Interceptor>();

   private static ExecutorService globalThreadPool;

   private Executor startExecutor;

   private static ScheduledExecutorService globalScheduledThreadPool;
   
   private AfterConnectInternalListener afterConnectListener;

   private String groupID;

   private String nodeID;

   private TransportConfiguration clusterTransportConfiguration;

   private boolean backup;

   private final Exception e = new Exception();

   // To be called when there are ServerLocator being finalized.
   // To be used on test assertions
   public static Runnable finalizeCallback = null;

   public static synchronized void clearThreadPools()
   {

      if (globalThreadPool != null)
      {
         globalThreadPool.shutdown();
         try
         {
            if (!globalThreadPool.awaitTermination(10, TimeUnit.SECONDS))
            {
               throw new IllegalStateException("Couldn't finish the globalThreadPool");
            }
         }
         catch (InterruptedException e)
         {
         }
         finally
         {
            globalThreadPool = null;
         }
      }

      if (globalScheduledThreadPool != null)
      {
         globalScheduledThreadPool.shutdown();
         try
         {
            if (!globalScheduledThreadPool.awaitTermination(10, TimeUnit.SECONDS))
            {
               throw new IllegalStateException("Couldn't finish the globalScheduledThreadPool");
            }
         }
         catch (InterruptedException e)
         {
         }
         finally
         {
            globalScheduledThreadPool = null;
         }
      }
   }

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
      if (threadPool != null)
      {
         return;
      }
      else if (useGlobalPools)
      {
         threadPool = getGlobalThreadPool();

         scheduledThreadPool = getGlobalScheduledThreadPool();
      }
      else
      {
         this.shutdownPool = true;

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
               loadBalancingPolicy = (ConnectionLoadBalancingPolicy)clazz.newInstance();
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

         if (discoveryGroupConfiguration != null)
         {
            InetAddress groupAddress = InetAddress.getByName(discoveryGroupConfiguration.getGroupAddress());

            InetAddress lbAddress;

            if (discoveryGroupConfiguration.getLocalBindAddress() != null)
            {
               lbAddress = InetAddress.getByName(discoveryGroupConfiguration.getLocalBindAddress());
            }
            else
            {
               lbAddress = null;
            }

            discoveryGroup = new DiscoveryGroupImpl(nodeID,
                                                    discoveryGroupConfiguration.getName(),
                                                    lbAddress,
                                                    groupAddress,
                                                    discoveryGroupConfiguration.getGroupPort(),
                                                    discoveryGroupConfiguration.getRefreshTimeout());

            discoveryGroup.registerListener(this);

            discoveryGroup.start();
         }

         readOnly = true;
      }
   }

   private ServerLocatorImpl(final Topology topology,
                             final boolean useHA,
                             final DiscoveryGroupConfiguration discoveryGroupConfiguration,
                             final TransportConfiguration[] transportConfigs)
   {
      e.fillInStackTrace();

      this.topology = topology;

      this.ha = useHA;

      this.discoveryGroupConfiguration = discoveryGroupConfiguration;

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

      compressLargeMessage = HornetQClient.DEFAULT_COMPRESS_LARGE_MESSAGES;

      clusterConnection = false;
   }

   /**
    * Create a ServerLocatorImpl using UDP discovery to lookup cluster
    *
    * @param discoveryAddress
    * @param discoveryPort
    */
   public ServerLocatorImpl(final boolean useHA, final DiscoveryGroupConfiguration groupConfiguration)
   {
      this(useHA ? new Topology(null) : null, useHA, groupConfiguration, null);
      if (useHA)
      {
         // We only set the owner at where the Topology was created.
         // For that reason we can't set it at the main constructor
         topology.setOwner(this);
      }
   }

   /**
    * Create a ServerLocatorImpl using a static list of live servers
    *
    * @param transportConfigs
    */
   public ServerLocatorImpl(final boolean useHA, final TransportConfiguration... transportConfigs)
   {
      this(useHA ? new Topology(null) : null, useHA, null, transportConfigs);
      if (useHA)
      {
         // We only set the owner at where the Topology was created.
         // For that reason we can't set it at the main constructor
         topology.setOwner(this);
      }
   }

   /**
    * Create a ServerLocatorImpl using UDP discovery to lookup cluster
    *
    * @param discoveryAddress
    * @param discoveryPort
    */
   public ServerLocatorImpl(final Topology topology,
                            final boolean useHA,
                            final DiscoveryGroupConfiguration groupConfiguration)
   {
      this(topology, useHA, groupConfiguration, null);

   }

   /**
    * Create a ServerLocatorImpl using a static list of live servers
    *
    * @param transportConfigs
    */
   public ServerLocatorImpl(final Topology topology,
                            final boolean useHA,
                            final TransportConfiguration... transportConfigs)
   {
      this(topology, useHA, null, transportConfigs);
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

   public void start(Executor executor) throws Exception
   {
      initialise();

      this.startExecutor = executor;

      executor.execute(new Runnable()
      {
         public void run()
         {
            try
            {
               connect();
            }
            catch (Exception e)
            {
               if (!closing)
               {
                  log.warn("did not connect the cluster connection to other nodes", e);
               }
            }
         }
      });
   }
   
   public Executor getExecutor()
   {
      return startExecutor;
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.client.ServerLocator#disableFinalizeCheck()
    */
   public void disableFinalizeCheck()
   {
      finalizeCheck = false;
   }

   public ClientSessionFactoryInternal connect() throws Exception
   {
      ClientSessionFactoryInternal sf;
      // static list of initial connectors
      if (initialConnectors != null && discoveryGroup == null)
      {
         sf = (ClientSessionFactoryInternal)staticConnector.connect();
      }
      // wait for discovery group to get the list of initial connectors
      else
      {
         sf = (ClientSessionFactoryInternal)createSessionFactory();
      }
      addFactory(sf);
      return sf;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.client.impl.ServerLocatorInternal#setAfterConnectionInternalListener(org.hornetq.core.client.impl.AfterConnectInternalListener)
    */
   public void setAfterConnectionInternalListener(AfterConnectInternalListener listener)
   {
      this.afterConnectListener = listener;
   }

   public AfterConnectInternalListener getAfterConnectInternalListener()
   {
      return afterConnectListener;
   }
   
   public boolean isClosed()
   {
      return closed || closing;
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
      if (closed || closing)
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
         long timeout = clusterConnection ? 0 : discoveryGroupConfiguration.getDiscoveryInitialWaitTimeout();
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

         if (ha || clusterConnection)
         {
            long timeout = System.currentTimeMillis() + 30000;
            while (!ServerLocatorImpl.this.closed && !ServerLocatorImpl.this.closing &&
                   !receivedTopology &&
                   timeout > System.currentTimeMillis())
            {
               // Now wait for the topology

               try
               {
                  wait(1000);
               }
               catch (InterruptedException ignore)
               {
               }

            }

            if (System.currentTimeMillis() > timeout && !receivedTopology && !closed && !closing)
            {
               throw new HornetQException(HornetQException.CONNECTION_TIMEDOUT,
                                          "Timed out waiting to receive cluster topology");
            }

         }

         addFactory(factory);

         return factory;
      }

   }

   public boolean isHA()
   {
      return ha;
   }

   public boolean isCacheLargeMessagesClient()
   {
      return cacheLargeMessagesClient;
   }

   public void setCacheLargeMessagesClient(final boolean cached)
   {
      cacheLargeMessagesClient = cached;
   }

   public long getClientFailureCheckPeriod()
   {
      return clientFailureCheckPeriod;
   }

   public void setClientFailureCheckPeriod(final long clientFailureCheckPeriod)
   {
      checkWrite();
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
   }

   public long getConnectionTTL()
   {
      return connectionTTL;
   }

   public void setConnectionTTL(final long connectionTTL)
   {
      checkWrite();
      this.connectionTTL = connectionTTL;
   }

   public long getCallTimeout()
   {
      return callTimeout;
   }

   public void setCallTimeout(final long callTimeout)
   {
      checkWrite();
      this.callTimeout = callTimeout;
   }

   public int getMinLargeMessageSize()
   {
      return minLargeMessageSize;
   }

   public void setMinLargeMessageSize(final int minLargeMessageSize)
   {
      checkWrite();
      this.minLargeMessageSize = minLargeMessageSize;
   }

   public int getConsumerWindowSize()
   {
      return consumerWindowSize;
   }

   public void setConsumerWindowSize(final int consumerWindowSize)
   {
      checkWrite();
      this.consumerWindowSize = consumerWindowSize;
   }

   public int getConsumerMaxRate()
   {
      return consumerMaxRate;
   }

   public void setConsumerMaxRate(final int consumerMaxRate)
   {
      checkWrite();
      this.consumerMaxRate = consumerMaxRate;
   }

   public int getConfirmationWindowSize()
   {
      return confirmationWindowSize;
   }

   public void setConfirmationWindowSize(final int confirmationWindowSize)
   {
      checkWrite();
      this.confirmationWindowSize = confirmationWindowSize;
   }

   public int getProducerWindowSize()
   {
      return producerWindowSize;
   }

   public void setProducerWindowSize(final int producerWindowSize)
   {
      checkWrite();
      this.producerWindowSize = producerWindowSize;
   }

   public int getProducerMaxRate()
   {
      return producerMaxRate;
   }

   public void setProducerMaxRate(final int producerMaxRate)
   {
      checkWrite();
      this.producerMaxRate = producerMaxRate;
   }

   public boolean isBlockOnAcknowledge()
   {
      return blockOnAcknowledge;
   }

   public void setBlockOnAcknowledge(final boolean blockOnAcknowledge)
   {
      checkWrite();
      this.blockOnAcknowledge = blockOnAcknowledge;
   }

   public boolean isBlockOnDurableSend()
   {
      return blockOnDurableSend;
   }

   public void setBlockOnDurableSend(final boolean blockOnDurableSend)
   {
      checkWrite();
      this.blockOnDurableSend = blockOnDurableSend;
   }

   public boolean isBlockOnNonDurableSend()
   {
      return blockOnNonDurableSend;
   }

   public void setBlockOnNonDurableSend(final boolean blockOnNonDurableSend)
   {
      checkWrite();
      this.blockOnNonDurableSend = blockOnNonDurableSend;
   }

   public boolean isAutoGroup()
   {
      return autoGroup;
   }

   public void setAutoGroup(final boolean autoGroup)
   {
      checkWrite();
      this.autoGroup = autoGroup;
   }

   public boolean isPreAcknowledge()
   {
      return preAcknowledge;
   }

   public void setPreAcknowledge(final boolean preAcknowledge)
   {
      checkWrite();
      this.preAcknowledge = preAcknowledge;
   }

   public int getAckBatchSize()
   {
      return ackBatchSize;
   }

   public void setAckBatchSize(final int ackBatchSize)
   {
      checkWrite();
      this.ackBatchSize = ackBatchSize;
   }

   public boolean isUseGlobalPools()
   {
      return useGlobalPools;
   }

   public void setUseGlobalPools(final boolean useGlobalPools)
   {
      checkWrite();
      this.useGlobalPools = useGlobalPools;
   }

   public int getScheduledThreadPoolMaxSize()
   {
      return scheduledThreadPoolMaxSize;
   }

   public void setScheduledThreadPoolMaxSize(final int scheduledThreadPoolMaxSize)
   {
      checkWrite();
      this.scheduledThreadPoolMaxSize = scheduledThreadPoolMaxSize;
   }

   public int getThreadPoolMaxSize()
   {
      return threadPoolMaxSize;
   }

   public void setThreadPoolMaxSize(final int threadPoolMaxSize)
   {
      checkWrite();
      this.threadPoolMaxSize = threadPoolMaxSize;
   }

   public long getRetryInterval()
   {
      return retryInterval;
   }

   public void setRetryInterval(final long retryInterval)
   {
      checkWrite();
      this.retryInterval = retryInterval;
   }

   public long getMaxRetryInterval()
   {
      return maxRetryInterval;
   }

   public void setMaxRetryInterval(final long retryInterval)
   {
      checkWrite();
      maxRetryInterval = retryInterval;
   }

   public double getRetryIntervalMultiplier()
   {
      return retryIntervalMultiplier;
   }

   public void setRetryIntervalMultiplier(final double retryIntervalMultiplier)
   {
      checkWrite();
      this.retryIntervalMultiplier = retryIntervalMultiplier;
   }

   public int getReconnectAttempts()
   {
      return reconnectAttempts;
   }

   public void setReconnectAttempts(final int reconnectAttempts)
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

   public boolean isFailoverOnInitialConnection()
   {
      return this.failoverOnInitialConnection;
   }

   public void setFailoverOnInitialConnection(final boolean failover)
   {
      checkWrite();
      this.failoverOnInitialConnection = failover;
   }

   public String getConnectionLoadBalancingPolicyClassName()
   {
      return connectionLoadBalancingPolicyClassName;
   }

   public void setConnectionLoadBalancingPolicyClassName(final String loadBalancingPolicyClassName)
   {
      checkWrite();
      connectionLoadBalancingPolicyClassName = loadBalancingPolicyClassName;
   }

   public TransportConfiguration[] getStaticTransportConfigurations()
   {
      return this.initialConnectors;
   }

   public DiscoveryGroupConfiguration getDiscoveryGroupConfiguration()
   {
      return discoveryGroupConfiguration;
   }

   public void addInterceptor(final Interceptor interceptor)
   {
      interceptors.add(interceptor);
   }

   public boolean removeInterceptor(final Interceptor interceptor)
   {
      return interceptors.remove(interceptor);
   }

   public int getInitialMessagePacketSize()
   {
      return initialMessagePacketSize;
   }

   public void setInitialMessagePacketSize(final int size)
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

   /* (non-Javadoc)
    * @see org.hornetq.api.core.client.ServerLocator#isCompressLargeMessage()
    */
   public boolean isCompressLargeMessage()
   {
      return compressLargeMessage;
   }

   /* (non-Javadoc)
    * @see org.hornetq.api.core.client.ServerLocator#setCompressLargeMessage(boolean)
    */
   public void setCompressLargeMessage(boolean compress)
   {
      this.compressLargeMessage = compress;
   }

   private void checkWrite()
   {
      if (readOnly)
      {
         throw new IllegalStateException("Cannot set attribute on SessionFactory after it has been used");
      }
   }

   public String getIdentity()
   {
      return identity;
   }
   
   public void setIdentity(String identity)
   {
      this.identity = identity;
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
      if (finalizeCheck)
      {
         close();
      }

      super.finalize();
   }

   public void cleanup()
   {
      doClose(false);
   }

   public void close()
   {
      doClose(true);
   }

   protected void doClose(final boolean sendClose)
   {
      if (closed)
      {
         if (log.isDebugEnabled())
         {
            log.debug(this + " is already closed when calling closed");
         }
         return;
      }

      if (log.isDebugEnabled())
      {
         log.debug(this + " is calling close", new Exception("trace"));
      }

      closing = true;

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

      Set<ClientSessionFactoryInternal> clonedFactory = new HashSet<ClientSessionFactoryInternal>(factories);

      for (ClientSessionFactory factory : clonedFactory)
      {
         if (sendClose)
         {
            factory.close();
         }
         else
         {
            factory.cleanup();
         }
      }

      factories.clear();

      if (shutdownPool)
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
      readOnly = false;

      closed = true;
   }

   /** This is directly called when the connection to the node is gone,
    *  or when the node sends a disconnection.
    *  Look for callers of this method! */
   public void notifyNodeDown(final long eventTime, final String nodeID)
   {

      if (topology == null)
      {
         // there's no topology here
         return;
      }

      if (log.isDebugEnabled())
      {
         log.debug("nodeDown " + this + " nodeID=" + nodeID + " as being down", new Exception("trace"));
      }
      
      if (!(isClusterConnection() && nodeID.equals(this.getNodeID())) && topology.removeMember(eventTime, nodeID))
      {
         if (topology.isEmpty())
         {
            // Resetting the topology to its original condition as it was brand new
            topologyArray = null;

            receivedTopology = false;
         }
         else
         {
            updateArraysAndPairs();

            if (topology.nodes() == 1 && topology.getMember(this.nodeID) != null)
            {
               // Resetting the topology to its original condition as it was brand new
               receivedTopology = false;
            }
         }
      }

   }

   public void notifyNodeUp(long uniqueEventID,
                            final String nodeID,
                            final Pair<TransportConfiguration, TransportConfiguration> connectorPair,
                            final boolean last)
   {
      if (topology == null)
      {
         // there's no topology
         return;
      }

      if (log.isDebugEnabled())
      {
         log.debug("NodeUp " + this + "::nodeID=" + nodeID + ", connectorPair=" + connectorPair, new Exception("trace"));
      }

      TopologyMember member = new TopologyMember(connectorPair.a, connectorPair.b);

      if (topology.updateMember(uniqueEventID, nodeID, member))
      {

         TopologyMember actMember = topology.getMember(nodeID);

         if (actMember != null && actMember.getConnector().a != null && actMember.getConnector().b != null)
         {
            for (ClientSessionFactory factory : factories)
            {
               ((ClientSessionFactoryInternal)factory).setBackupConnector(actMember.getConnector().a,
                                                                          actMember.getConnector().b);
            }
         }

         updateArraysAndPairs();
      }

      if (last)
      {
         synchronized (this)
         {
            receivedTopology = true;
            // Notify if waiting on getting topology
            notifyAll();
         }
      }
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      if (identity != null)
      {
         return "ServerLocatorImpl (identity=" + identity +
                ") [initialConnectors=" +
                Arrays.toString(initialConnectors) +
                ", discoveryGroupConfiguration=" +
                discoveryGroupConfiguration +
                "]";
      }
      else
      {
         return "ServerLocatorImpl [initialConnectors=" + Arrays.toString(initialConnectors) +
                ", discoveryGroupConfiguration=" +
                discoveryGroupConfiguration +
                "]";
      }
   }

   private synchronized void updateArraysAndPairs()
   {
      Collection<TopologyMember> membersCopy = topology.getMembers();

      topologyArray = (Pair<TransportConfiguration, TransportConfiguration>[])Array.newInstance(Pair.class,
                                                                                                membersCopy.size());

      int count = 0;
      for (TopologyMember pair : membersCopy)
      {
         topologyArray[count++] = pair.getConnector();
      }
   }

   public synchronized void connectorsChanged()
   {
      List<DiscoveryEntry> newConnectors = discoveryGroup.getDiscoveryEntries();

      this.initialConnectors = (TransportConfiguration[])Array.newInstance(TransportConfiguration.class,
                                                                           newConnectors.size());

      int count = 0;
      for (DiscoveryEntry entry : newConnectors)
      {
         this.initialConnectors[count++] = entry.getConnector();
         
         if (topology != null && topology.getMember(entry.getNodeID()) == null)
         {
            TopologyMember member = new TopologyMember(entry.getConnector(), null);
            // on this case we set it as zero as any update coming from server should be accepted
            topology.updateMember(0, entry.getNodeID(), member);
         }
      }

      if (clusterConnection && !receivedTopology && initialConnectors.length > 0)
      {
         // FIXME the node is alone in the cluster. We create a connection to the new node
         // to trigger the node notification to form the cluster.
         try
         {
            connect();
         }
         catch (Exception e)
         {
            e.printStackTrace(); // To change body of catch statement use File | Settings | File Templates.
         }
      }
   }

   public synchronized void factoryClosed(final ClientSessionFactory factory)
   {
      factories.remove(factory);

      if (!clusterConnection && factories.isEmpty())
      {
         // Go back to using the broadcast or static list

         receivedTopology = false;

         topologyArray = null;
      }
   }

   public Topology getTopology()
   {
      return topology;
   }

   public void addClusterTopologyListener(final ClusterTopologyListener listener)
   {
      topology.addClusterTopologyListener(listener);
   }

   public void removeClusterTopologyListener(final ClusterTopologyListener listener)
   {
      topology.removeClusterTopologyListener(listener);
   }

   public synchronized void addFactory(ClientSessionFactoryInternal factory)
   {
      if (factory != null)
      {
         TransportConfiguration backup = null;

         if (topology != null)
         {
            backup = topology.getBackupForConnector(factory.getConnectorConfiguration());
         }

         factory.setBackupConnector(factory.getConnectorConfiguration(), backup);
         factories.add(factory);
      }
   }

   class StaticConnector implements Serializable
   {
      private static final long serialVersionUID = 6772279632415242634l;

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

            int retryNumber = 0;
            while (csf == null && !ServerLocatorImpl.this.closed && !ServerLocatorImpl.this.closing)
            {
               retryNumber++;
               for (Connector conn : connectors)
               {
                  if (log.isDebugEnabled())
                  {
                     log.debug(this + "::Submitting connect towards " + conn);
                  }

                  csf = conn.tryConnect();

                  if (csf != null)
                  {
                     csf.getConnection().addFailureListener(new FailureListener()
                     {
                        // Case the node where the cluster connection was connected is gone, we need to restart the
                        // connection
                        public void connectionFailed(HornetQException exception, boolean failedOver)
                        {
                           if (clusterConnection && exception.getCode() == HornetQException.DISCONNECTED)
                           {
                              try
                              {
                                 ServerLocatorImpl.this.start(startExecutor);
                              }
                              catch (Exception e)
                              {
                                 // There isn't much to be done if this happens here
                                 log.warn(e.getMessage());
                              }
                           }
                        }
                     });

                     if (log.isDebugEnabled())
                     {
                        log.debug("Returning " + csf +
                                  " after " +
                                  retryNumber +
                                  " retries on StaticConnector " +
                                  ServerLocatorImpl.this);
                     }

                     return csf;
                  }
               }

               if (initialConnectAttempts >= 0 && retryNumber > initialConnectAttempts)
               {
                  break;
               }

               if (!closed && !closing)
               {
                  Thread.sleep(retryInterval);
               }
            }

         }
         catch (Exception e)
         {
            log.warn(e.getMessage(), e);
            throw new HornetQException(HornetQException.NOT_CONNECTED, "Failed to connect to any static connectors", e);
         }

         if (csf == null && !closed)
         {
            log.warn("Failed to connecto to any static connector, throwing exception now");
            throw new HornetQException(HornetQException.NOT_CONNECTED, "Failed to connect to any static connectors");
         }
         if (log.isDebugEnabled())
         {
            log.debug("Returning " + csf + " on " + ServerLocatorImpl.this);
         }
         return csf;
      }

      private synchronized void createConnectors()
      {
         if (connectors != null)
         {
            for (Connector conn : connectors)
            {
               if (conn != null)
               {
                  conn.disconnect();
               }
            }
         }
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

            factory.disableFinalizeCheck();

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
         if (!closed && finalizeCheck)
         {
            log.warn("I'm closing a core ServerLocator you left open. Please make sure you close all ServerLocators explicitly " + "before letting them go out of scope! " +
                     System.identityHashCode(this));

            log.warn("The ServerLocator you didn't close was created here:", e);

            if (ServerLocatorImpl.finalizeCallback != null)
            {
               ServerLocatorImpl.finalizeCallback.run();
            }

            close();
         }

         super.finalize();
      }

      class Connector
      {
         private TransportConfiguration initialConnector;

         private volatile ClientSessionFactoryInternal factory;

         private boolean interrupted = false;

         private Exception e;

         public Connector(TransportConfiguration initialConnector, ClientSessionFactoryInternal factory)
         {
            this.initialConnector = initialConnector;
            this.factory = factory;
         }

         public ClientSessionFactory tryConnect() throws HornetQException
         {
            if (log.isDebugEnabled())
            {
               log.debug(this + "::Trying to connect to " + factory);
            }
            try
            {
               ClientSessionFactoryInternal factoryToUse = factory;
               if (factoryToUse != null)
               {
                  factory.connect(1, false);
               }
               return factoryToUse;
            }
            catch (HornetQException e)
            {
               log.debug(this + "::Exception on establish connector initial connection", e);
               return null;
            }
         }

         public void disconnect()
         {
            interrupted = true;

            if (factory != null)
            {
               factory.causeExit();
               factory.cleanup();
               factory = null;
            }
         }

         /* (non-Javadoc)
          * @see java.lang.Object#toString()
          */
         @Override
         public String toString()
         {
            return "Connector [initialConnector=" + initialConnector + "]";
         }

      }
   }
}
