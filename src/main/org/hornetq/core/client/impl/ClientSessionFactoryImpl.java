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
package org.hornetq.core.client.impl;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.ConnectionLoadBalancingPolicy;
import org.hornetq.core.cluster.DiscoveryEntry;
import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.cluster.DiscoveryListener;
import org.hornetq.core.cluster.impl.DiscoveryGroupImpl;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.Interceptor;
import org.hornetq.utils.HornetQThreadFactory;
import org.hornetq.utils.Pair;
import org.hornetq.utils.UUIDGenerator;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @version <tt>$Revision: 3602 $</tt>
 * 
 */
public class ClientSessionFactoryImpl implements ClientSessionFactoryInternal, DiscoveryListener, Serializable
{
   // Constants
   // ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 2512460695662741413L;

   private static final Logger log = Logger.getLogger(ClientSessionFactoryImpl.class);

   public static final String DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME = "org.hornetq.core.client.impl.RoundRobinConnectionLoadBalancingPolicy";

   public static final long DEFAULT_CLIENT_FAILURE_CHECK_PERIOD = 30000;

   // 1 minute - this should be higher than ping period
   public static final long DEFAULT_CONNECTION_TTL = 1 * 60 * 1000;

   // Any message beyond this size is considered a large message (to be sent in chunks)
   public static final int DEFAULT_MIN_LARGE_MESSAGE_SIZE = 100 * 1024;

   public static final int DEFAULT_CONSUMER_WINDOW_SIZE = 1024 * 1024;

   public static final int DEFAULT_CONSUMER_MAX_RATE = -1;

   public static final int DEFAULT_CONFIRMATION_WINDOW_SIZE = -1;

   public static final int DEFAULT_PRODUCER_WINDOW_SIZE = 1024 * 1024;

   public static final int DEFAULT_PRODUCER_MAX_RATE = -1;

   public static final boolean DEFAULT_BLOCK_ON_ACKNOWLEDGE = false;

   public static final boolean DEFAULT_BLOCK_ON_PERSISTENT_SEND = true;

   public static final boolean DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND = false;

   public static final boolean DEFAULT_AUTO_GROUP = false;

   public static final long DEFAULT_CALL_TIMEOUT = 30000;

   public static final int DEFAULT_ACK_BATCH_SIZE = 1024 * 1024;

   public static final boolean DEFAULT_PRE_ACKNOWLEDGE = false;

   public static final long DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT = 2000;

   public static final long DEFAULT_DISCOVERY_REFRESH_TIMEOUT = 10000;

   public static final long DEFAULT_RETRY_INTERVAL = 2000;

   public static final double DEFAULT_RETRY_INTERVAL_MULTIPLIER = 1d;

   public static final long DEFAULT_MAX_RETRY_INTERVAL = 2000;

   public static final int DEFAULT_RECONNECT_ATTEMPTS = 0;

   public static final boolean DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN = false;

   public static final boolean DEFAULT_USE_GLOBAL_POOLS = true;

   public static final int DEFAULT_THREAD_POOL_MAX_SIZE = -1;

   public static final int DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE = 5;

   public static final boolean DEFAULT_CACHE_LARGE_MESSAGE_CLIENT = false;
   
   public static final int DEFAULT_INITIAL_MESSAGE_PACKET_SIZE = 1500;

   // Attributes
   // -----------------------------------------------------------------------------------

   private final Map<Pair<TransportConfiguration, TransportConfiguration>, FailoverManager> failoverManagerMap = new LinkedHashMap<Pair<TransportConfiguration, TransportConfiguration>, FailoverManager>();

   private volatile boolean receivedBroadcast = false;

   private ExecutorService threadPool;

   private ScheduledExecutorService scheduledThreadPool;

   private DiscoveryGroup discoveryGroup;

   private ConnectionLoadBalancingPolicy loadBalancingPolicy;

   private FailoverManager[] failoverManagerArray;

   private boolean readOnly;

   // Settable attributes:

   private boolean cacheLargeMessagesClient = DEFAULT_CACHE_LARGE_MESSAGE_CLIENT;

   private List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors;

   private String discoveryAddress;

   private int discoveryPort;

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

   private boolean blockOnPersistentSend;

   private boolean blockOnNonPersistentSend;

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
      
   private int initialMessagePacketSize;

   private volatile boolean closed;

   private boolean failoverOnServerShutdown;

   private final List<Interceptor> interceptors = new CopyOnWriteArrayList<Interceptor>();

   private static ExecutorService globalThreadPool;

   private static ScheduledExecutorService globalScheduledThreadPool;

   private static synchronized ExecutorService getGlobalThreadPool()
   {
      if (globalThreadPool == null)
      {
         ThreadFactory factory = new HornetQThreadFactory("HornetQ-client-global-threads", true);

         globalThreadPool = Executors.newCachedThreadPool(factory);
      }

      return globalThreadPool;
   }

   private static synchronized ScheduledExecutorService getGlobalScheduledThreadPool()
   {
      if (globalScheduledThreadPool == null)
      {
         ThreadFactory factory = new HornetQThreadFactory("HornetQ-client-global-scheduled-threads", true);

         globalScheduledThreadPool = Executors.newScheduledThreadPool(DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE, factory);
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
                                                          true);

         if (threadPoolMaxSize == -1)
         {
            threadPool = Executors.newCachedThreadPool(factory);
         }
         else
         {
            threadPool = Executors.newFixedThreadPool(threadPoolMaxSize, factory);
         }

         factory = new HornetQThreadFactory("HornetQ-client-factory-pinger-threads-" + System.identityHashCode(this),
                                            true);

         scheduledThreadPool = Executors.newScheduledThreadPool(scheduledThreadPoolMaxSize, factory);
      }
   }

   private void initialise() throws Exception
   {
      setThreadPools();

      instantiateLoadBalancingPolicy();

      if (discoveryAddress != null)
      {
         InetAddress groupAddress = InetAddress.getByName(discoveryAddress);

         discoveryGroup = new DiscoveryGroupImpl(UUIDGenerator.getInstance().generateStringUUID(),
                                                 discoveryAddress,
                                                 groupAddress,
                                                 discoveryPort,
                                                 discoveryRefreshTimeout);

         discoveryGroup.registerListener(this);

         discoveryGroup.start();
      }
      else if (staticConnectors != null)
      {
         for (Pair<TransportConfiguration, TransportConfiguration> pair : staticConnectors)
         {
            FailoverManager cm = new FailoverManagerImpl(this,
                                                         pair.a,
                                                         pair.b,
                                                         failoverOnServerShutdown,
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

            failoverManagerMap.put(pair, cm);
         }

         updatefailoverManagerArray();
      }
      else
      {
         throw new IllegalStateException("Before using a session factory you must either set discovery address and port or " + "provide some static transport configuration");
      }
   }

   // Static
   // ---------------------------------------------------------------------------------------

   // Constructors
   // ---------------------------------------------------------------------------------

   public ClientSessionFactoryImpl(final ClientSessionFactory other)
   {
      discoveryAddress = other.getDiscoveryAddress();
      
      discoveryPort = other.getDiscoveryPort();
      
      staticConnectors = other.getStaticConnectors();
      
      discoveryRefreshTimeout = other.getDiscoveryRefreshTimeout();

      clientFailureCheckPeriod = other.getClientFailureCheckPeriod();

      connectionTTL = other.getConnectionTTL();

      callTimeout = other.getCallTimeout();

      minLargeMessageSize = other.getMinLargeMessageSize();

      consumerWindowSize = other.getConsumerWindowSize();

      consumerMaxRate = other.getConsumerMaxRate();

      confirmationWindowSize = other.getConfirmationWindowSize();
      
      producerWindowSize = other.getProducerWindowSize();

      producerMaxRate = other.getProducerMaxRate();

      blockOnAcknowledge = other.isBlockOnAcknowledge();

      blockOnPersistentSend = other.isBlockOnPersistentSend();

      blockOnNonPersistentSend = other.isBlockOnNonPersistentSend();

      autoGroup = other.isAutoGroup();

      preAcknowledge = other.isPreAcknowledge();

      ackBatchSize = other.getAckBatchSize();

      connectionLoadBalancingPolicyClassName = other.getConnectionLoadBalancingPolicyClassName();

      discoveryInitialWaitTimeout = other.getDiscoveryInitialWaitTimeout();

      useGlobalPools = other.isUseGlobalPools();

      scheduledThreadPoolMaxSize = other.getScheduledThreadPoolMaxSize();

      threadPoolMaxSize = other.getThreadPoolMaxSize();

      retryInterval = other.getRetryInterval();

      retryIntervalMultiplier = other.getRetryIntervalMultiplier();

      maxRetryInterval = other.getMaxRetryInterval();

      reconnectAttempts = other.getReconnectAttempts();

      failoverOnServerShutdown = other.isFailoverOnServerShutdown();
      
      cacheLargeMessagesClient = other.isCacheLargeMessagesClient();
      
      initialMessagePacketSize = other.getInitialMessagePacketSize();
   }
   
   public ClientSessionFactoryImpl()
   {
      discoveryRefreshTimeout = DEFAULT_DISCOVERY_REFRESH_TIMEOUT;

      clientFailureCheckPeriod = DEFAULT_CLIENT_FAILURE_CHECK_PERIOD;

      connectionTTL = DEFAULT_CONNECTION_TTL;

      callTimeout = DEFAULT_CALL_TIMEOUT;

      minLargeMessageSize = DEFAULT_MIN_LARGE_MESSAGE_SIZE;

      consumerWindowSize = DEFAULT_CONSUMER_WINDOW_SIZE;

      consumerMaxRate = DEFAULT_CONSUMER_MAX_RATE;

      confirmationWindowSize = DEFAULT_CONFIRMATION_WINDOW_SIZE;

      producerWindowSize = DEFAULT_PRODUCER_WINDOW_SIZE;

      producerMaxRate = DEFAULT_PRODUCER_MAX_RATE;

      blockOnAcknowledge = DEFAULT_BLOCK_ON_ACKNOWLEDGE;

      blockOnPersistentSend = DEFAULT_BLOCK_ON_PERSISTENT_SEND;

      blockOnNonPersistentSend = DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;

      autoGroup = DEFAULT_AUTO_GROUP;

      preAcknowledge = DEFAULT_PRE_ACKNOWLEDGE;

      ackBatchSize = DEFAULT_ACK_BATCH_SIZE;

      connectionLoadBalancingPolicyClassName = DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;

      discoveryInitialWaitTimeout = DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT;

      useGlobalPools = DEFAULT_USE_GLOBAL_POOLS;

      scheduledThreadPoolMaxSize = DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;

      threadPoolMaxSize = DEFAULT_THREAD_POOL_MAX_SIZE;

      retryInterval = DEFAULT_RETRY_INTERVAL;

      retryIntervalMultiplier = DEFAULT_RETRY_INTERVAL_MULTIPLIER;

      maxRetryInterval = DEFAULT_MAX_RETRY_INTERVAL;

      reconnectAttempts = DEFAULT_RECONNECT_ATTEMPTS;

      failoverOnServerShutdown = DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN;
      
      cacheLargeMessagesClient = DEFAULT_CACHE_LARGE_MESSAGE_CLIENT;
      
      initialMessagePacketSize = DEFAULT_INITIAL_MESSAGE_PACKET_SIZE;
   }

   public ClientSessionFactoryImpl(final String discoveryAddress, final int discoveryPort)
   {
      this();

      this.discoveryAddress = discoveryAddress;

      this.discoveryPort = discoveryPort;
   }

   public ClientSessionFactoryImpl(final List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors)
   {
      this();

      this.staticConnectors = staticConnectors;
   }

   public ClientSessionFactoryImpl(final TransportConfiguration connectorConfig,
                                   final TransportConfiguration backupConnectorConfig)
   {
      this();

      staticConnectors = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();

      staticConnectors.add(new Pair<TransportConfiguration, TransportConfiguration>(connectorConfig,
                                                                                    backupConnectorConfig));
   }

   public ClientSessionFactoryImpl(final TransportConfiguration connectorConfig)
   {
      this(connectorConfig, null);
   }

   // ClientSessionFactory implementation------------------------------------------------------------

   public synchronized boolean isCacheLargeMessagesClient()
   {
      return cacheLargeMessagesClient;
   }

   public synchronized void setCacheLargeMessagesClient(boolean cached)
   {
      this.cacheLargeMessagesClient = cached;
   }

   public synchronized List<Pair<TransportConfiguration, TransportConfiguration>> getStaticConnectors()
   {
      return staticConnectors;
   }

   public synchronized void setStaticConnectors(List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors)
   {
      checkWrite();

      this.staticConnectors = staticConnectors;
   }

   public synchronized long getClientFailureCheckPeriod()
   {
      return clientFailureCheckPeriod;
   }

   public synchronized void setClientFailureCheckPeriod(long clientFailureCheckPeriod)
   {
      checkWrite();
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
   }

   public synchronized long getConnectionTTL()
   {
      return connectionTTL;
   }

   public synchronized void setConnectionTTL(long connectionTTL)
   {
      checkWrite();
      this.connectionTTL = connectionTTL;
   }

   public synchronized long getCallTimeout()
   {
      return callTimeout;
   }

   public synchronized void setCallTimeout(long callTimeout)
   {
      checkWrite();
      this.callTimeout = callTimeout;
   }

   public synchronized int getMinLargeMessageSize()
   {
      return minLargeMessageSize;
   }

   public synchronized void setMinLargeMessageSize(int minLargeMessageSize)
   {
      checkWrite();
      this.minLargeMessageSize = minLargeMessageSize;
   }

   public synchronized int getConsumerWindowSize()
   {
      return consumerWindowSize;
   }

   public synchronized void setConsumerWindowSize(int consumerWindowSize)
   {
      checkWrite();
      this.consumerWindowSize = consumerWindowSize;
   }

   public synchronized int getConsumerMaxRate()
   {
      return consumerMaxRate;
   }

   public synchronized void setConsumerMaxRate(int consumerMaxRate)
   {
      checkWrite();
      this.consumerMaxRate = consumerMaxRate;
   }

   public synchronized int getConfirmationWindowSize()
   {
      return confirmationWindowSize;
   }

   public synchronized void setConfirmationWindowSize(int confirmationWindowSize)
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

   public synchronized void setProducerMaxRate(int producerMaxRate)
   {
      checkWrite();
      this.producerMaxRate = producerMaxRate;
   }

   public synchronized boolean isBlockOnAcknowledge()
   {
      return blockOnAcknowledge;
   }

   public synchronized void setBlockOnAcknowledge(boolean blockOnAcknowledge)
   {
      checkWrite();
      this.blockOnAcknowledge = blockOnAcknowledge;
   }

   public synchronized boolean isBlockOnPersistentSend()
   {
      return blockOnPersistentSend;
   }

   public synchronized void setBlockOnPersistentSend(boolean blockOnPersistentSend)
   {
      checkWrite();
      this.blockOnPersistentSend = blockOnPersistentSend;
   }

   public synchronized boolean isBlockOnNonPersistentSend()
   {
      return blockOnNonPersistentSend;
   }

   public synchronized void setBlockOnNonPersistentSend(boolean blockOnNonPersistentSend)
   {
      checkWrite();
      this.blockOnNonPersistentSend = blockOnNonPersistentSend;
   }

   public synchronized boolean isAutoGroup()
   {
      return autoGroup;
   }

   public synchronized void setAutoGroup(boolean autoGroup)
   {
      checkWrite();
      this.autoGroup = autoGroup;
   }

   public synchronized boolean isPreAcknowledge()
   {
      return preAcknowledge;
   }

   public synchronized void setPreAcknowledge(boolean preAcknowledge)
   {
      checkWrite();
      this.preAcknowledge = preAcknowledge;
   }

   public synchronized int getAckBatchSize()
   {
      return ackBatchSize;
   }

   public synchronized void setAckBatchSize(int ackBatchSize)
   {
      checkWrite();
      this.ackBatchSize = ackBatchSize;
   }

   public synchronized long getDiscoveryInitialWaitTimeout()
   {
      return discoveryInitialWaitTimeout;
   }

   public synchronized void setDiscoveryInitialWaitTimeout(long initialWaitTimeout)
   {
      checkWrite();
      this.discoveryInitialWaitTimeout = initialWaitTimeout;
   }

   public synchronized boolean isUseGlobalPools()
   {
      return useGlobalPools;
   }

   public synchronized void setUseGlobalPools(boolean useGlobalPools)
   {
      checkWrite();
      this.useGlobalPools = useGlobalPools;
   }

   public synchronized int getScheduledThreadPoolMaxSize()
   {
      return scheduledThreadPoolMaxSize;
   }

   public synchronized void setScheduledThreadPoolMaxSize(int scheduledThreadPoolMaxSize)
   {
      checkWrite();
      this.scheduledThreadPoolMaxSize = scheduledThreadPoolMaxSize;
   }

   public synchronized int getThreadPoolMaxSize()
   {
      return threadPoolMaxSize;
   }

   public synchronized void setThreadPoolMaxSize(int threadPoolMaxSize)
   {
      checkWrite();
      this.threadPoolMaxSize = threadPoolMaxSize;
   }

   public synchronized long getRetryInterval()
   {
      return retryInterval;
   }

   public synchronized void setRetryInterval(long retryInterval)
   {
      checkWrite();
      this.retryInterval = retryInterval;
   }

   public synchronized long getMaxRetryInterval()
   {
      return maxRetryInterval;
   }

   public synchronized void setMaxRetryInterval(long retryInterval)
   {
      checkWrite();
      this.maxRetryInterval = retryInterval;
   }

   public synchronized double getRetryIntervalMultiplier()
   {
      return retryIntervalMultiplier;
   }

   public synchronized void setRetryIntervalMultiplier(double retryIntervalMultiplier)
   {
      checkWrite();
      this.retryIntervalMultiplier = retryIntervalMultiplier;
   }

   public synchronized int getReconnectAttempts()
   {
      return reconnectAttempts;
   }

   public synchronized void setReconnectAttempts(int reconnectAttempts)
   {
      checkWrite();
      this.reconnectAttempts = reconnectAttempts;
   }

   public synchronized boolean isFailoverOnServerShutdown()
   {
      return failoverOnServerShutdown;
   }

   public synchronized void setFailoverOnServerShutdown(boolean failoverOnServerShutdown)
   {
      checkWrite();
      this.failoverOnServerShutdown = failoverOnServerShutdown;
   }

   public synchronized String getConnectionLoadBalancingPolicyClassName()
   {
      return connectionLoadBalancingPolicyClassName;
   }

   public synchronized void setConnectionLoadBalancingPolicyClassName(String loadBalancingPolicyClassName)
   {
      checkWrite();
      this.connectionLoadBalancingPolicyClassName = loadBalancingPolicyClassName;
   }

   public synchronized String getDiscoveryAddress()
   {
      return discoveryAddress;
   }

   public synchronized void setDiscoveryAddress(String discoveryAddress)
   {
      checkWrite();
      this.discoveryAddress = discoveryAddress;
   }

   public synchronized int getDiscoveryPort()
   {
      return discoveryPort;
   }

   public synchronized void setDiscoveryPort(int discoveryPort)
   {
      checkWrite();
      this.discoveryPort = discoveryPort;
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

   public synchronized void setDiscoveryRefreshTimeout(long discoveryRefreshTimeout)
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
      this.initialMessagePacketSize = size;
   }

   public ClientSession createSession(final String username,
                                      final String password,
                                      final boolean xa,
                                      final boolean autoCommitSends,
                                      final boolean autoCommitAcks,
                                      final boolean preAcknowledge,
                                      final int ackBatchSize) throws HornetQException
   {
      return createSessionInternal(username,
                                   password,
                                   xa,
                                   autoCommitSends,
                                   autoCommitAcks,
                                   preAcknowledge,
                                   ackBatchSize);
   }

   public ClientSession createSession(boolean autoCommitSends, boolean autoCommitAcks, int ackBatchSize) throws HornetQException
   {
      return createSessionInternal(null, null, false, autoCommitSends, autoCommitAcks, preAcknowledge, ackBatchSize);
   }

   public ClientSession createXASession() throws HornetQException
   {
      return createSessionInternal(null, null, true, false, false, preAcknowledge, this.ackBatchSize);
   }

   public ClientSession createTransactedSession() throws HornetQException
   {
      return createSessionInternal(null, null, false, false, false, preAcknowledge, this.ackBatchSize);
   }

   public ClientSession createSession() throws HornetQException
   {
      return createSessionInternal(null, null, false, true, true, preAcknowledge, this.ackBatchSize);
   }

   public ClientSession createSession(final boolean autoCommitSends, final boolean autoCommitAcks) throws HornetQException
   {
      return createSessionInternal(null,
                                   null,
                                   false,
                                   autoCommitSends,
                                   autoCommitAcks,
                                   preAcknowledge,
                                   this.ackBatchSize);
   }

   public ClientSession createSession(final boolean xa, final boolean autoCommitSends, final boolean autoCommitAcks) throws HornetQException
   {
      return createSessionInternal(null, null, xa, autoCommitSends, autoCommitAcks, preAcknowledge, this.ackBatchSize);
   }

   public ClientSession createSession(final boolean xa,
                                      final boolean autoCommitSends,
                                      final boolean autoCommitAcks,
                                      final boolean preAcknowledge) throws HornetQException
   {
      return createSessionInternal(null, null, xa, autoCommitSends, autoCommitAcks, preAcknowledge, this.ackBatchSize);
   }

   public int numSessions()
   {
      int num = 0;

      for (FailoverManager failoverManager : failoverManagerMap.values())
      {
         num += failoverManager.numSessions();
      }

      return num;
   }

   public int numConnections()
   {
      int num = 0;

      for (FailoverManager failoverManager : failoverManagerMap.values())
      {
         num += failoverManager.numConnections();
      }

      return num;
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

      for (FailoverManager failoverManager : failoverManagerMap.values())
      {
         failoverManager.causeExit();
      }

      failoverManagerMap.clear();

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
   
   public ClientSessionFactory copy()
   {
      return new ClientSessionFactoryImpl(this);
   }
   
   // DiscoveryListener implementation --------------------------------------------------------

   public synchronized void connectorsChanged()
   {
      receivedBroadcast = true;

      Map<String, DiscoveryEntry> newConnectors = discoveryGroup.getDiscoveryEntryMap();

      Set<Pair<TransportConfiguration, TransportConfiguration>> connectorSet = new HashSet<Pair<TransportConfiguration, TransportConfiguration>>();

      for (DiscoveryEntry entry : newConnectors.values())
      {
         connectorSet.add(entry.getConnectorPair());
      }

      Iterator<Map.Entry<Pair<TransportConfiguration, TransportConfiguration>, FailoverManager>> iter = failoverManagerMap.entrySet()
                                                                                                                          .iterator();
      while (iter.hasNext())
      {
         Map.Entry<Pair<TransportConfiguration, TransportConfiguration>, FailoverManager> entry = iter.next();

         if (!connectorSet.contains(entry.getKey()))
         {
            // failoverManager no longer there - we should remove it

            iter.remove();
         }
      }

      for (Pair<TransportConfiguration, TransportConfiguration> connectorPair : connectorSet)
      {
         if (!failoverManagerMap.containsKey(connectorPair))
         {
            // Create a new failoverManager

            FailoverManager failoverManager = new FailoverManagerImpl(this,
                                                                      connectorPair.a,
                                                                      connectorPair.b,
                                                                      failoverOnServerShutdown,
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

            failoverManagerMap.put(connectorPair, failoverManager);
         }
      }

      updatefailoverManagerArray();
   }

   public FailoverManager[] getFailoverManagers()
   {
      return failoverManagerArray;
   }

   // Protected ------------------------------------------------------------------------------

   @Override
   protected void finalize() throws Throwable
   {
      close();

      super.finalize();
   }

   // Private --------------------------------------------------------------------------------

   private void checkWrite()
   {
      if (readOnly)
      {
         throw new IllegalStateException("Cannot set attribute on SessionFactory after it has been used");
      }
   }

   private ClientSession createSessionInternal(final String username,
                                               final String password,
                                               final boolean xa,
                                               final boolean autoCommitSends,
                                               final boolean autoCommitAcks,
                                               final boolean preAcknowledge,
                                               final int ackBatchSize) throws HornetQException
   {
      if (closed)
      {
         throw new IllegalStateException("Cannot create session, factory is closed (maybe it has been garbage collected)");
      }

      if (!readOnly)
      {
         try
         {
            initialise();
         }
         catch (Exception e)
         {
            throw new HornetQException(HornetQException.INTERNAL_ERROR, "Failed to initialise session factory", e);
         }

         readOnly = true;
      }

      if (discoveryGroup != null && !receivedBroadcast)
      {
         boolean ok = discoveryGroup.waitForBroadcast(discoveryInitialWaitTimeout);

         if (!ok)
         {
            throw new HornetQException(HornetQException.CONNECTION_TIMEDOUT,
                                       "Timed out waiting to receive initial broadcast from discovery group");
         }
      }

      synchronized (this)
      {
         int pos = loadBalancingPolicy.select(failoverManagerArray.length);

         FailoverManager failoverManager = failoverManagerArray[pos];

         ClientSession session = failoverManager.createSession(username,
                                                               password,
                                                               xa,
                                                               autoCommitSends,
                                                               autoCommitAcks,
                                                               preAcknowledge,
                                                               ackBatchSize,
                                                               cacheLargeMessagesClient,
                                                               minLargeMessageSize,
                                                               blockOnAcknowledge,
                                                               autoGroup,
                                                               confirmationWindowSize,
                                                               producerWindowSize,
                                                               consumerWindowSize,
                                                               producerMaxRate,
                                                               consumerMaxRate,
                                                               blockOnNonPersistentSend,
                                                               blockOnPersistentSend,
                                                               initialMessagePacketSize);

         return session;
      }
   }

   private void instantiateLoadBalancingPolicy()
   {
      if (connectionLoadBalancingPolicyClassName == null)
      {
         throw new IllegalStateException("Please specify a load balancing policy class name on the session factory");
      }

      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      try
      {
         Class<?> clazz = loader.loadClass(connectionLoadBalancingPolicyClassName);
         loadBalancingPolicy = (ConnectionLoadBalancingPolicy)clazz.newInstance();
      }
      catch (Exception e)
      {
         throw new IllegalArgumentException("Unable to instantiate load balancing policy \"" + connectionLoadBalancingPolicyClassName +
                                                     "\"",
                                            e);
      }
   }

   private synchronized void updatefailoverManagerArray()
   {
      failoverManagerArray = new FailoverManager[failoverManagerMap.size()];

      failoverManagerMap.values().toArray(failoverManagerArray);
   }

   

}
