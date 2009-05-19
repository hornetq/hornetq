/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors by
 * the @authors tag. See the copyright.txt in the distribution for a full listing of individual contributors. This is
 * free software; you can redistribute it and/or modify it under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of the License, or (at your option) any later version.
 * This software is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without even the implied
 * warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details. You should have received a copy of the GNU Lesser General Public License along with this software; if not,
 * write to the Free Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */
package org.jboss.messaging.core.client.impl;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ConnectionLoadBalancingPolicy;
import org.jboss.messaging.core.cluster.DiscoveryEntry;
import org.jboss.messaging.core.cluster.DiscoveryGroup;
import org.jboss.messaging.core.cluster.DiscoveryListener;
import org.jboss.messaging.core.cluster.impl.DiscoveryGroupImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.utils.JBMThreadFactory;
import org.jboss.messaging.utils.Pair;
import org.jboss.messaging.utils.UUIDGenerator;

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

   public static final String DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME = "org.jboss.messaging.core.client.impl.RoundRobinConnectionLoadBalancingPolicy";

   public static final long DEFAULT_PING_PERIOD = 100000;

   // 5 minutes - normally this should be much higher than ping period, this allows clients to re-attach on live
   // or backup without fear of session having already been closed when connection times out.
   public static final long DEFAULT_CONNECTION_TTL = 5 * 60000;

   // Any message beyond this size is considered a large message (to be sent in chunks)
   public static final int DEFAULT_MIN_LARGE_MESSAGE_SIZE = 100 * 1024;

   public static final int DEFAULT_CONSUMER_WINDOW_SIZE = 1024 * 1024;

   public static final int DEFAULT_CONSUMER_MAX_RATE = -1;

   public static final int DEFAULT_PRODUCER_WINDOW_SIZE = 1024 * 1024;

   public static final int DEFAULT_PRODUCER_MAX_RATE = -1;

   public static final boolean DEFAULT_BLOCK_ON_ACKNOWLEDGE = false;

   public static final boolean DEFAULT_BLOCK_ON_PERSISTENT_SEND = false;

   public static final boolean DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND = false;

   public static final boolean DEFAULT_AUTO_GROUP = false;

   public static final long DEFAULT_CALL_TIMEOUT = 30000;

   public static final int DEFAULT_MAX_CONNECTIONS = 8;

   public static final int DEFAULT_ACK_BATCH_SIZE = 1024 * 1024;

   public static final boolean DEFAULT_PRE_ACKNOWLEDGE = false;

   public static final long DEFAULT_DISCOVERY_INITIAL_WAIT = 2000;

   public static final long DEFAULT_DISCOVERY_REFRESH_TIMEOUT = 10000;

   public static final long DEFAULT_RETRY_INTERVAL = 2000;

   public static final double DEFAULT_RETRY_INTERVAL_MULTIPLIER = 1d;

   public static final int DEFAULT_RECONNECT_ATTEMPTS = 0;

   public static final boolean DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN = false;

   public static final boolean DEFAULT_USE_GLOBAL_POOLS = true;

   public static final int DEFAULT_THREAD_POOL_MAX_SIZE = -1;

   public static final int DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE = 2;

   // Attributes
   // -----------------------------------------------------------------------------------

   private final Map<Pair<TransportConfiguration, TransportConfiguration>, ConnectionManager> connectionManagerMap = new LinkedHashMap<Pair<TransportConfiguration, TransportConfiguration>, ConnectionManager>();

   private volatile boolean receivedBroadcast = false;

   private ExecutorService threadPool;

   private ScheduledExecutorService scheduledThreadPool;

   private DiscoveryGroup discoveryGroup;

   private ConnectionLoadBalancingPolicy loadBalancingPolicy;

   private ConnectionManager[] connectionManagerArray;

   private boolean readOnly;

   // Settable attributes:

   private List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors;

   private String discoveryAddress;

   private int discoveryPort;

   private long discoveryRefreshTimeout;

   private long pingPeriod;

   private long connectionTTL;

   private long callTimeout;

   private int maxConnections;

   private int minLargeMessageSize;

   private int consumerWindowSize;

   private int consumerMaxRate;

   private int producerWindowSize;

   private int producerMaxRate;

   private boolean blockOnAcknowledge;

   private boolean blockOnPersistentSend;

   private boolean blockOnNonPersistentSend;

   private boolean autoGroup;

   private boolean preAcknowledge;

   private String loadBalancingPolicyClassName;

   private int ackBatchSize;

   private long initialWaitTimeout;

   private boolean useGlobalPools;

   private int scheduledThreadPoolMaxSize;

   private int threadPoolMaxSize;

   private long retryInterval;

   private double retryIntervalMultiplier;

   private int reconnectAttempts;

   private boolean failoverOnServerShutdown;

   private static ExecutorService globalThreadPool;

   private static ScheduledExecutorService globalScheduledThreadPool;

   private static synchronized ExecutorService getGlobalThreadPool()
   {
      if (globalThreadPool == null)
      {
         ThreadFactory factory = new JBMThreadFactory("JBM-client-global-threads", true);

         globalThreadPool = Executors.newCachedThreadPool(factory);
      }

      return globalThreadPool;
   }

   private static synchronized ScheduledExecutorService getGlobalScheduledThreadPool()
   {
      if (globalScheduledThreadPool == null)
      {
         ThreadFactory factory = new JBMThreadFactory("JBM-client-global-scheduled-threads", true);

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
         ThreadFactory factory = new JBMThreadFactory("JBM-client-factory-threads-" + System.identityHashCode(this),
                                                      true);

         if (threadPoolMaxSize == -1)
         {
            threadPool = Executors.newCachedThreadPool(factory);
         }
         else
         {
            threadPool = Executors.newFixedThreadPool(threadPoolMaxSize, factory);
         }

         factory = new JBMThreadFactory("JBM-client-factory-pinger-threads-" + System.identityHashCode(this), true);

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
            ConnectionManager cm = new ConnectionManagerImpl(pair.a,
                                                             pair.b,
                                                             failoverOnServerShutdown,
                                                             maxConnections,
                                                             callTimeout,
                                                             pingPeriod,
                                                             connectionTTL,
                                                             retryInterval,
                                                             retryIntervalMultiplier,
                                                             reconnectAttempts,
                                                             threadPool,
                                                             scheduledThreadPool);

            connectionManagerMap.put(pair, cm);
         }

         updateConnectionManagerArray();
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

   public ClientSessionFactoryImpl()
   {
      discoveryRefreshTimeout = DEFAULT_DISCOVERY_REFRESH_TIMEOUT;

      pingPeriod = DEFAULT_PING_PERIOD;

      connectionTTL = DEFAULT_CONNECTION_TTL;

      callTimeout = DEFAULT_CALL_TIMEOUT;

      maxConnections = DEFAULT_MAX_CONNECTIONS;

      minLargeMessageSize = DEFAULT_MIN_LARGE_MESSAGE_SIZE;

      consumerWindowSize = DEFAULT_CONSUMER_WINDOW_SIZE;

      consumerMaxRate = DEFAULT_CONSUMER_MAX_RATE;

      producerWindowSize = DEFAULT_PRODUCER_WINDOW_SIZE;

      producerMaxRate = DEFAULT_PRODUCER_MAX_RATE;

      blockOnAcknowledge = DEFAULT_BLOCK_ON_ACKNOWLEDGE;

      blockOnPersistentSend = DEFAULT_BLOCK_ON_PERSISTENT_SEND;

      blockOnNonPersistentSend = DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;

      autoGroup = DEFAULT_AUTO_GROUP;

      preAcknowledge = DEFAULT_PRE_ACKNOWLEDGE;

      ackBatchSize = DEFAULT_ACK_BATCH_SIZE;

      loadBalancingPolicyClassName = DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;

      initialWaitTimeout = DEFAULT_DISCOVERY_INITIAL_WAIT;

      useGlobalPools = DEFAULT_USE_GLOBAL_POOLS;

      scheduledThreadPoolMaxSize = DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;

      threadPoolMaxSize = DEFAULT_THREAD_POOL_MAX_SIZE;

      retryInterval = DEFAULT_RETRY_INTERVAL;

      retryIntervalMultiplier = DEFAULT_RETRY_INTERVAL_MULTIPLIER;

      reconnectAttempts = DEFAULT_RECONNECT_ATTEMPTS;

      failoverOnServerShutdown = DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN;
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

   public synchronized List<Pair<TransportConfiguration, TransportConfiguration>> getStaticConnectors()
   {
      return staticConnectors;
   }

   public synchronized void setStaticConnectors(List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors)
   {
      checkWrite();

      this.staticConnectors = staticConnectors;
   }

   public synchronized long getPingPeriod()
   {
      return pingPeriod;
   }

   public synchronized void setPingPeriod(long pingPeriod)
   {
      checkWrite();
      this.pingPeriod = pingPeriod;
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

   public synchronized int getMaxConnections()
   {
      return maxConnections;
   }

   public synchronized void setMaxConnections(int maxConnections)
   {
      checkWrite();
      this.maxConnections = maxConnections;
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

   public synchronized int getProducerWindowSize()
   {
      return producerWindowSize;
   }

   public synchronized void setProducerWindowSize(int producerWindowSize)
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

   public synchronized long getInitialWaitTimeout()
   {
      return initialWaitTimeout;
   }

   public synchronized void setInitialWaitTimeout(long initialWaitTimeout)
   {
      checkWrite();
      this.initialWaitTimeout = initialWaitTimeout;
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

   public synchronized String getLoadBalancingPolicyClassName()
   {
      return loadBalancingPolicyClassName;
   }

   public synchronized void setLoadBalancingPolicyClassName(String loadBalancingPolicyClassName)
   {
      checkWrite();
      this.loadBalancingPolicyClassName = loadBalancingPolicyClassName;
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

   public synchronized void setDiscoveryRefreshTimeout(long discoveryRefreshTimeout)
   {
      checkWrite();
      this.discoveryRefreshTimeout = discoveryRefreshTimeout;
   }

   public ClientSession createSession(final String username,
                                      final String password,
                                      final boolean xa,
                                      final boolean autoCommitSends,
                                      final boolean autoCommitAcks,
                                      final boolean preAcknowledge,
                                      final int ackBatchSize) throws MessagingException
   {
      return createSessionInternal(username,
                                   password,
                                   xa,
                                   autoCommitSends,
                                   autoCommitAcks,
                                   preAcknowledge,
                                   ackBatchSize);
   }
   
   public ClientSession createXASession() throws MessagingException
   {
      return createSessionInternal(null, null, true, false, false, preAcknowledge, this.ackBatchSize);
   }
   
   public ClientSession createTransactedSession() throws MessagingException
   {
      return createSessionInternal(null, null, false, false, false, preAcknowledge, this.ackBatchSize);
   }
   
   public ClientSession createSession() throws MessagingException
   {
      return createSessionInternal(null, null, false, true, true, preAcknowledge, this.ackBatchSize);
   }
   
   public ClientSession createSession(final boolean autoCommitSends, final boolean autoCommitAcks) throws MessagingException
   {
      return createSessionInternal(null, null, false, autoCommitSends, autoCommitAcks, preAcknowledge, this.ackBatchSize);
   }

   public ClientSession createSession(final boolean xa, final boolean autoCommitSends, final boolean autoCommitAcks) throws MessagingException
   {
      return createSessionInternal(null, null, xa, autoCommitSends, autoCommitAcks, preAcknowledge, this.ackBatchSize);
   }

   public ClientSession createSession(final boolean xa,
                                      final boolean autoCommitSends,
                                      final boolean autoCommitAcks,
                                      final boolean preAcknowledge) throws MessagingException
   {
      return createSessionInternal(null, null, xa, autoCommitSends, autoCommitAcks, preAcknowledge, this.ackBatchSize);
   }

   public int numSessions()
   {
      int num = 0;

      for (ConnectionManager connectionManager : connectionManagerMap.values())
      {
         num += connectionManager.numSessions();
      }

      return num;
   }

   public int numConnections()
   {
      int num = 0;

      for (ConnectionManager connectionManager : connectionManagerMap.values())
      {
         num += connectionManager.numConnections();
      }

      return num;
   }

   public void close()
   {
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

      for (ConnectionManager connectionManager : connectionManagerMap.values())
      {
         connectionManager.close();
      }

      connectionManagerMap.clear();

      if (!useGlobalPools)
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

      Iterator<Map.Entry<Pair<TransportConfiguration, TransportConfiguration>, ConnectionManager>> iter = connectionManagerMap.entrySet()
                                                                                                                              .iterator();
      while (iter.hasNext())
      {
         Map.Entry<Pair<TransportConfiguration, TransportConfiguration>, ConnectionManager> entry = iter.next();

         if (!connectorSet.contains(entry.getKey()))
         {
            // ConnectionManager no longer there - we should remove it

            iter.remove();
         }
      }

      for (Pair<TransportConfiguration, TransportConfiguration> connectorPair : connectorSet)
      {
         if (!connectionManagerMap.containsKey(connectorPair))
         {
            // Create a new ConnectionManager

            ConnectionManager connectionManager = new ConnectionManagerImpl(connectorPair.a,
                                                                            connectorPair.b,
                                                                            failoverOnServerShutdown,
                                                                            maxConnections,
                                                                            callTimeout,
                                                                            pingPeriod,
                                                                            connectionTTL,
                                                                            retryInterval,
                                                                            retryIntervalMultiplier,
                                                                            reconnectAttempts,
                                                                            threadPool,
                                                                            scheduledThreadPool);

            connectionManagerMap.put(connectorPair, connectionManager);
         }
      }

      updateConnectionManagerArray();
   }

   // Protected ------------------------------------------------------------------------------

   protected void finalize() throws Throwable
   {
      if (discoveryGroup != null)
      {
         discoveryGroup.stop();
      }
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
                                               final int ackBatchSize) throws MessagingException
   {
      if (!readOnly)
      {
         try
         {
            initialise();
         }
         catch (Exception e)
         {
            throw new MessagingException(MessagingException.INTERNAL_ERROR, "Failed to initialise session factory", e);
         }

         readOnly = true;
      }

      if (discoveryGroup != null && !receivedBroadcast)
      {
         boolean ok = discoveryGroup.waitForBroadcast(initialWaitTimeout);

         if (!ok)
         {
            throw new MessagingException(MessagingException.CONNECTION_TIMEDOUT,
                                         "Timed out waiting to receive initial broadcast from discovery group");
         }
      }

      synchronized (this)
      {
         int pos = loadBalancingPolicy.select(connectionManagerArray.length);

         ConnectionManager connectionManager = connectionManagerArray[pos];

         return connectionManager.createSession(username,
                                                password,
                                                xa,
                                                autoCommitSends,
                                                autoCommitAcks,
                                                preAcknowledge,
                                                ackBatchSize,
                                                minLargeMessageSize,
                                                blockOnAcknowledge,
                                                autoGroup,
                                                producerWindowSize,
                                                consumerWindowSize,
                                                producerMaxRate,
                                                consumerMaxRate,
                                                blockOnNonPersistentSend,
                                                blockOnPersistentSend);
      }
   }

   private void instantiateLoadBalancingPolicy()
   {
      if (loadBalancingPolicyClassName == null)
      {
         throw new IllegalStateException("Please specify a load balancing policy class name on the session factory");
      }

      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      try
      {
         Class<?> clazz = loader.loadClass(loadBalancingPolicyClassName);
         loadBalancingPolicy = (ConnectionLoadBalancingPolicy)clazz.newInstance();
      }
      catch (Exception e)
      {
         throw new IllegalArgumentException("Unable to instantiate load balancing policy \"" + loadBalancingPolicyClassName +
                                                     "\"",
                                            e);
      }
   }

   private synchronized void updateConnectionManagerArray()
   {
      connectionManagerArray = new ConnectionManager[connectionManagerMap.size()];

      connectionManagerMap.values().toArray(connectionManagerArray);
   }
}
