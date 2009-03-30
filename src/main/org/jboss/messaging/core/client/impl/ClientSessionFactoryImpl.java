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

import java.net.InetAddress;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ConnectionLoadBalancingPolicy;
import org.jboss.messaging.core.cluster.DiscoveryGroup;
import org.jboss.messaging.core.cluster.DiscoveryListener;
import org.jboss.messaging.core.cluster.impl.DiscoveryGroupImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
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
public class ClientSessionFactoryImpl implements ClientSessionFactoryInternal, DiscoveryListener
{
   // Constants
   // ------------------------------------------------------------------------------------

   private static final long serialVersionUID = 2512460695662741413L;

   private static final Logger log = Logger.getLogger(ClientSessionFactoryImpl.class);

   public static final String DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME = "org.jboss.messaging.core.client.impl.RoundRobinConnectionLoadBalancingPolicy";

   public static final long DEFAULT_PING_PERIOD = 5000;

   // 5 minutes - normally this should be much higher than ping period, this allows clients to re-attach on live
   // or backup without fear of session having already been closed when connection times out.
   public static final long DEFAULT_CONNECTION_TTL = 5 * 60000;

   // Any message beyond this size is considered a large message (to be sent in chunks)
   public static final int DEFAULT_MIN_LARGE_MESSAGE_SIZE = 100 * 1024;

   public static final int DEFAULT_CONSUMER_WINDOW_SIZE = 1024 * 1024;

   public static final int DEFAULT_CONSUMER_MAX_RATE = -1;

   public static final int DEFAULT_SEND_WINDOW_SIZE = 1024 * 1024;

   public static final int DEFAULT_PRODUCER_MAX_RATE = -1;

   public static final boolean DEFAULT_BLOCK_ON_ACKNOWLEDGE = false;

   public static final boolean DEFAULT_BLOCK_ON_PERSISTENT_SEND = false;

   public static final boolean DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND = false;

   public static final boolean DEFAULT_AUTO_GROUP = false;

   public static final long DEFAULT_CALL_TIMEOUT = 30000;

   public static final int DEFAULT_MAX_CONNECTIONS = 8;

   public static final int DEFAULT_ACK_BATCH_SIZE = 1024 * 1024;

   public static final boolean DEFAULT_PRE_ACKNOWLEDGE = false;

   public static final long DEFAULT_DISCOVERY_INITIAL_WAIT = 10000;

   public static final long DEFAULT_RETRY_INTERVAL = 2000;

   public static final double DEFAULT_RETRY_INTERVAL_MULTIPLIER = 1d;

   public static final int DEFAULT_RECONNECT_ATTEMPTS = 0;
   
   public static final boolean DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN = false;


   // Attributes
   // -----------------------------------------------------------------------------------

   private final Map<Pair<TransportConfiguration, TransportConfiguration>, ConnectionManager> connectionManagerMap = new LinkedHashMap<Pair<TransportConfiguration, TransportConfiguration>, ConnectionManager>();

   private ConnectionManager[] connectionManagerArray;

   private final long pingPeriod;

   private final long connectionTTL;

   private final long callTimeout;

   private final int maxConnections;

   // Some of these attributes are mutable and can be updated by different threads so
   // must be volatile

   private volatile int minLargeMessageSize;

   private volatile int consumerWindowSize;

   private volatile int consumerMaxRate;

   private volatile int sendWindowSize;

   private volatile int producerMaxRate;

   private volatile boolean blockOnAcknowledge;

   private volatile boolean blockOnPersistentSend;

   private volatile boolean blockOnNonPersistentSend;

   private volatile boolean autoGroup;

   private boolean preAcknowledge;

   private volatile int ackBatchSize;

   private final ConnectionLoadBalancingPolicy loadBalancingPolicy;

   private final DiscoveryGroup discoveryGroup;

   private volatile boolean receivedBroadcast = false;

   private final long initialWaitTimeout;

   // Reconnect params

   private final long retryInterval;

   private final double retryIntervalMultiplier; // For exponential backoff

   private final int reconnectAttempts;
   
   private final boolean failoverOnServerShutdown;

   // Static
   // ---------------------------------------------------------------------------------------

   // Constructors
   // ---------------------------------------------------------------------------------

   public ClientSessionFactoryImpl(final String discoveryGroupAddress, final int discoveryGroupPort) throws Exception
   {
      this(discoveryGroupAddress,
           discoveryGroupPort,
           ConfigurationImpl.DEFAULT_BROADCAST_REFRESH_TIMEOUT,
           DEFAULT_DISCOVERY_INITIAL_WAIT);
   }

   public ClientSessionFactoryImpl(final String discoveryGroupAddress,
                                   final int discoveryGroupPort,
                                   final long discoveryRefreshTimeout,
                                   final long initialWaitTimeout) throws Exception
   {
      InetAddress groupAddress = InetAddress.getByName(discoveryGroupAddress);

      discoveryGroup = new DiscoveryGroupImpl(UUIDGenerator.getInstance().generateStringUUID(),
                                              discoveryGroupAddress,
                                              groupAddress,
                                              discoveryGroupPort,
                                              discoveryRefreshTimeout);

      discoveryGroup.registerListener(this);

      discoveryGroup.start();

      this.initialWaitTimeout = initialWaitTimeout;
      this.loadBalancingPolicy = instantiateLoadBalancingPolicy(DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME);
      this.pingPeriod = DEFAULT_PING_PERIOD;
      this.connectionTTL = DEFAULT_CONNECTION_TTL;
      this.callTimeout = DEFAULT_CALL_TIMEOUT;
      this.consumerWindowSize = DEFAULT_CONSUMER_WINDOW_SIZE;
      this.consumerMaxRate = DEFAULT_CONSUMER_MAX_RATE;
      this.sendWindowSize = DEFAULT_SEND_WINDOW_SIZE;
      this.producerMaxRate = DEFAULT_PRODUCER_MAX_RATE;
      this.blockOnAcknowledge = DEFAULT_BLOCK_ON_ACKNOWLEDGE;
      this.blockOnNonPersistentSend = DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;
      this.blockOnPersistentSend = DEFAULT_BLOCK_ON_PERSISTENT_SEND;
      this.minLargeMessageSize = DEFAULT_MIN_LARGE_MESSAGE_SIZE;
      this.autoGroup = DEFAULT_AUTO_GROUP;
      this.maxConnections = DEFAULT_MAX_CONNECTIONS;
      this.ackBatchSize = DEFAULT_ACK_BATCH_SIZE;
      this.preAcknowledge = DEFAULT_PRE_ACKNOWLEDGE;
      this.retryInterval = DEFAULT_RETRY_INTERVAL;
      this.retryIntervalMultiplier = DEFAULT_RETRY_INTERVAL_MULTIPLIER;
      this.reconnectAttempts = DEFAULT_RECONNECT_ATTEMPTS;
      this.failoverOnServerShutdown = DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN;
   }

   public ClientSessionFactoryImpl(final String discoveryGroupAddress,
                                   final int discoveryGroupPort,
                                   final long discoveryRefreshTimeout,
                                   final long initialWaitTimeout,
                                   final String connectionloadBalancingPolicyClassName,
                                   final long pingPeriod,
                                   final long connectionTTL,
                                   final long callTimeout,
                                   final int consumerWindowSize,
                                   final int consumerMaxRate,
                                   final int sendWindowSize,
                                   final int producerMaxRate,
                                   final int minLargeMessageSize,
                                   final boolean blockOnAcknowledge,
                                   final boolean blockOnNonPersistentSend,
                                   final boolean blockOnPersistentSend,
                                   final boolean autoGroup,
                                   final int maxConnections,
                                   final boolean preAcknowledge,
                                   final int ackBatchSize,
                                   final long retryInterval,
                                   final double retryIntervalMultiplier,
                                   final int reconnectAttempts,
                                   final boolean failoverOnServerShutdown) throws MessagingException
   {
      try
      {
         InetAddress groupAddress = InetAddress.getByName(discoveryGroupAddress);

         discoveryGroup = new DiscoveryGroupImpl(UUIDGenerator.getInstance().generateStringUUID(),
                                                 discoveryGroupAddress,
                                                 groupAddress,
                                                 discoveryGroupPort,
                                                 discoveryRefreshTimeout);

         discoveryGroup.registerListener(this);

         discoveryGroup.start();
      }
      catch (Exception e)
      {
         // TODO - handle exceptions better
         throw new MessagingException(MessagingException.NOT_CONNECTED, e.toString());
      }

      this.initialWaitTimeout = initialWaitTimeout;
      this.loadBalancingPolicy = instantiateLoadBalancingPolicy(connectionloadBalancingPolicyClassName);
      this.pingPeriod = pingPeriod;
      this.connectionTTL = connectionTTL;
      this.callTimeout = callTimeout;
      this.consumerWindowSize = consumerWindowSize;
      this.consumerMaxRate = consumerMaxRate;
      this.sendWindowSize = sendWindowSize;
      this.producerMaxRate = producerMaxRate;
      this.blockOnAcknowledge = blockOnAcknowledge;
      this.blockOnNonPersistentSend = blockOnNonPersistentSend;
      this.blockOnPersistentSend = blockOnPersistentSend;
      this.minLargeMessageSize = minLargeMessageSize;
      this.autoGroup = autoGroup;
      this.maxConnections = maxConnections;
      this.ackBatchSize = ackBatchSize;
      this.preAcknowledge = preAcknowledge;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.reconnectAttempts = reconnectAttempts;
      this.failoverOnServerShutdown = failoverOnServerShutdown;
   }

   public ClientSessionFactoryImpl(final List<Pair<TransportConfiguration, TransportConfiguration>> connectors,
                                   final String connectionloadBalancingPolicyClassName,
                                   final long pingPeriod,
                                   final long connectionTTL,
                                   final long callTimeout,
                                   final int consumerWindowSize,
                                   final int consumerMaxRate,
                                   final int sendWindowSize,
                                   final int producerMaxRate,
                                   final int minLargeMessageSize,
                                   final boolean blockOnAcknowledge,
                                   final boolean blockOnNonPersistentSend,
                                   final boolean blockOnPersistentSend,
                                   final boolean autoGroup,
                                   final int maxConnections,
                                   final boolean preAcknowledge,
                                   final int ackBatchSize,
                                   final long retryInterval,
                                   final double retryIntervalMultiplier,
                                   final int reconnectAttempts,
                                   final boolean failoverOnServerShutdown)
   {
      this.loadBalancingPolicy = instantiateLoadBalancingPolicy(connectionloadBalancingPolicyClassName);
      this.pingPeriod = pingPeriod;
      this.connectionTTL = connectionTTL;
      this.callTimeout = callTimeout;
      this.consumerWindowSize = consumerWindowSize;
      this.consumerMaxRate = consumerMaxRate;
      this.sendWindowSize = sendWindowSize;
      this.producerMaxRate = producerMaxRate;
      this.blockOnAcknowledge = blockOnAcknowledge;
      this.blockOnNonPersistentSend = blockOnNonPersistentSend;
      this.blockOnPersistentSend = blockOnPersistentSend;
      this.minLargeMessageSize = minLargeMessageSize;
      this.autoGroup = autoGroup;
      this.maxConnections = maxConnections;
      this.ackBatchSize = ackBatchSize;
      this.preAcknowledge = preAcknowledge;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.reconnectAttempts = reconnectAttempts;
      this.failoverOnServerShutdown = failoverOnServerShutdown;

      this.initialWaitTimeout = -1;

      for (Pair<TransportConfiguration, TransportConfiguration> pair : connectors)
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
                                                          reconnectAttempts);

         connectionManagerMap.put(pair, cm);
      }

      updateConnectionManagerArray();

      this.discoveryGroup = null;
   }

   public ClientSessionFactoryImpl(final TransportConfiguration connectorConfig,
                                   final TransportConfiguration backupConnectorConfig,
                                   final boolean failoverOnServerShutdown,
                                   final long retryInterval,
                                   final double retryIntervalMultiplier,
                                   final int reconnectAttempts)
   {
      this.loadBalancingPolicy = new FirstElementConnectionLoadBalancingPolicy();
      this.pingPeriod = DEFAULT_PING_PERIOD;
      this.connectionTTL = DEFAULT_CONNECTION_TTL;
      this.callTimeout = DEFAULT_CALL_TIMEOUT;
      this.consumerWindowSize = DEFAULT_CONSUMER_WINDOW_SIZE;
      this.consumerMaxRate = DEFAULT_CONSUMER_MAX_RATE;
      this.sendWindowSize = DEFAULT_SEND_WINDOW_SIZE;
      this.producerMaxRate = DEFAULT_PRODUCER_MAX_RATE;
      this.blockOnAcknowledge = DEFAULT_BLOCK_ON_ACKNOWLEDGE;
      this.blockOnNonPersistentSend = DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;
      this.blockOnPersistentSend = DEFAULT_BLOCK_ON_PERSISTENT_SEND;
      this.minLargeMessageSize = DEFAULT_MIN_LARGE_MESSAGE_SIZE;
      this.autoGroup = DEFAULT_AUTO_GROUP;
      this.maxConnections = DEFAULT_MAX_CONNECTIONS;
      this.ackBatchSize = DEFAULT_ACK_BATCH_SIZE;
      this.preAcknowledge = DEFAULT_PRE_ACKNOWLEDGE;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.reconnectAttempts = reconnectAttempts;
      this.failoverOnServerShutdown = failoverOnServerShutdown;

      this.initialWaitTimeout = -1;

      Pair<TransportConfiguration, TransportConfiguration> pair = new Pair<TransportConfiguration, TransportConfiguration>(connectorConfig,
                                                                                                                           backupConnectorConfig);

      ConnectionManager cm = new ConnectionManagerImpl(pair.a,
                                                       pair.b,
                                                       failoverOnServerShutdown,
                                                       maxConnections,
                                                       callTimeout,
                                                       pingPeriod,
                                                       connectionTTL,
                                                       retryInterval,
                                                       retryIntervalMultiplier,
                                                       reconnectAttempts);

      connectionManagerMap.put(pair, cm);

      updateConnectionManagerArray();

      discoveryGroup = null;
   }

   public ClientSessionFactoryImpl(final TransportConfiguration connectorConfig,
                                   final long retryInterval,
                                   final double retryIntervalMultiplier,
                                   final int reconnectAttempts)
   {
      this.loadBalancingPolicy = new FirstElementConnectionLoadBalancingPolicy();
      this.pingPeriod = DEFAULT_PING_PERIOD;
      this.connectionTTL = DEFAULT_CONNECTION_TTL;
      this.callTimeout = DEFAULT_CALL_TIMEOUT;
      this.consumerWindowSize = DEFAULT_CONSUMER_WINDOW_SIZE;
      this.consumerMaxRate = DEFAULT_CONSUMER_MAX_RATE;
      this.sendWindowSize = DEFAULT_SEND_WINDOW_SIZE;
      this.producerMaxRate = DEFAULT_PRODUCER_MAX_RATE;
      this.blockOnAcknowledge = DEFAULT_BLOCK_ON_ACKNOWLEDGE;
      this.blockOnNonPersistentSend = DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;
      this.blockOnPersistentSend = DEFAULT_BLOCK_ON_PERSISTENT_SEND;
      this.minLargeMessageSize = DEFAULT_MIN_LARGE_MESSAGE_SIZE;
      this.autoGroup = DEFAULT_AUTO_GROUP;
      this.maxConnections = DEFAULT_MAX_CONNECTIONS;
      this.ackBatchSize = DEFAULT_ACK_BATCH_SIZE;
      this.preAcknowledge = DEFAULT_PRE_ACKNOWLEDGE;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.reconnectAttempts = reconnectAttempts;
      this.failoverOnServerShutdown = DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN;

      this.initialWaitTimeout = -1;

      Pair<TransportConfiguration, TransportConfiguration> pair = new Pair<TransportConfiguration, TransportConfiguration>(connectorConfig,
                                                                                                                           null);

      ConnectionManager cm = new ConnectionManagerImpl(pair.a,
                                                       pair.b,
                                                       failoverOnServerShutdown,
                                                       maxConnections,
                                                       callTimeout,
                                                       pingPeriod,
                                                       connectionTTL,
                                                       retryInterval,
                                                       retryIntervalMultiplier,
                                                       reconnectAttempts);

      connectionManagerMap.put(pair, cm);

      updateConnectionManagerArray();

      discoveryGroup = null;
   }

   public ClientSessionFactoryImpl(final TransportConfiguration connectorConfig,
                                   final TransportConfiguration backupConfig)
   {      
      this.loadBalancingPolicy = new FirstElementConnectionLoadBalancingPolicy();
      this.pingPeriod = DEFAULT_PING_PERIOD;
      this.callTimeout = DEFAULT_CALL_TIMEOUT;
      this.connectionTTL = DEFAULT_CONNECTION_TTL;
      this.consumerWindowSize = DEFAULT_CONSUMER_WINDOW_SIZE;
      this.consumerMaxRate = DEFAULT_CONSUMER_MAX_RATE;
      this.sendWindowSize = DEFAULT_SEND_WINDOW_SIZE;
      this.producerMaxRate = DEFAULT_PRODUCER_MAX_RATE;
      this.blockOnAcknowledge = DEFAULT_BLOCK_ON_ACKNOWLEDGE;
      this.blockOnNonPersistentSend = DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;
      this.blockOnPersistentSend = DEFAULT_BLOCK_ON_PERSISTENT_SEND;
      this.minLargeMessageSize = DEFAULT_MIN_LARGE_MESSAGE_SIZE;
      this.autoGroup = DEFAULT_AUTO_GROUP;
      this.maxConnections = DEFAULT_MAX_CONNECTIONS;
      this.ackBatchSize = DEFAULT_ACK_BATCH_SIZE;
      this.preAcknowledge = DEFAULT_PRE_ACKNOWLEDGE;
      this.retryInterval = DEFAULT_RETRY_INTERVAL;
      this.retryIntervalMultiplier = DEFAULT_RETRY_INTERVAL_MULTIPLIER;
      this.reconnectAttempts = DEFAULT_RECONNECT_ATTEMPTS;
      this.failoverOnServerShutdown = DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN;

      this.initialWaitTimeout = -1;

      Pair<TransportConfiguration, TransportConfiguration> pair = new Pair<TransportConfiguration, TransportConfiguration>(connectorConfig,
                                                                                                                           backupConfig);

      ConnectionManager cm = new ConnectionManagerImpl(pair.a,
                                                       pair.b,
                                                       failoverOnServerShutdown,
                                                       maxConnections,
                                                       callTimeout,
                                                       pingPeriod,
                                                       connectionTTL,
                                                       retryInterval,
                                                       retryIntervalMultiplier,
                                                       reconnectAttempts);

      connectionManagerMap.put(pair, cm);

      updateConnectionManagerArray();

      discoveryGroup = null;
   }

   public ClientSessionFactoryImpl(final TransportConfiguration connectorConfig,
                                   final TransportConfiguration backupConfig,
                                   final boolean failoverOnServerShutdown,
                                   final String connectionloadBalancingPolicyClassName,
                                   final long pingPeriod,
                                   final long connectionTTL,
                                   final long callTimeout,
                                   final int consumerWindowSize,
                                   final int consumerMaxRate,
                                   final int sendWindowSize,
                                   final int producerMaxRate,
                                   final int minLargeMessageSize,
                                   final boolean blockOnAcknowledge,
                                   final boolean blockOnNonPersistentSend,
                                   final boolean blockOnPersistentSend,
                                   final boolean autoGroup,
                                   final int maxConnections,
                                   final boolean preAcknowledge,
                                   final int ackBatchSize,
                                   final long retryInterval,
                                   final double retryIntervalMultiplier,
                                   final int reconnectAttempts)
   {
      this.loadBalancingPolicy = instantiateLoadBalancingPolicy(connectionloadBalancingPolicyClassName);
      this.pingPeriod = pingPeriod;
      this.connectionTTL = connectionTTL;
      this.callTimeout = callTimeout;
      this.consumerWindowSize = consumerWindowSize;
      this.consumerMaxRate = consumerMaxRate;
      this.sendWindowSize = sendWindowSize;
      this.producerMaxRate = producerMaxRate;
      this.blockOnAcknowledge = blockOnAcknowledge;
      this.blockOnNonPersistentSend = blockOnNonPersistentSend;
      this.blockOnPersistentSend = blockOnPersistentSend;
      this.minLargeMessageSize = minLargeMessageSize;
      this.autoGroup = autoGroup;
      this.maxConnections = maxConnections;
      this.ackBatchSize = ackBatchSize;
      this.preAcknowledge = preAcknowledge;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.reconnectAttempts = reconnectAttempts;
      this.failoverOnServerShutdown = failoverOnServerShutdown;

      this.initialWaitTimeout = -1;

      Pair<TransportConfiguration, TransportConfiguration> pair = new Pair<TransportConfiguration, TransportConfiguration>(connectorConfig,
                                                                                                                           backupConfig);
      ConnectionManager cm = new ConnectionManagerImpl(pair.a,
                                                       pair.b,
                                                       failoverOnServerShutdown,
                                                       maxConnections,
                                                       callTimeout,
                                                       pingPeriod,
                                                       connectionTTL,
                                                       retryInterval,
                                                       retryIntervalMultiplier,
                                                       reconnectAttempts);

      connectionManagerMap.put(pair, cm);

      updateConnectionManagerArray();

      discoveryGroup = null;
   }

   /**
   * Create a ClientSessionFactoryImpl specify transport type and using defaults
   */
   public ClientSessionFactoryImpl(final TransportConfiguration connectorConfig)
   {
      this(connectorConfig,
           null,
           DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN,
           DEFAULT_RETRY_INTERVAL,
           DEFAULT_RETRY_INTERVAL_MULTIPLIER,
           DEFAULT_RECONNECT_ATTEMPTS);
   }

   // ClientSessionFactory implementation------------------------------------------------------------

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

   public int getConsumerWindowSize()
   {
      return consumerWindowSize;
   }

   public void setConsumerWindowSize(final int size)
   {
      consumerWindowSize = size;
   }

   public int getSendWindowSize()
   {
      return sendWindowSize;
   }

   public void setSendWindowSize(final int size)
   {
      sendWindowSize = size;
   }

   public int getProducerMaxRate()
   {
      return producerMaxRate;
   }

   public void setProducerMaxRate(final int rate)
   {
      producerMaxRate = rate;
   }

   public int getConsumerMaxRate()
   {
      return consumerMaxRate;
   }

   public void setConsumerMaxRate(final int rate)
   {
      consumerMaxRate = rate;
   }

   public boolean isBlockOnPersistentSend()
   {
      return blockOnPersistentSend;
   }

   public void setBlockOnPersistentSend(final boolean blocking)
   {
      blockOnPersistentSend = blocking;
   }

   public boolean isBlockOnNonPersistentSend()
   {
      return blockOnNonPersistentSend;
   }

   public void setBlockOnNonPersistentSend(final boolean blocking)
   {
      blockOnNonPersistentSend = blocking;
   }

   public boolean isBlockOnAcknowledge()
   {
      return blockOnAcknowledge;
   }

   public void setBlockOnAcknowledge(final boolean blocking)
   {
      blockOnAcknowledge = blocking;
   }

   public boolean isAutoGroup()
   {
      return autoGroup;
   }

   public void setAutoGroup(boolean autoGroup)
   {
      this.autoGroup = autoGroup;
   }

   public int getAckBatchSize()
   {
      return ackBatchSize;
   }

   public void setAckBatchSize(int ackBatchSize)
   {
      this.ackBatchSize = ackBatchSize;
   }

   public boolean isPreAcknowledge()
   {
      return preAcknowledge;
   }

   public void setPreAcknowledge(boolean preAcknowledge)
   {
      this.preAcknowledge = preAcknowledge;
   }

   public long getPingPeriod()
   {
      return pingPeriod;
   }

   public long getCallTimeout()
   {
      return callTimeout;
   }

   public int getMaxConnections()
   {
      return maxConnections;
   }

   /**
    * @return the minLargeMessageSize
    */
   public int getMinLargeMessageSize()
   {
      return minLargeMessageSize;
   }

   /**
    * @param minLargeMessageSize the minLargeMessageSize to set
    */
   public void setMinLargeMessageSize(int minLargeMessageSize)
   {
      this.minLargeMessageSize = minLargeMessageSize;
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
   }

   // DiscoveryListener implementation --------------------------------------------------------

   public synchronized void connectorsChanged()
   {
      receivedBroadcast = true;

      List<Pair<TransportConfiguration, TransportConfiguration>> newConnectors = discoveryGroup.getConnectors();

      Set<Pair<TransportConfiguration, TransportConfiguration>> connectorSet = new HashSet<Pair<TransportConfiguration, TransportConfiguration>>();

      connectorSet.addAll(newConnectors);

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

      for (Pair<TransportConfiguration, TransportConfiguration> connectorPair : newConnectors)
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
                                                                            reconnectAttempts);

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

   private ClientSession createSessionInternal(final String username,
                                               final String password,
                                               final boolean xa,
                                               final boolean autoCommitSends,
                                               final boolean autoCommitAcks,
                                               final boolean preAcknowledge,
                                               final int ackBatchSize) throws MessagingException
   {
      if (discoveryGroup != null && !receivedBroadcast)
      {
         boolean ok = discoveryGroup.waitForBroadcast(initialWaitTimeout);

         if (!ok)
         {
            throw new MessagingException(MessagingException.CONNECTION_TIMEDOUT,
                                         "Timed out waiting to receive intial broadcast from discovery group");
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
                                                sendWindowSize,
                                                consumerWindowSize,
                                                consumerMaxRate,
                                                producerMaxRate,
                                                blockOnNonPersistentSend,
                                                blockOnPersistentSend);
      }
   }

   private ConnectionLoadBalancingPolicy instantiateLoadBalancingPolicy(final String className)
   {
      ClassLoader loader = Thread.currentThread().getContextClassLoader();
      ConnectionLoadBalancingPolicy lbPolicy;
      try
      {
         Class<?> clazz = loader.loadClass(className);
         lbPolicy = (ConnectionLoadBalancingPolicy)clazz.newInstance();
      }
      catch (Exception e)
      {
         throw new IllegalArgumentException("Unable to instantiate load balancing policy \"" + className + "\"", e);
      }

      return lbPolicy;
   }

   private void updateConnectionManagerArray()
   {
      connectionManagerArray = new ConnectionManager[connectionManagerMap.size()];

      connectionManagerMap.values().toArray(connectionManagerArray);
   }

}
