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

package org.hornetq.jms.server.config.impl;

import java.util.ArrayList;
import java.util.List;

import org.hornetq.Pair;
import org.hornetq.core.client.ClientSessionFactoryImpl;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;

/**
 * A ConnectionFactoryConfigurationImpl
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class ConnectionFactoryConfigurationImpl implements ConnectionFactoryConfiguration
{
   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final String[] bindings;

   private final String name;

   private String discoveryAddress;

   private int discoveryPort;

   private List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs;

   private String clientID = null;

   private long discoveryRefreshTimeout = ClientSessionFactoryImpl.DEFAULT_DISCOVERY_REFRESH_TIMEOUT;

   private long clientFailureCheckPeriod = ClientSessionFactoryImpl.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD;

   private long connectionTTL = ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL;

   private long callTimeout = ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT;

   private boolean cacheLargeMessagesClient = ClientSessionFactoryImpl.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT;

   private int minLargeMessageSize = ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;

   private int consumerWindowSize = ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE;

   private int consumerMaxRate = ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;

   private int confirmationWindowSize = ClientSessionFactoryImpl.DEFAULT_CONFIRMATION_WINDOW_SIZE;

   private int producerWindowSize = ClientSessionFactoryImpl.DEFAULT_PRODUCER_WINDOW_SIZE;

   private int producerMaxRate = ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;

   private boolean blockOnAcknowledge = ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE;

   private boolean blockOnDurableSend = ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_DURABLE_SEND;

   private boolean blockOnNonDurableSend = ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_DURABLE_SEND;

   private boolean autoGroup = ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;

   private boolean preAcknowledge = ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE;

   private String loadBalancingPolicyClassName = ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;

   private int transactionBatchSize = ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;

   private int dupsOKBatchSize = ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;

   private long initialWaitTimeout = ClientSessionFactoryImpl.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT;

   private boolean useGlobalPools = ClientSessionFactoryImpl.DEFAULT_USE_GLOBAL_POOLS;

   private int scheduledThreadPoolMaxSize = ClientSessionFactoryImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;

   private int threadPoolMaxSize = ClientSessionFactoryImpl.DEFAULT_THREAD_POOL_MAX_SIZE;

   private long retryInterval = ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;

   private double retryIntervalMultiplier = ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;

   private long maxRetryInterval = ClientSessionFactoryImpl.DEFAULT_MAX_RETRY_INTERVAL;

   private int reconnectAttempts = ClientSessionFactoryImpl.DEFAULT_RECONNECT_ATTEMPTS;

   private boolean failoverOnServerShutdown = ClientSessionFactoryImpl.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN;

   private String groupID = null;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConnectionFactoryConfigurationImpl(final String name,
                                             final String discoveryAddress,
                                             final int discoveryPort,
                                             final String... bindings)
   {
      this(name, bindings);
      this.discoveryAddress = discoveryAddress;
      this.discoveryPort = discoveryPort;
   }

   public ConnectionFactoryConfigurationImpl(final String name,
                                             final TransportConfiguration liveConfig,
                                             final String... bindings)
   {
      this(name, liveConfig, null, bindings);
   }

   public ConnectionFactoryConfigurationImpl(final String name,
                                             final TransportConfiguration liveConfig,
                                             final TransportConfiguration backupConfig,
                                             final String... bindings)
   {
      this(name, bindings);
      connectorConfigs = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();
      connectorConfigs.add(new Pair<TransportConfiguration, TransportConfiguration>(liveConfig, backupConfig));
   }

   public ConnectionFactoryConfigurationImpl(final String name,
                                             final List<Pair<TransportConfiguration, TransportConfiguration>> transportConfigs,
                                             final TransportConfiguration backupConfig,
                                             final String... bindings)
   {
      this(name, bindings);
      connectorConfigs = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();
      connectorConfigs.addAll(transportConfigs);
   }

   private ConnectionFactoryConfigurationImpl(final String name, final String... bindings)
   {
      this.name = name;
      this.bindings = new String[bindings.length];
      System.arraycopy(bindings, 0, this.bindings, 0, bindings.length);
   }

   // ConnectionFactoryConfiguration implementation -----------------

   public String[] getBindings()
   {
      return bindings;
   }

   public String getName()
   {
      return name;
   }

   public String getDiscoveryAddress()
   {
      return discoveryAddress;
   }

   public void setDiscoveryAddress(final String discoveryAddress)
   {
      this.discoveryAddress = discoveryAddress;
   }

   public int getDiscoveryPort()
   {
      return discoveryPort;
   }

   public void setDiscoveryPort(final int discoveryPort)
   {
      this.discoveryPort = discoveryPort;
   }

   public List<Pair<TransportConfiguration, TransportConfiguration>> getConnectorConfigs()
   {
      return connectorConfigs;
   }

   public void setConnectorConfigs(final List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs)
   {
      this.connectorConfigs = connectorConfigs;
   }

   public String getClientID()
   {
      return clientID;
   }

   public void setClientID(final String clientID)
   {
      this.clientID = clientID;
   }

   public long getDiscoveryRefreshTimeout()
   {
      return discoveryRefreshTimeout;
   }

   public void setDiscoveryRefreshTimeout(final long discoveryRefreshTimeout)
   {
      this.discoveryRefreshTimeout = discoveryRefreshTimeout;
   }

   public long getClientFailureCheckPeriod()
   {
      return clientFailureCheckPeriod;
   }

   public void setClientFailureCheckPeriod(final long clientFailureCheckPeriod)
   {
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
   }

   public long getConnectionTTL()
   {
      return connectionTTL;
   }

   public void setConnectionTTL(final long connectionTTL)
   {
      this.connectionTTL = connectionTTL;
   }

   public long getCallTimeout()
   {
      return callTimeout;
   }

   public void setCallTimeout(final long callTimeout)
   {
      this.callTimeout = callTimeout;
   }

   public boolean isCacheLargeMessagesClient()
   {
      return cacheLargeMessagesClient;
   }

   public void setCacheLargeMessagesClient(final boolean cacheLargeMessagesClient)
   {
      this.cacheLargeMessagesClient = cacheLargeMessagesClient;
   }

   public int getMinLargeMessageSize()
   {
      return minLargeMessageSize;
   }

   public void setMinLargeMessageSize(final int minLargeMessageSize)
   {
      this.minLargeMessageSize = minLargeMessageSize;
   }

   public int getConsumerWindowSize()
   {
      return consumerWindowSize;
   }

   public void setConsumerWindowSize(final int consumerWindowSize)
   {
      this.consumerWindowSize = consumerWindowSize;
   }

   public int getConsumerMaxRate()
   {
      return consumerMaxRate;
   }

   public void setConsumerMaxRate(final int consumerMaxRate)
   {
      this.consumerMaxRate = consumerMaxRate;
   }

   public int getConfirmationWindowSize()
   {
      return confirmationWindowSize;
   }

   public void setConfirmationWindowSize(final int confirmationWindowSize)
   {
      this.confirmationWindowSize = confirmationWindowSize;
   }

   public int getProducerMaxRate()
   {
      return producerMaxRate;
   }

   public void setProducerMaxRate(final int producerMaxRate)
   {
      this.producerMaxRate = producerMaxRate;
   }

   public int getProducerWindowSize()
   {
      return producerWindowSize;
   }

   public void setProducerWindowSize(final int producerWindowSize)
   {
      this.producerWindowSize = producerWindowSize;
   }

   public boolean isBlockOnAcknowledge()
   {
      return blockOnAcknowledge;
   }

   public void setBlockOnAcknowledge(final boolean blockOnAcknowledge)
   {
      this.blockOnAcknowledge = blockOnAcknowledge;
   }

   public boolean isBlockOnDurableSend()
   {
      return blockOnDurableSend;
   }

   public void setBlockOnDurableSend(final boolean blockOnDurableSend)
   {
      this.blockOnDurableSend = blockOnDurableSend;
   }

   public boolean isBlockOnNonDurableSend()
   {
      return blockOnNonDurableSend;
   }

   public void setBlockOnNonDurableSend(final boolean blockOnNonDurableSend)
   {
      this.blockOnNonDurableSend = blockOnNonDurableSend;
   }

   public boolean isAutoGroup()
   {
      return autoGroup;
   }

   public void setAutoGroup(final boolean autoGroup)
   {
      this.autoGroup = autoGroup;
   }

   public boolean isPreAcknowledge()
   {
      return preAcknowledge;
   }

   public void setPreAcknowledge(final boolean preAcknowledge)
   {
      this.preAcknowledge = preAcknowledge;
   }

   public String getLoadBalancingPolicyClassName()
   {
      return loadBalancingPolicyClassName;
   }

   public void setLoadBalancingPolicyClassName(final String loadBalancingPolicyClassName)
   {
      this.loadBalancingPolicyClassName = loadBalancingPolicyClassName;
   }

   public int getTransactionBatchSize()
   {
      return transactionBatchSize;
   }

   public void setTransactionBatchSize(final int transactionBatchSize)
   {
      this.transactionBatchSize = transactionBatchSize;
   }

   public int getDupsOKBatchSize()
   {
      return dupsOKBatchSize;
   }

   public void setDupsOKBatchSize(final int dupsOKBatchSize)
   {
      this.dupsOKBatchSize = dupsOKBatchSize;
   }

   public long getInitialWaitTimeout()
   {
      return initialWaitTimeout;
   }

   public void setInitialWaitTimeout(final long initialWaitTimeout)
   {
      this.initialWaitTimeout = initialWaitTimeout;
   }

   public boolean isUseGlobalPools()
   {
      return useGlobalPools;
   }

   public void setUseGlobalPools(final boolean useGlobalPools)
   {
      this.useGlobalPools = useGlobalPools;
   }

   public int getScheduledThreadPoolMaxSize()
   {
      return scheduledThreadPoolMaxSize;
   }

   public void setScheduledThreadPoolMaxSize(final int scheduledThreadPoolMaxSize)
   {
      this.scheduledThreadPoolMaxSize = scheduledThreadPoolMaxSize;
   }

   public int getThreadPoolMaxSize()
   {
      return threadPoolMaxSize;
   }

   public void setThreadPoolMaxSize(final int threadPoolMaxSize)
   {
      this.threadPoolMaxSize = threadPoolMaxSize;
   }

   public long getRetryInterval()
   {
      return retryInterval;
   }

   public void setRetryInterval(final long retryInterval)
   {
      this.retryInterval = retryInterval;
   }

   public double getRetryIntervalMultiplier()
   {
      return retryIntervalMultiplier;
   }

   public void setRetryIntervalMultiplier(final double retryIntervalMultiplier)
   {
      this.retryIntervalMultiplier = retryIntervalMultiplier;
   }

   public long getMaxRetryInterval()
   {
      return maxRetryInterval;
   }

   public void setMaxRetryInterval(final long maxRetryInterval)
   {
      this.maxRetryInterval = maxRetryInterval;
   }

   public int getReconnectAttempts()
   {
      return reconnectAttempts;
   }

   public void setReconnectAttempts(final int reconnectAttempts)
   {
      this.reconnectAttempts = reconnectAttempts;
   }

   public boolean isFailoverOnServerShutdown()
   {
      return failoverOnServerShutdown;
   }

   public void setFailoverOnServerShutdown(final boolean failoverOnServerShutdown)
   {
      this.failoverOnServerShutdown = failoverOnServerShutdown;
   }

   public String getGroupID()
   {
      return groupID;
   }

   public void setGroupID(final String groupID)
   {
      this.groupID = groupID;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
