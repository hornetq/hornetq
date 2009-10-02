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

import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;
import org.hornetq.utils.Pair;

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

   public final String[] bindings;

   public final String name;

   public String discoveryAddress;

   public int discoveryPort;

   public List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs;

   public String clientID = null;

   public long discoveryRefreshTimeout = ClientSessionFactoryImpl.DEFAULT_DISCOVERY_REFRESH_TIMEOUT;

   public long clientFailureCheckPeriod = ClientSessionFactoryImpl.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD;

   public long connectionTTL = ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL;

   public long callTimeout = ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT;

   public boolean cacheLargeMessagesClient = ClientSessionFactoryImpl.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT;

   public int minLargeMessageSize = ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;

   public int consumerWindowSize = ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE;

   public int consumerMaxRate = ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;

   public int producerWindowSize = ClientSessionFactoryImpl.DEFAULT_PRODUCER_WINDOW_SIZE;

   public int producerMaxRate = ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;

   public boolean blockOnAcknowledge = ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE;

   public boolean blockOnPersistentSend = ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND;

   public boolean blockOnNonPersistentSend = ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;

   public boolean autoGroup = ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;

   public boolean preAcknowledge = ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE;

   public String loadBalancingPolicyClassName = ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;

   public int transactionBatchSize = ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;

   public int dupsOKBatchSize = ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;

   public long initialWaitTimeout = ClientSessionFactoryImpl.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT;

   public boolean useGlobalPools = ClientSessionFactoryImpl.DEFAULT_USE_GLOBAL_POOLS;

   public int scheduledThreadPoolMaxSize = ClientSessionFactoryImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;

   public int threadPoolMaxSize = ClientSessionFactoryImpl.DEFAULT_THREAD_POOL_MAX_SIZE;

   public long retryInterval = ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;

   public double retryIntervalMultiplier = ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;

   public long maxRetryInterval = ClientSessionFactoryImpl.DEFAULT_MAX_RETRY_INTERVAL;

   public int reconnectAttempts = ClientSessionFactoryImpl.DEFAULT_RECONNECT_ATTEMPTS;

   public boolean failoverOnServerShutdown = ClientSessionFactoryImpl.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public ConnectionFactoryConfigurationImpl(String name, String discoveryAddress, int discoveryPort, String... bindings)
   {
      this(name, bindings);
      this.discoveryAddress = discoveryAddress;
      this.discoveryPort = discoveryPort;
   }

   public ConnectionFactoryConfigurationImpl(String name, TransportConfiguration liveConfig, String... bindings)
   {
      this(name, liveConfig, null, bindings);
   }

   public ConnectionFactoryConfigurationImpl(String name, TransportConfiguration liveConfig, TransportConfiguration backupConfig, String... bindings)
   {
      this(name, bindings);
      connectorConfigs = new ArrayList<Pair<TransportConfiguration,TransportConfiguration>>();
      connectorConfigs.add(new Pair<TransportConfiguration, TransportConfiguration>(liveConfig, backupConfig));
   }

   public ConnectionFactoryConfigurationImpl(String name, List<Pair<TransportConfiguration,TransportConfiguration>> transportConfigs, TransportConfiguration backupConfig, String... bindings)
   {
      this(name, bindings);
      connectorConfigs = new ArrayList<Pair<TransportConfiguration,TransportConfiguration>>();
      connectorConfigs.addAll(transportConfigs);
   }

   private ConnectionFactoryConfigurationImpl(String name, String... bindings)
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

   public void setDiscoveryAddress(String discoveryAddress)
   {
      this.discoveryAddress = discoveryAddress;
   }

   public int getDiscoveryPort()
   {
      return discoveryPort;
   }

   public void setDiscoveryPort(int discoveryPort)
   {
      this.discoveryPort = discoveryPort;
   }

   public List<Pair<TransportConfiguration, TransportConfiguration>> getConnectorConfigs()
   {
      return connectorConfigs;
   }

   public void setConnectorConfigs(List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs)
   {
      this.connectorConfigs = connectorConfigs;
   }

   public String getClientID()
   {
      return clientID;
   }

   public void setClientID(String clientID)
   {
      this.clientID = clientID;
   }

   public long getDiscoveryRefreshTimeout()
   {
      return discoveryRefreshTimeout;
   }

   public void setDiscoveryRefreshTimeout(long discoveryRefreshTimeout)
   {
      this.discoveryRefreshTimeout = discoveryRefreshTimeout;
   }

   public long getClientFailureCheckPeriod()
   {
      return clientFailureCheckPeriod;
   }

   public void setClientFailureCheckPeriod(long clientFailureCheckPeriod)
   {
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
   }

   public long getConnectionTTL()
   {
      return connectionTTL;
   }

   public void setConnectionTTL(long connectionTTL)
   {
      this.connectionTTL = connectionTTL;
   }

   public long getCallTimeout()
   {
      return callTimeout;
   }

   public void setCallTimeout(long callTimeout)
   {
      this.callTimeout = callTimeout;
   }

   public boolean isCacheLargeMessagesClient()
   {
      return cacheLargeMessagesClient;
   }

   public void setCacheLargeMessagesClient(boolean cacheLargeMessagesClient)
   {
      this.cacheLargeMessagesClient = cacheLargeMessagesClient;
   }

   public int getMinLargeMessageSize()
   {
      return minLargeMessageSize;
   }

   public void setMinLargeMessageSize(int minLargeMessageSize)
   {
      this.minLargeMessageSize = minLargeMessageSize;
   }

   public int getConsumerWindowSize()
   {
      return consumerWindowSize;
   }

   public void setConsumerWindowSize(int consumerWindowSize)
   {
      this.consumerWindowSize = consumerWindowSize;
   }

   public int getConsumerMaxRate()
   {
      return consumerMaxRate;
   }

   public void setConsumerMaxRate(int consumerMaxRate)
   {
      this.consumerMaxRate = consumerMaxRate;
   }

   public int getProducerWindowSize()
   {
      return producerWindowSize;
   }

   public void setProducerWindowSize(int producerWindowSize)
   {
      this.producerWindowSize = producerWindowSize;
   }

   public int getProducerMaxRate()
   {
      return producerMaxRate;
   }

   public void setProducerMaxRate(int producerMaxRate)
   {
      this.producerMaxRate = producerMaxRate;
   }

   public boolean isBlockOnAcknowledge()
   {
      return blockOnAcknowledge;
   }

   public void setBlockOnAcknowledge(boolean blockOnAcknowledge)
   {
      this.blockOnAcknowledge = blockOnAcknowledge;
   }

   public boolean isBlockOnPersistentSend()
   {
      return blockOnPersistentSend;
   }

   public void setBlockOnPersistentSend(boolean blockOnPersistentSend)
   {
      this.blockOnPersistentSend = blockOnPersistentSend;
   }

   public boolean isBlockOnNonPersistentSend()
   {
      return blockOnNonPersistentSend;
   }

   public void setBlockOnNonPersistentSend(boolean blockOnNonPersistentSend)
   {
      this.blockOnNonPersistentSend = blockOnNonPersistentSend;
   }

   public boolean isAutoGroup()
   {
      return autoGroup;
   }

   public void setAutoGroup(boolean autoGroup)
   {
      this.autoGroup = autoGroup;
   }

   public boolean isPreAcknowledge()
   {
      return preAcknowledge;
   }

   public void setPreAcknowledge(boolean preAcknowledge)
   {
      this.preAcknowledge = preAcknowledge;
   }

   public String getLoadBalancingPolicyClassName()
   {
      return loadBalancingPolicyClassName;
   }

   public void setLoadBalancingPolicyClassName(String loadBalancingPolicyClassName)
   {
      this.loadBalancingPolicyClassName = loadBalancingPolicyClassName;
   }

   public int getTransactionBatchSize()
   {
      return transactionBatchSize;
   }

   public void setTransactionBatchSize(int transactionBatchSize)
   {
      this.transactionBatchSize = transactionBatchSize;
   }

   public int getDupsOKBatchSize()
   {
      return dupsOKBatchSize;
   }

   public void setDupsOKBatchSize(int dupsOKBatchSize)
   {
      this.dupsOKBatchSize = dupsOKBatchSize;
   }

   public long getInitialWaitTimeout()
   {
      return initialWaitTimeout;
   }

   public void setInitialWaitTimeout(long initialWaitTimeout)
   {
      this.initialWaitTimeout = initialWaitTimeout;
   }

   public boolean isUseGlobalPools()
   {
      return useGlobalPools;
   }

   public void setUseGlobalPools(boolean useGlobalPools)
   {
      this.useGlobalPools = useGlobalPools;
   }

   public int getScheduledThreadPoolMaxSize()
   {
      return scheduledThreadPoolMaxSize;
   }

   public void setScheduledThreadPoolMaxSize(int scheduledThreadPoolMaxSize)
   {
      this.scheduledThreadPoolMaxSize = scheduledThreadPoolMaxSize;
   }

   public int getThreadPoolMaxSize()
   {
      return threadPoolMaxSize;
   }

   public void setThreadPoolMaxSize(int threadPoolMaxSize)
   {
      this.threadPoolMaxSize = threadPoolMaxSize;
   }

   public long getRetryInterval()
   {
      return retryInterval;
   }

   public void setRetryInterval(long retryInterval)
   {
      this.retryInterval = retryInterval;
   }

   public double getRetryIntervalMultiplier()
   {
      return retryIntervalMultiplier;
   }

   public void setRetryIntervalMultiplier(double retryIntervalMultiplier)
   {
      this.retryIntervalMultiplier = retryIntervalMultiplier;
   }

   public long getMaxRetryInterval()
   {
      return maxRetryInterval;
   }

   public void setMaxRetryInterval(long maxRetryInterval)
   {
      this.maxRetryInterval = maxRetryInterval;
   }

   public int getReconnectAttempts()
   {
      return reconnectAttempts;
   }

   public void setReconnectAttempts(int reconnectAttempts)
   {
      this.reconnectAttempts = reconnectAttempts;
   }

   public boolean isFailoverOnServerShutdown()
   {
      return failoverOnServerShutdown;
   }

   public void setFailoverOnServerShutdown(boolean failoverOnServerShutdown)
   {
      this.failoverOnServerShutdown = failoverOnServerShutdown;
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
