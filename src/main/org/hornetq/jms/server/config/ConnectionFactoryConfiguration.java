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

package org.hornetq.jms.server.config;

import java.util.List;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.jms.server.JMSServerManager;

/**
 * A ConnectionFactoryConfiguration
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public interface ConnectionFactoryConfiguration extends EncodingSupport
{
   String getName();

   String[] getBindings();

   String getDiscoveryAddress();

   void setDiscoveryAddress(String discoveryAddress);

   int getDiscoveryPort();

   void setDiscoveryPort(int discoveryPort);


   /**
    * A Reference to the group configuration.
    */
   String getDiscoveryGroupName();
   
   /**
    * A Reference to the group configuration.
    */
   void setDiscoveryGroupName(String groupName);
   
   
   /**
    * A List of connector names that will be converted into ConnnectorConfigs.
    * This is useful when using the method {@link JMSServerManager#createConnectionFactory(ConnectionFactoryConfiguration)}
    * 
    * @return
    */
   List<Pair<String, String>> getConnectorNames();

   /**
    * A List of connector names that will be converted into ConnnectorConfigs.
    * This is useful when using the method {@link JMSServerManager#createConnectionFactory(ConnectionFactoryConfiguration)}
    * 
    * @return
    */
   void setConnectorNames(List<Pair<String, String>> connectors);

   List<Pair<TransportConfiguration, TransportConfiguration>> getConnectorConfigs();

   void setConnectorConfigs(List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs);

   String getClientID();

   void setClientID(String clientID);

   long getDiscoveryRefreshTimeout();

   void setDiscoveryRefreshTimeout(long discoveryRefreshTimeout);

   long getClientFailureCheckPeriod();

   void setClientFailureCheckPeriod(long clientFailureCheckPeriod);

   long getConnectionTTL();

   void setConnectionTTL(long connectionTTL);

   long getCallTimeout();

   void setCallTimeout(long callTimeout);

   boolean isCacheLargeMessagesClient();

   void setCacheLargeMessagesClient(boolean cacheLargeMessagesClient);

   int getMinLargeMessageSize();

   void setMinLargeMessageSize(int minLargeMessageSize);

   int getConsumerWindowSize();

   void setConsumerWindowSize(int consumerWindowSize);

   int getConsumerMaxRate();

   void setConsumerMaxRate(int consumerMaxRate);

   int getConfirmationWindowSize();

   void setConfirmationWindowSize(int confirmationWindowSize);

   int getProducerWindowSize();

   void setProducerWindowSize(int producerWindowSize);

   int getProducerMaxRate();

   void setProducerMaxRate(int producerMaxRate);

   boolean isBlockOnAcknowledge();

   void setBlockOnAcknowledge(boolean blockOnAcknowledge);

   boolean isBlockOnDurableSend();

   void setBlockOnDurableSend(boolean blockOnDurableSend);

   boolean isBlockOnNonDurableSend();

   void setBlockOnNonDurableSend(boolean blockOnNonDurableSend);

   boolean isAutoGroup();

   void setAutoGroup(boolean autoGroup);

   boolean isPreAcknowledge();

   void setPreAcknowledge(boolean preAcknowledge);

   String getLoadBalancingPolicyClassName();

   void setLoadBalancingPolicyClassName(String loadBalancingPolicyClassName);

   int getTransactionBatchSize();

   void setTransactionBatchSize(int transactionBatchSize);

   int getDupsOKBatchSize();

   void setDupsOKBatchSize(int dupsOKBatchSize);

   long getInitialWaitTimeout();

   void setInitialWaitTimeout(long initialWaitTimeout);

   boolean isUseGlobalPools();

   void setUseGlobalPools(boolean useGlobalPools);

   int getScheduledThreadPoolMaxSize();

   void setScheduledThreadPoolMaxSize(int scheduledThreadPoolMaxSize);

   int getThreadPoolMaxSize();

   void setThreadPoolMaxSize(int threadPoolMaxSize);

   long getRetryInterval();

   void setRetryInterval(long retryInterval);

   double getRetryIntervalMultiplier();

   void setRetryIntervalMultiplier(double retryIntervalMultiplier);

   long getMaxRetryInterval();

   void setMaxRetryInterval(long maxRetryInterval);

   int getReconnectAttempts();

   void setReconnectAttempts(int reconnectAttempts);

   boolean isFailoverOnServerShutdown();

   void setFailoverOnServerShutdown(boolean failoverOnServerShutdown);

   String getGroupID();

   void setGroupID(String groupID);
}
