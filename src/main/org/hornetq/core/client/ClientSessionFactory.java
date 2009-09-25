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

package org.hornetq.core.client;

import java.util.List;

import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.remoting.Interceptor;
import org.hornetq.utils.Pair;

/**
 * A ClientSessionFactory
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface ClientSessionFactory
{
   ClientSession createXASession() throws HornetQException;

   ClientSession createTransactedSession() throws HornetQException;

   ClientSession createSession() throws HornetQException;

   ClientSession createSession(boolean autoCommitSends, boolean autoCommitAcks) throws HornetQException;

   ClientSession createSession(boolean autoCommitSends, boolean autoCommitAcks, int ackBatchSize) throws HornetQException;
   
   ClientSession createSession(boolean xa, boolean autoCommitSends, boolean autoCommitAcks) throws HornetQException;

   ClientSession createSession(boolean xa, boolean autoCommitSends, boolean autoCommitAcks, boolean preAcknowledge) throws HornetQException;
   
   ClientSession createSession(String username,
                               String password,
                               boolean xa,
                               boolean autoCommitSends,
                               boolean autoCommitAcks,
                               boolean preAcknowledge,
                               int ackBatchSize) throws HornetQException;


   List<Pair<TransportConfiguration, TransportConfiguration>> getStaticConnectors();

   void setStaticConnectors(List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors);

   long getClientFailureCheckPeriod();

   void setClientFailureCheckPeriod(long clientFailureCheckPeriod);

   boolean isCacheLargeMessagesClient();

   void setCacheLargeMessagesClient(boolean cached);

   long getConnectionTTL();

   void setConnectionTTL(long connectionTTL);

   long getCallTimeout();

   void setCallTimeout(long callTimeout);

   int getMaxConnections();

   void setMaxConnections(int maxConnections);

   int getMinLargeMessageSize();

   void setMinLargeMessageSize(int minLargeMessageSize);

   int getConsumerWindowSize();

   void setConsumerWindowSize(int consumerWindowSize);

   int getConsumerMaxRate();

   void setConsumerMaxRate(int consumerMaxRate);

   int getProducerWindowSize();

   void setProducerWindowSize(int producerWindowSize);

   int getProducerMaxRate();

   void setProducerMaxRate(int producerMaxRate);

   boolean isBlockOnAcknowledge();

   void setBlockOnAcknowledge(boolean blockOnAcknowledge);

   boolean isBlockOnPersistentSend();

   void setBlockOnPersistentSend(boolean blockOnPersistentSend);

   boolean isBlockOnNonPersistentSend();

   void setBlockOnNonPersistentSend(boolean blockOnNonPersistentSend);

   boolean isAutoGroup();

   void setAutoGroup(boolean autoGroup);

   boolean isPreAcknowledge();

   void setPreAcknowledge(boolean preAcknowledge);

   int getAckBatchSize();

   void setAckBatchSize(int ackBatchSize);

   long getDiscoveryInitialWaitTimeout();

   void setDiscoveryInitialWaitTimeout(long initialWaitTimeout);

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

   int getReconnectAttempts();

   void setReconnectAttempts(int reconnectAttempts);
   
   boolean isUseReattach();
   
   void setUseReattach(boolean reattach);

   boolean isFailoverOnServerShutdown();

   void setFailoverOnServerShutdown(boolean failoverOnServerShutdown);

   String getConnectionLoadBalancingPolicyClassName();

   void setConnectionLoadBalancingPolicyClassName(String loadBalancingPolicyClassName);

   String getDiscoveryAddress();

   void setDiscoveryAddress(String discoveryAddress);

   int getDiscoveryPort();

   void setDiscoveryPort(int discoveryPort);

   long getDiscoveryRefreshTimeout();

   void setDiscoveryRefreshTimeout(long discoveryRefreshTimeout);
   
   void addInterceptor(Interceptor interceptor);

   boolean removeInterceptor(Interceptor interceptor);

   void close();
}
