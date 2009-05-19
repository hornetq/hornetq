/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.core.client;

import java.util.List;

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.utils.Pair;

/**
 * A ClientSessionFactory
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public interface ClientSessionFactory
{
   ClientSession createXASession() throws MessagingException;
   
   ClientSession createTransactedSession() throws MessagingException;
   
   ClientSession createSession() throws MessagingException;
   
   ClientSession createSession(boolean autoCommitSends, boolean autoCommitAcks) throws MessagingException;
   
   ClientSession createSession(boolean xa, boolean autoCommitSends, boolean autoCommitAcks) throws MessagingException;

   ClientSession createSession(String username,
                               String password,
                               boolean xa,
                               boolean autoCommitSends,
                               boolean autoCommitAcks,
                               boolean preAcknowledge,
                               int ackBatchSize) throws MessagingException;

   ClientSession createSession(boolean xa, boolean autoCommitSends, boolean autoCommitAcks, boolean preAcknowledge) throws MessagingException;
 
   List<Pair<TransportConfiguration, TransportConfiguration>> getStaticConnectors();
   
   void setStaticConnectors(List<Pair<TransportConfiguration, TransportConfiguration>> staticConnectors);
   
   long getPingPeriod();

   void setPingPeriod(long pingPeriod);

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

   int getReconnectAttempts();

   void setReconnectAttempts(int reconnectAttempts);

   boolean isFailoverOnServerShutdown();

   void setFailoverOnServerShutdown(boolean failoverOnServerShutdown);
   
   String getLoadBalancingPolicyClassName();

   void setLoadBalancingPolicyClassName(String loadBalancingPolicyClassName);
   
   String getDiscoveryAddress();   

   void setDiscoveryAddress(String discoveryAddress);

   int getDiscoveryPort();

   void setDiscoveryPort(int discoveryPort);
   
   long getDiscoveryRefreshTimeout();

   void setDiscoveryRefreshTimeout(long discoveryRefreshTimeout);

   void close();
}
