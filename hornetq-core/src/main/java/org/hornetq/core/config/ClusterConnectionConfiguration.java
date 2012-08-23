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

package org.hornetq.core.config;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;

import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.server.HornetQMessageBundle;

/**
 * A ClusterConnectionConfiguration
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public final class ClusterConnectionConfiguration implements Serializable
{
   private static final long serialVersionUID = 8948303813427795935L;

   private final String name;

   private final String address;

   private final String connectorName;

   private long clientFailureCheckPeriod;

   private long connectionTTL;

   private long retryInterval;

   private double retryIntervalMultiplier;

   private long maxRetryInterval;

   private int reconnectAttempts;

   private long callTimeout;

   private long callFailoverTimeout;

   private boolean duplicateDetection;

   private boolean forwardWhenNoConsumers;

   private final List<String> staticConnectors;

   private final String discoveryGroupName;

   private final int maxHops;

   private final int confirmationWindowSize;

   private final boolean allowDirectConnectionsOnly;

   private final int minLargeMessageSize;

   public ClusterConnectionConfiguration(final String name,
                                         final String address,
                                         final String connectorName,
                                         final long retryInterval,
                                         final boolean duplicateDetection,
                                         final boolean forwardWhenNoConsumers,
                                         final int maxHops,
                                         final int confirmationWindowSize,
                                         final List<String> staticConnectors,
                                         final boolean allowDirectConnectionsOnly)
   {
      this(name,
         address,
         connectorName,
         HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
         ConfigurationImpl.DEFAULT_CLUSTER_FAILURE_CHECK_PERIOD,
         ConfigurationImpl.DEFAULT_CLUSTER_CONNECTION_TTL,
         retryInterval,
         ConfigurationImpl.DEFAULT_CLUSTER_RETRY_INTERVAL_MULTIPLIER,
         ConfigurationImpl.DEFAULT_CLUSTER_MAX_RETRY_INTERVAL,
         ConfigurationImpl.DEFAULT_CLUSTER_RECONNECT_ATTEMPTS,
         HornetQClient.DEFAULT_CALL_TIMEOUT,
         HornetQClient.DEFAULT_CALL_FAILOVER_TIMEOUT,
         duplicateDetection,
         forwardWhenNoConsumers,
         maxHops,
         confirmationWindowSize,
         staticConnectors,
         allowDirectConnectionsOnly);
   }


   public ClusterConnectionConfiguration(final String name,
                                         final String address,
                                         final String connectorName,
                                         final int minLargeMessageSize,
                                         final long clientFailureCheckPeriod,
                                         final long connectionTTL,
                                         final long retryInterval,
                                         final double retryIntervalMultiplier,
                                         final long maxRetryInterval,
                                         final int reconnectAttempts,
                                         final long callTimeout,
                                         final long callFAiloverTimeout,
                                         final boolean duplicateDetection,
                                         final boolean forwardWhenNoConsumers,
                                         final int maxHops,
                                         final int confirmationWindowSize,
                                         final List<String> staticConnectors,
                                         final boolean allowDirectConnectionsOnly)
   {
      this.name = name;
      this.address = address;
      this.connectorName = connectorName;
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
      this.connectionTTL = connectionTTL;
      if (retryInterval <= 0)
         throw HornetQMessageBundle.BUNDLE.invalidRetryInterval(retryInterval);
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.maxRetryInterval = maxRetryInterval;
      this.reconnectAttempts = reconnectAttempts;
      if (staticConnectors != null)
      {
         this.staticConnectors = staticConnectors;
      }
      else
      {
         this.staticConnectors = Collections.emptyList();
      }
      this.duplicateDetection = duplicateDetection;
      this.callTimeout = callTimeout;
      this.callFailoverTimeout = callFAiloverTimeout;
      this.forwardWhenNoConsumers = forwardWhenNoConsumers;
      discoveryGroupName = null;
      this.maxHops = maxHops;
      this.confirmationWindowSize = confirmationWindowSize;
      this.allowDirectConnectionsOnly = allowDirectConnectionsOnly;
      this.minLargeMessageSize = minLargeMessageSize;
   }


   public ClusterConnectionConfiguration(final String name,
                                         final String address,
                                         final String connectorName,
                                         final long retryInterval,
                                         final boolean duplicateDetection,
                                         final boolean forwardWhenNoConsumers,
                                         final int maxHops,
                                         final int confirmationWindowSize,
                                         final String discoveryGroupName)
   {
      this(name,
         address,
         connectorName,
         HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
         ConfigurationImpl.DEFAULT_CLUSTER_FAILURE_CHECK_PERIOD,
         ConfigurationImpl.DEFAULT_CLUSTER_CONNECTION_TTL,
         retryInterval,
         ConfigurationImpl.DEFAULT_CLUSTER_RETRY_INTERVAL_MULTIPLIER,
         ConfigurationImpl.DEFAULT_CLUSTER_MAX_RETRY_INTERVAL,
         ConfigurationImpl.DEFAULT_CLUSTER_RECONNECT_ATTEMPTS,
         HornetQClient.DEFAULT_CALL_TIMEOUT,
         HornetQClient.DEFAULT_CALL_FAILOVER_TIMEOUT,
         duplicateDetection,
         forwardWhenNoConsumers,
         maxHops,
         confirmationWindowSize,
         discoveryGroupName);
   }


   public ClusterConnectionConfiguration(final String name,
                                         final String address,
                                         final String connectorName,
                                         final int minLargeMessageSize,
                                         final long clientFailureCheckPeriod,
                                         final long connectionTTL,
                                         final long retryInterval,
                                         final double retryIntervalMultiplier,
                                         final long maxRetryInterval,
                                         final int reconnectAttempts,
                                         final long callTimeout,
                                         final long callFailoverTimeout,
                                         final boolean duplicateDetection,
                                         final boolean forwardWhenNoConsumers,
                                         final int maxHops,
                                         final int confirmationWindowSize,
                                         final String discoveryGroupName)
   {
      this.name = name;
      this.address = address;
      this.connectorName = connectorName;
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
      this.connectionTTL = connectionTTL;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.maxRetryInterval = maxRetryInterval;
      this.reconnectAttempts = reconnectAttempts;
      this.callTimeout = callTimeout;
      this.callFailoverTimeout = callFailoverTimeout;
      this.duplicateDetection = duplicateDetection;
      this.forwardWhenNoConsumers = forwardWhenNoConsumers;
      this.discoveryGroupName = discoveryGroupName;
      this.staticConnectors = Collections.emptyList();
      this.maxHops = maxHops;
      this.confirmationWindowSize = confirmationWindowSize;
      this.minLargeMessageSize = minLargeMessageSize;
      allowDirectConnectionsOnly = false;
   }

   public String getName()
   {
      return name;
   }

   public String getAddress()
   {
      return address;
   }

   /**
    * @return the clientFailureCheckPeriod
    */
   public long getClientFailureCheckPeriod()
   {
      return clientFailureCheckPeriod;
   }

   /**
    * @return the connectionTTL
    */
   public long getConnectionTTL()
   {
      return connectionTTL;
   }

   /**
    * @return the retryIntervalMultiplier
    */
   public double getRetryIntervalMultiplier()
   {
      return retryIntervalMultiplier;
   }

   /**
    * @return the maxRetryInterval
    */
   public long getMaxRetryInterval()
   {
      return maxRetryInterval;
   }

   /**
    * @return the reconnectAttempts
    */
   public int getReconnectAttempts()
   {
      return reconnectAttempts;
   }

   public long getCallTimeout()
   {
      return callTimeout;
   }

   public long getCallFailoverTimeout()
   {
      return callFailoverTimeout;
   }

   public String getConnectorName()
   {
      return connectorName;
   }

   public boolean isDuplicateDetection()
   {
      return duplicateDetection;
   }

   public boolean isForwardWhenNoConsumers()
   {
      return forwardWhenNoConsumers;
   }

   public int getMaxHops()
   {
      return maxHops;
   }

   public int getConfirmationWindowSize()
   {
      return confirmationWindowSize;
   }

   public List<String> getStaticConnectors()
   {
      return staticConnectors;
   }

   public String getDiscoveryGroupName()
   {
      return discoveryGroupName;
   }

   public long getRetryInterval()
   {
      return retryInterval;
   }

   public boolean isAllowDirectConnectionsOnly()
   {
      return allowDirectConnectionsOnly;
   }


   /**
    * @return the minLargeMessageSize
    */
   public int getMinLargeMessageSize()
   {
      return minLargeMessageSize;
   }

   /**
    * @param clientFailureCheckPeriod the clientFailureCheckPeriod to set
    */
   public void setClientFailureCheckPeriod(long clientFailureCheckPeriod)
   {
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
   }

   /**
    * @param connectionTTL the connectionTTL to set
    */
   public void setConnectionTTL(long connectionTTL)
   {
      this.connectionTTL = connectionTTL;
   }

   /**
    * @param retryInterval the retryInterval to set
    */
   public void setRetryInterval(long retryInterval)
   {
      this.retryInterval = retryInterval;
   }

   /**
    * @param retryIntervalMultiplier the retryIntervalMultiplier to set
    */
   public void setRetryIntervalMultiplier(double retryIntervalMultiplier)
   {
      this.retryIntervalMultiplier = retryIntervalMultiplier;
   }

   /**
    * @param maxRetryInterval the maxRetryInterval to set
    */
   public void setMaxRetryInterval(long maxRetryInterval)
   {
      this.maxRetryInterval = maxRetryInterval;
   }

   /**
    * @param reconnectAttempts the reconnectAttempts to set
    */
   public void setReconnectAttempts(int reconnectAttempts)
   {
      this.reconnectAttempts = reconnectAttempts;
   }

   /**
    * @param callTimeout the callTimeout to set
    */
   public void setCallTimeout(long callTimeout)
   {
      this.callTimeout = callTimeout;
   }

   /**
    * @param callFailoverTimeout the callTimeout to set
    */
   public void setCallFailoverTimeout(long callFailoverTimeout)
   {
      this.callFailoverTimeout = callFailoverTimeout;
   }

   /**
    * @param duplicateDetection the duplicateDetection to set
    */
   public void setDuplicateDetection(boolean duplicateDetection)
   {
      this.duplicateDetection = duplicateDetection;
   }

   /**
    * @param forwardWhenNoConsumers the forwardWhenNoConsumers to set
    */
   public void setForwardWhenNoConsumers(boolean forwardWhenNoConsumers)
   {
      this.forwardWhenNoConsumers = forwardWhenNoConsumers;
   }
}
