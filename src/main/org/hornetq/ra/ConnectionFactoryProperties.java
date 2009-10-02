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
package org.hornetq.ra;

import java.util.Map;

import org.hornetq.core.logging.Logger;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class ConnectionFactoryProperties
{
   /**
    * The logger
    */
   private static final Logger log = Logger.getLogger(HornetQRAMCFProperties.class);

   /**
    * Trace enabled
    */
   private static boolean trace = log.isTraceEnabled();

   private boolean hasBeenUpdated = false;

   /**
    * The transport type, changing the default configured from the RA
    */
   private String connectorClassName;

   /**
    * The transport config, changing the default configured from the RA
    */
   private Map<String, Object> connectionParameters;
   /**
    * The transport config, changing the default configured from the RA
    */
   private Map<String, Object> backupConnectionParameters;

   private String backupConnectorClassName;

   private String connectionLoadBalancingPolicyClassName;

   private String discoveryAddress;

   private Integer discoveryPort;

   private Long discoveryRefreshTimeout;

   private Long discoveryInitialWaitTimeout;

   private String clientID;

   private Integer dupsOKBatchSize;

   private Integer transactionBatchSize;

   private Long clientFailureCheckPeriod;

   private Long connectionTTL;

   private Long callTimeout;

   private Integer consumerWindowSize;

   private Integer consumerMaxRate;

   private Integer producerWindowSize;

   private Integer producerMaxRate;

   private Integer minLargeMessageSize;

   private Boolean blockOnAcknowledge;

   private Boolean blockOnNonPersistentSend;

   private Boolean blockOnPersistentSend;

   private Boolean autoGroup;

   private Boolean preAcknowledge;

   private Long retryInterval;

   private Double retryIntervalMultiplier;

   private Integer reconnectAttempts;

   private Boolean failoverOnServerShutdown;

   private Boolean useGlobalPools;

   private Integer scheduledThreadPoolMaxSize;

   private Integer threadPoolMaxSize;

   /**
    * @return the transportType
    */
   public String getConnectorClassName()
   {
      return connectorClassName;
   }

   public Map<String, Object> getParsedConnectionParameters()
   {
      return connectionParameters;
   }

   public void setParsedConnectionParameters(Map<String, Object> connectionParameters)
   {
      this.connectionParameters = connectionParameters;
      hasBeenUpdated = true;
   }

   public void setConnectorClassName(final String value)
   {
      connectorClassName = value;
      hasBeenUpdated = true;
   }

   public String getBackupConnectorClassName()
   {
      return backupConnectorClassName;
   }


   public Map<String, Object> getParsedBackupConnectionParameters()
   {
      return backupConnectionParameters;
   }

   public void setParsedBackupConnectionParameters(Map<String, Object> backupConnectionParameters)
   {
      this.backupConnectionParameters = backupConnectionParameters;
      hasBeenUpdated = true;
   }

   public void setBackupConnectorClassName(String backupConnectorClassName)
   {
      this.backupConnectorClassName = backupConnectorClassName;
      hasBeenUpdated = true;
   }

   public String getConnectionLoadBalancingPolicyClassName()
   {
      if (trace)
      {
         log.trace("getConnectionLoadBalancingPolicyClassName()");
      }
      hasBeenUpdated = true;
      return connectionLoadBalancingPolicyClassName;
   }

   public void setConnectionLoadBalancingPolicyClassName(String connectionLoadBalancingPolicyClassName)
   {
      if (trace)
      {
         log.trace("setSessionDefaultType(" + connectionLoadBalancingPolicyClassName + ")");
      }
      hasBeenUpdated = true;
      this.connectionLoadBalancingPolicyClassName = connectionLoadBalancingPolicyClassName;
   }

   public String getDiscoveryAddress()
   {
      if (trace)
      {
         log.trace("getDiscoveryAddress()");
      }
      hasBeenUpdated = true;
      return discoveryAddress;
   }

   public void setDiscoveryAddress(String discoveryAddress)
   {
      if (trace)
      {
         log.trace("setDiscoveryAddress(" + discoveryAddress + ")");
      }
      hasBeenUpdated = true;
      this.discoveryAddress = discoveryAddress;
   }

   public Integer getDiscoveryPort()
   {
      if (trace)
      {
         log.trace("getDiscoveryPort()");
      }
      hasBeenUpdated = true;
      return discoveryPort;
   }

   public void setDiscoveryPort(Integer discoveryPort)
   {
      if (trace)
      {
         log.trace("setDiscoveryPort(" + discoveryPort + ")");
      }
      hasBeenUpdated = true;
      this.discoveryPort = discoveryPort;
   }

   public Long getDiscoveryRefreshTimeout()
   {
      if (trace)
      {
         log.trace("getDiscoveryRefreshTimeout()");
      }
      hasBeenUpdated = true;
      return discoveryRefreshTimeout;
   }

   public void setDiscoveryRefreshTimeout(Long discoveryRefreshTimeout)
   {
      if (trace)
      {
         log.trace("setDiscoveryRefreshTimeout(" + discoveryRefreshTimeout + ")");
      }
      hasBeenUpdated = true;
      this.discoveryRefreshTimeout = discoveryRefreshTimeout;
   }

   public Long getDiscoveryInitialWaitTimeout()
   {
      if (trace)
      {
         log.trace("getDiscoveryInitialWaitTimeout()");
      }
      hasBeenUpdated = true;
      return discoveryInitialWaitTimeout;
   }

   public void setDiscoveryInitialWaitTimeout(Long discoveryInitialWaitTimeout)
   {
      if (trace)
      {
         log.trace("setDiscoveryInitialWaitTimeout(" + discoveryInitialWaitTimeout + ")");
      }
      hasBeenUpdated = true;
      this.discoveryInitialWaitTimeout = discoveryInitialWaitTimeout;
   }

   public String getClientID()
   {
      if (trace)
      {
         log.trace("getClientID()");
      }
      hasBeenUpdated = true;
      return clientID;
   }

   public void setClientID(String clientID)
   {
      if (trace)
      {
         log.trace("setClientID(" + clientID + ")");
      }
      hasBeenUpdated = true;
      this.clientID = clientID;
   }

   public Integer getDupsOKBatchSize()
   {
      if (trace)
      {
         log.trace("getDupsOKBatchSize()");
      }
      hasBeenUpdated = true;
      return dupsOKBatchSize;
   }

   public void setDupsOKBatchSize(Integer dupsOKBatchSize)
   {
      if (trace)
      {
         log.trace("setDupsOKBatchSize(" + dupsOKBatchSize + ")");
      }
      hasBeenUpdated = true;
      this.dupsOKBatchSize = dupsOKBatchSize;
   }

   public Integer getTransactionBatchSize()
   {
      if (trace)
      {
         log.trace("getTransactionBatchSize()");
      }
      hasBeenUpdated = true;
      return transactionBatchSize;
   }

   public void setTransactionBatchSize(Integer transactionBatchSize)
   {
      if (trace)
      {
         log.trace("setTransactionBatchSize(" + transactionBatchSize + ")");
      }
      hasBeenUpdated = true;
      this.transactionBatchSize = transactionBatchSize;
   }

   public Long getClientFailureCheckPeriod()
   {
      if (trace)
      {
         log.trace("getClientFailureCheckPeriod()");
      }
      hasBeenUpdated = true;
      return clientFailureCheckPeriod;
   }

   public void setClientFailureCheckPeriod(Long clientFailureCheckPeriod)
   {
      if (trace)
      {
         log.trace("setClientFailureCheckPeriod(" + clientFailureCheckPeriod + ")");
      }
      hasBeenUpdated = true;
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
   }

   public Long getConnectionTTL()
   {
      if (trace)
      {
         log.trace("getConnectionTTL()");
      }
      hasBeenUpdated = true;
      return connectionTTL;
   }

   public void setConnectionTTL(Long connectionTTL)
   {
      if (trace)
      {
         log.trace("setConnectionTTL(" + connectionTTL + ")");
      }
      hasBeenUpdated = true;
      this.connectionTTL = connectionTTL;
   }

   public Long getCallTimeout()
   {
      if (trace)
      {
         log.trace("getCallTimeout()");
      }
      hasBeenUpdated = true;
      return callTimeout;
   }

   public void setCallTimeout(Long callTimeout)
   {
      if (trace)
      {
         log.trace("setCallTimeout(" + callTimeout + ")");
      }
      hasBeenUpdated = true;
      this.callTimeout = callTimeout;
   }

   public Integer getConsumerWindowSize()
   {
      if (trace)
      {
         log.trace("getConsumerWindowSize()");
      }
      hasBeenUpdated = true;
      return consumerWindowSize;
   }

   public void setConsumerWindowSize(Integer consumerWindowSize)
   {
      if (trace)
      {
         log.trace("setConsumerWindowSize(" + consumerWindowSize + ")");
      }
      hasBeenUpdated = true;
      this.consumerWindowSize = consumerWindowSize;
   }

   public Integer getConsumerMaxRate()
   {
      if (trace)
      {
         log.trace("getConsumerMaxRate()");
      }
      hasBeenUpdated = true;
      return consumerMaxRate;
   }

   public void setConsumerMaxRate(Integer consumerMaxRate)
   {
      if (trace)
      {
         log.trace("setConsumerMaxRate(" + consumerMaxRate + ")");
      }
      hasBeenUpdated = true;
      this.consumerMaxRate = consumerMaxRate;
   }

   public Integer getProducerWindowSize()
   {
      if (trace)
      {
         log.trace("getProducerWindowSize()");
      }
      hasBeenUpdated = true;
      return producerWindowSize;
   }

   public void setProducerWindowSize(Integer producerWindowSize)
   {
      if (trace)
      {
         log.trace("setProducerWindowSize(" + producerWindowSize + ")");
      }
      hasBeenUpdated = true;
      this.producerWindowSize = producerWindowSize;
   }

   public Integer getProducerMaxRate()
   {
      if (trace)
      {
         log.trace("getProducerMaxRate()");
      }
      hasBeenUpdated = true;
      return producerMaxRate;
   }

   public void setProducerMaxRate(Integer producerMaxRate)
   {
      if (trace)
      {
         log.trace("setProducerMaxRate(" + producerMaxRate + ")");
      }
      hasBeenUpdated = true;
      this.producerMaxRate = producerMaxRate;
   }

   public Integer getMinLargeMessageSize()
   {
      if (trace)
      {
         log.trace("getMinLargeMessageSize()");
      }
      hasBeenUpdated = true;
      return minLargeMessageSize;
   }

   public void setMinLargeMessageSize(Integer minLargeMessageSize)
   {
      if (trace)
      {
         log.trace("setMinLargeMessageSize(" + minLargeMessageSize + ")");
      }
      hasBeenUpdated = true;
      this.minLargeMessageSize = minLargeMessageSize;
   }

   public Boolean isBlockOnAcknowledge()
   {
      if (trace)
      {
         log.trace("isBlockOnAcknowledge()");
      }
      hasBeenUpdated = true;
      return blockOnAcknowledge;
   }

   public void setBlockOnAcknowledge(Boolean blockOnAcknowledge)
   {
      if (trace)
      {
         log.trace("setBlockOnAcknowledge(" + blockOnAcknowledge + ")");
      }
      hasBeenUpdated = true;
      this.blockOnAcknowledge = blockOnAcknowledge;
   }

   public Boolean isBlockOnNonPersistentSend()
   {
      if (trace)
      {
         log.trace("isBlockOnNonPersistentSend()");
      }
      hasBeenUpdated = true;
      return blockOnNonPersistentSend;
   }

   public void setBlockOnNonPersistentSend(Boolean blockOnNonPersistentSend)
   {
      if (trace)
      {
         log.trace("setBlockOnNonPersistentSend(" + blockOnNonPersistentSend + ")");
      }
      hasBeenUpdated = true;
      this.blockOnNonPersistentSend = blockOnNonPersistentSend;
   }

   public Boolean isBlockOnPersistentSend()
   {
      if (trace)
      {
         log.trace("isBlockOnPersistentSend()");
      }
      hasBeenUpdated = true;
      return blockOnPersistentSend;
   }

   public void setBlockOnPersistentSend(Boolean blockOnPersistentSend)
   {
      if (trace)
      {
         log.trace("setBlockOnPersistentSend(" + blockOnPersistentSend + ")");
      }
      hasBeenUpdated = true;
      this.blockOnPersistentSend = blockOnPersistentSend;
   }

   public Boolean isAutoGroup()
   {
      if (trace)
      {
         log.trace("isAutoGroup()");
      }
      hasBeenUpdated = true;
      return autoGroup;
   }

   public void setAutoGroup(Boolean autoGroup)
   {
      if (trace)
      {
         log.trace("setAutoGroup(" + autoGroup + ")");
      }
      hasBeenUpdated = true;
      this.autoGroup = autoGroup;
   }

   
   public Boolean isPreAcknowledge()
   {
      if (trace)
      {
         log.trace("isPreAcknowledge()");
      }
      hasBeenUpdated = true;
      return preAcknowledge;
   }

   public void setPreAcknowledge(Boolean preAcknowledge)
   {
      if (trace)
      {
         log.trace("setPreAcknowledge(" + preAcknowledge + ")");
      }
      hasBeenUpdated = true;
      this.preAcknowledge = preAcknowledge;
   }

   public Long getRetryInterval()
   {
      if (trace)
      {
         log.trace("getRetryInterval()");
      }
      hasBeenUpdated = true;
      return retryInterval;
   }

   public void setRetryInterval(Long retryInterval)
   {
      if (trace)
      {
         log.trace("setRetryInterval(" + retryInterval + ")");
      }
      hasBeenUpdated = true;
      this.retryInterval = retryInterval;
   }

   public Double getRetryIntervalMultiplier()
   {
      if (trace)
      {
         log.trace("getRetryIntervalMultiplier()");
      }
      hasBeenUpdated = true;
      return retryIntervalMultiplier;
   }

   public void setRetryIntervalMultiplier(Double retryIntervalMultiplier)
   {
      if (trace)
      {
         log.trace("setRetryIntervalMultiplier(" + retryIntervalMultiplier + ")");
      }
      hasBeenUpdated = true;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
   }

   public Integer getReconnectAttempts()
   {
      if (trace)
      {
         log.trace("getReconnectAttempts()");
      }
      hasBeenUpdated = true;
      return reconnectAttempts;
   }

   public void setReconnectAttempts(Integer reconnectAttempts)
   {
      if (trace)
      {
         log.trace("setReconnectAttempts(" + reconnectAttempts + ")");
      }
      hasBeenUpdated = true;
      this.reconnectAttempts = reconnectAttempts;
   }

   public Boolean isFailoverOnServerShutdown()
   {
      if (trace)
      {
         log.trace("isFailoverOnServerShutdown()");
      }
      hasBeenUpdated = true;
      return failoverOnServerShutdown;
   }

   public void setFailoverOnServerShutdown(Boolean failoverOnServerShutdown)
   {
      if (trace)
      {
         log.trace("setFailoverOnServerShutdown(" + failoverOnServerShutdown + ")");
      }
      hasBeenUpdated = true;
      this.failoverOnServerShutdown = failoverOnServerShutdown;
   }

   public Boolean isUseGlobalPools()
   {
      if (trace)
      {
         log.trace("isUseGlobalPools()");
      }
      hasBeenUpdated = true;
      return useGlobalPools;
   }

   public void setUseGlobalPools(Boolean useGlobalPools)
   {
      if (trace)
      {
         log.trace("setUseGlobalPools(" + useGlobalPools + ")");
      }
      hasBeenUpdated = true;
      this.useGlobalPools = useGlobalPools;
   }

   public Integer getScheduledThreadPoolMaxSize()
   {
      if (trace)
      {
         log.trace("getScheduledThreadPoolMaxSize()");
      }
      hasBeenUpdated = true;
      return scheduledThreadPoolMaxSize;
   }

   public void setScheduledThreadPoolMaxSize(Integer scheduledThreadPoolMaxSize)
   {
      if (trace)
      {
         log.trace("setScheduledThreadPoolMaxSize(" + scheduledThreadPoolMaxSize + ")");
      }
      hasBeenUpdated = true;
      this.scheduledThreadPoolMaxSize = scheduledThreadPoolMaxSize;
   }

   public Integer getThreadPoolMaxSize()
   {
      if (trace)
      {
         log.trace("getThreadPoolMaxSize()");
      }
      hasBeenUpdated = true;
      return threadPoolMaxSize;
   }

   public void setThreadPoolMaxSize(Integer threadPoolMaxSize)
   {
      if (trace)
      {
         log.trace("setThreadPoolMaxSize(" + threadPoolMaxSize + ")");
      }
      hasBeenUpdated = true;
      this.threadPoolMaxSize = threadPoolMaxSize;
   }

   public boolean isHasBeenUpdated()
   {
      return hasBeenUpdated;
   }
}
