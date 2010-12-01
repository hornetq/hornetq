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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.Session;
import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterInternalException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.WorkManager;
import javax.transaction.TransactionManager;
import javax.transaction.xa.XAResource;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.core.logging.Logger;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.server.impl.JMSFactoryType;
import org.hornetq.ra.inflow.HornetQActivation;
import org.hornetq.ra.inflow.HornetQActivationSpec;

/**
 * The resource adapter for HornetQ
 *
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @version $Revision: $
 */
public class HornetQResourceAdapter implements ResourceAdapter, Serializable
{
   /**
    * 
    */
   private static final long serialVersionUID = 4756893709825838770L;

   /**
    * The logger
    */
   private static final Logger log = Logger.getLogger(HornetQResourceAdapter.class);

   /**
    * Trace enabled
    */
   private static boolean trace = HornetQResourceAdapter.log.isTraceEnabled();

   /**
    * The bootstrap context
    */
   private BootstrapContext ctx;

   /**
    * The resource adapter properties
    */
   private final HornetQRAProperties raProperties;
   
   /**
    * The resource adapter properties before parsing
    */
   private String unparsedProperties;

   /**
    * Have the factory been configured
    */
   private final AtomicBoolean configured;

   /**
    * The activations by activation spec
    */
   private final Map<ActivationSpec, HornetQActivation> activations;

   private HornetQConnectionFactory defaultHornetQConnectionFactory;
   
   private TransactionManager tm;

   /**
    * Constructor
    */
   public HornetQResourceAdapter()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("constructor()");
      }

      raProperties = new HornetQRAProperties();
      configured = new AtomicBoolean(false);
      activations = new ConcurrentHashMap<ActivationSpec, HornetQActivation>();
   }

   public TransactionManager getTM()
   {
      return tm;
   }

   /**
    * Endpoint activation
    *
    * @param endpointFactory The endpoint factory
    * @param spec            The activation spec
    * @throws ResourceException Thrown if an error occurs
    */
   public void endpointActivation(final MessageEndpointFactory endpointFactory, final ActivationSpec spec) throws ResourceException
   {
      if (!configured.getAndSet(true))
      {
         try
         {
            setup();
         }
         catch (HornetQException e)
         {
            throw new ResourceException("Unable to create activation", e);
         }
      }
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("endpointActivation(" + endpointFactory + ", " + spec + ")");
      }

      HornetQActivation activation = new HornetQActivation(this, endpointFactory, (HornetQActivationSpec)spec);
      activations.put(spec, activation);
      activation.start();
   }

   /**
    * Endpoint deactivation
    *
    * @param endpointFactory The endpoint factory
    * @param spec            The activation spec
    */
   public void endpointDeactivation(final MessageEndpointFactory endpointFactory, final ActivationSpec spec)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("endpointDeactivation(" + endpointFactory + ", " + spec + ")");
      }

      HornetQActivation activation = activations.remove(spec);
      if (activation != null)
      {
         activation.stop();
      }
   }

   /**
    * Get XA resources
    *
    * @param specs The activation specs
    * @return The XA resources
    * @throws ResourceException Thrown if an error occurs or unsupported
    */
   public XAResource[] getXAResources(final ActivationSpec[] specs) throws ResourceException
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getXAResources(" + specs + ")");
      }

      throw new ResourceException("Unsupported");
   }

   /**
    * Start
    *
    * @param ctx The bootstrap context
    * @throws ResourceAdapterInternalException
    *          Thrown if an error occurs
    */
   public void start(final BootstrapContext ctx) throws ResourceAdapterInternalException
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("start(" + ctx + ")");
      }
      
      locateTM();

      this.ctx = ctx;

      HornetQResourceAdapter.log.info("HornetQ resource adaptor started");
   }

   /**
    * Stop
    */
   public void stop()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("stop()");
      }

      for (Map.Entry<ActivationSpec, HornetQActivation> entry : activations.entrySet())
      {
         try
         {
            entry.getValue().stop();
         }
         catch (Exception ignored)
         {
            HornetQResourceAdapter.log.debug("Ignored", ignored);
         }
      }

      activations.clear();

      HornetQResourceAdapter.log.info("HornetQ resource adapter stopped");
   }

   public void setConnectorClassName(final String connectorClassName)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setTransportType(" + connectorClassName + ")");
      }

      raProperties.setConnectorClassName(connectorClassName);
   }

   public String getConnectorClassName()
   {
      return raProperties.getConnectorClassName();
   }

   public String getConnectionParameters()
   {
      return unparsedProperties;
   }

   public void setConnectionParameters(final String config)
   {
      if (config != null)
      {
         this.unparsedProperties = config;
         raProperties.setParsedConnectionParameters(Util.parseConfig(config));
      }
   }

   /**
    * Get the discovery group name
    *
    * @return The value
    */
   public String getDiscoveryAddress()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getDiscoveryGroupAddress()");
      }

      return raProperties.getDiscoveryAddress();
   }

   /**
    * Set the discovery group name
    *
    * @param dgn The value
    */
   public void setDiscoveryAddress(final String dgn)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setDiscoveryGroupAddress(" + dgn + ")");
      }

      raProperties.setDiscoveryAddress(dgn);
   }

   /**
    * Get the discovery group port
    *
    * @return The value
    */
   public Integer getDiscoveryPort()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getDiscoveryGroupPort()");
      }

      return raProperties.getDiscoveryPort();
   }

   /**
    * Set the discovery group port
    *
    * @param dgp The value
    */
   public void setDiscoveryPort(final Integer dgp)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setDiscoveryGroupPort(" + dgp + ")");
      }

      raProperties.setDiscoveryPort(dgp);
   }
   
   public Boolean isHA()
   {
      return raProperties.isHA();
   }
   
   public void setHA(final Boolean ha)
   {
      this.raProperties.setHA(ha);
   }

   /**
    * Get discovery refresh timeout
    *
    * @return The value
    */
   public Long getDiscoveryRefreshTimeout()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getDiscoveryRefreshTimeout()");
      }

      return raProperties.getDiscoveryRefreshTimeout();
   }

   /**
    * Set discovery refresh timeout
    *
    * @param discoveryRefreshTimeout The value
    */
   public void setDiscoveryRefreshTimeout(final Long discoveryRefreshTimeout)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setDiscoveryRefreshTimeout(" + discoveryRefreshTimeout + ")");
      }

      raProperties.setDiscoveryRefreshTimeout(discoveryRefreshTimeout);
   }

   /**
    * Get discovery initial wait timeout
    *
    * @return The value
    */
   public Long getDiscoveryInitialWaitTimeout()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getDiscoveryInitialWaitTimeout()");
      }

      return raProperties.getDiscoveryInitialWaitTimeout();
   }

   /**
    * Set discovery initial wait timeout
    *
    * @param discoveryInitialWaitTimeout The value
    */
   public void setDiscoveryInitialWaitTimeout(final Long discoveryInitialWaitTimeout)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setDiscoveryInitialWaitTimeout(" + discoveryInitialWaitTimeout + ")");
      }

      raProperties.setDiscoveryInitialWaitTimeout(discoveryInitialWaitTimeout);
   }

   /**
    * Get load balancing policy class name
    *
    * @return The value
    */
   public String getLoadBalancingPolicyClassName()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getLoadBalancingPolicyClassName()");
      }

      return raProperties.getConnectionLoadBalancingPolicyClassName();
   }

   /**
    * Set load balancing policy class name
    *
    * @param loadBalancingPolicyClassName The value
    */
   public void setLoadBalancingPolicyClassName(final String loadBalancingPolicyClassName)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setLoadBalancingPolicyClassName(" + loadBalancingPolicyClassName + ")");
      }

      raProperties.setConnectionLoadBalancingPolicyClassName(loadBalancingPolicyClassName);
   }

   /**
    * Get client failure check period
    *
    * @return The value
    */
   public Long getClientFailureCheckPeriod()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getClientFailureCheckPeriod()");
      }

      return raProperties.getClientFailureCheckPeriod();
   }

   /**
    * Set client failure check period
    *
    * @param clientFailureCheckPeriod The value
    */
   public void setClientFailureCheckPeriod(final Long clientFailureCheckPeriod)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setClientFailureCheckPeriod(" + clientFailureCheckPeriod + ")");
      }

      raProperties.setClientFailureCheckPeriod(clientFailureCheckPeriod);
   }

   /**
    * Get connection TTL
    *
    * @return The value
    */
   public Long getConnectionTTL()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getConnectionTTL()");
      }

      return raProperties.getConnectionTTL();
   }

   /**
    * Set connection TTL
    *
    * @param connectionTTL The value
    */
   public void setConnectionTTL(final Long connectionTTL)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setConnectionTTL(" + connectionTTL + ")");
      }

      raProperties.setConnectionTTL(connectionTTL);
   }

   /**
    * Get call timeout
    *
    * @return The value
    */
   public Long getCallTimeout()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getCallTimeout()");
      }

      return raProperties.getCallTimeout();
   }

   /**
    * Set call timeout
    *
    * @param callTimeout The value
    */
   public void setCallTimeout(final Long callTimeout)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setCallTimeout(" + callTimeout + ")");
      }

      raProperties.setCallTimeout(callTimeout);
   }

   /**
    * Get dups ok batch size
    *
    * @return The value
    */
   public Integer getDupsOKBatchSize()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getDupsOKBatchSize()");
      }

      return raProperties.getDupsOKBatchSize();
   }

   /**
    * Set dups ok batch size
    *
    * @param dupsOKBatchSize The value
    */
   public void setDupsOKBatchSize(final Integer dupsOKBatchSize)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setDupsOKBatchSize(" + dupsOKBatchSize + ")");
      }

      raProperties.setDupsOKBatchSize(dupsOKBatchSize);
   }

   /**
    * Get transaction batch size
    *
    * @return The value
    */
   public Integer getTransactionBatchSize()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getTransactionBatchSize()");
      }

      return raProperties.getTransactionBatchSize();
   }

   /**
    * Set transaction batch size
    *
    * @param transactionBatchSize The value
    */
   public void setTransactionBatchSize(final Integer transactionBatchSize)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setTransactionBatchSize(" + transactionBatchSize + ")");
      }

      raProperties.setTransactionBatchSize(transactionBatchSize);
   }

   /**
    * Get consumer window size
    *
    * @return The value
    */
   public Integer getConsumerWindowSize()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getConsumerWindowSize()");
      }

      return raProperties.getConsumerWindowSize();
   }

   /**
    * Set consumer window size
    *
    * @param consumerWindowSize The value
    */
   public void setConsumerWindowSize(final Integer consumerWindowSize)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setConsumerWindowSize(" + consumerWindowSize + ")");
      }

      raProperties.setConsumerWindowSize(consumerWindowSize);
   }

   /**
    * Get consumer max rate
    *
    * @return The value
    */
   public Integer getConsumerMaxRate()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getConsumerMaxRate()");
      }

      return raProperties.getConsumerMaxRate();
   }

   /**
    * Set consumer max rate
    *
    * @param consumerMaxRate The value
    */
   public void setConsumerMaxRate(final Integer consumerMaxRate)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setConsumerMaxRate(" + consumerMaxRate + ")");
      }

      raProperties.setConsumerMaxRate(consumerMaxRate);
   }

   /**
    * Get confirmation window size
    *
    * @return The value
    */
   public Integer getConfirmationWindowSize()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getConfirmationWindowSize()");
      }

      return raProperties.getConfirmationWindowSize();
   }

   /**
    * Set confirmation window size
    *
    * @param confirmationWindowSize The value
    */
   public void setConfirmationWindowSize(final Integer confirmationWindowSize)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setConfirmationWindowSize(" + confirmationWindowSize + ")");
      }

      raProperties.setConfirmationWindowSize(confirmationWindowSize);
   }

   /**
    * Get producer max rate
    *
    * @return The value
    */
   public Integer getProducerMaxRate()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getProducerMaxRate()");
      }

      return raProperties.getProducerMaxRate();
   }

   /**
    * Set producer max rate
    *
    * @param producerMaxRate The value
    */
   public void setProducerMaxRate(final Integer producerMaxRate)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setProducerMaxRate(" + producerMaxRate + ")");
      }

      raProperties.setProducerMaxRate(producerMaxRate);
   }

   /**
    * Get min large message size
    *
    * @return The value
    */
   public Integer getMinLargeMessageSize()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getMinLargeMessageSize()");
      }

      return raProperties.getMinLargeMessageSize();
   }

   /**
    * Set min large message size
    *
    * @param minLargeMessageSize The value
    */
   public void setMinLargeMessageSize(final Integer minLargeMessageSize)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setMinLargeMessageSize(" + minLargeMessageSize + ")");
      }

      raProperties.setMinLargeMessageSize(minLargeMessageSize);
   }

   /**
    * Get block on acknowledge
    *
    * @return The value
    */
   public Boolean getBlockOnAcknowledge()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getBlockOnAcknowledge()");
      }

      return raProperties.isBlockOnAcknowledge();
   }

   /**
    * Set block on acknowledge
    *
    * @param blockOnAcknowledge The value
    */
   public void setBlockOnAcknowledge(final Boolean blockOnAcknowledge)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setBlockOnAcknowledge(" + blockOnAcknowledge + ")");
      }

      raProperties.setBlockOnAcknowledge(blockOnAcknowledge);
   }

   /**
    * Get block on non durable send
    *
    * @return The value
    */
   public Boolean getBlockOnNonDurableSend()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getBlockOnNonDurableSend()");
      }

      return raProperties.isBlockOnNonDurableSend();
   }

   /**
    * Set block on non durable send
    *
    * @param blockOnNonDurableSend The value
    */
   public void setBlockOnNonDurableSend(final Boolean blockOnNonDurableSend)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setBlockOnNonDurableSend(" + blockOnNonDurableSend + ")");
      }

      raProperties.setBlockOnNonDurableSend(blockOnNonDurableSend);
   }

   /**
    * Get block on durable send
    *
    * @return The value
    */
   public Boolean getBlockOnDurableSend()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getBlockOnDurableSend()");
      }

      return raProperties.isBlockOnDurableSend();
   }

   /**
    * Set block on durable send
    *
    * @param blockOnDurableSend The value
    */
   public void setBlockOnDurableSend(final Boolean blockOnDurableSend)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setBlockOnDurableSend(" + blockOnDurableSend + ")");
      }

      raProperties.setBlockOnDurableSend(blockOnDurableSend);
   }

   /**
    * Get auto group
    *
    * @return The value
    */
   public Boolean getAutoGroup()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getAutoGroup()");
      }

      return raProperties.isAutoGroup();
   }

   /**
    * Set auto group
    *
    * @param autoGroup The value
    */
   public void setAutoGroup(final Boolean autoGroup)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setAutoGroup(" + autoGroup + ")");
      }

      raProperties.setAutoGroup(autoGroup);
   }

   /**
    * Get pre acknowledge
    *
    * @return The value
    */
   public Boolean getPreAcknowledge()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getPreAcknowledge()");
      }

      return raProperties.isPreAcknowledge();
   }

   /**
    * Set pre acknowledge
    *
    * @param preAcknowledge The value
    */
   public void setPreAcknowledge(final Boolean preAcknowledge)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setPreAcknowledge(" + preAcknowledge + ")");
      }

      raProperties.setPreAcknowledge(preAcknowledge);
   }

   /**
    * Get retry interval
    *
    * @return The value
    */
   public Long getRetryInterval()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getRetryInterval()");
      }

      return raProperties.getRetryInterval();
   }

   /**
    * Set retry interval
    *
    * @param retryInterval The value
    */
   public void setRetryInterval(final Long retryInterval)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setRetryInterval(" + retryInterval + ")");
      }

      raProperties.setRetryInterval(retryInterval);
   }

   /**
    * Get retry interval multiplier
    *
    * @return The value
    */
   public Double getRetryIntervalMultiplier()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getRetryIntervalMultiplier()");
      }

      return raProperties.getRetryIntervalMultiplier();
   }

   /**
    * Set retry interval multiplier
    *
    * @param retryIntervalMultiplier The value
    */
   public void setRetryIntervalMultiplier(final Double retryIntervalMultiplier)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setRetryIntervalMultiplier(" + retryIntervalMultiplier + ")");
      }

      raProperties.setRetryIntervalMultiplier(retryIntervalMultiplier);
   }

   /**
    * Get number of reconnect attempts
    *
    * @return The value
    */
   public Integer getReconnectAttempts()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getReconnectAttempts()");
      }

      return raProperties.getReconnectAttempts();
   }

   /**
    * Set number of reconnect attempts
    *
    * @param reconnectAttempts The value
    */
   public void setReconnectAttempts(final Integer reconnectAttempts)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setReconnectAttempts(" + reconnectAttempts + ")");
      }

      raProperties.setReconnectAttempts(reconnectAttempts);
   }

   /**
    * Get failover on server shutdown
    *
    * @return The value
    */
   public Boolean isFailoverOnServerShutdown()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("isFailoverOnServerShutdown()");
      }

      return raProperties.isFailoverOnServerShutdown();
   }

   /**
    * Get failover on server shutdown
    *
    * @return The value
    */
   public Boolean getFailoverOnServerShutdown()
   {
      return isFailoverOnServerShutdown();
   }

   /**
    * Set failover on server shutdown
    *
    * @param failoverOnServerShutdown The value
    */
   public void setFailoverOnServerShutdown(final Boolean failoverOnServerShutdown)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setFailoverOnServerShutdown(" + failoverOnServerShutdown + ")");
      }

      raProperties.setFailoverOnServerShutdown(failoverOnServerShutdown);
   }

   public String getConnectionLoadBalancingPolicyClassName()
   {
      return raProperties.getConnectionLoadBalancingPolicyClassName();
   }

   public void setConnectionLoadBalancingPolicyClassName(final String connectionLoadBalancingPolicyClassName)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setFailoverOnServerShutdown(" + connectionLoadBalancingPolicyClassName + ")");
      }
      raProperties.setConnectionLoadBalancingPolicyClassName(connectionLoadBalancingPolicyClassName);
   }

   public Integer getScheduledThreadPoolMaxSize()
   {
      return raProperties.getScheduledThreadPoolMaxSize();
   }

   public void setScheduledThreadPoolMaxSize(final Integer scheduledThreadPoolMaxSize)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setFailoverOnServerShutdown(" + scheduledThreadPoolMaxSize + ")");
      }
      raProperties.setScheduledThreadPoolMaxSize(scheduledThreadPoolMaxSize);
   }

   public Integer getThreadPoolMaxSize()
   {
      return raProperties.getThreadPoolMaxSize();
   }

   public void setThreadPoolMaxSize(final Integer threadPoolMaxSize)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setFailoverOnServerShutdown(" + threadPoolMaxSize + ")");
      }
      raProperties.setThreadPoolMaxSize(threadPoolMaxSize);
   }

   public Boolean getUseGlobalPools()
   {
      return raProperties.isUseGlobalPools();
   }

   public void setUseGlobalPools(final Boolean useGlobalPools)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setFailoverOnServerShutdown(" + useGlobalPools + ")");
      }
      raProperties.setUseGlobalPools(useGlobalPools);
   }

   /**
    * Get the user name
    *
    * @return The value
    */
   public String getUserName()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getUserName()");
      }

      return raProperties.getUserName();
   }

   /**
    * Set the user name
    *
    * @param userName The value
    */
   public void setUserName(final String userName)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setUserName(" + userName + ")");
      }

      raProperties.setUserName(userName);
   }

   /**
    * Get the password
    *
    * @return The value
    */
   public String getPassword()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getPassword()");
      }

      return raProperties.getPassword();
   }

   /**
    * Set the password
    *
    * @param password The value
    */
   public void setPassword(final String password)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setPassword(****)");
      }

      raProperties.setPassword(password);
   }

   /**
    * Get the client ID
    *
    * @return The value
    */
   public String getClientID()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getClientID()");
      }

      return raProperties.getClientID();
   }

   /**
    * Set the client ID
    *
    * @param clientID The client id
    */
   public void setClientID(final String clientID)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setClientID(" + clientID + ")");
      }

      raProperties.setClientID(clientID);
   }

   /**
    * Get the use XA flag
    *
    * @return The value
    */
   public Boolean getUseLocalTx()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getUseLocalTx()");
      }

      return raProperties.getUseLocalTx();
   }

   /**
    * Set the use XA flag
    *
    * @param localTx The value
    */
   public void setUseLocalTx(final Boolean localTx)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setUseXA(" + localTx + ")");
      }

      raProperties.setUseLocalTx(localTx);
   }

   public int getSetupAttempts()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getSetupAttempts()");
      }
      return raProperties.getSetupAttempts();
   }

   public void setSetupAttempts(int setupAttempts)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setSetupAttempts(" + setupAttempts + ")");
      }
      raProperties.setSetupAttempts(setupAttempts);
   }

   public long getSetupInterval()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getSetupInterval()");
      }
      return raProperties.getSetupInterval();
   }

   public void setSetupInterval(long interval)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("setSetupInterval(" + interval + ")");
      }
      raProperties.setSetupInterval(interval);
   }


   /**
    * Indicates whether some other object is "equal to" this one.
    *
    * @param obj Object with which to compare
    * @return True if this object is the same as the obj argument; false otherwise.
    */
   public boolean equals(final Object obj)
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("equals(" + obj + ")");
      }

      if (obj == null)
      {
         return false;
      }

      if (obj instanceof HornetQResourceAdapter)
      {
         return raProperties.equals(((HornetQResourceAdapter)obj).getProperties());
      }
      else
      {
         return false;
      }
   }

   /**
    * Return the hash code for the object
    *
    * @return The hash code
    */
   public int hashCode()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("hashCode()");
      }

      return raProperties.hashCode();
   }

   /**
    * Get the work manager
    *
    * @return The manager
    */
   public WorkManager getWorkManager()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getWorkManager()");
      }

      if (ctx == null)
      {
         return null;
      }

      return ctx.getWorkManager();
   }

   public ClientSession createSession(final ClientSessionFactory parameterFactory,
                                      final int ackMode,
                                      final String user,
                                      final String pass,
                                      final Boolean preAck,
                                      final Integer dupsOkBatchSize,
                                      final Integer transactionBatchSize,
                                      final boolean deliveryTransacted,
                                      final boolean useLocalTx,
                                      final Integer txTimeout) throws Exception
   {

      ClientSession result;

      // if we are CMP or BMP using local tx we ignore the ack mode as we are transactional
      if (deliveryTransacted || useLocalTx)
      {
         int actTxBatchSize = transactionBatchSize != null ? transactionBatchSize
                                                          : HornetQClient.DEFAULT_ACK_BATCH_SIZE;
         if (useLocalTx)
         {
            result = parameterFactory.createSession(user, pass, false, false, false, false, actTxBatchSize);
         }
         else
         {
            result = parameterFactory.createSession(user, pass, true, false, false, false, actTxBatchSize);
         }
      }
      else
      {
         if (preAck != null && preAck)
         {
            result = parameterFactory.createSession(user, pass, false, true, true, true, -1);
         }
         else
         {
            // only auto ack and dups ok are supported
            switch (ackMode)
            {
               case Session.AUTO_ACKNOWLEDGE:
                  result = parameterFactory.createSession(user, pass, false, true, true, false, 0);
                  break;
               case Session.DUPS_OK_ACKNOWLEDGE:
                  int actDupsOkBatchSize = dupsOkBatchSize != null ? dupsOkBatchSize
                                                                  : HornetQClient.DEFAULT_ACK_BATCH_SIZE;
                  result = parameterFactory.createSession(user, pass, false, true, true, false, actDupsOkBatchSize);
                  break;
               default:
                  throw new IllegalArgumentException("Invalid ackmode: " + ackMode);
            }
         }
      }

      HornetQResourceAdapter.log.debug("Using queue connection " + result);

      return result;

   }

   /**
    * Get the resource adapter properties
    *
    * @return The properties
    */
   protected HornetQRAProperties getProperties()
   {
      if (HornetQResourceAdapter.trace)
      {
         HornetQResourceAdapter.log.trace("getProperties()");
      }

      return raProperties;
   }

   /**
    * Setup the factory
    */
   protected void setup() throws HornetQException
   {
      defaultHornetQConnectionFactory = createHornetQConnectionFactory(raProperties);
   }

   public HornetQConnectionFactory getDefaultHornetQConnectionFactory() throws ResourceException
   {
      if (!configured.getAndSet(true))
      {
         try
         {
            setup();
         }
         catch (HornetQException e)
         {
            throw new ResourceException("Unable to create activation", e);
         }
      }
      return defaultHornetQConnectionFactory;
   }

   //TODO - currently RA only allows a single target server to be specified we should allow a list of servers to be passed in
   public HornetQConnectionFactory createHornetQConnectionFactory(final ConnectionFactoryProperties overrideProperties)
   {
      HornetQConnectionFactory cf;
      String connectorClassName = overrideProperties.getConnectorClassName() != null ? overrideProperties.getConnectorClassName()
                                                                                    : getConnectorClassName();
      String discoveryAddress = overrideProperties.getDiscoveryAddress() != null ? overrideProperties.getDiscoveryAddress()
                                                                                : getDiscoveryAddress();
      
      Boolean ha = overrideProperties.isHA() != null ? overrideProperties.isHA() : isHA();
      
      if (connectorClassName != null)
      {
         Map<String, Object> connectionParams =
               overrideConnectionParameters(overrideProperties.getParsedConnectionParameters(),raProperties.getParsedConnectionParameters());
         
         TransportConfiguration transportConf = new TransportConfiguration(connectorClassName, connectionParams);
         
         if (ha)
         {
            cf = HornetQJMSClient.createConnectionFactoryWithHA(JMSFactoryType.XA_CF, new TransportConfiguration[] {transportConf});
         }
         else
         {
            cf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.XA_CF, new TransportConfiguration[] {transportConf});
         }
      }
      else if (discoveryAddress != null)
      {
         Integer discoveryPort = overrideProperties.getDiscoveryPort() != null ? overrideProperties.getDiscoveryPort()
                                                                              : getDiscoveryPort();
         
         if (ha)
         {
            cf = HornetQJMSClient.createConnectionFactoryWithHA(discoveryAddress, discoveryPort, JMSFactoryType.XA_CF);
         }
         else
         {
            cf = HornetQJMSClient.createConnectionFactoryWithoutHA(discoveryAddress, discoveryPort, JMSFactoryType.XA_CF);
         }
      }
      else
      {
         throw new IllegalArgumentException("must provide either TransportType or DiscoveryGroupAddress and DiscoveryGroupPort for HornetQ ResourceAdapter Connection Factory");
      }
      setParams(cf, overrideProperties);
      return cf;
   }

   public Map<String, Object> overrideConnectionParameters(final Map<String, Object> connectionParams,
                                                           final Map<String, Object> overrideConnectionParams)
   {
      Map<String, Object> map = new HashMap<String, Object>();
      if(connectionParams != null)
      {
         map.putAll(connectionParams);
      }
      if(overrideConnectionParams != null)
      {
         for (Map.Entry<String, Object> stringObjectEntry : overrideConnectionParams.entrySet())
         {
            map.put(stringObjectEntry.getKey(), stringObjectEntry.getValue());
         }
      }
      return map;
   }
   
   private void locateTM()
   {
      String locatorClasses[] = raProperties.getTransactionManagerLocatorClass().split(";");
      String locatorMethods[] = raProperties.getTransactionManagerLocatorMethod().split(";");
      
      for (int i = 0 ; i < locatorClasses.length; i++)
      {
         tm = Util.locateTM(locatorClasses[i], locatorMethods[i]);
         if (tm != null)
         {
            break;
         }
      }
      
      if (tm == null)
      {
         log.warn("It wasn't possible to lookup for a Transaction Manager through the configured properties TransactionManagerLocatorClass and TransactionManagerLocatorMethod");
         log.warn("HornetQ Resource Adapter won't be able to set and verify transaction timeouts in certain cases.");
      }
      else
      {
         log.debug("TM located = " + tm);
      }
   }
   
   private void setParams(final HornetQConnectionFactory cf,
                          final ConnectionFactoryProperties overrideProperties)
   {
      Boolean val = overrideProperties.isAutoGroup() != null ? overrideProperties.isAutoGroup()
                                                            : raProperties.isAutoGroup();
      if (val != null)
      {
         cf.setAutoGroup(val);
      }
      val = overrideProperties.isBlockOnAcknowledge() != null ? overrideProperties.isBlockOnAcknowledge()
                                                             : raProperties.isBlockOnAcknowledge();
      if (val != null)
      {
         cf.setBlockOnAcknowledge(val);
      }
      val = overrideProperties.isBlockOnNonDurableSend() != null ? overrideProperties.isBlockOnNonDurableSend()
                                                                   : raProperties.isBlockOnNonDurableSend();
      if (val != null)
      {
         cf.setBlockOnNonDurableSend(val);
      }
      val = overrideProperties.isBlockOnDurableSend() != null ? overrideProperties.isBlockOnDurableSend()
                                                                : raProperties.isBlockOnDurableSend();
      if (val != null)
      {
         cf.setBlockOnDurableSend(val);
      }
      val = overrideProperties.isPreAcknowledge() != null ? overrideProperties.isPreAcknowledge()
                                                         : raProperties.isPreAcknowledge();
      if (val != null)
      {
         cf.setPreAcknowledge(val);
      }
      val = overrideProperties.isUseGlobalPools() != null ? overrideProperties.isUseGlobalPools()
                                                         : raProperties.isUseGlobalPools();
      if (val != null)
      {
         cf.setUseGlobalPools(val);
      }
      Integer val2 = overrideProperties.getConsumerMaxRate() != null ? overrideProperties.getConsumerMaxRate()
                                                                    : raProperties.getConsumerMaxRate();
      if (val2 != null)
      {
         cf.setConsumerMaxRate(val2);
      }
      val2 = overrideProperties.getConsumerWindowSize() != null ? overrideProperties.getConsumerWindowSize()
                                                               : raProperties.getConsumerWindowSize();
      if (val2 != null)
      {
         cf.setConsumerWindowSize(val2);
      }
      val2 = overrideProperties.getDupsOKBatchSize() != null ? overrideProperties.getDupsOKBatchSize()
                                                            : raProperties.getDupsOKBatchSize();
      if (val2 != null)
      {
         cf.setDupsOKBatchSize(val2);
      }

      val2 = overrideProperties.getMinLargeMessageSize() != null ? overrideProperties.getMinLargeMessageSize()
                                                                : raProperties.getMinLargeMessageSize();
      if (val2 != null)
      {
         cf.setMinLargeMessageSize(val2);
      }
      val2 = overrideProperties.getProducerMaxRate() != null ? overrideProperties.getProducerMaxRate()
                                                            : raProperties.getProducerMaxRate();
      if (val2 != null)
      {
         cf.setProducerMaxRate(val2);
      }
      val2 = overrideProperties.getConfirmationWindowSize() != null ? overrideProperties.getConfirmationWindowSize()
                                                                   : raProperties.getConfirmationWindowSize();
      if (val2 != null)
      {
         cf.setConfirmationWindowSize(val2);
      }
      val2 = overrideProperties.getReconnectAttempts() != null ? overrideProperties.getReconnectAttempts()
                                                              : raProperties.getReconnectAttempts();
      if (val2 != null)
      {
         cf.setReconnectAttempts(val2);
      }
      val2 = overrideProperties.getThreadPoolMaxSize() != null ? overrideProperties.getThreadPoolMaxSize()
                                                              : raProperties.getThreadPoolMaxSize();
      if (val2 != null)
      {
         cf.setThreadPoolMaxSize(val2);
      }
      val2 = overrideProperties.getScheduledThreadPoolMaxSize() != null ? overrideProperties.getScheduledThreadPoolMaxSize()
                                                                       : raProperties.getScheduledThreadPoolMaxSize();
      if (val2 != null)
      {
         cf.setScheduledThreadPoolMaxSize(val2);
      }
      val2 = overrideProperties.getTransactionBatchSize() != null ? overrideProperties.getTransactionBatchSize()
                                                                 : raProperties.getTransactionBatchSize();
      if (val2 != null)
      {
         cf.setTransactionBatchSize(val2);
      }
      Long val3 = overrideProperties.getClientFailureCheckPeriod() != null ? overrideProperties.getClientFailureCheckPeriod()
                                                                          : raProperties.getClientFailureCheckPeriod();
      if (val3 != null)
      {
         cf.setClientFailureCheckPeriod(val3);
      }
      val3 = overrideProperties.getCallTimeout() != null ? overrideProperties.getCallTimeout()
                                                        : raProperties.getCallTimeout();
      if (val3 != null)
      {
         cf.setCallTimeout(val3);
      }
      val3 = overrideProperties.getConnectionTTL() != null ? overrideProperties.getConnectionTTL()
                                                          : raProperties.getConnectionTTL();
      if (val3 != null)
      {
         cf.setConnectionTTL(val3);
      }
      val3 = overrideProperties.getDiscoveryInitialWaitTimeout() != null ? overrideProperties.getDiscoveryInitialWaitTimeout()
                                                                        : raProperties.getDiscoveryInitialWaitTimeout();
      if (val3 != null)
      {
         cf.setDiscoveryInitialWaitTimeout(val3);
      }
      val3 = overrideProperties.getDiscoveryRefreshTimeout() != null ? overrideProperties.getDiscoveryRefreshTimeout()
                                                                    : raProperties.getDiscoveryRefreshTimeout();
      if (val3 != null)
      {
         cf.setDiscoveryRefreshTimeout(val3);
      }
      val3 = overrideProperties.getRetryInterval() != null ? overrideProperties.getRetryInterval()
                                                          : raProperties.getRetryInterval();
      if (val3 != null)
      {
         cf.setRetryInterval(val3);
      }
      Double val4 = overrideProperties.getRetryIntervalMultiplier() != null ? overrideProperties.getRetryIntervalMultiplier()
                                                                           : raProperties.getRetryIntervalMultiplier();
      if (val4 != null)
      {
         cf.setRetryIntervalMultiplier(val4);
      }
      String val5 = overrideProperties.getClientID() != null ? overrideProperties.getClientID()
                                                            : raProperties.getClientID();
      if (val5 != null)
      {
         cf.setClientID(val5);
      }
      val5 = overrideProperties.getConnectionLoadBalancingPolicyClassName() != null ? overrideProperties.getConnectionLoadBalancingPolicyClassName()
                                                                                   : raProperties.getConnectionLoadBalancingPolicyClassName();
      if (val5 != null)
      {
         cf.setConnectionLoadBalancingPolicyClassName(val5);
      }
   }

}
