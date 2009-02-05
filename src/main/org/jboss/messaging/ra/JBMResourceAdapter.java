/*
 * JBoss, Home of Professional Open Source.
 * Copyright 2009, Red Hat Middleware LLC, and individual contributors
 * as indicated by the @author tags. See the copyright.txt file in the
 * distribution for a full listing of individual contributors.
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
package org.jboss.messaging.ra;

import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.messaging.ra.inflow.JBMActivation;
import org.jboss.messaging.ra.inflow.JBMActivationSpec;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.resource.NotSupportedException;
import javax.resource.ResourceException;
import javax.resource.spi.ActivationSpec;
import javax.resource.spi.BootstrapContext;
import javax.resource.spi.ResourceAdapter;
import javax.resource.spi.ResourceAdapterInternalException;
import javax.resource.spi.endpoint.MessageEndpointFactory;
import javax.resource.spi.work.WorkManager;
import javax.transaction.xa.XAResource;

/**
 * The resource adapter for JBoss Messaging
 * 
 * @author <a href="adrian@jboss.com">Adrian Brock</a>
 * @author <a href="jesper.pedersen@jboss.org">Jesper Pedersen</a>
 * @version $Revision: $
 */
public class JBMResourceAdapter implements ResourceAdapter
{
   /** The logger */
   private static final Logger log = Logger.getLogger(JBMResourceAdapter.class);

   /** Trace enabled */
   private static boolean trace = log.isTraceEnabled();

   /** The bootstrap context */
   private BootstrapContext ctx;

   /** The resource adapter properties */
   private JBMRAProperties raProperties;

   /** The JBoss connection factory */
   private JBossConnectionFactory factory;
   
   /** Have the factory been configured */
   private AtomicBoolean configured;

   /** The activations by activation spec */
   private Map activations;

   /**
    * Constructor
    */
   public JBMResourceAdapter()
   {
      if (trace)
         log.trace("constructor()");

      raProperties = new JBMRAProperties();
      factory = null;
      configured = new AtomicBoolean(false);
      activations = new ConcurrentHashMap();
   }

   /**
    * Endpoint activation
    * @param endpointFactory The endpoint factory
    * @param spec The activation spec
    * @exception ResourceException Thrown if an error occurs
    */
   public void endpointActivation(MessageEndpointFactory endpointFactory, ActivationSpec spec) throws ResourceException
   {
      if (trace)
         log.trace("endpointActivation(" + endpointFactory + ", " + spec + ")");

      JBMActivation activation = new JBMActivation(this, endpointFactory, (JBMActivationSpec) spec);
      activations.put(spec, activation);
      activation.start();
   }

   /**
    * Endpoint deactivation
    * @param endpointFactory The endpoint factory
    * @param spec The activation spec
    */
   public void endpointDeactivation(MessageEndpointFactory endpointFactory, ActivationSpec spec)
   {
      if (trace)
         log.trace("endpointDeactivation(" + endpointFactory + ", " + spec + ")");

      JBMActivation activation = (JBMActivation) activations.remove(spec);
      if (activation != null)
         activation.stop();
   }
   
   /**
    * Get XA resources
    * @param specs The activation specs
    * @return The XA resources
    * @exception ResourceException Thrown if an error occurs or unsupported
    */
   public XAResource[] getXAResources(ActivationSpec[] specs) throws ResourceException
   {
      if (trace)
         log.trace("getXAResources(" + specs + ")");

      throw new ResourceException("Unsupported");
   }
   
   /**
    * Start
    * @param ctx The bootstrap context
    * @exception ResourceAdapterInternalException Thrown if an error occurs
    */
   public void start(BootstrapContext ctx) throws ResourceAdapterInternalException
   {
      if (trace)
         log.trace("start(" + ctx + ")");

      this.ctx = ctx;

      log.info("JBoss Messaging resource adapter started");
   }

   /**
    * Stop
    */
   public void stop()
   {
      if (trace)
         log.trace("stop()");

      for (Iterator i = activations.entrySet().iterator(); i.hasNext();)
      {
         Map.Entry entry = (Map.Entry) i.next();
         try
         {
            JBMActivation activation = (JBMActivation) entry.getValue();
            if (activation != null)
               activation.stop();
         }
         catch (Exception ignored)
         {
            log.debug("Ignored", ignored);
         }
         i.remove();
      }

      log.info("JBoss Messaging resource adapter stopped");
   }

   /**
    * Get the discovery group name
    * @return The value
    */
   public String getDiscoveryGroupName()
   {
      if (trace)
         log.trace("getDiscoveryGroupName()");

      return raProperties.getDiscoveryGroupName();
   }

   /**
    * Set the discovery group name
    * @param dgn The value
    */
   public void setDiscoveryGroupName(String dgn)
   {
      if (trace)
         log.trace("setDiscoveryGroupName(" + dgn + ")");

      raProperties.setDiscoveryGroupName(dgn);
   }

   /**
    * Get the discovery group port
    * @return The value
    */
   public Integer getDiscoveryGroupPort()
   {
      if (trace)
         log.trace("getDiscoveryGroupPort()");

      return raProperties.getDiscoveryGroupPort();
   }

   /**
    * Set the discovery group port
    * @param dgp The value
    */
   public void setDiscoveryGroupPort(Integer dgp)
   {
      if (trace)
         log.trace("setDiscoveryGroupPort(" + dgp + ")");

      raProperties.setDiscoveryGroupPort(dgp);
   }

   /**
    * Get discovery refresh timeout
    * @return The value
    */
   public Long getDiscoveryRefreshTimeout()
   {
      if (trace)
         log.trace("getDiscoveryRefreshTimeout()");

      return raProperties.getDiscoveryRefreshTimeout();
   }

   /**
    * Set discovery refresh timeout
    * @param discoveryRefreshTimeout The value
    */
   public void setDiscoveryRefreshTimeout(Long discoveryRefreshTimeout)
   {
      if (trace)
         log.trace("setDiscoveryRefreshTimeout(" + discoveryRefreshTimeout + ")");

      raProperties.setDiscoveryRefreshTimeout(discoveryRefreshTimeout);
   }

   /**
    * Get discovery initial wait timeout
    * @return The value
    */
   public Long getDiscoveryInitialWaitTimeout()
   {
      if (trace)
         log.trace("getDiscoveryInitialWaitTimeout()");

      return raProperties.getDiscoveryInitialWaitTimeout();
   }

   /**
    * Set discovery initial wait timeout
    * @param discoveryInitialWaitTimeout The value
    */
   public void setDiscoveryInitialWaitTimeout(Long discoveryInitialWaitTimeout)
   {
      if (trace)
         log.trace("setDiscoveryInitialWaitTimeout(" + discoveryInitialWaitTimeout + ")");

      raProperties.setDiscoveryInitialWaitTimeout(discoveryInitialWaitTimeout);
   }

   /**
    * Get load balancing policy class name
    * @return The value
    */
   public String getLoadBalancingPolicyClassName()
   {
      if (trace)
         log.trace("getLoadBalancingPolicyClassName()");

      return raProperties.getLoadBalancingPolicyClassName();
   }

   /**
    * Set load balancing policy class name
    * @param loadBalancingPolicyClassName The value
    */
   public void setLoadBalancingPolicyClassName(String loadBalancingPolicyClassName)
   {
      if (trace)
         log.trace("setLoadBalancingPolicyClassName(" + loadBalancingPolicyClassName + ")");

      raProperties.setLoadBalancingPolicyClassName(loadBalancingPolicyClassName);
   }

   /**
    * Get ping period
    * @return The value
    */
   public Long getPingPeriod()
   {
      if (trace)
         log.trace("getPingPeriod()");

      return raProperties.getPingPeriod();
   }

   /**
    * Set ping period
    * @param pingPeriod The value
    */
   public void setPingPeriod(Long pingPeriod)
   {
      if (trace)
         log.trace("setPingPeriod(" + pingPeriod + ")");

      raProperties.setPingPeriod(pingPeriod);
   }

   /**
    * Get connection TTL
    * @return The value
    */
   public Long getConnectionTTL()
   {
      if (trace)
         log.trace("getConnectionTTL()");

      return raProperties.getConnectionTTL();
   }

   /**
    * Set connection TTL
    * @param connectionTTL The value
    */
   public void setConnectionTTL(Long connectionTTL)
   {
      if (trace)
         log.trace("setConnectionTTL(" + connectionTTL + ")");

      raProperties.setConnectionTTL(connectionTTL);
   }

   /**
    * Get call timeout
    * @return The value
    */
   public Long getCallTimeout()
   {
      if (trace)
         log.trace("getCallTimeout()");

      return raProperties.getCallTimeout();
   }

   /**
    * Set call timeout
    * @param callTimeout The value
    */
   public void setCallTimeout(Long callTimeout)
   {
      if (trace)
         log.trace("setCallTimeout(" + callTimeout + ")");

      raProperties.setCallTimeout(callTimeout);
   }

   /**
    * Get dups ok batch size
    * @return The value
    */
   public Integer getDupsOKBatchSize()
   {
      if (trace)
         log.trace("getDupsOKBatchSize()");

      return raProperties.getDupsOKBatchSize();
   }

   /**
    * Set dups ok batch size
    * @param dupsOKBatchSize The value
    */
   public void setDupsOKBatchSize(Integer dupsOKBatchSize)
   {
      if (trace)
         log.trace("setDupsOKBatchSize(" + dupsOKBatchSize + ")");

      raProperties.setDupsOKBatchSize(dupsOKBatchSize);
   }

   /**
    * Get transaction batch size
    * @return The value
    */
   public Integer getTransactionBatchSize()
   {
      if (trace)
         log.trace("getTransactionBatchSize()");

      return raProperties.getTransactionBatchSize();
   }

   /**
    * Set transaction batch size
    * @param transactionBatchSize The value
    */
   public void setTransactionBatchSize(Integer transactionBatchSize)
   {
      if (trace)
         log.trace("setTransactionBatchSize(" + transactionBatchSize + ")");

      raProperties.setTransactionBatchSize(transactionBatchSize);
   }

   /**
    * Get consumer window size
    * @return The value
    */
   public Integer getConsumerWindowSize()
   {
      if (trace)
         log.trace("getConsumerWindowSize()");

      return raProperties.getConsumerWindowSize();
   }

   /**
    * Set consumer window size
    * @param consumerWindowSize The value
    */
   public void setConsumerWindowSize(Integer consumerWindowSize)
   {
      if (trace)
         log.trace("setConsumerWindowSize(" + consumerWindowSize + ")");

      raProperties.setConsumerWindowSize(consumerWindowSize);
   }

   /**
    * Get consumer max rate
    * @return The value
    */
   public Integer getConsumerMaxRate()
   {
      if (trace)
         log.trace("getConsumerMaxRate()");

      return raProperties.getConsumerMaxRate();
   }

   /**
    * Set consumer max rate
    * @param consumerMaxRate The value
    */
   public void setConsumerMaxRate(Integer consumerMaxRate)
   {
      if (trace)
         log.trace("setConsumerMaxRate(" + consumerMaxRate + ")");

      raProperties.setConsumerMaxRate(consumerMaxRate);
   }

   /**
    * Get send window size
    * @return The value
    */
   public Integer getSendWindowSize()
   {
      if (trace)
         log.trace("getSendWindowSize()");

      return raProperties.getSendWindowSize();
   }

   /**
    * Set send window size
    * @param sendWindowSize The value
    */
   public void setSendWindowSize(Integer sendWindowSize)
   {
      if (trace)
         log.trace("setSendWindowSize(" + sendWindowSize + ")");

      raProperties.setSendWindowSize(sendWindowSize);
   }

   /**
    * Get producer max rate
    * @return The value
    */
   public Integer getProducerMaxRate()
   {
      if (trace)
         log.trace("getProducerMaxRate()");

      return raProperties.getProducerMaxRate();
   }

   /**
    * Set producer max rate
    * @param producerMaxRate The value
    */
   public void setProducerMaxRate(Integer producerMaxRate)
   {
      if (trace)
         log.trace("setProducerMaxRate(" + producerMaxRate + ")");

      raProperties.setProducerMaxRate(producerMaxRate);
   }

   /**
    * Get min large message size
    * @return The value
    */
   public Integer getMinLargeMessageSize()
   {
      if (trace)
         log.trace("getMinLargeMessageSize()");

      return raProperties.getMinLargeMessageSize();
   }

   /**
    * Set min large message size
    * @param minLargeMessageSize The value
    */
   public void setMinLargeMessageSize(Integer minLargeMessageSize)
   {
      if (trace)
         log.trace("setMinLargeMessageSize(" + minLargeMessageSize + ")");

      raProperties.setMinLargeMessageSize(minLargeMessageSize);
   }

   /**
    * Get block on acknowledge
    * @return The value
    */
   public Boolean getBlockOnAcknowledge()
   {
      if (trace)
         log.trace("getBlockOnAcknowledge()");

      return raProperties.getBlockOnAcknowledge();
   }

   /**
    * Set block on acknowledge
    * @param blockOnAcknowledge The value
    */
   public void setBlockOnAcknowledge(Boolean blockOnAcknowledge)
   {
      if (trace)
         log.trace("setBlockOnAcknowledge(" + blockOnAcknowledge + ")");

      raProperties.setBlockOnAcknowledge(blockOnAcknowledge);
   }

   /**
    * Get block on non persistent send
    * @return The value
    */
   public Boolean getBlockOnNonPersistentSend()
   {
      if (trace)
         log.trace("getBlockOnNonPersistentSend()");

      return raProperties.getBlockOnNonPersistentSend();
   }

   /**
    * Set block on non persistent send
    * @param blockOnNonPersistentSend The value
    */
   public void setBlockOnNonPersistentSend(Boolean blockOnNonPersistentSend)
   {
      if (trace)
         log.trace("setBlockOnNonPersistentSend(" + blockOnNonPersistentSend + ")");

      raProperties.setBlockOnNonPersistentSend(blockOnNonPersistentSend);
   }

   /**
    * Get block on persistent send
    * @return The value
    */
   public Boolean getBlockOnPersistentSend()
   {
      if (trace)
         log.trace("getBlockOnPersistentSend()");

      return raProperties.getBlockOnPersistentSend();
   }

   /**
    * Set block on persistent send
    * @param blockOnPersistentSend The value
    */
   public void setBlockOnPersistentSend(Boolean blockOnPersistentSend)
   {
      if (trace)
         log.trace("setBlockOnPersistentSend(" + blockOnPersistentSend + ")");

      raProperties.setBlockOnPersistentSend(blockOnPersistentSend);
   }

   /**
    * Get auto group
    * @return The value
    */
   public Boolean getAutoGroup()
   {
      if (trace)
         log.trace("getAutoGroup()");

      return raProperties.getAutoGroup();
   }

   /**
    * Set auto group
    * @param autoGroup The value
    */
   public void setAutoGroup(Boolean autoGroup)
   {
      if (trace)
         log.trace("setAutoGroup(" + autoGroup + ")");

      raProperties.setAutoGroup(autoGroup);
   }

   /**
    * Get max connections
    * @return The value
    */
   public Integer getMaxConnections()
   {
      if (trace)
         log.trace("getMaxConnections()");

      return raProperties.getMaxConnections();
   }

   /**
    * Set max connections
    * @param maxConnections The value
    */
   public void setMaxConnections(Integer maxConnections)
   {
      if (trace)
         log.trace("setMaxConnections(" + maxConnections + ")");

      raProperties.setMaxConnections(maxConnections);
   }

   /**
    * Get pre acknowledge
    * @return The value
    */
   public Boolean getPreAcknowledge()
   {
      if (trace)
         log.trace("getPreAcknowledge()");

      return raProperties.getPreAcknowledge();
   }

   /**
    * Set pre acknowledge
    * @param preAcknowledge The value
    */
   public void setPreAcknowledge(Boolean preAcknowledge)
   {
      if (trace)
         log.trace("setPreAcknowledge(" + preAcknowledge + ")");

      raProperties.setPreAcknowledge(preAcknowledge);
   }

   /**
    * Get retry interval
    * @return The value
    */
   public Long getRetryInterval()
   {
      if (trace)
         log.trace("getRetryInterval()");

      return raProperties.getRetryInterval();
   }

   /**
    * Set retry interval
    * @param retryInterval The value
    */
   public void setRetryInterval(Long retryInterval)
   {
      if (trace)
         log.trace("setRetryInterval(" + retryInterval + ")");

      raProperties.setRetryInterval(retryInterval);
   }

   /**
    * Get retry interval multiplier
    * @return The value
    */
   public Double getRetryIntervalMultiplier()
   {
      if (trace)
         log.trace("getRetryIntervalMultiplier()");

      return raProperties.getRetryIntervalMultiplier();
   }

   /**
    * Set retry interval multiplier
    * @param retryIntervalMultiplier The value
    */
   public void setRetryIntervalMultiplier(Double retryIntervalMultiplier)
   {
      if (trace)
         log.trace("setRetryIntervalMultiplier(" + retryIntervalMultiplier + ")");

      raProperties.setRetryIntervalMultiplier(retryIntervalMultiplier);
   }

   /**
    * Get max retries before failover
    * @return The value
    */
   public Integer getMaxRetriesBeforeFailover()
   {
      if (trace)
         log.trace("getMaxRetriesBeforeFailover()");

      return raProperties.getMaxRetriesBeforeFailover();
   }

   /**
    * Set max retries before failover
    * @param maxRetriesBeforeFailover The value
    */
   public void setMaxRetriesBeforeFailover(Integer maxRetriesBeforeFailover)
   {
      if (trace)
         log.trace("setMaxRetriesBeforeFailover(" + maxRetriesBeforeFailover + ")");

      raProperties.setMaxRetriesBeforeFailover(maxRetriesBeforeFailover);
   }

   /**
    * Get max retries after failover
    * @return The value
    */
   public Integer getMaxRetriesAfterFailover()
   {
      if (trace)
         log.trace("getMaxRetriesAfterFailover()");

      return raProperties.getMaxRetriesAfterFailover();
   }

   /**
    * Set max retries after failover
    * @param maxRetriesAfterFailover The value
    */
   public void setMaxRetriesAfterFailover(Integer maxRetriesAfterFailover)
   {
      if (trace)
         log.trace("setMaxRetriesAfterFailover(" + maxRetriesAfterFailover + ")");

      raProperties.setMaxRetriesAfterFailover(maxRetriesAfterFailover);
   }

   /**
    * Get the user name
    * @return The value
    */
   public String getUserName()
   {
      if (trace)
         log.trace("getUserName()");

      return raProperties.getUserName();
   }

   /**
    * Set the user name
    * @param userName The value
    */
   public void setUserName(String userName)
   {
      if (trace)
         log.trace("setUserName(" + userName + ")");

      raProperties.setUserName(userName);
   }

   /**
    * Get the password
    * @return The value
    */
   public String getPassword()
   {
      if (trace)
         log.trace("getPassword()");

      return raProperties.getPassword();
   }

   /**
    * Set the password
    * @param password The value
    */
   public void setPassword(String password)
   {
      if (trace)
         log.trace("setPassword(****)");

      raProperties.setPassword(password);
   }
   
   /**
    * Get the client ID
    * @return The value
    */
   public String getClientID()
   {
      if (trace)
         log.trace("getClientID()");

      return raProperties.getClientID();
   }

   /**
    * Set the client ID
    * @param clientId The client id
    */
   public void setClientID(String clientID)
   {
      if (trace)
         log.trace("setClientID(" + clientID + ")");

      raProperties.setClientID(clientID);
   }
   
   /**
    * Get the use XA flag
    * @return The value
    */
   public Boolean getUseXA()
   {
      if (trace)
         log.trace("getUseXA()");

      return raProperties.getUseXA();
   }

   /**
    * Set the use XA flag
    * @param xa The value
    */
   public void setUseXA(Boolean xa)
   {
      if (trace)
         log.trace("setUseXA(" + xa + ")");

      raProperties.setUseXA(xa);
   }

   /**
    * Indicates whether some other object is "equal to" this one.
    * @param obj Object with which to compare
    * @return True if this object is the same as the obj argument; false otherwise.
    */
   public boolean equals(Object obj)
   {
      if (trace)
         log.trace("equals(" + obj + ")");

      if (obj == null)
         return false;

      if (obj instanceof JBMResourceAdapter)
      {
         return raProperties.equals(((JBMResourceAdapter)obj).getProperties());
      }
      else
      {
         return false;
      }
   }

   /**
    * Return the hash code for the object
    * @return The hash code
    */
   public int hashCode()
   {
      if (trace)
         log.trace("hashCode()");

      return raProperties.hashCode();
   }

   /**
    * Get the work manager
    * @return The manager
    */
   public WorkManager getWorkManager()
   {
      if (trace)
         log.trace("getWorkManager()");

      if (ctx == null)
         return null;

      return ctx.getWorkManager();
   }

   /**
    * Get the JBoss connection factory
    * @return The factory
    */
   public JBossConnectionFactory getJBossConnectionFactory()
   {
      if (!configured.get()) {
         setup();
      }

      return factory;
   }

   /**
    * Get the resource adapter properties
    * @return The properties
    */
   protected JBMRAProperties getProperties()
   {
      if (trace)
         log.trace("getProperties()");

      return raProperties;
   }

   /**
    * Setup the factory
    */
   protected void setup()
   {
      if (getDiscoveryGroupName() != null &&
          !getDiscoveryGroupName().trim().equals("") &&
          getDiscoveryGroupPort() != null &&
          getDiscoveryRefreshTimeout() != null &&
          getDiscoveryInitialWaitTimeout() != null &&
          getLoadBalancingPolicyClassName() != null &&
          getPingPeriod() != null &&
          getConnectionTTL() != null &&
          getCallTimeout() != null &&
          getClientID() != null &&
          getDupsOKBatchSize() != null &&
          getTransactionBatchSize() != null &&
          getConsumerWindowSize() != null &&
          getConsumerMaxRate() != null &&
          getSendWindowSize() != null &&
          getProducerMaxRate() != null &&
          getMinLargeMessageSize() != null &&
          getBlockOnAcknowledge() != null &&
          getBlockOnNonPersistentSend() != null &&
          getBlockOnPersistentSend() != null &&
          getAutoGroup() != null &&
          getMaxConnections() != null &&
          getPreAcknowledge() != null &&
          getRetryInterval() != null &&
          getRetryIntervalMultiplier() != null &&
          getMaxRetriesBeforeFailover() != null &&
          getMaxRetriesAfterFailover() != null)
      {
         factory = new JBossConnectionFactory(getDiscoveryGroupName(), 
                                              getDiscoveryGroupPort().intValue(),
                                              getDiscoveryRefreshTimeout().longValue(),
                                              getDiscoveryInitialWaitTimeout().longValue(),
                                              getLoadBalancingPolicyClassName(),
                                              getPingPeriod().longValue(),
                                              getConnectionTTL().longValue(),
                                              getCallTimeout().longValue(),
                                              getClientID(),
                                              getDupsOKBatchSize().intValue(),
                                              getTransactionBatchSize().intValue(),
                                              getConsumerWindowSize().intValue(),
                                              getConsumerMaxRate().intValue(),
                                              getSendWindowSize().intValue(),
                                              getProducerMaxRate().intValue(),
                                              getMinLargeMessageSize().intValue(),
                                              getBlockOnAcknowledge().booleanValue(),
                                              getBlockOnNonPersistentSend().booleanValue(),
                                              getBlockOnPersistentSend().booleanValue(),
                                              getAutoGroup().booleanValue(),
                                              getMaxConnections().intValue(),
                                              getPreAcknowledge().booleanValue(),
                                              getRetryInterval().longValue(),
                                              getRetryIntervalMultiplier().doubleValue(),
                                              getMaxRetriesBeforeFailover().intValue(),
                                              getMaxRetriesAfterFailover().intValue());

         configured.set(true);

      } else if (getDiscoveryGroupName() != null &&
                 !getDiscoveryGroupName().trim().equals("") &&
                 getDiscoveryGroupPort() != null &&
                 getDiscoveryRefreshTimeout() != null &&
                 getDiscoveryInitialWaitTimeout() != null)
      {
         factory = new JBossConnectionFactory(getDiscoveryGroupName(), 
                                              getDiscoveryGroupPort().intValue(),
                                              getDiscoveryRefreshTimeout().longValue(),
                                              getDiscoveryInitialWaitTimeout().longValue());

         configured.set(true);

      } 
      else if (getDiscoveryGroupName() != null &&
               !getDiscoveryGroupName().trim().equals("") &&
               getDiscoveryGroupPort() != null)
      {
         factory = new JBossConnectionFactory(getDiscoveryGroupName(), 
                                              getDiscoveryGroupPort().intValue());

         configured.set(true);
      }
      else
      {
         log.fatal("Unable to configure phsyical connection factory");
      }
   }
}
