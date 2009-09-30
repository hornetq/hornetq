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

package org.hornetq.core.config.cluster;

import java.io.Serializable;

import org.hornetq.utils.Pair;

/**
 * A BridgeConfiguration
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 13 Jan 2009 09:32:43
 *
 *
 */
public class BridgeConfiguration implements Serializable
{
   private static final long serialVersionUID = -1057244274380572226L;

   private String name;

   private String queueName;

   private String forwardingAddress;

   private String filterString;

   private Pair<String, String> connectorPair;

   private String discoveryGroupName;

   private String transformerClassName;

   private long retryInterval;

   private double retryIntervalMultiplier;

   private int reconnectAttempts;
   
   private boolean failoverOnServerShutdown;

   private boolean useDuplicateDetection;

   public BridgeConfiguration(final String name,
                              final String queueName,
                              final String forwardingAddress,
                              final String filterString,
                              final String transformerClassName,
                              final long retryInterval,
                              final double retryIntervalMultiplier,
                              final int reconnectAttempts,
                              final boolean failoverOnServerShutdown,
                              final boolean useDuplicateDetection,
                              final Pair<String, String> connectorPair)
   {
      this.name = name;
      this.queueName = queueName;
      this.forwardingAddress = forwardingAddress;
      this.filterString = filterString;
      this.transformerClassName = transformerClassName;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.reconnectAttempts = reconnectAttempts;
      this.failoverOnServerShutdown = failoverOnServerShutdown;
      this.useDuplicateDetection = useDuplicateDetection;
      this.connectorPair = connectorPair;
      this.discoveryGroupName = null;
   }

   public BridgeConfiguration(final String name,
                              final String queueName,
                              final String forwardingAddress,
                              final String filterString,
                              final String transformerClassName,
                              final long retryInterval,
                              final double retryIntervalMultiplier,
                              final int reconnectAttempts,
                              final boolean failoverOnServerShutdown,
                              final boolean useDuplicateDetection,
                              final String discoveryGroupName)
   {
      this.name = name;
      this.queueName = queueName;
      this.forwardingAddress = forwardingAddress;
      this.filterString = filterString;
      this.transformerClassName = transformerClassName;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.reconnectAttempts = reconnectAttempts;
      this.failoverOnServerShutdown = failoverOnServerShutdown;
      this.useDuplicateDetection = useDuplicateDetection;
      this.connectorPair = null;
      this.discoveryGroupName = discoveryGroupName;
   }

   public String getName()
   {
      return name;
   }

   public String getQueueName()
   {
      return queueName;
   }

   public String getForwardingAddress()
   {
      return forwardingAddress;
   }

   public String getFilterString()
   {
      return filterString;
   }

   public String getTransformerClassName()
   {
      return transformerClassName;
   }

   public Pair<String, String> getConnectorPair()
   {
      return connectorPair;
   }

   public String getDiscoveryGroupName()
   {
      return discoveryGroupName;
   }

   public long getRetryInterval()
   {
      return retryInterval;
   }

   public double getRetryIntervalMultiplier()
   {
      return retryIntervalMultiplier;
   }

   public int getReconnectAttempts()
   {
      return reconnectAttempts;
   }

   public boolean isFailoverOnServerShutdown()
   {
      return failoverOnServerShutdown;
   }

   public boolean isUseDuplicateDetection()
   {
      return useDuplicateDetection;
   }

   /**
    * @param name the name to set
    */
   public void setName(String name)
   {
      this.name = name;
   }

   /**
    * @param queueName the queueName to set
    */
   public void setQueueName(String queueName)
   {
      this.queueName = queueName;
   }

   /**
    * @param forwardingAddress the forwardingAddress to set
    */
   public void setForwardingAddress(String forwardingAddress)
   {
      this.forwardingAddress = forwardingAddress;
   }

   /**
    * @param filterString the filterString to set
    */
   public void setFilterString(String filterString)
   {
      this.filterString = filterString;
   }

   /**
    * @param connectorPair the connectorPair to set
    */
   public void setConnectorPair(Pair<String, String> connectorPair)
   {
      this.connectorPair = connectorPair;
   }

   /**
    * @param discoveryGroupName the discoveryGroupName to set
    */
   public void setDiscoveryGroupName(String discoveryGroupName)
   {
      this.discoveryGroupName = discoveryGroupName;
   }

   /**
    * @param transformerClassName the transformerClassName to set
    */
   public void setTransformerClassName(String transformerClassName)
   {
      this.transformerClassName = transformerClassName;
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
    * @param reconnectAttempts the reconnectAttempts to set
    */
   public void setReconnectAttempts(int reconnectAttempts)
   {
      this.reconnectAttempts = reconnectAttempts;
   }

   /**
    * @param failoverOnServerShutdown the failoverOnServerShutdown to set
    */
   public void setFailoverOnServerShutdown(boolean failoverOnServerShutdown)
   {
      this.failoverOnServerShutdown = failoverOnServerShutdown;
   }

   /**
    * @param useDuplicateDetection the useDuplicateDetection to set
    */
   public void setUseDuplicateDetection(boolean useDuplicateDetection)
   {
      this.useDuplicateDetection = useDuplicateDetection;
   }
}
