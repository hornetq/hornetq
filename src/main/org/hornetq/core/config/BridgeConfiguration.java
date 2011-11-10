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
import java.util.List;

import org.hornetq.api.core.client.HornetQClient;

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

   private List<String> staticConnectors;

   private String discoveryGroupName;

   private boolean ha;

   private String transformerClassName;

   private long retryInterval;

   private double retryIntervalMultiplier;

   private int reconnectAttempts;

   private boolean useDuplicateDetection;

   private int confirmationWindowSize;

   private final long clientFailureCheckPeriod;

   private String user;

   private String password;

   private final long connectionTTL;

   private final long maxRetryInterval;

   private final int minLargeMessageSize;

   /**
    *  For backward compatibility on the API... no MinLargeMessage on this constructor
    */
   public BridgeConfiguration(final String name,
                              final String queueName,
                              final String forwardingAddress,
                              final String filterString,
                              final String transformerClassName,
                              final long retryInterval,
                              final double retryIntervalMultiplier,
                              final int reconnectAttempts,
                              final boolean useDuplicateDetection,
                              final int confirmationWindowSize,
                              final long clientFailureCheckPeriod,
                              final List<String> staticConnectors,
                              final boolean ha,
                              final String user,
                              final String password)
   {
      this(name,
           queueName,
           forwardingAddress,
           filterString,
           transformerClassName,
           HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
           clientFailureCheckPeriod,
           HornetQClient.DEFAULT_CONNECTION_TTL,
           retryInterval,
           retryInterval,
           retryIntervalMultiplier,
           reconnectAttempts,
           useDuplicateDetection,
           confirmationWindowSize,
           staticConnectors,
           ha,
           user,
           password);
   }

   public BridgeConfiguration(final String name,
                              final String queueName,
                              final String forwardingAddress,
                              final String filterString,
                              final String transformerClassName,
                              final int minLargeMessageSize,
                              final long clientFailureCheckPeriod,
                              final long connectionTTL,
                              final long retryInterval,
                              final long maxRetryInterval,
                              final double retryIntervalMultiplier,
                              final int reconnectAttempts,
                              final boolean useDuplicateDetection,
                              final int confirmationWindowSize,
                              final List<String> staticConnectors,
                              final boolean ha,
                              final String user,
                              final String password)
   {
      this.name = name;
      this.queueName = queueName;
      this.forwardingAddress = forwardingAddress;
      this.minLargeMessageSize = minLargeMessageSize;
      this.filterString = filterString;
      this.transformerClassName = transformerClassName;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.reconnectAttempts = reconnectAttempts;
      this.useDuplicateDetection = useDuplicateDetection;
      this.confirmationWindowSize = confirmationWindowSize;
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
      this.staticConnectors = staticConnectors;
      this.user = user;
      this.password = password;
      this.connectionTTL = connectionTTL;
      this.maxRetryInterval = maxRetryInterval;
      discoveryGroupName = null;
   }

   /**
    *  For backward compatibility on the API... no MinLareMessage, checkPeriod and TTL  on this constructor
    */            
   
    public BridgeConfiguration(final String name,
                              final String queueName,
                              final String forwardingAddress,
                              final String filterString,
                              final String transformerClassName,
                              final long retryInterval,
                              final double retryIntervalMultiplier,
                              final int reconnectAttempts,
                              final boolean useDuplicateDetection,
                              final int confirmationWindowSize,
                              final long clientFailureCheckPeriod,
                              final String discoveryGroupName,
                              final boolean ha,
                              final String user,
                              final String password)
   {
      this(name,
           queueName,
           forwardingAddress,
           filterString,
           transformerClassName,
           HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
           clientFailureCheckPeriod,
           HornetQClient.DEFAULT_CONNECTION_TTL,
           retryInterval,
           retryInterval,
           retryIntervalMultiplier,
           reconnectAttempts,
           useDuplicateDetection,
           confirmationWindowSize,
           discoveryGroupName,
           ha,
           user,
           password);
   }

   public BridgeConfiguration(final String name,
                              final String queueName,
                              final String forwardingAddress,
                              final String filterString,
                              final String transformerClassName,
                              final int minLargeMessageSize,
                              final long clientFailureCheckPeriod,
                              final long connectionTTL,
                              final long retryInterval,
                              final long maxRetryInterval,
                              final double retryIntervalMultiplier,
                              final int reconnectAttempts,
                              final boolean useDuplicateDetection,
                              final int confirmationWindowSize,
                              final String discoveryGroupName,
                              final boolean ha,
                              final String user,
                              final String password)
   {
      this.name = name;
      this.queueName = queueName;
      this.forwardingAddress = forwardingAddress;
      this.filterString = filterString;
      this.transformerClassName = transformerClassName;
      this.minLargeMessageSize = minLargeMessageSize;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.reconnectAttempts = reconnectAttempts;
      this.useDuplicateDetection = useDuplicateDetection;
      this.confirmationWindowSize = confirmationWindowSize;
      this.clientFailureCheckPeriod = clientFailureCheckPeriod;
      this.staticConnectors = null;
      this.discoveryGroupName = discoveryGroupName;
      this.ha = ha;
      this.user = user;
      this.password = password;
      this.connectionTTL = connectionTTL;
      this.maxRetryInterval = maxRetryInterval;
   }

   public String getName()
   {
      return name;
   }

   public String getQueueName()
   {
      return queueName;
   }

   /**
    * @return the connectionTTL
    */
   public long getConnectionTTL()
   {
      return connectionTTL;
   }

   /**
    * @return the maxRetryInterval
    */
   public long getMaxRetryInterval()
   {
      return maxRetryInterval;
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

   public List<String> getStaticConnectors()
   {
      return staticConnectors;
   }

   public String getDiscoveryGroupName()
   {
      return discoveryGroupName;
   }

   public boolean isHA()
   {
      return ha;
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

   public boolean isUseDuplicateDetection()
   {
      return useDuplicateDetection;
   }

   public int getConfirmationWindowSize()
   {
      return confirmationWindowSize;
   }

   public long getClientFailureCheckPeriod()
   {
      return clientFailureCheckPeriod;
   }

   /**
    * @param name the name to set
    */
   public void setName(final String name)
   {
      this.name = name;
   }

   /**
    * @param queueName the queueName to set
    */
   public void setQueueName(final String queueName)
   {
      this.queueName = queueName;
   }

   /**
    * @return the minLargeMessageSize
    */
   public int getMinLargeMessageSize()
   {
      return minLargeMessageSize;
   }

   /**
    * @param forwardingAddress the forwardingAddress to set
    */
   public void setForwardingAddress(final String forwardingAddress)
   {
      this.forwardingAddress = forwardingAddress;
   }

   /**
    * @param filterString the filterString to set
    */
   public void setFilterString(final String filterString)
   {
      this.filterString = filterString;
   }

   /**
    * @param staticConnectors the staticConnectors to set
    */
   public void setStaticConnectors(final List<String> staticConnectors)
   {
      this.staticConnectors = staticConnectors;
   }

   /**
    * @param discoveryGroupName the discoveryGroupName to set
    */
   public void setDiscoveryGroupName(final String discoveryGroupName)
   {
      this.discoveryGroupName = discoveryGroupName;
   }

   /**
    * 
    * @param ha is the bridge supporting HA?
    */
   public void setHA(final boolean ha)
   {
      this.ha = ha;
   }

   /**
    * @param transformerClassName the transformerClassName to set
    */
   public void setTransformerClassName(final String transformerClassName)
   {
      this.transformerClassName = transformerClassName;
   }

   /**
    * @param retryInterval the retryInterval to set
    */
   public void setRetryInterval(final long retryInterval)
   {
      this.retryInterval = retryInterval;
   }

   /**
    * @param retryIntervalMultiplier the retryIntervalMultiplier to set
    */
   public void setRetryIntervalMultiplier(final double retryIntervalMultiplier)
   {
      this.retryIntervalMultiplier = retryIntervalMultiplier;
   }

   /**
    * @param reconnectAttempts the reconnectAttempts to set
    */
   public void setReconnectAttempts(final int reconnectAttempts)
   {
      this.reconnectAttempts = reconnectAttempts;
   }

   /**
    * @param useDuplicateDetection the useDuplicateDetection to set
    */
   public void setUseDuplicateDetection(final boolean useDuplicateDetection)
   {
      this.useDuplicateDetection = useDuplicateDetection;
   }

   /**
    * @param confirmationWindowSize the confirmationWindowSize to set
    */
   public void setConfirmationWindowSize(final int confirmationWindowSize)
   {
      this.confirmationWindowSize = confirmationWindowSize;
   }

   public String getUser()
   {
      return user;
   }

   public String getPassword()
   {
      return password;
   }

   public void setUser(String user)
   {
      this.user = user;
   }

   public void setPassword(String password)
   {
      this.password = password;
   }
}
