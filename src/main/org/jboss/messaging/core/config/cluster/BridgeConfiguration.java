/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

package org.jboss.messaging.core.config.cluster;

import java.io.Serializable;
import java.util.List;

import org.jboss.messaging.util.Pair;

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

   private final String name;
   
   private final String queueName;

   private final String forwardingAddress;

   private final String filterString;

   private final int maxBatchSize;

   private final long maxBatchTime;

   private final Pair<String, String> connectorPair;

   private final String discoveryGroupName;

   private final String transformerClassName;

   private final long retryInterval;

   private final double retryIntervalMultiplier;

   private final int maxRetriesBeforeFailover;

   private final int maxRetriesAfterFailover;

   private final boolean useDuplicateDetection;

   private final int maxHops;

   public BridgeConfiguration(final String name,
                              final String queueName,
                              final String forwardingAddress,
                              final String filterString,
                              final int maxBatchSize,
                              final long maxBatchTime,
                              final String transformerClassName,
                              final long retryInterval,
                              final double retryIntervalMultiplier,
                              final int maxRetriesBeforeFailover,
                              final int maxRetriesAfterFailover,
                              final boolean useDuplicateDetection,
                              final int maxHops,
                              final Pair<String, String> connectorPair)
   {
      this.name = name;
      this.queueName = queueName;
      this.forwardingAddress = forwardingAddress;
      this.filterString = filterString;
      this.maxBatchSize = maxBatchSize;
      this.maxBatchTime = maxBatchTime;
      this.transformerClassName = transformerClassName;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.maxRetriesBeforeFailover = maxRetriesBeforeFailover;
      this.maxRetriesAfterFailover = maxRetriesAfterFailover;
      this.useDuplicateDetection = useDuplicateDetection;
      this.maxHops = maxHops;
      this.connectorPair = connectorPair;
      this.discoveryGroupName = null;
   }

   public BridgeConfiguration(final String name,
                              final String queueName,
                              final String forwardingAddress,
                              final String filterString,
                              final int maxBatchSize,
                              final long maxBatchTime,
                              final String transformerClassName,
                              final long retryInterval,
                              final double retryIntervalMultiplier,
                              final int maxRetriesBeforeFailover,
                              final int maxRetriesAfterFailover,
                              final boolean useDuplicateDetection,
                              final int maxHops,
                              final String discoveryGroupName)
   {
      this.name = name;
      this.queueName = queueName;
      this.forwardingAddress = forwardingAddress;
      this.filterString = filterString;
      this.maxBatchSize = maxBatchSize;
      this.maxBatchTime = maxBatchTime;
      this.transformerClassName = transformerClassName;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.maxRetriesBeforeFailover = maxRetriesBeforeFailover;
      this.maxRetriesAfterFailover = maxRetriesAfterFailover;
      this.useDuplicateDetection = useDuplicateDetection;
      this.maxHops = maxHops;
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

   public int getMaxBatchSize()
   {
      return maxBatchSize;
   }

   public long getMaxBatchTime()
   {
      return maxBatchTime;
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
      return this.discoveryGroupName;
   }

   public long getRetryInterval()
   {
      return retryInterval;
   }

   public double getRetryIntervalMultiplier()
   {
      return retryIntervalMultiplier;
   }

   public int getMaxRetriesBeforeFailover()
   {
      return maxRetriesBeforeFailover;
   }

   public int getMaxRetriesAfterFailover()
   {
      return maxRetriesAfterFailover;
   }

   public boolean isUseDuplicateDetection()
   {
      return useDuplicateDetection;
   }

   public int getMaxHops()
   {
      return maxHops;
   }
}
