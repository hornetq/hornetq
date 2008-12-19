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

package org.jboss.messaging.core.config.cluster;

import java.io.Serializable;
import java.util.List;

import org.jboss.messaging.util.Pair;

/**
 * A MessageFlowConfiguration
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 12 Nov 2008 13:52:22
 *
 *
 */
public class MessageFlowConfiguration implements Serializable
{
   private static final long serialVersionUID = 6583525368508418953L;

   private final String name;

   private final String address;

   private final String filterString;

   private final boolean exclusive;

   private final int maxBatchSize;

   private final long maxBatchTime;

   private final List<Pair<String, String>> staticConnectorNamePairs;

   private final String discoveryGroupName;

   private final String transformerClassName;

   private final long retryInterval;

   private final double retryIntervalMultiplier;

   private final int maxRetriesBeforeFailover;

   private final int maxRetriesAfterFailover;
   
   private final boolean useDuplicateDetection;

   public MessageFlowConfiguration(final String name,
                                   final String address,
                                   final String filterString,
                                   final boolean exclusive,
                                   final int maxBatchSize,
                                   final long maxBatchTime,
                                   final String transformerClassName,
                                   final long retryInterval,
                                   final double retryIntervalMultiplier,
                                   final int maxRetriesBeforeFailover,
                                   final int maxRetriesAfterFailover,
                                   final boolean useDuplicateDetection,
                                   final List<Pair<String, String>> staticConnectorNamePairs)
   {
      this.name = name;
      this.address = address;
      this.filterString = filterString;
      this.exclusive = exclusive;
      this.maxBatchSize = maxBatchSize;
      this.maxBatchTime = maxBatchTime;
      this.transformerClassName = transformerClassName;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.maxRetriesBeforeFailover = maxRetriesBeforeFailover;
      this.maxRetriesAfterFailover = maxRetriesAfterFailover;
      this.useDuplicateDetection = useDuplicateDetection;
      this.staticConnectorNamePairs = staticConnectorNamePairs;
      this.discoveryGroupName = null;
   }

   public MessageFlowConfiguration(final String name,
                                   final String address,
                                   final String filterString,
                                   final boolean exclusive,
                                   final int maxBatchSize,
                                   final long maxBatchTime,
                                   final String transformerClassName,
                                   final long retryInterval,
                                   final double retryIntervalMultiplier,
                                   final int maxRetriesBeforeFailover,
                                   final int maxRetriesAfterFailover,
                                   final boolean useDuplicateDetection,
                                   final String discoveryGroupName)
   {
      this.name = name;
      this.address = address;
      this.filterString = filterString;
      this.exclusive = exclusive;
      this.maxBatchSize = maxBatchSize;
      this.maxBatchTime = maxBatchTime;
      this.transformerClassName = transformerClassName;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.maxRetriesBeforeFailover = maxRetriesBeforeFailover;
      this.maxRetriesAfterFailover = maxRetriesAfterFailover;
      this.useDuplicateDetection = useDuplicateDetection;
      this.staticConnectorNamePairs = null;
      this.discoveryGroupName = discoveryGroupName;
   }

   public String getName()
   {
      return name;
   }

   public String getAddress()
   {
      return address;
   }

   public String getFilterString()
   {
      return filterString;
   }

   public boolean isExclusive()
   {
      return exclusive;
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

   public List<Pair<String, String>> getConnectorNamePairs()
   {
      return staticConnectorNamePairs;
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
}
