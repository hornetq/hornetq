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
 * A ClusterConnectionConfiguration
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 13 Jan 2009 09:42:17
 *
 *
 */
public class ClusterConnectionConfiguration implements Serializable
{
   private static final long serialVersionUID = 8948303813427795935L;

   private final String name;

   private final String address;

   private final long retryInterval;

   private final double retryIntervalMultiplier;

   private final int maxRetriesBeforeFailover;

   private final int maxRetriesAfterFailover;

   private final boolean duplicateDetection;
   
   private final boolean forwardWhenNoConsumers;

   private final List<Pair<String, String>> staticConnectorNamePairs;

   private final String discoveryGroupName;
   
   private final int maxHops;

   public ClusterConnectionConfiguration(final String name,
                                         final String address,
                                         final long retryInterval,
                                         final double retryIntervalMultiplier,
                                         final int maxRetriesBeforeFailover,
                                         final int maxRetriesAfterFailover,
                                         final boolean duplicateDetection,
                                         final boolean forwardWhenNoConsumers,
                                         final int maxHops,
                                         final List<Pair<String, String>> staticConnectorNamePairs)
   {
      this.name = name;
      this.address = address;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.maxRetriesBeforeFailover = maxRetriesBeforeFailover;
      this.maxRetriesAfterFailover = maxRetriesAfterFailover;
      this.staticConnectorNamePairs = staticConnectorNamePairs;
      this.duplicateDetection = duplicateDetection;
      this.forwardWhenNoConsumers = forwardWhenNoConsumers;
      this.discoveryGroupName = null;
      this.maxHops = maxHops;
   }

   public ClusterConnectionConfiguration(final String name,
                                         final String address,
                                         final long retryInterval,
                                         final double retryIntervalMultiplier,
                                         final int maxRetriesBeforeFailover,
                                         final int maxRetriesAfterFailover,
                                         final boolean duplicateDetection,
                                         final boolean forwardWhenNoConsumers,
                                         final int maxHops,
                                         final String discoveryGroupName)
   {
      this.name = name;
      this.address = address;
      this.retryInterval = retryInterval;
      this.retryIntervalMultiplier = retryIntervalMultiplier;
      this.maxRetriesBeforeFailover = maxRetriesBeforeFailover;
      this.maxRetriesAfterFailover = maxRetriesAfterFailover;
      this.duplicateDetection = duplicateDetection;
      this.forwardWhenNoConsumers = forwardWhenNoConsumers;
      this.discoveryGroupName = discoveryGroupName;
      this.staticConnectorNamePairs = null;
      this.maxHops = maxHops;
   }

   public String getName()
   {
      return name;
   }

   public String getAddress()
   {
      return address;
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

   public List<Pair<String, String>> getStaticConnectorNamePairs()
   {
      return staticConnectorNamePairs;
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

   public int getMaxRetriesBeforeFailover()
   {
      return maxRetriesBeforeFailover;
   }

   public int getMaxRetriesAfterFailover()
   {
      return maxRetriesAfterFailover;
   }

}
