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

import org.hornetq.api.core.Pair;

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

   private final boolean duplicateDetection;

   private final boolean forwardWhenNoConsumers;

   private final List<Pair<String, String>> staticConnectorNamePairs;

   private final String discoveryGroupName;

   private final int maxHops;

   private final int confirmationWindowSize;

   public ClusterConnectionConfiguration(final String name,
                                         final String address,
                                         final long retryInterval,
                                         final boolean duplicateDetection,
                                         final boolean forwardWhenNoConsumers,
                                         final int maxHops,
                                         final int confirmationWindowSize,
                                         final List<Pair<String, String>> staticConnectorNamePairs)
   {
      this.name = name;
      this.address = address;
      this.retryInterval = retryInterval;
      this.staticConnectorNamePairs = staticConnectorNamePairs;
      this.duplicateDetection = duplicateDetection;
      this.forwardWhenNoConsumers = forwardWhenNoConsumers;
      discoveryGroupName = null;
      this.maxHops = maxHops;
      this.confirmationWindowSize = confirmationWindowSize;
   }

   public ClusterConnectionConfiguration(final String name,
                                         final String address,
                                         final long retryInterval,
                                         final boolean duplicateDetection,
                                         final boolean forwardWhenNoConsumers,
                                         final int maxHops,
                                         final int confirmationWindowSize,
                                         final String discoveryGroupName)
   {
      this.name = name;
      this.address = address;
      this.retryInterval = retryInterval;
      this.duplicateDetection = duplicateDetection;
      this.forwardWhenNoConsumers = forwardWhenNoConsumers;
      this.discoveryGroupName = discoveryGroupName;
      staticConnectorNamePairs = null;
      this.maxHops = maxHops;
      this.confirmationWindowSize = confirmationWindowSize;
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

   public int getConfirmationWindowSize()
   {
      return confirmationWindowSize;
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
}
