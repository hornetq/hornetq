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

package org.hornetq.api.core;

import java.io.Serializable;
import java.util.List;


/**
 * The basic configuration used to determine how the server will broadcast members
 * This is analogous to {@link org.hornetq.api.core.DiscoveryGroupConfiguration}
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:hgao@redhat.com">Howard Gao</a>
 *
 */
public final class BroadcastGroupConfiguration implements Serializable
{
   private static final long serialVersionUID = 2335634694112319124L;

   private String name;

   private long broadcastPeriod;

   private final BroadcastEndpointFactoryConfiguration endpointFactoryConfiguration;

   private List<String> connectorInfos;

   public BroadcastGroupConfiguration(final String name,
                                      final long broadcastPeriod,
                                      final List<String> connectorInfos,
                                      final BroadcastEndpointFactoryConfiguration endpointFactoryConfiguration)
   {
      this.name = name;
      this.broadcastPeriod = broadcastPeriod;
      this.connectorInfos = connectorInfos;
      this.endpointFactoryConfiguration = endpointFactoryConfiguration;
   }

   public String getName()
   {
      return name;
   }

   public long getBroadcastPeriod()
   {
      return broadcastPeriod;
   }

   public List<String> getConnectorInfos()
   {
      return connectorInfos;
   }

   public void setName(final String name)
   {
      this.name = name;
   }

   public void setBroadcastPeriod(final long broadcastPeriod)
   {
      this.broadcastPeriod = broadcastPeriod;
   }

   public void setConnectorInfos(final List<String> connectorInfos)
   {
      this.connectorInfos = connectorInfos;
   }

   public BroadcastEndpointFactoryConfiguration getEndpointFactoryConfiguration()
   {
      return endpointFactoryConfiguration;
   }
}
