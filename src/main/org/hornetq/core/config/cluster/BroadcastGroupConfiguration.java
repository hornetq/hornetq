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
import java.util.List;

import org.hornetq.core.logging.Logger;
import org.hornetq.utils.Pair;

/**
 * A BroadcastGroupConfiguration
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 18 Nov 2008 08:44:30
 *
 *
 */
public class BroadcastGroupConfiguration implements Serializable
{
   private static final long serialVersionUID = 1052413739064253955L;
   
   private static final Logger log = Logger.getLogger(BroadcastGroupConfiguration.class);
  
   private final String name;
   
   private final String localBindAddress;

   private final int localBindPort;
   
   private final String groupAddress;
   
   private final int groupPort;
   
   private final long broadcastPeriod;
   
   private final List<Pair<String, String>> connectorInfos;
   
   public BroadcastGroupConfiguration(final String name,
                                      final String localBindAddress,
                                      final int localBindPort,
                                      final String groupAddress,
                                      final int groupPort,
                                      final long broadcastPeriod,
                                      final List<Pair<String, String>> connectorInfos)
   {
      super();
      this.name = name;
      this.localBindAddress = localBindAddress;
      this.localBindPort = localBindPort;
      this.groupAddress = groupAddress;
      this.groupPort = groupPort;
      this.broadcastPeriod = broadcastPeriod;
      this.connectorInfos = connectorInfos;
   }

   public String getName()
   {
      return name;
   }

   public String getLocalBindAddress()
   {
      return localBindAddress;
   }

   public int getLocalBindPort()
   {
      return localBindPort;
   }

   public String getGroupAddress()
   {
      return groupAddress;
   }

   public int getGroupPort()
   {
      return groupPort;
   }

   public long getBroadcastPeriod()
   {
      return broadcastPeriod;
   }
   
   public List<Pair<String, String>> getConnectorInfos()
   {
      return connectorInfos;
   }

}
