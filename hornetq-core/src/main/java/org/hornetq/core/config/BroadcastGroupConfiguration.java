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


/**
 * The configuration used to determine how the server will broadcast members
 * This is analogous to {@link org.hornetq.api.core.DiscoveryGroupConfiguration}
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 18 Nov 2008 08:44:30
 *
 */
public class BroadcastGroupConfiguration implements Serializable
{
   private static final long serialVersionUID = 1052413739064253955L;

   private String name;

   private String localBindAddress;

   private int localBindPort;

   private String groupAddress;

   private int groupPort;

   private long broadcastPeriod;

   private String jgroupsFile;

   private String jgroupsChannel;

   private List<String> connectorInfos;
   
   private String jgroupsRef;
   
   private transient Object channelInstance;

   public BroadcastGroupConfiguration(final String name,
                                      final String localBindAddress,
                                      final int localBindPort,
                                      final String groupAddress,
                                      final int groupPort,
                                      final long broadcastPeriod,
                                      final List<String> connectorInfos,
                                      final String jgroupsRef)
   {
      super();
      this.name = name;
      this.localBindAddress = localBindAddress;
      this.localBindPort = localBindPort;
      this.groupAddress = groupAddress;
      this.groupPort = groupPort;
      this.broadcastPeriod = broadcastPeriod;
      this.connectorInfos = connectorInfos;
      this.jgroupsRef = jgroupsRef;
   }

   public BroadcastGroupConfiguration(final String name,
                                      final String localBindAddress,
                                      final int localBindPort,
                                      final String groupAddress,
                                      final int groupPort,
                                      final long broadcastPeriod,
                                      final List<String> connectorInfos)
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

   public BroadcastGroupConfiguration(final String name,
                                      final String jgroupsFile,
                                      final String jgropusChannel,
                                      final long broadcastPeriod,
                                      final List<String> connectorInfos)
   {
      super();
      this.name = name;
      this.jgroupsFile = jgroupsFile;
      this.jgroupsChannel = jgropusChannel;
      this.broadcastPeriod = broadcastPeriod;
      this.connectorInfos = connectorInfos;
   }

   public BroadcastGroupConfiguration(String name, long broadcastPeriod, List<String> connectorNames)
   {
      this.name = name;
      this.broadcastPeriod = broadcastPeriod;
      this.connectorInfos = connectorNames;
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

   public List<String> getConnectorInfos()
   {
      return connectorInfos;
   }

   /**
    * @param name the name to set
    */
   public void setName(final String name)
   {
      this.name = name;
   }

   /**
    * @param localBindAddress the localBindAddress to set
    */
   public void setLocalBindAddress(final String localBindAddress)
   {
      this.localBindAddress = localBindAddress;
   }

   /**
    * @param localBindPort the localBindPort to set
    */
   public void setLocalBindPort(final int localBindPort)
   {
      this.localBindPort = localBindPort;
   }

   /**
    * @param groupAddress the groupAddress to set
    */
   public void setGroupAddress(final String groupAddress)
   {
      this.groupAddress = groupAddress;
   }

   /**
    * @param groupPort the groupPort to set
    */
   public void setGroupPort(final int groupPort)
   {
      this.groupPort = groupPort;
   }

   /**
    * @param broadcastPeriod the broadcastPeriod to set
    */
   public void setBroadcastPeriod(final long broadcastPeriod)
   {
      this.broadcastPeriod = broadcastPeriod;
   }

   /**
    * @param connectorInfos the connectorInfos to set
    */
   public void setConnectorInfos(final List<String> connectorInfos)
   {
      this.connectorInfos = connectorInfos;
   }

   public String getJgroupsFile()
   {
      return jgroupsFile;
   }

   public void setJgroupsFile(String jgroupsFile)
   {
      this.jgroupsFile = jgroupsFile;
   }

   public String getJgroupsChannel()
   {
      return jgroupsChannel;
   }

   public void setJgroupsChannel(String jgroupsChannel)
   {
      this.jgroupsChannel = jgroupsChannel;
   }

   public String getJgroupsRef()
   {
      return jgroupsRef;
   }

   public void setJgroupsRef(String jgroupsRef)
   {
      this.jgroupsRef = jgroupsRef;
   }
   
   public Object getChannelInstance()
   {
      return channelInstance;
   }
   
   public void setChannelInstance(Object instance)
   {
      this.channelInstance = instance;
   }
}
