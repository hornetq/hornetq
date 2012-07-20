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

import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.utils.UUIDGenerator;

/**
 * This file represents how we are using Discovery.
 * <p>
 * The discovery configuration could either use plain UDP, or JGroups.<br/>
 * If using UDP, all the UDP properties will be filled and the jgroups properties will be
 * {@code null}.<br/>
 * If using JGroups, all the UDP properties will be -1 or {@code null} and the jgroups properties
 * will be filled.<br/>
 * If by any reason, on an user misconfiguration both properties are filled, the JGroups takes
 * precedence, that means. if jgroupsFile != null the Grouping method used will be JGroups
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author Clebert Suconic
 */
public class DiscoveryGroupConfiguration implements Serializable
{
   private static final long serialVersionUID = 8657206421727863400L;

   private String name;

   private String localBindAddress;

   private int localBindPort;

   private String groupAddress;

   private int groupPort;

   private long refreshTimeout;

   private long discoveryInitialWaitTimeout;

   private String jgroupsFile;

   private String jgroupsChannelName;

   public DiscoveryGroupConfiguration(final String name,
                                      final String localBindAddress,
                                      final int localBindPort,
                                      final String groupAddress,
                                      final int groupPort,
                                      final long refreshTimeout,
                                      final long discoveryInitialWaitTimeout)
   {
      this.name = name;
      this.localBindPort = localBindPort;
      this.groupAddress = groupAddress;
      this.localBindAddress = localBindAddress;
      this.groupPort = groupPort;
      this.refreshTimeout = refreshTimeout;
      this.discoveryInitialWaitTimeout = discoveryInitialWaitTimeout;
   }

   /**
    * @deprecated  use the other constructors
    * @param name
    * @param localBindAddress
    * @param groupAddress
    * @param groupPort
    * @param refreshTimeout
    * @param discoveryInitialWaitTimeout
    */
   @Deprecated
   public DiscoveryGroupConfiguration(final String name,
                                      final String localBindAddress,
                                      final String groupAddress,
                                      final int groupPort,
                                      final long refreshTimeout,
                                      final long discoveryInitialWaitTimeout)
   {
      this(name, localBindAddress, -1, groupAddress, groupPort, refreshTimeout, discoveryInitialWaitTimeout);
   }

   public DiscoveryGroupConfiguration(final String groupAddress,
                                      final int groupPort)
   {
      this(UUIDGenerator.getInstance().generateStringUUID(), null, -1,  groupAddress, groupPort, HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT, HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT);
   }

   public DiscoveryGroupConfiguration(String name, long refreshTimeout, long discoveryInitialWaitTimeout,
                                      String jgroupsFile, String channelName)
   {
      this.name = name;
      this.refreshTimeout = refreshTimeout;
      this.discoveryInitialWaitTimeout = discoveryInitialWaitTimeout;
      this.jgroupsFile = jgroupsFile;
      this.jgroupsChannelName = channelName;
   }

   public String getName()
   {
      return name;
   }

   public String getLocalBindAddress()
   {
      return localBindAddress;
   }

   public String getGroupAddress()
   {
      return groupAddress;
   }

   public int getGroupPort()
   {
      return groupPort;
   }

   public long getRefreshTimeout()
   {
      return refreshTimeout;
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
   public void setLocalBindAdress(final String localBindAddress)
   {
      this.localBindAddress = localBindAddress;
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
    * @param refreshTimeout the refreshTimeout to set
    */
   public void setRefreshTimeout(final long refreshTimeout)
   {
      this.refreshTimeout = refreshTimeout;
   }

   /**
    * @return the discoveryInitialWaitTimeout
    */
   public long getDiscoveryInitialWaitTimeout()
   {
      return discoveryInitialWaitTimeout;
   }

   /**
    * @param discoveryInitialWaitTimeout the discoveryInitialWaitTimeout to set
    */
   public void setDiscoveryInitialWaitTimeout(long discoveryInitialWaitTimeout)
   {
      this.discoveryInitialWaitTimeout = discoveryInitialWaitTimeout;
   }

   public int getLocalBindPort()
   {
      return localBindPort;
   }

   public void setLocalBindPort(int localBindPort)
   {
      this.localBindPort = localBindPort;
   }

   public String getJgroupsFile()
   {
      return jgroupsFile;
   }

   public void setJgroupsFile(String jgroupsFile)
   {
      this.jgroupsFile = jgroupsFile;
   }

   public String getJgroupsChannelName()
   {
      return jgroupsChannelName;
   }

   public void setJgroupsChannelName(String jgroupsChannelName)
   {
      this.jgroupsChannelName = jgroupsChannelName;
   }

   @Override
   public boolean equals(Object o)
   {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DiscoveryGroupConfiguration that = (DiscoveryGroupConfiguration) o;

      if (discoveryInitialWaitTimeout != that.discoveryInitialWaitTimeout) return false;
      if (groupPort != that.groupPort) return false;
      if (localBindPort != that.localBindPort) return false;
      if (refreshTimeout != that.refreshTimeout) return false;
      if (groupAddress != null ? !groupAddress.equals(that.groupAddress) : that.groupAddress != null) return false;
      if (jgroupsChannelName != null ? !jgroupsChannelName.equals(that.jgroupsChannelName) : that.jgroupsChannelName != null)
         return false;
      if (jgroupsFile != null ? !jgroupsFile.equals(that.jgroupsFile) : that.jgroupsFile != null) return false;
      if (localBindAddress != null ? !localBindAddress.equals(that.localBindAddress) : that.localBindAddress != null)
         return false;
      if (name != null ? !name.equals(that.name) : that.name != null) return false;

      return true;
   }

   @Override
   public int hashCode()
   {
      int result = name != null ? name.hashCode() : 0;
      result = 31 * result + (localBindAddress != null ? localBindAddress.hashCode() : 0);
      result = 31 * result + localBindPort;
      result = 31 * result + (groupAddress != null ? groupAddress.hashCode() : 0);
      result = 31 * result + groupPort;
      result = 31 * result + (int) (refreshTimeout ^ (refreshTimeout >>> 32));
      result = 31 * result + (int) (discoveryInitialWaitTimeout ^ (discoveryInitialWaitTimeout >>> 32));
      result = 31 * result + (jgroupsFile != null ? jgroupsFile.hashCode() : 0);
      result = 31 * result + (jgroupsChannelName != null ? jgroupsChannelName.hashCode() : 0);
      return result;
   }

   @Override
   public String toString()
   {
      return "DiscoveryGroupConfiguration{" +
         "name='" + name + '\'' +
         ", localBindAddress='" + localBindAddress + '\'' +
         ", localBindPort=" + localBindPort +
         ", groupAddress='" + groupAddress + '\'' +
         ", groupPort=" + groupPort +
         ", refreshTimeout=" + refreshTimeout +
         ", discoveryInitialWaitTimeout=" + discoveryInitialWaitTimeout +
         ", jgroupsFile='" + jgroupsFile + '\'' +
         ", jgroupsChannelName='" + jgroupsChannelName + '\'' +
         '}';
   }
}
