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
import org.hornetq.core.logging.Logger;
import org.hornetq.utils.UUIDGenerator;

/**
 * A DiscoveryGroupConfiguration
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 18 Nov 2008 08:47:30
 *
 *
 */
public class DiscoveryGroupConfiguration implements Serializable
{
   private static final long serialVersionUID = 8657206421727863400L;
   
   private static final Logger log = Logger.getLogger(DiscoveryGroupConfiguration.class);


   private String name;
   
   private String localBindAddress;

   private String groupAddress;

   private int groupPort;

   private long refreshTimeout;
   
   private long discoveryInitialWaitTimeout;

   public DiscoveryGroupConfiguration(final String name,
                                      final String localBindAddress,
                                      final String groupAddress,
                                      final int groupPort,
                                      final long refreshTimeout,
                                      final long discoveryInitialWaitTimeout)
   {
      this.name = name;
      this.groupAddress = groupAddress;
      this.localBindAddress = localBindAddress;
      this.groupPort = groupPort;
      this.refreshTimeout = refreshTimeout;
      this.discoveryInitialWaitTimeout = discoveryInitialWaitTimeout;
   }

   public DiscoveryGroupConfiguration(final String groupAddress,
                                      final int groupPort)
   {
      this(UUIDGenerator.getInstance().generateStringUUID(), null, groupAddress, groupPort, HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT, HornetQClient.DEFAULT_DISCOVERY_INITIAL_WAIT_TIMEOUT);
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

   @Override
   public boolean equals(Object o)
   {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      DiscoveryGroupConfiguration that = (DiscoveryGroupConfiguration) o;

      if (discoveryInitialWaitTimeout != that.discoveryInitialWaitTimeout) return false;
      if (groupPort != that.groupPort) return false;
      if (refreshTimeout != that.refreshTimeout) return false;
      if (groupAddress != null ? !groupAddress.equals(that.groupAddress) : that.groupAddress != null) return false;
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
      result = 31 * result + (groupAddress != null ? groupAddress.hashCode() : 0);
      result = 31 * result + groupPort;
      result = 31 * result + (int) (refreshTimeout ^ (refreshTimeout >>> 32));
      result = 31 * result + (int) (discoveryInitialWaitTimeout ^ (discoveryInitialWaitTimeout >>> 32));
      return result;
   }
}
