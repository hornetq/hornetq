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

   private String name;
   
   private String groupAddress;
   
   private int groupPort;
   
   private long refreshTimeout;

   public DiscoveryGroupConfiguration(final String name,                      
                                      final String groupAddress,
                                      final int groupPort,
                                      final long refreshTimeout)
   {
      this.name = name;
      this.groupAddress = groupAddress;
      this.groupPort = groupPort;
      this.refreshTimeout = refreshTimeout;
   }

   public String getName()
   {
      return name;
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
   public void setName(String name)
   {
      this.name = name;
   }

   /**
    * @param groupAddress the groupAddress to set
    */
   public void setGroupAddress(String groupAddress)
   {
      this.groupAddress = groupAddress;
   }

   /**
    * @param groupPort the groupPort to set
    */
   public void setGroupPort(int groupPort)
   {
      this.groupPort = groupPort;
   }

   /**
    * @param refreshTimeout the refreshTimeout to set
    */
   public void setRefreshTimeout(long refreshTimeout)
   {
      this.refreshTimeout = refreshTimeout;
   }
}
