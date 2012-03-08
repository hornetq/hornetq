/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package org.hornetq.jms.server.recovery;

import java.util.Arrays;

import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.jms.client.HornetQConnectionFactory;

/**
 * 
 * This represents the configuration of a single connection factory.
 * 
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 *
 * A wrapper around info needed for the xa recovery resource
 *         Date: 3/23/11
 *         Time: 10:15 AM
 */
public class XARecoveryConfig
{
   
   private final boolean ha;
   private final TransportConfiguration[] transportConfiguration;
   private final DiscoveryGroupConfiguration discoveryConfiguration;
   private final String username;
   private final String password;

   public XARecoveryConfig(final boolean ha, final TransportConfiguration[] transportConfiguration, final String username, final String password)
   {
      this.transportConfiguration = transportConfiguration;
      this.discoveryConfiguration = null;
      this.username = username;
      this.password = password;
      this.ha = ha;
   }

   public XARecoveryConfig(final boolean ha, final DiscoveryGroupConfiguration discoveryConfiguration, final String username, final String password)
   {
      this.discoveryConfiguration = discoveryConfiguration;
      this.transportConfiguration = null;
      this.username = username;
      this.password = password;
      this.ha = ha;
   }
   
   public boolean isHA()
   {
      return ha;
   }

   public DiscoveryGroupConfiguration getDiscoveryConfiguration()
   {
      return discoveryConfiguration;
   }
   
   public TransportConfiguration[] getTransportConfig()
   {
      return transportConfiguration;
   }

   public String getUsername()
   {
      return username;
   }

   public String getPassword()
   {
      return password;
   }
   
   /* (non-Javadoc)
    * @see java.lang.Object#hashCode()
    */
   @Override
   public int hashCode()
   {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((discoveryConfiguration == null) ? 0 : discoveryConfiguration.hashCode());
      result = prime * result + Arrays.hashCode(transportConfiguration);
      return result;
   }

   /* (non-Javadoc)
    * @see java.lang.Object#equals(java.lang.Object)
    */
   @Override
   public boolean equals(Object obj)
   {
      if (this == obj)
         return true;
      if (obj == null)
         return false;
      if (getClass() != obj.getClass())
         return false;
      XARecoveryConfig other = (XARecoveryConfig)obj;
      if (discoveryConfiguration == null)
      {
         if (other.discoveryConfiguration != null)
            return false;
      }
      else if (!discoveryConfiguration.equals(other.discoveryConfiguration))
         return false;
      if (!Arrays.equals(transportConfiguration, other.transportConfiguration))
         return false;
      return true;
   }

   /* (non-Javadoc)
    * @see java.lang.Object#toString()
    */
   @Override
   public String toString()
   {
      return "XARecoveryConfig [transportConfiguration = " + Arrays.toString(transportConfiguration) +
             ", discoveryConfiguration = " + discoveryConfiguration +
             ", username=" +
             username +
             ", password=" +
             password +
             "]";
   }
}
