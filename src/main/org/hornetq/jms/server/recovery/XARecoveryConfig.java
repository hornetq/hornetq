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

import org.hornetq.api.core.TransportConfiguration;

/**
 * @author <a href="mailto:andy.taylor@jboss.com">Andy Taylor</a>
 *
 * A wrapper around info needed for the xa recovery resource
 *         Date: 3/23/11
 *         Time: 10:15 AM
 */
public class XARecoveryConfig
{
   private final TransportConfiguration transportConfiguration;
   private final String username;
   private final String password;

   public XARecoveryConfig(TransportConfiguration transportConfiguration, String username, String password)
   {
      this.transportConfiguration = transportConfiguration;
      this.username = username;
      this.password = password;
   }

   public TransportConfiguration getTransportConfiguration()
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
}
