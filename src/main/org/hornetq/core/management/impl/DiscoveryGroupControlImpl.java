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

package org.hornetq.core.management.impl;

import javax.management.StandardMBean;

import org.hornetq.core.cluster.DiscoveryGroup;
import org.hornetq.core.config.cluster.DiscoveryGroupConfiguration;
import org.hornetq.core.management.DiscoveryGroupControl;

/**
 * A AcceptorControl
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:09:04
 */
public class DiscoveryGroupControlImpl extends StandardMBean implements DiscoveryGroupControl
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private final DiscoveryGroup discoveryGroup;

   private final DiscoveryGroupConfiguration configuration;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   public DiscoveryGroupControlImpl(final DiscoveryGroup acceptor, final DiscoveryGroupConfiguration configuration)
      throws Exception
   {
      super(DiscoveryGroupControl.class);
      this.discoveryGroup = acceptor;
      this.configuration = configuration;
   }

   // DiscoveryGroupControlMBean implementation ---------------------------

   public String getName()
   {
      return configuration.getName();
   }

   public String getGroupAddress()
   {
      return configuration.getGroupAddress();
   }

   public int getGroupPort()
   {
      return configuration.getGroupPort();
   }

   public long getRefreshTimeout()
   {
      return configuration.getRefreshTimeout();
   }

   public boolean isStarted()
   {
      return discoveryGroup.isStarted();
   }

   public void start() throws Exception
   {
      discoveryGroup.start();
   }

   public void stop() throws Exception
   {
      discoveryGroup.stop();
   }

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
