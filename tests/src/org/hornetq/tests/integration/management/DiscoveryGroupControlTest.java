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

package org.hornetq.tests.integration.management;

import junit.framework.Assert;

import org.hornetq.api.core.config.TransportConfiguration;
import org.hornetq.api.core.management.DiscoveryGroupControl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.server.cluster.DiscoveryGroupConfiguration;
import org.hornetq.tests.util.RandomUtil;

/**
 * A AcceptorControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:38:58
 *
 *
 */
public class DiscoveryGroupControlTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer service;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAttributes() throws Exception
   {
      DiscoveryGroupConfiguration discoveryGroupConfig = new DiscoveryGroupConfiguration(RandomUtil.randomString(),
                                                                                         "231.7.7.7",
                                                                                         2000,
                                                                                         RandomUtil.randomPositiveLong());

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.setClustered(true);
      conf.getDiscoveryGroupConfigurations().put(discoveryGroupConfig.getName(), discoveryGroupConfig);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      service = HornetQServers.newHornetQServer(conf, mbeanServer, false);
      service.start();

      DiscoveryGroupControl discoveryGroupControl = createManagementControl(discoveryGroupConfig.getName());

      Assert.assertEquals(discoveryGroupConfig.getName(), discoveryGroupControl.getName());
      Assert.assertEquals(discoveryGroupConfig.getGroupAddress(), discoveryGroupControl.getGroupAddress());
      Assert.assertEquals(discoveryGroupConfig.getGroupPort(), discoveryGroupControl.getGroupPort());
      Assert.assertEquals(discoveryGroupConfig.getRefreshTimeout(), discoveryGroupControl.getRefreshTimeout());
   }

   public void testStartStop() throws Exception
   {
      DiscoveryGroupConfiguration discoveryGroupConfig = new DiscoveryGroupConfiguration(RandomUtil.randomString(),
                                                                                         "231.7.7.7",
                                                                                         2000,
                                                                                         RandomUtil.randomPositiveLong());

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.setClustered(true);
      conf.getDiscoveryGroupConfigurations().put(discoveryGroupConfig.getName(), discoveryGroupConfig);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      service = HornetQServers.newHornetQServer(conf, mbeanServer, false);
      service.start();

      DiscoveryGroupControl discoveryGroupControl = createManagementControl(discoveryGroupConfig.getName());

      // started by the server
      Assert.assertTrue(discoveryGroupControl.isStarted());

      discoveryGroupControl.stop();
      Assert.assertFalse(discoveryGroupControl.isStarted());

      discoveryGroupControl.start();
      Assert.assertTrue(discoveryGroupControl.isStarted());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void tearDown() throws Exception
   {
      if (service != null)
      {
         service.stop();
      }

      service = null;

      super.tearDown();
   }

   protected DiscoveryGroupControl createManagementControl(final String name) throws Exception
   {
      return ManagementControlHelper.createDiscoveryGroupControl(name, mbeanServer);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
