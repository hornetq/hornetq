/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.integration.management;

import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomDouble;
import static org.jboss.messaging.tests.util.RandomUtil.randomPositiveInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomPositiveLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;
import junit.framework.TestCase;

import org.jboss.messaging.core.config.cluster.BridgeConfiguration;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.util.Pair;

/**
 * A BridgeControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:38:58
 *
 */
public class BridgeControlTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingService service;

   // Static --------------------------------------------------------

   public static BridgeConfiguration randomBridgeConfigurationWithDiscoveryGroup(String discoveryGroupName)
   {
      Pair<String, String> connectorPair = new Pair<String, String>(randomString(), randomString());
      
      return new BridgeConfiguration(randomString(),
                                     randomString(),
                                     randomString(),
                                     null,                             
                                     null,
                                     randomPositiveLong(),
                                     randomDouble(),
                                     randomPositiveInt(),
                                     randomPositiveInt(),
                                     randomBoolean(),                              
                                     connectorPair);
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

//   public void testAttributes() throws Exception
//   {
//      DiscoveryGroupConfiguration discoveryGroupConfig = new DiscoveryGroupConfiguration(randomString(),
//                                                                                         "231.7.7.7",
//                                                                                         2000,
//                                                                                         randomPositiveLong());
//      BridgeConfiguration bridgeConfig = randomBridgeConfigurationWithDiscoveryGroup(discoveryGroupConfig.getName());
//
//      MBeanServer mbeanServer = MBeanServerFactory.createMBeanServer();
//      Configuration conf = new ConfigurationImpl();
//      conf.setSecurityEnabled(false);
//      conf.setJMXManagementEnabled(true);
//      conf.setClustered(true);
//      conf.getDiscoveryGroupConfigurations().put(discoveryGroupConfig.getName(), discoveryGroupConfig);
//      conf.getBridgeConfigurations().add(bridgeConfig);
//      service = Messaging.newNullStorageMessagingService(conf, mbeanServer);
//      service.start();
//
//      BridgeControlMBean bridgeControl = createBridgeControl(bridgeConfig.getName(), mbeanServer);
//
//      assertEquals(bridgeConfig.getName(), bridgeControl.getName());
//      assertEquals(bridgeConfig.getDiscoveryGroupName(), bridgeControl.getDiscoveryGroupName());
//   }
//
//   public void testStartStop() throws Exception
//   {
//      DiscoveryGroupConfiguration discoveryGroupConfig = new DiscoveryGroupConfiguration(randomString(),
//                                                                                         "231.7.7.7",
//                                                                                         2000,
//                                                                                         randomPositiveLong());
//      BridgeConfiguration bridgeConfig = randomBridgeConfigurationWithDiscoveryGroup(discoveryGroupConfig.getName());
//
//      MBeanServer mbeanServer = MBeanServerFactory.createMBeanServer();
//      Configuration conf = new ConfigurationImpl();
//      conf.setSecurityEnabled(false);
//      conf.setJMXManagementEnabled(true);
//      conf.setClustered(true);
//      conf.getDiscoveryGroupConfigurations().put(discoveryGroupConfig.getName(), discoveryGroupConfig);
//      conf.getBridgeConfigurations().add(bridgeConfig);
//      service = Messaging.newNullStorageMessagingService(conf, mbeanServer);
//      service.start();
//
//      BridgeControlMBean bridgeControl = createBridgeControl(bridgeConfig.getName(), mbeanServer);
//
//      // started by the service
//      assertTrue(bridgeControl.isStarted());
//
//      bridgeControl.stop();
//      assertFalse(bridgeControl.isStarted());
//
//      bridgeControl.start();
//      assertTrue(bridgeControl.isStarted());
//   }
   
   public void testFoo()
   {      
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

      super.tearDown();
   }
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
