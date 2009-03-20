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

import java.util.HashMap;

import javax.management.MBeanServer;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.BridgeConfiguration;
import org.jboss.messaging.core.config.cluster.QueueConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.management.BridgeControlMBean;
import org.jboss.messaging.core.management.ObjectNames;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.utils.Pair;

/**
 * A BridgeControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:38:58
 *
 */
public class BridgeControlTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingService service;

   private BridgeConfiguration bridgeConfig;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAttributes() throws Exception
   {
      checkResource(ObjectNames.getBridgeObjectName(bridgeConfig.getName()));
      BridgeControlMBean bridgeControl = createBridgeControl(bridgeConfig.getName(), mbeanServer);

      assertEquals(bridgeConfig.getName(), bridgeControl.getName());
      assertEquals(bridgeConfig.getDiscoveryGroupName(), bridgeControl.getDiscoveryGroupName());
      assertTrue(bridgeControl.isStarted());
   }

   public void testStartStop() throws Exception
   {
      checkResource(ObjectNames.getBridgeObjectName(bridgeConfig.getName()));
      BridgeControlMBean bridgeControl = createBridgeControl(bridgeConfig.getName(), mbeanServer);

      // started by the service
      assertTrue(bridgeControl.isStarted());

      bridgeControl.stop();
      assertFalse(bridgeControl.isStarted());

      bridgeControl.start();
      assertTrue(bridgeControl.isStarted());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      TransportConfiguration connectorConfig = new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                          new HashMap<String, Object>(),
                                                                          randomString());
      QueueConfiguration sourceQueueConfig = new QueueConfiguration(randomString(), randomString(), null, false);
      QueueConfiguration targetQueueConfig = new QueueConfiguration(randomString(), randomString(), null, false);
      Pair<String, String> connectorPair = new Pair<String, String>(connectorConfig.getName(), null);
      bridgeConfig = new BridgeConfiguration(randomString(),
                                             sourceQueueConfig.getName(),
                                             targetQueueConfig.getAddress(),
                                             null,
                                             null,
                                             randomPositiveLong(),
                                             randomDouble(),
                                             randomPositiveInt(),
                                             randomPositiveInt(),
                                             randomBoolean(),
                                             connectorPair);

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.setClustered(true);
      conf.getConnectorConfigurations().put(connectorConfig.getName(), connectorConfig);
      conf.getQueueConfigurations().add(sourceQueueConfig);
      conf.getQueueConfigurations().add(targetQueueConfig);
      conf.getBridgeConfigurations().add(bridgeConfig);
      service = Messaging.newNullStorageMessagingService(conf, mbeanServer);
      service.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      service.stop();

      super.tearDown();
   }

   protected BridgeControlMBean createBridgeControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return ManagementControlHelper.createBridgeControl(name, mbeanServer);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
