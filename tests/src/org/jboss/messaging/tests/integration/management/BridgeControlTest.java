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

import static org.jboss.messaging.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME;
import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomDouble;
import static org.jboss.messaging.tests.util.RandomUtil.randomPositiveInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomPositiveLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.management.openmbean.CompositeData;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.BridgeConfiguration;
import org.jboss.messaging.core.config.cluster.QueueConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.management.BridgeControlMBean;
import org.jboss.messaging.core.management.ObjectNames;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
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

   private MessagingServer server_0;

   private BridgeConfiguration bridgeConfig;

   private MessagingServer server_1;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAttributes() throws Exception
   {
      checkResource(ObjectNames.getBridgeObjectName(bridgeConfig.getName()));
      BridgeControlMBean bridgeControl = createBridgeControl(bridgeConfig.getName(), mbeanServer);

      assertEquals(bridgeConfig.getName(), bridgeControl.getName());
      assertEquals(bridgeConfig.getDiscoveryGroupName(), bridgeControl.getDiscoveryGroupName());
      assertEquals(bridgeConfig.getQueueName(), bridgeControl.getQueueName());
      assertEquals(bridgeConfig.getForwardingAddress(), bridgeControl.getForwardingAddress());
      assertEquals(bridgeConfig.getFilterString(), bridgeControl.getFilterString());
      assertEquals(bridgeConfig.getRetryInterval(), bridgeControl.getRetryInterval());
      assertEquals(bridgeConfig.getRetryIntervalMultiplier(), bridgeControl.getRetryIntervalMultiplier());
      assertEquals(bridgeConfig.getReconnectAttempts(), bridgeControl.getReconnectAttempts());
      assertEquals(bridgeConfig.isFailoverOnServerShutdown(), bridgeControl.isFailoverOnServerShutdown());
      assertEquals(bridgeConfig.isUseDuplicateDetection(), bridgeControl.isUseDuplicateDetection());

      CompositeData connectorPairData = bridgeControl.getConnectorPair();
      assertEquals(bridgeConfig.getConnectorPair().a, connectorPairData.get("a"));
      assertEquals(bridgeConfig.getConnectorPair().b, connectorPairData.get("b"));

      assertTrue(bridgeControl.isStarted());
   }

   public void testStartStop() throws Exception
   {
      checkResource(ObjectNames.getBridgeObjectName(bridgeConfig.getName()));
      BridgeControlMBean bridgeControl = createBridgeControl(bridgeConfig.getName(), mbeanServer);

      // started by the server
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

      Map<String, Object> acceptorParams = new HashMap<String, Object>();
      acceptorParams.put(SERVER_ID_PROP_NAME, 1);
      TransportConfiguration acceptorConfig = new TransportConfiguration(InVMAcceptorFactory.class.getName(),
                                                                         acceptorParams,
                                                                         randomString());

      TransportConfiguration connectorConfig = new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                          acceptorParams,
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
                                             randomBoolean(),
                                             randomBoolean(),
                                             connectorPair);

      Configuration conf_1 = new ConfigurationImpl();
      conf_1.setSecurityEnabled(false);
      conf_1.setJMXManagementEnabled(true);
      conf_1.setClustered(true);
      conf_1.getAcceptorConfigurations().add(acceptorConfig);
      conf_1.getQueueConfigurations().add(targetQueueConfig);

      Configuration conf_0 = new ConfigurationImpl();
      conf_0.setSecurityEnabled(false);
      conf_0.setJMXManagementEnabled(true);
      conf_0.setClustered(true);
      conf_0.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      conf_0.getConnectorConfigurations().put(connectorConfig.getName(), connectorConfig);
      conf_0.getQueueConfigurations().add(sourceQueueConfig);
      conf_0.getBridgeConfigurations().add(bridgeConfig);

      server_1 = Messaging.newMessagingServer(conf_1, MBeanServerFactory.createMBeanServer(), false);
      server_1.start();

      server_0 = Messaging.newMessagingServer(conf_0, mbeanServer, false);
      server_0.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      server_0.stop();
      server_1.stop();

      super.tearDown();
   }

   protected BridgeControlMBean createBridgeControl(String name, MBeanServer mbeanServer) throws Exception
   {
      return ManagementControlHelper.createBridgeControl(name, mbeanServer);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
