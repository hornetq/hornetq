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

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;

import javax.management.MBeanServerInvocationHandler;
import javax.management.openmbean.TabularData;

import junit.framework.TestCase;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.BroadcastGroupConfiguration;
import org.jboss.messaging.core.config.cluster.DiscoveryGroupConfiguration;
import org.jboss.messaging.core.config.cluster.MessageFlowConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.management.BroadcastGroupConfigurationInfo;
import org.jboss.messaging.core.management.DiscoveryGroupConfigurationInfo;
import org.jboss.messaging.core.management.MessageFlowConfigurationInfo;
import org.jboss.messaging.core.management.MessagingServerControlMBean;
import org.jboss.messaging.core.management.TransportConfigurationInfo;
import org.jboss.messaging.core.management.impl.ManagementServiceImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.util.Pair;

/**
 * A QueueControlTest
 *
 * @author jmesnil
 * 
 * Created 26 nov. 2008 14:18:48
 *
 *
 */
public class MessagingServerControlTest extends TestCase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private MessagingService service;

   // Static --------------------------------------------------------

   private static MessagingServerControlMBean createServerControl() throws Exception
   {
      MessagingServerControlMBean control = (MessagingServerControlMBean)MBeanServerInvocationHandler.newProxyInstance(ManagementFactory.getPlatformMBeanServer(),
                                                                                                                       ManagementServiceImpl.getMessagingServerObjectName(),
                                                                                                                       MessagingServerControlMBean.class,
                                                                                                                       false);
      return control;
   }

   public static MessageFlowConfiguration randomMessageFlowConfigurationWithStaticConnectors()
   {
      ArrayList<Pair<String, String>> connectorInfos = new ArrayList<Pair<String, String>>();
      connectorInfos.add(randomPair());
      connectorInfos.add(randomPair());

      return new MessageFlowConfiguration(randomString(),
                                          randomString(),
                                          randomString(),
                                          randomBoolean(),
                                          randomPositiveInt(),
                                          randomPositiveLong(),
                                          randomString(),
                                          randomPositiveLong(),
                                          randomDouble(),
                                          randomPositiveInt(),
                                          randomPositiveInt(),
                                          randomBoolean(),
                                          connectorInfos);
   }

   public static MessageFlowConfiguration randomMessageFlowConfigurationWithDiscoveryGroup()
   {
      String discoveryGroupName = randomString();

      return new MessageFlowConfiguration(randomString(),
                                          randomString(),
                                          randomString(),
                                          randomBoolean(),
                                          randomPositiveInt(),
                                          randomPositiveLong(),
                                          randomString(),
                                          randomPositiveLong(),
                                          randomDouble(),
                                          randomPositiveInt(),
                                          randomPositiveInt(),
                                          randomBoolean(),
                                          discoveryGroupName);
   }

   public static DiscoveryGroupConfiguration randomDiscoveryGroupConfiguration()
   {
      return new DiscoveryGroupConfiguration(randomString(), randomString(), randomPositiveInt(), randomPositiveLong());
   }

   public static BroadcastGroupConfiguration randomBroadcastGroupConfiguration()
   {
      ArrayList<Pair<String, String>> connectorInfos = new ArrayList<Pair<String, String>>();
      connectorInfos.add(randomPair());
      connectorInfos.add(randomPair());

      return new BroadcastGroupConfiguration(randomString(),
                                             randomString(),
                                             randomPositiveInt(),
                                             randomString(),
                                             randomPositiveInt(),
                                             randomPositiveLong(),
                                             connectorInfos);
   }

   public static Pair<String, String> randomPair()
   {
      return new Pair<String, String>(randomString(), randomString());
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetMessageFlows() throws Exception
   {
      MessageFlowConfiguration flowConfig_1 = randomMessageFlowConfigurationWithDiscoveryGroup();
      MessageFlowConfiguration flowConfig_2 = randomMessageFlowConfigurationWithStaticConnectors();

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getMessageFlowConfigurations().add(flowConfig_1);
      conf.getMessageFlowConfigurations().add(flowConfig_2);

      service = MessagingServiceImpl.newNullStorageMessagingService(conf);
      service.start();

      MessagingServerControlMBean serverControl = createServerControl();
      TabularData messageFlowData = serverControl.getMessageFlows();
      assertNotNull(messageFlowData);
      assertEquals(2, messageFlowData.size());

      MessageFlowConfiguration[] messageFlowConfigurations = MessageFlowConfigurationInfo.from(messageFlowData);
      assertEquals(2, messageFlowConfigurations.length);
      if (flowConfig_1.getName().equals(messageFlowConfigurations[0].getName()))
      {
         assertEquals(flowConfig_2.getName(), messageFlowConfigurations[1].getName());
      }
      else if (flowConfig_1.getName().equals(messageFlowConfigurations[1].getName()))
      {
         assertEquals(flowConfig_2.getName(), messageFlowConfigurations[0].getName());
      }
      else
      {
         fail("configuration is not identical");
      }
   }

   public void testGetDiscoveryGroups() throws Exception
   {
      DiscoveryGroupConfiguration discoveryGroupConfig = randomDiscoveryGroupConfiguration();

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getDiscoveryGroupConfigurations().put(discoveryGroupConfig.getName(), discoveryGroupConfig);
      service = MessagingServiceImpl.newNullStorageMessagingService(conf);
      service.start();

      MessagingServerControlMBean serverControl = createServerControl();
      TabularData discoveryGroupData = serverControl.getDiscoveryGroups();
      assertNotNull(discoveryGroupData);
      assertEquals(1, discoveryGroupData.size());

      DiscoveryGroupConfiguration[] discoveryGroupConfigurations = DiscoveryGroupConfigurationInfo.from(discoveryGroupData);
      assertEquals(1, discoveryGroupConfigurations.length);
      assertEquals(discoveryGroupConfig.getName(), discoveryGroupConfigurations[0].getName());
   }

   public void testGetBroadcastGroups() throws Exception
   {
      BroadcastGroupConfiguration broadcastGroupConfig = randomBroadcastGroupConfiguration();

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getBroadcastGroupConfigurations().add(broadcastGroupConfig);
      service = MessagingServiceImpl.newNullStorageMessagingService(conf);
      service.start();

      MessagingServerControlMBean serverControl = createServerControl();
      TabularData broadcastGroupData = serverControl.getBroadcastGroups();
      assertNotNull(broadcastGroupData);
      assertEquals(1, broadcastGroupData.size());

      BroadcastGroupConfiguration[] broadcastGroupConfigurations = BroadcastGroupConfigurationInfo.from(broadcastGroupData);
      assertEquals(1, broadcastGroupConfigurations.length);

      assertEquals(broadcastGroupConfig.getName(), broadcastGroupConfigurations[0].getName());
   }

   public void testGetAcceptors() throws Exception
   {
      TransportConfiguration acceptorConfig = new TransportConfiguration(InVMAcceptorFactory.class.getName(),
                                                                         new HashMap<String, Object>(),
                                                                         randomString());

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations().add(acceptorConfig);
      service = MessagingServiceImpl.newNullStorageMessagingService(conf);
      service.start();

      MessagingServerControlMBean serverControl = createServerControl();
      TabularData acceptorData = serverControl.getAcceptors();
      assertNotNull(acceptorData);
      assertEquals(1, acceptorData.size());

      TransportConfiguration[] acceptorConfigurations = TransportConfigurationInfo.from(acceptorData);
      assertEquals(1, acceptorConfigurations.length);

      assertEquals(acceptorConfig.getName(), acceptorConfigurations[0].getName());
   }

   public void testGetConnectors() throws Exception
   {
      TransportConfiguration connectorConfig = new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                          new HashMap<String, Object>(),
                                                                          randomString());

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getConnectorConfigurations().put(connectorConfig.getName(), connectorConfig);
      service = MessagingServiceImpl.newNullStorageMessagingService(conf);
      service.start();

      MessagingServerControlMBean serverControl = createServerControl();
      TabularData acceptorData = serverControl.getConnectors();
      assertNotNull(acceptorData);
      assertEquals(1, acceptorData.size());

      TransportConfiguration[] connectorConfigurations = TransportConfigurationInfo.from(acceptorData);
      assertEquals(1, connectorConfigurations.length);

      assertEquals(connectorConfig.getName(), connectorConfigurations[0].getName());
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
