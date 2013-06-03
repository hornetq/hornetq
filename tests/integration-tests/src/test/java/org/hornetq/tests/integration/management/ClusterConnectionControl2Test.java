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
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;

import org.junit.Assert;

import org.hornetq.api.core.BroadcastGroupConfiguration;
import org.hornetq.api.core.DiscoveryGroupConfiguration;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.UDPBroadcastGroupConfiguration;
import org.hornetq.api.core.management.ClusterConnectionControl;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.tests.util.RandomUtil;

/**
 * A BridgeControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * Created 11 dec. 2008 17:38:58
 *
 */
public class ClusterConnectionControl2Test extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server0;

   private HornetQServer server1;

   private MBeanServer mbeanServer_1;

   private final int port_1 = TransportConstants.DEFAULT_PORT + 1000;

   private ClusterConnectionConfiguration clusterConnectionConfig_0;

   private final String clusterName = "cluster";

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testNodes() throws Exception
   {
      ClusterConnectionControl clusterConnectionControl_0 = createManagementControl(clusterConnectionConfig_0.getName());
      Assert.assertTrue(clusterConnectionControl_0.isStarted());
      Map<String, String> nodes = clusterConnectionControl_0.getNodes();
      Assert.assertEquals(0, nodes.size());

      server1.start();
      waitForServer(server1);
      long start = System.currentTimeMillis();

      while (true)
      {
         nodes = clusterConnectionControl_0.getNodes();

         if (nodes.size() == 1 || System.currentTimeMillis() - start > 30000)
         {
            break;
         }
         Thread.sleep(50);
      }

      Assert.assertEquals(1, nodes.size());

      String remoteAddress = nodes.values().iterator().next();
      Assert.assertTrue(remoteAddress.endsWith(":" + port_1));
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      String discoveryName = RandomUtil.randomString();
      String groupAddress = "231.7.7.7";
      int groupPort = 9876;

      Map<String, Object> acceptorParams_1 = new HashMap<String, Object>();
      acceptorParams_1.put(TransportConstants.PORT_PROP_NAME, port_1);
      TransportConfiguration acceptorConfig_0 = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY);

      TransportConfiguration acceptorConfig_1 = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, acceptorParams_1);

      TransportConfiguration connectorConfig_1 = new TransportConfiguration(NETTY_CONNECTOR_FACTORY, acceptorParams_1);
      TransportConfiguration connectorConfig_0 = new TransportConfiguration(NETTY_CONNECTOR_FACTORY);

      CoreQueueConfiguration queueConfig = new CoreQueueConfiguration(RandomUtil.randomString(),
                                                              RandomUtil.randomString(),
                                                              null,
                                                              false);
      List<String> connectorInfos = new ArrayList<String>();
      connectorInfos.add("netty");
      BroadcastGroupConfiguration broadcastGroupConfig = new BroadcastGroupConfiguration(discoveryName,
                                                                                         250,
                                                                                         connectorInfos,
                                             new UDPBroadcastGroupConfiguration(groupAddress, groupPort, null, -1));
      DiscoveryGroupConfiguration discoveryGroupConfig = new DiscoveryGroupConfiguration(discoveryName,
                                                                                         0,
                                                                                         0,
                                             new UDPBroadcastGroupConfiguration(groupAddress, groupPort, null, -1));

      Configuration conf_1 = createBasicConfig();
      conf_1.setSecurityEnabled(false);
      conf_1.setJMXManagementEnabled(true);

      clusterConnectionConfig_0 =
               new ClusterConnectionConfiguration(clusterName, queueConfig.getAddress(), "netty", 1000, false, false,
                                                  1, 1024,
                                                  discoveryName);
      conf_1.getClusterConfigurations().add(clusterConnectionConfig_0);
      conf_1.getAcceptorConfigurations().add(acceptorConfig_1);
      conf_1.getConnectorConfigurations().put("netty", connectorConfig_1);
      conf_1.getQueueConfigurations().add(queueConfig);
      conf_1.getDiscoveryGroupConfigurations().put(discoveryName, discoveryGroupConfig);
      conf_1.getBroadcastGroupConfigurations().add(broadcastGroupConfig);

      Configuration conf_0 = createBasicConfig(1);
      conf_0.setSecurityEnabled(false);
      conf_0.setJMXManagementEnabled(true);
      conf_0.getAcceptorConfigurations().add(acceptorConfig_0);
      conf_0.getConnectorConfigurations().put("netty", connectorConfig_0);
      conf_0.getClusterConfigurations().add(clusterConnectionConfig_0);
      conf_0.getDiscoveryGroupConfigurations().put(discoveryName, discoveryGroupConfig);
      conf_0.getBroadcastGroupConfigurations().add(broadcastGroupConfig);

      mbeanServer_1 = MBeanServerFactory.createMBeanServer();
      server1 = addServer(HornetQServers.newHornetQServer(conf_1, mbeanServer_1, false));

      server0 = addServer(HornetQServers.newHornetQServer(conf_0, mbeanServer, false));
      server0.start();
      waitForServer(server0);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      server0 = null;
      server1 = null;

      MBeanServerFactory.releaseMBeanServer(mbeanServer_1);
      mbeanServer_1 = null;

      super.tearDown();
   }

   protected ClusterConnectionControl createManagementControl(final String name) throws Exception
   {
      return ManagementControlHelper.createClusterConnectionControl(name, mbeanServer);
   }
}
