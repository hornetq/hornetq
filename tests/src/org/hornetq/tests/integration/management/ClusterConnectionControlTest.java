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

import static org.hornetq.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME;
import static org.hornetq.tests.util.RandomUtil.randomBoolean;
import static org.hornetq.tests.util.RandomUtil.randomPositiveInt;
import static org.hornetq.tests.util.RandomUtil.randomPositiveLong;
import static org.hornetq.tests.util.RandomUtil.randomString;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.cluster.ClusterConnectionConfiguration;
import org.hornetq.core.config.cluster.QueueConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.management.ClusterConnectionControl;
import org.hornetq.core.management.ObjectNameBuilder;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.utils.Pair;
import org.hornetq.utils.json.JSONArray;
import org.hornetq.utils.json.JSONObject;

/**
 * A BridgeControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:38:58
 *
 */
public class ClusterConnectionControlTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server_0;

   private ClusterConnectionConfiguration clusterConnectionConfig1;
   
   private ClusterConnectionConfiguration clusterConnectionConfig2;

   private HornetQServer server_1;

   private MBeanServer mbeanServer_1;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAttributes1() throws Exception
   {
      checkResource(ObjectNameBuilder.DEFAULT.getClusterConnectionObjectName(clusterConnectionConfig1.getName()));

      ClusterConnectionControl clusterConnectionControl = createManagementControl(clusterConnectionConfig1.getName());

      assertEquals(clusterConnectionConfig1.getName(), clusterConnectionControl.getName());
      assertEquals(clusterConnectionConfig1.getAddress(), clusterConnectionControl.getAddress());
      assertEquals(clusterConnectionConfig1.getDiscoveryGroupName(), clusterConnectionControl.getDiscoveryGroupName());
      assertEquals(clusterConnectionConfig1.getRetryInterval(), clusterConnectionControl.getRetryInterval());
      assertEquals(clusterConnectionConfig1.isDuplicateDetection(), clusterConnectionControl.isDuplicateDetection());
      assertEquals(clusterConnectionConfig1.isForwardWhenNoConsumers(),
                   clusterConnectionControl.isForwardWhenNoConsumers());
      assertEquals(clusterConnectionConfig1.getMaxHops(), clusterConnectionControl.getMaxHops());

      Object[] connectorPairs = clusterConnectionControl.getStaticConnectorNamePairs();
      assertEquals(1, connectorPairs.length);
      Object[] connectorPairData = (Object[])connectorPairs[0];
      assertEquals(clusterConnectionConfig1.getStaticConnectorNamePairs().get(0).a, connectorPairData[0]);
      assertEquals(clusterConnectionConfig1.getStaticConnectorNamePairs().get(0).b, connectorPairData[1]);

      String jsonString = clusterConnectionControl.getStaticConnectorNamePairsAsJSON();
      assertNotNull(jsonString);
      JSONArray array = new JSONArray(jsonString);
      assertEquals(1, array.length());
      JSONObject data = array.getJSONObject(0);
      assertEquals(clusterConnectionConfig1.getStaticConnectorNamePairs().get(0).a, data.optString("a"));
      assertEquals(clusterConnectionConfig1.getStaticConnectorNamePairs().get(0).b, data.optString("b", null));

      assertNull(clusterConnectionControl.getDiscoveryGroupName());
      
      assertTrue(clusterConnectionControl.isStarted());
   }
   
   public void testAttributes2() throws Exception
   {
      checkResource(ObjectNameBuilder.DEFAULT.getClusterConnectionObjectName(clusterConnectionConfig2.getName()));

      ClusterConnectionControl clusterConnectionControl = createManagementControl(clusterConnectionConfig2.getName());

      assertEquals(clusterConnectionConfig2.getName(), clusterConnectionControl.getName());
      assertEquals(clusterConnectionConfig2.getAddress(), clusterConnectionControl.getAddress());
      assertEquals(clusterConnectionConfig2.getDiscoveryGroupName(), clusterConnectionControl.getDiscoveryGroupName());
      assertEquals(clusterConnectionConfig2.getRetryInterval(), clusterConnectionControl.getRetryInterval());
      assertEquals(clusterConnectionConfig2.isDuplicateDetection(), clusterConnectionControl.isDuplicateDetection());
      assertEquals(clusterConnectionConfig2.isForwardWhenNoConsumers(),
                   clusterConnectionControl.isForwardWhenNoConsumers());
      assertEquals(clusterConnectionConfig2.getMaxHops(), clusterConnectionControl.getMaxHops());

      Object[] connectorPairs = clusterConnectionControl.getStaticConnectorNamePairs();
      assertNull(connectorPairs);
      
      String jsonPairs = clusterConnectionControl.getStaticConnectorNamePairsAsJSON();
      assertNull(jsonPairs);
      
      assertEquals(clusterConnectionConfig2.getDiscoveryGroupName(), clusterConnectionControl.getDiscoveryGroupName());
   }

   public void testStartStop() throws Exception
   {
      checkResource(ObjectNameBuilder.DEFAULT.getClusterConnectionObjectName(clusterConnectionConfig1.getName()));
      ClusterConnectionControl clusterConnectionControl = createManagementControl(clusterConnectionConfig1.getName());

      // started by the server
      assertTrue(clusterConnectionControl.isStarted());

      clusterConnectionControl.stop();
      assertFalse(clusterConnectionControl.isStarted());

      clusterConnectionControl.start();
      assertTrue(clusterConnectionControl.isStarted());
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

      QueueConfiguration queueConfig = new QueueConfiguration(randomString(), randomString(), null, false);

      Pair<String, String> connectorPair = new Pair<String, String>(connectorConfig.getName(), null);
      List<Pair<String, String>> pairs = new ArrayList<Pair<String, String>>();
      pairs.add(connectorPair);

      clusterConnectionConfig1 = new ClusterConnectionConfiguration(randomString(),
                                                                   queueConfig.getAddress(),
                                                                   randomPositiveLong(),
                                                                   randomBoolean(),
                                                                   randomBoolean(),
                                                                   randomPositiveInt(),
                                                                   pairs);
      
      clusterConnectionConfig2 = new ClusterConnectionConfiguration(randomString(),
                                                                    queueConfig.getAddress(),
                                                                    randomPositiveLong(),
                                                                    randomBoolean(),
                                                                    randomBoolean(),
                                                                    randomPositiveInt(),
                                                                    randomString());

      Configuration conf_1 = new ConfigurationImpl();
      conf_1.setSecurityEnabled(false);
      conf_1.setJMXManagementEnabled(true);
      conf_1.setClustered(true);
      conf_1.getAcceptorConfigurations().add(acceptorConfig);
      conf_1.getQueueConfigurations().add(queueConfig);

      Configuration conf_0 = new ConfigurationImpl();
      conf_0.setSecurityEnabled(false);
      conf_0.setJMXManagementEnabled(true);
      conf_0.setClustered(true);
      conf_0.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      conf_0.getConnectorConfigurations().put(connectorConfig.getName(), connectorConfig);
      conf_0.getClusterConfigurations().add(clusterConnectionConfig1);
      conf_0.getClusterConfigurations().add(clusterConnectionConfig2);

      mbeanServer_1 = MBeanServerFactory.createMBeanServer();
      server_1 = HornetQ.newHornetQServer(conf_1, mbeanServer_1, false);
      server_1.start();

      server_0 = HornetQ.newHornetQServer(conf_0, mbeanServer, false);
      server_0.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      server_0.stop();
      server_1.stop();
      
      server_0 = null;
      
      server_1 = null;

      MBeanServerFactory.releaseMBeanServer(mbeanServer_1);
      mbeanServer_1 = null;

      super.tearDown();
   }
   
   protected ClusterConnectionControl createManagementControl(String name) throws Exception
   {
      return ManagementControlHelper.createClusterConnectionControl(name, mbeanServer);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
