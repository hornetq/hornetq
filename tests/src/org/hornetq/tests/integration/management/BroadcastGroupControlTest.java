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

import static org.hornetq.tests.util.RandomUtil.randomPositiveInt;
import static org.hornetq.tests.util.RandomUtil.randomString;

import java.util.ArrayList;
import java.util.List;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.cluster.BroadcastGroupConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.management.BroadcastGroupControl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.integration.transports.netty.NettyConnectorFactory;
import org.hornetq.utils.Pair;
import org.hornetq.utils.json.JSONArray;
import org.hornetq.utils.json.JSONObject;

/**
 * A AcceptorControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:38:58
 *
 *
 */
public class BroadcastGroupControlTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer service;

   // Static --------------------------------------------------------

   public static BroadcastGroupConfiguration randomBroadcastGroupConfiguration(List<Pair<String, String>> connectorInfos)
   {
      return new BroadcastGroupConfiguration(randomString(),
                                             null,
                                             1198,
                                             "231.7.7.7",
                                             1199,
                                             randomPositiveInt(),
                                             connectorInfos);
   }

   public static Pair<String, String> randomPair()
   {
      return new Pair<String, String>(randomString(), randomString());
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAttributes() throws Exception
   {
      TransportConfiguration connectorConfiguration = new TransportConfiguration(NettyConnectorFactory.class.getName());
      List<Pair<String, String>> connectorInfos = new ArrayList<Pair<String,String>>();
      connectorInfos.add(new Pair<String, String>(connectorConfiguration.getName(), null));
      BroadcastGroupConfiguration broadcastGroupConfig = randomBroadcastGroupConfiguration(connectorInfos);

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.setClustered(true);
      conf.getConnectorConfigurations().put(connectorConfiguration.getName(), connectorConfiguration);
      conf.getBroadcastGroupConfigurations().add(broadcastGroupConfig);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      service = HornetQ.newHornetQServer(conf, mbeanServer, false);
      service.start();

      BroadcastGroupControl broadcastGroupControl = createManagementControl(broadcastGroupConfig.getName());

      assertEquals(broadcastGroupConfig.getName(), broadcastGroupControl.getName());
      assertEquals(broadcastGroupConfig.getGroupAddress(), broadcastGroupControl.getGroupAddress());
      assertEquals(broadcastGroupConfig.getGroupPort(), broadcastGroupControl.getGroupPort());
      assertEquals(broadcastGroupConfig.getLocalBindPort(), broadcastGroupControl.getLocalBindPort());
      assertEquals(broadcastGroupConfig.getBroadcastPeriod(), broadcastGroupControl.getBroadcastPeriod());
      
      Object[] connectorPairs = broadcastGroupControl.getConnectorPairs();
      assertEquals(1, connectorPairs.length);
      Object[] connectorPairData = (Object[])connectorPairs[0];
      assertEquals(broadcastGroupConfig.getConnectorInfos().get(0).a, connectorPairData[0]);
      assertEquals(broadcastGroupConfig.getConnectorInfos().get(0).b, connectorPairData[1]);
      
      String jsonString = broadcastGroupControl.getConnectorPairsAsJSON();
      assertNotNull(jsonString);
      JSONArray array = new JSONArray(jsonString);
      assertEquals(1, array.length());
      JSONObject data = array.getJSONObject(0);
      assertEquals(broadcastGroupConfig.getConnectorInfos().get(0).a, data.optString("a"));
      assertEquals(broadcastGroupConfig.getConnectorInfos().get(0).b, data.optString("b", null));
      
      assertTrue(broadcastGroupControl.isStarted());
   }

   public void testStartStop() throws Exception
   {
      TransportConfiguration connectorConfiguration = new TransportConfiguration(NettyConnectorFactory.class.getName());
      List<Pair<String, String>> connectorInfos = new ArrayList<Pair<String,String>>();
      connectorInfos.add(new Pair<String, String>(connectorConfiguration.getName(), null));
      BroadcastGroupConfiguration broadcastGroupConfig = randomBroadcastGroupConfiguration(connectorInfos);

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.setClustered(true);
      conf.getConnectorConfigurations().put(connectorConfiguration.getName(), connectorConfiguration);
      conf.getBroadcastGroupConfigurations().add(broadcastGroupConfig);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      service = HornetQ.newHornetQServer(conf, mbeanServer, false);
      service.start();

      BroadcastGroupControl broadcastGroupControl = createManagementControl(broadcastGroupConfig.getName());

      // started by the server
      assertTrue(broadcastGroupControl.isStarted());

      broadcastGroupControl.stop();
      assertFalse(broadcastGroupControl.isStarted());

      broadcastGroupControl.start();
      assertTrue(broadcastGroupControl.isStarted());
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
   
   protected BroadcastGroupControl createManagementControl(String name) throws Exception
   {
      return ManagementControlHelper.createBroadcastGroupControl(name, mbeanServer);
   }
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
