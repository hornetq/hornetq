/*
 * Copyright 2010 Red Hat, Inc.
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

package org.hornetq.tests.integration.cluster.topology;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.client.impl.Topology;
import org.hornetq.core.client.impl.TopologyMember;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A IsolatedTopologyTest
 *
 * @author clebertsuconic
 *
 *
 */
public class IsolatedTopologyTest extends ServiceTestBase
{

   @Override
   public void setUp() throws Exception
   {
      super.setUp();
   }

   @Override
   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   public void testIsolatedClusters() throws Exception
   {

      HornetQServer server1 = createServer1();

      HornetQServer server2 = createServer2();

      try
      {
         server1.start();
         server2.start();

         waitForTopology(server1, "cc1", 2, 5000);

         waitForTopology(server1, "cc2", 2, 5000);

         waitForTopology(server2, "cc1", 2, 5000);

         waitForTopology(server2, "cc2", 2, 5000);

         String node1 = server1.getNodeID().toString();
         String node2 = server2.getNodeID().toString();

         checkTopology(server1,
                       "cc1",
                       node1,
                       node2,
                       createInVMTransportConnectorConfig(1, "srv1"),
                       createInVMTransportConnectorConfig(3, "srv1"));

         checkTopology(server2,
                       "cc1",
                       node1,
                       node2,
                       createInVMTransportConnectorConfig(1, "srv1"),
                       createInVMTransportConnectorConfig(3, "srv1"));

         checkTopology(server1,
                       "cc2",
                       node1,
                       node2,
                       createInVMTransportConnectorConfig(2, "srv1"),
                       createInVMTransportConnectorConfig(4, "srv1"));

         checkTopology(server2,
                       "cc2",
                       node1,
                       node2,
                       createInVMTransportConnectorConfig(2, "srv1"),
                       createInVMTransportConnectorConfig(4, "srv1"));
      }
      finally
      {
         Thread.sleep(500);
         try
         {
            server1.stop();
         }
         catch (Throwable ignored)
         {
         }

         try
         {
            server2.stop();
         }
         catch (Throwable ignored)
         {
         }
      }
   }

   private void checkTopology(final HornetQServer serverParameter,
                              final String clusterName,
                              final String nodeId1,
                              final String nodeId2,
                              final TransportConfiguration cfg1,
                              final TransportConfiguration cfg2)
   {
      Topology topology = serverParameter.getClusterManager().getClusterConnection(clusterName).getTopology();

      TopologyMember member1 = topology.getMember(nodeId1);
      TopologyMember member2 = topology.getMember(nodeId2);
      Assert.assertEquals(member1.getA().toString(), cfg1.toString());
      Assert.assertEquals(member2.getA().toString(), cfg2.toString());
   }

   private HornetQServer createServer1()
   {
      // Server1 with two acceptors, each acceptor on a different cluster connection
      // talking to a different connector.
      // i.e. two cluster connections isolated on the same node
      Configuration config1 = createBasicConfig(0);

      config1.setClustered(true);

      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.CLUSTER_CONNECTION, "cc1");
      params.put(org.hornetq.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME, "1");

      TransportConfiguration acceptor1VM1 = new TransportConfiguration(ServiceTestBase.INVM_ACCEPTOR_FACTORY,
                                                                       params,
                                                                       "acceptor-cc1");
      config1.getAcceptorConfigurations().add(acceptor1VM1);

      config1.getConnectorConfigurations().put("local-cc1", createInVMTransportConnectorConfig(1, "local-cc1"));
      config1.getConnectorConfigurations().put("local-cc2", createInVMTransportConnectorConfig(2, "local-cc2"));

      config1.getConnectorConfigurations().put("other-cc1", createInVMTransportConnectorConfig(3, "other-cc1"));
      config1.getConnectorConfigurations().put("other-cc2", createInVMTransportConnectorConfig(4, "other-cc2"));

      params = new HashMap<String, Object>();
      params.put(TransportConstants.CLUSTER_CONNECTION, "cc2");
      params.put(org.hornetq.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME, "2");

      TransportConfiguration acceptor2VM1 = new TransportConfiguration(ServiceTestBase.INVM_ACCEPTOR_FACTORY,
                                                                       params,
                                                                       "acceptor-cc2");
      config1.getAcceptorConfigurations().add(acceptor2VM1);

      List<String> connectTo = new ArrayList<String>();
      connectTo.add("other-cc1");

      ClusterConnectionConfiguration server1CC1 = new ClusterConnectionConfiguration("cc1",
                                                                                     "jms",
                                                                                     "local-cc1",
                                                                                     250,
                                                                                     true,
                                                                                     false,
                                                                                     1,
                                                                                     1024,
                                                                                     connectTo,
                                                                                     false);

      config1.getClusterConfigurations().add(server1CC1);

      connectTo = new ArrayList<String>();
      connectTo.add("other-cc2");

      ClusterConnectionConfiguration server1CC2 = new ClusterConnectionConfiguration("cc2",
                                                                                     "jms",
                                                                                     "local-cc2",
                                                                                     250,
                                                                                     true,
                                                                                     false,
                                                                                     1,
                                                                                     1024,
                                                                                     connectTo,
                                                                                     false);

      config1.getClusterConfigurations().add(server1CC2);

      return createServer(false, config1);
   }

   private HornetQServer createServer2()
   {
      // Server1 with two acceptors, each acceptor on a different cluster connection
      // talking to a different connector.
      // i.e. two cluster connections isolated on the same node
      Configuration config1 = createBasicConfig(3);

      config1.setClustered(true);

      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.CLUSTER_CONNECTION, "cc1");
      params.put(org.hornetq.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME, "3");

      TransportConfiguration acceptor1VM1 = new TransportConfiguration(ServiceTestBase.INVM_ACCEPTOR_FACTORY,
                                                                       params,
                                                                       "acceptor-cc1");
      config1.getAcceptorConfigurations().add(acceptor1VM1);

      config1.getConnectorConfigurations().put("local-cc1", createInVMTransportConnectorConfig(3, "local-cc1"));
      config1.getConnectorConfigurations().put("local-cc2", createInVMTransportConnectorConfig(4, "local-cc2"));

      config1.getConnectorConfigurations().put("other-cc1", createInVMTransportConnectorConfig(1, "other-cc1"));
      config1.getConnectorConfigurations().put("other-cc2", createInVMTransportConnectorConfig(2, "other-cc2"));

      params = new HashMap<String, Object>();
      params.put(TransportConstants.CLUSTER_CONNECTION, "cc2");
      params.put(org.hornetq.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME, "4");

      TransportConfiguration acceptor2VM1 = new TransportConfiguration(ServiceTestBase.INVM_ACCEPTOR_FACTORY,
                                                                       params,
                                                                       "acceptor-cc2");
      config1.getAcceptorConfigurations().add(acceptor2VM1);

      List<String> connectTo = new ArrayList<String>();
      connectTo.add("other-cc1");

      ClusterConnectionConfiguration server1CC1 = new ClusterConnectionConfiguration("cc1",
                                                                                     "jms",
                                                                                     "local-cc1",
                                                                                     250,
                                                                                     true,
                                                                                     false,
                                                                                     1,
                                                                                     1024,
                                                                                     connectTo,
                                                                                     false);

      config1.getClusterConfigurations().add(server1CC1);

      connectTo = new ArrayList<String>();
      connectTo.add("other-cc2");

      ClusterConnectionConfiguration server1CC2 = new ClusterConnectionConfiguration("cc2",
                                                                                     "jms",
                                                                                     "local-cc2",
                                                                                     250,
                                                                                     true,
                                                                                     false,
                                                                                     1,
                                                                                     1024,
                                                                                     connectTo,
                                                                                     false);

      config1.getClusterConfigurations().add(server1CC2);

      return createServer(false, config1);
   }

}
