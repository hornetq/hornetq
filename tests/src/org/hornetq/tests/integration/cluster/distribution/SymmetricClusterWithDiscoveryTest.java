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

package org.hornetq.tests.integration.cluster.distribution;

import org.hornetq.api.core.HornetQException;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.cluster.BroadcastGroup;
import org.hornetq.core.server.cluster.ClusterConnection;
import org.hornetq.core.server.cluster.MessageFlowRecord;
import org.hornetq.core.server.cluster.impl.ClusterConnectionBridge;
import org.hornetq.core.server.cluster.impl.ClusterConnectionImpl;

/**
 * A SymmetricClusterWithDiscoveryTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 3 Feb 2009 09:10:43
 *
 *
 */
public class SymmetricClusterWithDiscoveryTest extends SymmetricClusterTest
{
   private static final Logger log = Logger.getLogger(SymmetricClusterWithDiscoveryTest.class);

   protected final String groupAddress = getUDPDiscoveryAddress();

   protected final int groupPort = getUDPDiscoveryPort();

   protected boolean isNetty()
   {
      return false;
   }

   @Override
   protected void setupCluster() throws Exception
   {
      setupCluster(false);
   }

   @Override
   protected void setupCluster(final boolean forwardWhenNoConsumers) throws Exception
   {
      setupDiscoveryClusterConnection("cluster0", 0, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());

      setupDiscoveryClusterConnection("cluster1", 1, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());

      setupDiscoveryClusterConnection("cluster2", 2, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());

      setupDiscoveryClusterConnection("cluster3", 3, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());

      setupDiscoveryClusterConnection("cluster4", 4, "dg1", "queues", forwardWhenNoConsumers, 1, isNetty());
   }

   @Override
   protected void setupServers() throws Exception
   {
      setupLiveServerWithDiscovery(0, groupAddress, groupPort, isFileStorage(), isNetty(), false);
      setupLiveServerWithDiscovery(1, groupAddress, groupPort, isFileStorage(), isNetty(), false);
      setupLiveServerWithDiscovery(2, groupAddress, groupPort, isFileStorage(), isNetty(), false);
      setupLiveServerWithDiscovery(3, groupAddress, groupPort, isFileStorage(), isNetty(), false);
      setupLiveServerWithDiscovery(4, groupAddress, groupPort, isFileStorage(), isNetty(), false);
   }

   public void testTemporaryFailure() throws Throwable
   {
      setupCluster();

      for (ClusterConnectionConfiguration config : servers[0].getConfiguration().getClusterConfigurations())
      {
         config.setConnectionTTL(5000);
         config.setClientFailureCheckPeriod(1000);
      }

      startServers(0, 1);

      setupSessionFactory(0, isNetty());
      setupSessionFactory(1, isNetty());

      createQueue(0, "queues.testaddress", "queue0", null, false);
      createQueue(1, "queues.testaddress", "queue0", null, false);

      addConsumer(0, 0, "queue0", null);
      addConsumer(1, 1, "queue0", null);

      waitForBindings(0, "queues.testaddress", 1, 1, true);
      waitForBindings(1, "queues.testaddress", 1, 1, true);


      waitForBindings(0, "queues.testaddress", 1, 1, false);
      waitForBindings(1, "queues.testaddress", 1, 1, false);

      System.out.println(clusterDescription(servers[0]));
      
      
      for (BroadcastGroup group: servers[1].getClusterManager().getBroadcastGroups())
      {
         group.stop();
      }

      for (ClusterConnection conn : servers[0].getClusterManager().getClusterConnections())
      {
         ClusterConnectionImpl implConn = (ClusterConnectionImpl)conn;
         for (MessageFlowRecord record : implConn.getRecords().values())
         {
            ClusterConnectionBridge bridge = (ClusterConnectionBridge)record.getBridge();
            bridge.setupRetry(2, 1);
            bridge.connectionFailed(new HornetQException(1, "test"), false);
         }
         System.out.println("conn " + conn);
      }
      
      
      // More than 1 second timeout required to cleanup broadcasting
      Thread.sleep(1500);
      
      waitForTopology(servers[0], 1);
      
      waitForTopology(servers[1], 1);


      System.out.println(clusterDescription(servers[0]));

      waitForBindings(0, "queues.testaddress", 0, 0, false);
      waitForBindings(1, "queues.testaddress", 1, 1, false);

      
      
      for (BroadcastGroup group: servers[1].getClusterManager().getBroadcastGroups())
      {
         group.start();
      }

      waitForBindings(0, "queues.testaddress", 1, 1, false);
      waitForBindings(1, "queues.testaddress", 1, 1, false);
      
      waitForTopology(servers[0], 2);
      
      waitForTopology(servers[1], 2);

   }

   /*
    * This is like testStopStartServers but we make sure we pause longer than discovery group timeout
    * before restarting (5 seconds)
    */
   public void _testStartStopServersWithPauseBeforeRestarting() throws Exception
   {
      doTestStartStopServers(10000, 3000);
   }

}
