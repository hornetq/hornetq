/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package org.hornetq.tests.integration.cluster.failover;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorImpl;
import org.hornetq.core.client.impl.Topology;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.impl.InVMNodeManager;
import org.hornetq.tests.integration.cluster.util.SameProcessHornetQServer;
import org.hornetq.tests.integration.cluster.util.TestableServer;

/**
 */
public class SingleLiveMultipleBackupsFailoverTest extends MultipleBackupsFailoverTestBase
{

   protected Map<Integer, TestableServer> servers = new HashMap<Integer, TestableServer>();
   private NodeManager nodeManager;
   
   Logger log = Logger.getLogger(SingleLiveMultipleBackupsFailoverTest.class);

   public void _testLoop() throws Exception
   {
      for (int i = 0 ; i < 100; i++)
      {
         log.info("#test " + i);
         testMultipleFailovers();
         tearDown();
         setUp();
      }
   }
   public void testMultipleFailovers() throws Exception
   {
      nodeManager = new InVMNodeManager();
      createLiveConfig(0);
      createBackupConfig(0, 1, 0, 2, 3, 4, 5);
      createBackupConfig(0, 2, 0, 1, 3, 4, 5);
      createBackupConfig(0, 3, 0, 1, 2, 4, 5);
      createBackupConfig(0, 4, 0, 1, 2, 3, 5);
      createBackupConfig(0, 5, 0, 1, 2, 3, 4);
      
      servers.get(0).start();
      waitForServer(servers.get(0).getServer());
      servers.get(1).start();
      waitForServer(servers.get(1).getServer());
      servers.get(2).start();
      servers.get(3).start();
      servers.get(4).start();
      servers.get(5).start();

      ServerLocatorImpl locator = (ServerLocatorImpl)getServerLocator(0);
      
      Topology topology = locator.getTopology();
      
      // for logging and debugging
      topology.setOwner("testMultipleFailovers");

      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(true);
      locator.setReconnectAttempts(-1);
      ClientSessionFactoryInternal sf = createSessionFactoryAndWaitForTopology(locator, 2);
      int backupNode;
      ClientSession session = sendAndConsume(sf, true);

      log.info("failing node 0");
      servers.get(0).crash(session);
      
      session.close();
      backupNode = waitForNewLive(5, true, servers, 1, 2, 3, 4, 5);
      session = sendAndConsume(sf, false);
      log.info("failing node " + backupNode);
      servers.get(backupNode).crash(session);

      session.close();
      backupNode = waitForNewLive(5, true, servers, 1, 2, 3, 4, 5);
      session = sendAndConsume(sf, false);
      log.info("failing node " + backupNode);
      servers.get(backupNode).crash(session);

      session.close();
      backupNode = waitForNewLive(5, true, servers, 1, 2, 3, 4, 5);
      session = sendAndConsume(sf, false);
      log.info("failing node " + backupNode);
      servers.get(backupNode).crash(session);

      session.close();
      backupNode = waitForNewLive(5, true, servers, 1, 2, 3, 4, 5);
      session = sendAndConsume(sf, false);
      log.info("failing node " + backupNode);
      servers.get(backupNode).crash(session);

      session.close();
      backupNode = waitForNewLive(5, false, servers, 1, 2, 3, 4, 5);
      session = sendAndConsume(sf, false);
      session.close();
      servers.get(backupNode).stop();

      locator.close();
   }
   
   protected void createBackupConfig(int liveNode, int nodeid, int... nodes)
   {
      Configuration config1 = super.createDefaultConfig();
      config1.getAcceptorConfigurations().clear();
      config1.getAcceptorConfigurations().add(createTransportConfiguration(isNetty(), true,
                                                                           generateParams(nodeid, isNetty())));
      config1.setSecurityEnabled(false);
      config1.setSharedStore(true);
      config1.setBackup(true);
      config1.setClustered(true);
      List<String> staticConnectors = new ArrayList<String>();

      for (int node : nodes)
      {
         TransportConfiguration liveConnector =
                  createTransportConfiguration(isNetty(), false, generateParams(node, isNetty()));
         config1.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);
         staticConnectors.add(liveConnector.getName());
      }
      TransportConfiguration backupConnector =
               createTransportConfiguration(isNetty(), false, generateParams(nodeid, isNetty()));
      ClusterConnectionConfiguration ccc1 =
               new ClusterConnectionConfiguration("cluster1", "jms", backupConnector.getName(), -1, false, false, 1, 1,
                                                  staticConnectors, false);
      config1.getClusterConfigurations().add(ccc1);
      config1.getConnectorConfigurations().put(backupConnector.getName(), backupConnector);

      config1.setBindingsDirectory(config1.getBindingsDirectory() + "_" + liveNode);
      config1.setJournalDirectory(config1.getJournalDirectory() + "_" + liveNode);
      config1.setPagingDirectory(config1.getPagingDirectory() + "_" + liveNode);
      config1.setLargeMessagesDirectory(config1.getLargeMessagesDirectory() + "_" + liveNode);

      servers.put(nodeid, new SameProcessHornetQServer(createInVMFailoverServer(true, config1, nodeManager, nodeid)));
   }

   protected void createLiveConfig(int liveNode, int... otherLiveNodes)
   {
      TransportConfiguration liveConnector =
               createTransportConfiguration(isNetty(), false, generateParams(liveNode, isNetty()));
      Configuration config0 = super.createDefaultConfig();
      config0.getAcceptorConfigurations().clear();
      config0.getAcceptorConfigurations().add(createTransportConfiguration(isNetty(), true,
                                                                           generateParams(liveNode, isNetty())));
      config0.setSecurityEnabled(false);
      config0.setSharedStore(true);
      config0.setClustered(true);
      List<String> pairs = null;
      for (int node : otherLiveNodes)
      {
         TransportConfiguration otherLiveConnector =
                  createTransportConfiguration(isNetty(), false, generateParams(node, isNetty()));
         config0.getConnectorConfigurations().put(otherLiveConnector.getName(), otherLiveConnector);
         pairs.add(otherLiveConnector.getName());  

      }
      ClusterConnectionConfiguration ccc0 =
               new ClusterConnectionConfiguration("cluster1", "jms", liveConnector.getName(), -1, false, false, 1, 1,
                                                  pairs, false);
      config0.getClusterConfigurations().add(ccc0);
      config0.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);

      config0.setBindingsDirectory(config0.getBindingsDirectory() + "_" + liveNode);
      config0.setJournalDirectory(config0.getJournalDirectory() + "_" + liveNode);
      config0.setPagingDirectory(config0.getPagingDirectory() + "_" + liveNode);
      config0.setLargeMessagesDirectory(config0.getLargeMessagesDirectory() + "_" + liveNode);

      servers.put(liveNode, new SameProcessHornetQServer(createInVMFailoverServer(true, config0, nodeManager, liveNode)));
   }

   @Override
   protected boolean isNetty()
   {
      return false;
   }

   @Override
   protected void tearDown() throws Exception
   {
      for (Entry<Integer, TestableServer> entry : servers.entrySet())
      {
         try
         {
            entry.getValue().stop();
         }
         catch (Exception e)
         {
            // ignore
         }
      }
      servers.clear();
      super.tearDown();
   }

}
