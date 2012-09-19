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

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.impl.InVMNodeManager;
import org.hornetq.tests.integration.cluster.util.SameProcessHornetQServer;
import org.hornetq.tests.integration.cluster.util.TestableServer;

/**
 */
public class MultipleLivesMultipleBackupsFailoverTest extends MultipleBackupsFailoverTestBase
{
   protected Map<Integer, TestableServer> servers = new HashMap<Integer, TestableServer>();
   private ServerLocator locator2;
   private ServerLocator locator;

   @Override
   protected void tearDown() throws Exception
   {
      try
      {
         closeServerLocator(locator);
         closeServerLocator(locator2);
      }
      finally
      {
         super.tearDown();
      }
   }

   public void testMultipleFailovers2LiveServers() throws Exception
   {
      NodeManager nodeManager1 = new InVMNodeManager();
      NodeManager nodeManager2 = new InVMNodeManager();
      createLiveConfig(nodeManager1, 0, 3, 4, 5);
      createBackupConfig(nodeManager1, 0, 1, true, new int[] { 0, 2 }, 3, 4, 5);
      createBackupConfig(nodeManager1, 0, 2, true, new int[] { 0, 1 }, 3, 4, 5);
      createLiveConfig(nodeManager2, 3, 0);
      createBackupConfig(nodeManager2, 3, 4, true, new int[] { 3, 5 }, 0, 1, 2);
      createBackupConfig(nodeManager2, 3, 5, true, new int[] { 3, 4 }, 0, 1, 2);

      servers.get(0).start();
      waitForServer(servers.get(0).getServer());

      servers.get(3).start();
      waitForServer(servers.get(3).getServer());

      servers.get(1).start();
      waitForServer(servers.get(1).getServer());

      servers.get(2).start();

      servers.get(4).start();
      waitForServer(servers.get(4).getServer());

      servers.get(5).start();

      waitForServer(servers.get(4).getServer());

      locator = getServerLocator(0);

      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(true);
      locator.setReconnectAttempts(-1);
      ClientSessionFactoryInternal sf = createSessionFactoryAndWaitForTopology(locator, 4, servers.get(0).getServer());
      ClientSession session = sendAndConsume(sf, true);

      System.out.println(((ServerLocatorInternal)locator).getTopology().describe());
      Thread.sleep(500);
      servers.get(0).crash(session);

      int liveAfter0 = waitForNewLive(10000, true, servers, 1, 2);

      locator2 = getServerLocator(3);
      locator2.setBlockOnNonDurableSend(true);
      locator2.setBlockOnDurableSend(true);
      locator2.setBlockOnAcknowledge(true);
      locator2.setReconnectAttempts(-1);
      ClientSessionFactoryInternal sf2 = createSessionFactoryAndWaitForTopology(locator2, 4);
      ClientSession session2 = sendAndConsume(sf2, true);

      System.setProperty("foo", "bar");
      servers.get(3).crash(session2);
      int liveAfter3 = waitForNewLive(10000, true, servers, 4, 5);
      locator.close();
      locator2.close();
      if (liveAfter0 == 2)
      {
         Thread.sleep(500);
         servers.get(1).stop();
         Thread.sleep(500);
         servers.get(2).stop();
      }
      else
      {
         Thread.sleep(500);
         servers.get(2).stop();
         Thread.sleep(500);
         servers.get(1).stop();
      }

      if (liveAfter3 == 4)
      {
         Thread.sleep(500);
         servers.get(5).stop();
         Thread.sleep(500);
         servers.get(4).stop();
      }
      else
      {
         Thread.sleep(500);
         servers.get(4).stop();
         Thread.sleep(500);
         servers.get(5).stop();
      }
   }

   protected void createBackupConfig(NodeManager nodeManager,
                                     int liveNode,
                                     int nodeid,
                                     boolean createClusterConnections,
                                     int[] otherBackupNodes,
                                     int... otherClusterNodes) throws Exception
   {
      Configuration config1 = super.createDefaultConfig();
      config1.getAcceptorConfigurations().clear();
      config1.getAcceptorConfigurations().add(createTransportConfiguration(isNetty(),
                                                                           true,
                                                                           generateParams(nodeid, isNetty())));
      config1.setSecurityEnabled(false);
      config1.setSharedStore(true);
      config1.setBackup(true);

      List<String> staticConnectors = new ArrayList<String>();
      for (int node : otherBackupNodes)
      {
         TransportConfiguration liveConnector = createTransportConfiguration(isNetty(),
                                                                             false,
                                                                             generateParams(node, isNetty()));
         config1.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);
         staticConnectors.add(liveConnector.getName());
      }
      TransportConfiguration backupConnector = createTransportConfiguration(isNetty(),
                                                                            false,
                                                                            generateParams(nodeid, isNetty()));
      config1.getConnectorConfigurations().put(backupConnector.getName(), backupConnector);

      List<String> clusterNodes = new ArrayList<String>();
      for (int node : otherClusterNodes)
      {
         TransportConfiguration connector = createTransportConfiguration(isNetty(),
                                                                         false,
                                                                         generateParams(node, isNetty()));
         config1.getConnectorConfigurations().put(connector.getName(), connector);
         clusterNodes.add(connector.getName());
      }
      basicClusterConnectionConfig(config1, backupConnector.getName(), clusterNodes);
      config1.setBindingsDirectory(config1.getBindingsDirectory() + "_" + liveNode);
      config1.setJournalDirectory(config1.getJournalDirectory() + "_" + liveNode);
      config1.setPagingDirectory(config1.getPagingDirectory() + "_" + liveNode);
      config1.setLargeMessagesDirectory(config1.getLargeMessagesDirectory() + "_" + liveNode);

      servers.put(nodeid, new SameProcessHornetQServer(createInVMFailoverServer(true, config1, nodeManager, liveNode)));
   }

   protected void createLiveConfig(NodeManager nodeManager, int liveNode, int... otherLiveNodes) throws Exception
   {
      TransportConfiguration liveConnector = createTransportConfiguration(isNetty(),
                                                                          false,
                                                                          generateParams(liveNode, isNetty()));
      Configuration config0 = super.createDefaultConfig();
      config0.getAcceptorConfigurations().clear();
      config0.getAcceptorConfigurations().add(createTransportConfiguration(isNetty(),
                                                                           true,
                                                                           generateParams(liveNode, isNetty())));
      config0.setSecurityEnabled(false);
      config0.setSharedStore(true);
      List<String> pairs = new ArrayList<String>();
      for (int node : otherLiveNodes)
      {
         TransportConfiguration otherLiveConnector = createTransportConfiguration(isNetty(),
                                                                                  false,
                                                                                  generateParams(node, isNetty()));
         config0.getConnectorConfigurations().put(otherLiveConnector.getName(), otherLiveConnector);
         pairs.add(otherLiveConnector.getName());

      }
      basicClusterConnectionConfig(config0, liveConnector.getName(), pairs);
      config0.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);

      config0.setBindingsDirectory(config0.getBindingsDirectory() + "_" + liveNode);
      config0.setJournalDirectory(config0.getJournalDirectory() + "_" + liveNode);
      config0.setPagingDirectory(config0.getPagingDirectory() + "_" + liveNode);
      config0.setLargeMessagesDirectory(config0.getLargeMessagesDirectory() + "_" + liveNode);

      servers.put(liveNode,
                  new SameProcessHornetQServer(createInVMFailoverServer(true, config0, nodeManager, liveNode)));
   }

   @Override
   protected boolean isNetty()
   {
      return false;
   }
}
