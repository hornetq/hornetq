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

package org.hornetq.tests.integration.cluster.failover;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.server.JournalType;
import org.hornetq.tests.integration.cluster.util.RemoteProcessHornetQServer;
import org.hornetq.tests.integration.cluster.util.RemoteServerConfiguration;

/**
 * A RemoteMultipleLivesMultipleBackupsFailoverTest
 *
 * @author jmesnil
 *
 *
 */
public class RemoteMultipleLivesMultipleBackupsFailoverTest extends MultipleLivesMultipleBackupsFailoverTest
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static Map<Integer, String> lives = new HashMap<Integer, String>();
   private static Map<Integer, String> backups = new HashMap<Integer, String>();
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      lives.put(0, LiveServerConfiguration0.class.getName());
      lives.put(3, LiveServerConfiguration3.class.getName());

      backups.put(1, SharedBackupServerConfiguration1.class.getName());
      backups.put(2, SharedBackupServerConfiguration2.class.getName());
      backups.put(4, SharedBackupServerConfiguration4.class.getName());
      backups.put(5, SharedBackupServerConfiguration5.class.getName());
   }
   
   protected boolean isNetty()
   {
      return true;
   }


   protected void createLiveConfig(int liveNode, int... otherLiveNodes)
   {
      servers.put(liveNode, new RemoteProcessHornetQServer(lives.get(liveNode)));
   }
   
   
   
   protected void createBackupConfig(int liveNode,
                                     int nodeid,
                                     boolean createClusterConnections,
                                     int[] otherBackupNodes,
                                     int... otherClusterNodes)
   {
      servers.put(nodeid, new RemoteProcessHornetQServer(backups.get(nodeid)));
   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   
   public static class LiveServerConfiguration0 extends RemoteServerConfiguration
   {
      @Override
      public Configuration getConfiguration()
      {
         return createLiveConfiguration(0, 3, 4, 5);
      }
   }

   public static class LiveServerConfiguration3 extends RemoteServerConfiguration
   {
      @Override
      public Configuration getConfiguration()
      {
         return createLiveConfiguration(3, 0, 1, 2);
      }
   }
   
   public static class SharedBackupServerConfiguration1 extends RemoteServerConfiguration
   {
      @Override
      public Configuration getConfiguration()
      {
         return createBackupConfiguration(0, 1, true, new int[] {0, 2}, 3, 4, 5);
      }
   }

   public static class SharedBackupServerConfiguration2 extends RemoteServerConfiguration
   {
      @Override
      public Configuration getConfiguration()
      {
         return createBackupConfiguration(0, 2, true, new int[] {0, 1}, 3, 4, 5);
      }
   }

   public static class SharedBackupServerConfiguration4 extends RemoteServerConfiguration
   {
      @Override
      public Configuration getConfiguration()
      {
         return createBackupConfiguration(3, 4, true, new int[] {3, 5}, 0, 1, 2);
      }
   }

   public static class SharedBackupServerConfiguration5 extends RemoteServerConfiguration
   {
      @Override
      public Configuration getConfiguration()
      {
         return createBackupConfiguration(3, 5, true, new int[] {3, 4}, 0, 1, 2);
      }
   }

   protected static Configuration createBackupConfiguration(int liveNode, int nodeid, boolean createClusterConnections,  int[] otherBackupNodes, int... otherClusterNodes)
   {
      Configuration config1 = new ConfigurationImpl();
      config1.getAcceptorConfigurations().add(createTransportConfiguration(true, true, generateParams(nodeid, true)));
      config1.setSecurityEnabled(false);
      config1.setSharedStore(true);
      config1.setBackup(true);
      config1.setJournalType(JournalType.NIO);
      config1.setClustered(true);

      List<String> staticConnectors = new ArrayList<String>();
      for (int node : otherBackupNodes)
      {
         TransportConfiguration liveConnector = createTransportConfiguration(true, false, generateParams(node, true));
         config1.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);
         staticConnectors.add(liveConnector.getName());
      }
      TransportConfiguration backupConnector = createTransportConfiguration(true, false, generateParams(nodeid, true));
      config1.getConnectorConfigurations().put(backupConnector.getName(), backupConnector);

      List<String> clusterNodes = new ArrayList<String>();
      for (int node : otherClusterNodes)
      {
         TransportConfiguration connector = createTransportConfiguration(true, false, generateParams(node, true));
         config1.getConnectorConfigurations().put(connector.getName(), connector);
         clusterNodes.add(connector.getName());
      }
      ClusterConnectionConfiguration ccc1 = new ClusterConnectionConfiguration("cluster1", "jms", backupConnector.getName(), -1, false, false, 1, 1, clusterNodes);
      config1.getClusterConfigurations().add(ccc1);



      config1.setBindingsDirectory(config1.getBindingsDirectory() + "_" + liveNode);
      config1.setJournalDirectory(config1.getJournalDirectory() + "_" + liveNode);
      config1.setPagingDirectory(config1.getPagingDirectory() + "_" + liveNode);
      config1.setLargeMessagesDirectory(config1.getLargeMessagesDirectory() + "_" + liveNode);

      return config1;
   }

   protected static Configuration createLiveConfiguration(int liveNode, int... otherLiveNodes)
   {
      Configuration config0 = new ConfigurationImpl();
      TransportConfiguration liveConnector = createTransportConfiguration(true, false, generateParams(liveNode, true));
      config0.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);
      config0.getAcceptorConfigurations().add(createTransportConfiguration(true, true, generateParams(liveNode, true)));
      config0.setSecurityEnabled(false);
      config0.setSharedStore(true);
      config0.setJournalType(JournalType.NIO);
      config0.setClustered(true);
      List<String> pairs = new ArrayList<String>();
      for (int node : otherLiveNodes)
      {
         TransportConfiguration otherLiveConnector = createTransportConfiguration(true, false, generateParams(node, true));
         config0.getConnectorConfigurations().put(otherLiveConnector.getName(), otherLiveConnector);
         pairs.add(otherLiveConnector.getName());  

      }
      ClusterConnectionConfiguration ccc0 = new ClusterConnectionConfiguration("cluster1", "jms", liveConnector.getName(), -1, false, false, 1, 1,
            pairs);
      config0.getClusterConfigurations().add(ccc0);
      config0.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);

      config0.setBindingsDirectory(config0.getBindingsDirectory() + "_" + liveNode);
      config0.setJournalDirectory(config0.getJournalDirectory() + "_" + liveNode);
      config0.setPagingDirectory(config0.getPagingDirectory() + "_" + liveNode);
      config0.setLargeMessagesDirectory(config0.getLargeMessagesDirectory() + "_" + liveNode);
      
      return config0;
   }
}
