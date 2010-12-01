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
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.server.JournalType;
import org.hornetq.tests.integration.cluster.util.RemoteProcessHornetQServer;
import org.hornetq.tests.integration.cluster.util.RemoteServerConfiguration;
import org.hornetq.tests.integration.cluster.util.TestableServer;

public class RemoteSingleLiveMultipleBackupsFailoverTest extends SingleLiveMultipleBackupsFailoverTest
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static Map<Integer, String> backups = new HashMap<Integer, String>();
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Checks that if the live server is restarted, it will became live again after killin the current activated server.
    */
   public void testMultipleFailoversAndRestartLiveServer() throws Exception
   {
      createLiveConfig(0);
      createBackupConfig(0, 1, false, 0, 2, 3);
      createBackupConfig(0, 2, false, 0, 1, 3);
      createBackupConfig(0, 3, false, 0, 1, 2);
      servers.get(0).start();
      servers.get(1).start();
      servers.get(2).start();
      servers.get(3).start();

      ServerLocator locator = getServerLocator(0);

      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(true);
      locator.setReconnectAttempts(-1);
      ClientSessionFactoryInternal sf = createSessionFactoryAndWaitForTopology(locator, 2);
      int backupNode;
      ClientSession session = sendAndConsume(sf, true);
      System.out.println("failing live node ");
      servers.get(0).crash(session);

      session.close();
      backupNode = waitForNewLive(5, true, servers, 1, 2, 3);
      session = sendAndConsume(sf, false);

      System.out.println("restarting live node as a backup");
      createBackupConfig(0, 0, false, 1, 2, 3);
      servers.get(0).start();
      
      System.out.println("stopping waiting nodes");
      for (int i = 1; i <= 3; i++)
      {
         if (i != backupNode)
         {
            System.out.println("stopping node " + i);
            servers.get(i).stop();
         }
      }
      
      System.out.println("failing node " + backupNode);
      servers.get(backupNode).crash(session);
      session.close();
      backupNode = waitForNewLive(5, true, servers, 0);
      assertEquals(0, backupNode);
      session = sendAndConsume(sf, false);
     
      locator.close();
      
      servers.get(0).stop();
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      backups.put(0, SharedBackupServerConfiguration0.class.getName());
      backups.put(1, SharedBackupServerConfiguration1.class.getName());
      backups.put(2, SharedBackupServerConfiguration2.class.getName());
      backups.put(3, SharedBackupServerConfiguration3.class.getName());
      backups.put(4, SharedBackupServerConfiguration4.class.getName());
      backups.put(5, SharedBackupServerConfiguration5.class.getName());
   }

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();
      //make sure
      for (TestableServer testableServer : servers.values())
      {
         try
         {
            testableServer.destroy();
         }
         catch (Exception e)
         {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
         }
      }

   }

   protected boolean isNetty()
   {
      return true;
   }

   @Override
   protected void createLiveConfig(int liveNode, int... otherLiveNodes)
   {
      servers.put(liveNode, new RemoteProcessHornetQServer(SharedLiveServerConfiguration.class.getName()));
   }
   

   protected void createBackupConfig(int liveNode, int nodeid, boolean createClusterConnections, int... nodes)
   {
      servers.put(nodeid, new RemoteProcessHornetQServer(backups.get(nodeid)));
   }
   
   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   
   public static class SharedLiveServerConfiguration extends RemoteServerConfiguration
   {
      @Override
      public Configuration getConfiguration()
      {
         int liveNode = 0;
         int[] otherLiveNodes = new int[0];
         
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

   public static class SharedBackupServerConfiguration0 extends RemoteServerConfiguration
   {
      @Override
      public Configuration getConfiguration()
      {
         return createBackupConfiguration(0, 0, false, 1, 2, 3, 4, 5);
      }
   }
   
   public static class SharedBackupServerConfiguration1 extends RemoteServerConfiguration
   {
      @Override
      public Configuration getConfiguration()
      {
         return createBackupConfiguration(0, 1, false,  0, 2, 3, 4, 5);
      }
   }

   public static class SharedBackupServerConfiguration2 extends RemoteServerConfiguration
   {
      @Override
      public Configuration getConfiguration()
      {
         return createBackupConfiguration(0, 2, false,  0, 1, 3, 4, 5);
      }
   }

   public static class SharedBackupServerConfiguration3 extends RemoteServerConfiguration
   {
      @Override
      public Configuration getConfiguration()
      {
         return createBackupConfiguration(0, 3, false,  0, 1, 2, 4, 5);
      }
   }

   public static class SharedBackupServerConfiguration4 extends RemoteServerConfiguration
   {
      @Override
      public Configuration getConfiguration()
      {
         return createBackupConfiguration(0, 4, false,  0, 1, 2, 3, 5);
      }
   }

   public static class SharedBackupServerConfiguration5 extends RemoteServerConfiguration
   {
      @Override
      public Configuration getConfiguration()
      {
         return createBackupConfiguration(0, 5, false,  0, 1, 2, 3, 4);
      }
   }

   protected static Configuration createBackupConfiguration(int liveNode, int nodeid, boolean createClusterConnections, int... nodes)
   {
      Configuration config1 = new ConfigurationImpl();
      config1.getAcceptorConfigurations().add(createTransportConfiguration(true, true, generateParams(nodeid, true)));
      config1.setSecurityEnabled(false);
      config1.setSharedStore(true);
      config1.setBackup(true);
      config1.setJournalType(JournalType.NIO);
      config1.setClustered(true);
      List<String> staticConnectors = new ArrayList<String>();

      for (int node : nodes)
      {
         TransportConfiguration liveConnector = createTransportConfiguration(true, false, generateParams(node, true));
         config1.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);
         staticConnectors.add(liveConnector.getName());
      }
      TransportConfiguration backupConnector = createTransportConfiguration(true, false, generateParams(nodeid, true));
      List<String> pairs = null;
      ClusterConnectionConfiguration ccc1 = new ClusterConnectionConfiguration("cluster1", "jms", backupConnector.getName(), -1, false, false, 1, 1,
           createClusterConnections? staticConnectors:pairs);
      config1.getClusterConfigurations().add(ccc1);
      config1.getConnectorConfigurations().put(backupConnector.getName(), backupConnector);

System.out.println(config1.getBindingsDirectory());
      config1.setBindingsDirectory(config1.getBindingsDirectory() + "_" + liveNode);
      config1.setJournalDirectory(config1.getJournalDirectory() + "_" + liveNode);
      config1.setPagingDirectory(config1.getPagingDirectory() + "_" + liveNode);
      config1.setLargeMessagesDirectory(config1.getLargeMessagesDirectory() + "_" + liveNode);

      return config1;
   }
}
