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

package org.hornetq.tests.integration.cluster.failover;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnector;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.impl.InVMNodeManager;
import org.hornetq.tests.integration.cluster.util.SameProcessHornetQServer;
import org.hornetq.tests.integration.cluster.util.TestableServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A FailoverTestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public abstract class FailoverTestBase extends ServiceTestBase
{
   // Constants -----------------------------------------------------

   protected static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");
   protected static final String LIVE_NODE_NAME = "hqLIVE";

   // Attributes ----------------------------------------------------

   protected TestableServer liveServer;

   protected TestableServer backupServer;

   protected Configuration backupConfig;

   protected Configuration liveConfig;

   protected NodeManager nodeManager;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   /**
    * @param name
    */
   public FailoverTestBase(final String name)
   {
      super(name);
   }

   public FailoverTestBase()
   {
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      clearData();
      createConfigs();

      liveServer.start();

      if (backupServer != null)
      {
         backupServer.start();
      }
   }

   protected TestableServer createLiveServer()
   {
      return new SameProcessHornetQServer(createInVMFailoverServer(true, liveConfig, nodeManager));
   }

   protected TestableServer createBackupServer()
   {
      return new SameProcessHornetQServer(createInVMFailoverServer(true, backupConfig, nodeManager));
   }

   private ClusterConnectionConfiguration createClusterConnectionConf(String name, String... connectors)
   {
      List<String> conn = new ArrayList<String>(connectors.length);
      for (String iConn : connectors)
      {
         conn.add(iConn);
      }
      return new ClusterConnectionConfiguration("cluster1", "jms", name, -1, false, false, 1, 1, conn, false);
   }

   /**
    * @throws Exception
    */
   protected void createConfigs() throws Exception
   {
      nodeManager = new InVMNodeManager();

      backupConfig = super.createDefaultConfig();
      backupConfig.getAcceptorConfigurations().clear();
      backupConfig.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(false));
      backupConfig.setSecurityEnabled(false);
      backupConfig.setSharedStore(true);
      backupConfig.setBackup(true);
      backupConfig.setClustered(true);
      TransportConfiguration liveConnector = getConnectorTransportConfiguration(true);
      TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);
      backupConfig.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);
      backupConfig.getConnectorConfigurations().put(backupConnector.getName(), backupConnector);
      backupConfig.getClusterConfigurations().add(createClusterConnectionConf(backupConnector.getName(),
                                                                              liveConnector.getName()));
      backupServer = createBackupServer();

      liveConfig = super.createDefaultConfig();
      liveConfig.getAcceptorConfigurations().clear();
      liveConfig.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(true));
      liveConfig.setSecurityEnabled(false);
      liveConfig.setSharedStore(true);
      liveConfig.setClustered(true);
      liveConfig.getClusterConfigurations().add(createClusterConnectionConf(liveConnector.getName()));
      liveConfig.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);
      liveServer = createLiveServer();
   }

   protected void createReplicatedConfigs() throws Exception
   {
      nodeManager = new InVMNodeManager();

      backupConfig = super.createDefaultConfig();
      backupConfig.setBindingsDirectory(backupConfig.getBindingsDirectory() + "_backup");
      backupConfig.setJournalDirectory(backupConfig.getJournalDirectory() + "_backup");
      backupConfig.setPagingDirectory(backupConfig.getPagingDirectory() + "_backup");
      backupConfig.setLargeMessagesDirectory(backupConfig.getLargeMessagesDirectory() + "_backup");
      backupConfig.getAcceptorConfigurations().clear();
      backupConfig.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(false));
      TransportConfiguration liveConnector = getConnectorTransportConfiguration(true);
      TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);
      // probably not necessary...
      backupConfig.getConnectorConfigurations().put(backupConnector.getName(), backupConnector);
      backupConfig.getConnectorConfigurations().put(LIVE_NODE_NAME, liveConnector);
      backupConfig.getClusterConfigurations()
                  .add(createClusterConnectionConf(backupConnector.getName(), LIVE_NODE_NAME));
      backupConfig.getConnectorConfigurations().put(LIVE_NODE_NAME, getConnectorTransportConfiguration(true));

      backupConfig.setSecurityEnabled(false);
      backupConfig.setSharedStore(false);
      backupConfig.setBackup(true);
      backupConfig.setLiveConnectorName(LIVE_NODE_NAME);
      backupConfig.setClustered(true);

      backupServer = createBackupServer();
      backupServer.getServer().setIdentity("id_backup");

      liveConfig = super.createDefaultConfig();
      liveConfig.getAcceptorConfigurations().clear();
      liveConfig.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(true));
      liveConfig.setName(LIVE_NODE_NAME);
      liveConfig.setSecurityEnabled(false);
      liveConfig.setSharedStore(false);
      liveConfig.setClustered(true);
      liveConfig.getClusterConfigurations().add(createClusterConnectionConf(liveConnector.getName()));
      liveServer = createLiveServer();
      liveServer.getServer().setIdentity("id_live");

      liveServer.start();
      backupServer.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      backupServer.stop();
      liveServer.stop();

      Assert.assertEquals(0, InVMRegistry.instance.size());

      backupServer = null;
      liveServer = null;
      nodeManager = null;

      InVMConnector.failOnCreateConnection = false;

      super.tearDown();
      try
      {
         ServerSocket serverSocket = new ServerSocket(5445);
         serverSocket.close();
      }
      catch (IOException e)
      {
         e.printStackTrace();
         System.exit(9);
      }
      try
      {
         ServerSocket serverSocket = new ServerSocket(5446);
         serverSocket.close();
      }
      catch (IOException e)
      {
         e.printStackTrace();
         System.exit(9);
      }
   }

   protected ClientSessionFactoryInternal createSessionFactoryAndWaitForTopology(ServerLocator locator, int topologyMembers)
         throws Exception
   {
      ClientSessionFactoryInternal sf;
      CountDownLatch countDownLatch = new CountDownLatch(topologyMembers);

      locator.addClusterTopologyListener(new LatchClusterTopologyListener(countDownLatch));

      sf = (ClientSessionFactoryInternal) locator.createSessionFactory();

      assertTrue("topology members expected " + topologyMembers, countDownLatch.await(5, TimeUnit.SECONDS));
      return sf;
   }

   protected void waitForBackup(ClientSessionFactoryInternal sf, long seconds)
         throws Exception
   {
      long time = System.currentTimeMillis();
      long toWait = seconds * 1000;
      while (sf.getBackupConnector() == null)
      {
         try
         {
            Thread.sleep(100);
         }
         catch (InterruptedException e)
         {
            //ignore
         }
         if (sf.getBackupConnector() != null)
         {
            break;
         }
         else if (System.currentTimeMillis() > (time + toWait))
         {
            fail("backup server never started");
         }
      }
      System.out.println("sf.getBackupConnector() = " + sf.getBackupConnector());
   }

   protected void waitForBackup(long seconds)
   {
      long time = System.currentTimeMillis();
      long toWait = seconds * 1000;
      while (!backupServer.isInitialised())
      {
         try
         {
            Thread.sleep(100);
         }
         catch (InterruptedException e)
         {
            //ignore
         }
         if (backupServer.isInitialised())
         {
            break;
         }
         else if (System.currentTimeMillis() > (time + toWait))
         {
            fail("backup server never started");
         }
      }
   }

   protected void waitForBackup(long seconds, TestableServer server)
   {
      long time = System.currentTimeMillis();
      long toWait = seconds * 1000;
      while (!server.isInitialised())
      {
         try
         {
            Thread.sleep(100);
         }
         catch (InterruptedException e)
         {
            //ignore
         }
         if (server.isInitialised())
         {
            break;
         }
         else if (System.currentTimeMillis() > (time + toWait))
         {
            fail("server never started");
         }
      }
   }


   protected TransportConfiguration getInVMConnectorTransportConfiguration(final boolean live)
   {
      if (live)
      {
         return new TransportConfiguration(InVMConnectorFactory.class.getCanonicalName());
      }
      else
      {
         Map<String, Object> server1Params = new HashMap<String, Object>();

         server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);

         return new TransportConfiguration(InVMConnectorFactory.class.getCanonicalName(), server1Params);
      }
   }

   protected TransportConfiguration getInVMTransportAcceptorConfiguration(final boolean live)
   {
      if (live)
      {
         return new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName());
      }
      else
      {
         Map<String, Object> server1Params = new HashMap<String, Object>();

         server1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);

         return new TransportConfiguration(InVMAcceptorFactory.class.getCanonicalName(), server1Params);
      }
   }

   protected TransportConfiguration getNettyAcceptorTransportConfiguration(final boolean live)
   {
      if (live)
      {
         return new TransportConfiguration(NettyAcceptorFactory.class.getCanonicalName());
      }
      else
      {
         Map<String, Object> server1Params = new HashMap<String, Object>();

         server1Params.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME,
               org.hornetq.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 1);

         return new TransportConfiguration(NettyAcceptorFactory.class.getCanonicalName(),
               server1Params);
      }
   }

   protected TransportConfiguration getNettyConnectorTransportConfiguration(final boolean live)
   {
      if (live)
      {
         return new TransportConfiguration(NettyConnectorFactory.class.getCanonicalName());
      }
      else
      {
         Map<String, Object> server1Params = new HashMap<String, Object>();

         server1Params.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME,
               org.hornetq.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 1);

         return new TransportConfiguration(NettyConnectorFactory.class.getCanonicalName(), server1Params);
      }
   }

   protected abstract TransportConfiguration getAcceptorTransportConfiguration(boolean live);

   protected abstract TransportConfiguration getConnectorTransportConfiguration(final boolean live);

   protected ServerLocatorInternal getServerLocator() throws Exception
   {
      ServerLocator locator = HornetQClient.createServerLocatorWithHA(getConnectorTransportConfiguration(true), getConnectorTransportConfiguration(false));
      return (ServerLocatorInternal) locator;
   }

   protected void crash(final ClientSession... sessions) throws Exception
   {
      liveServer.crash(sessions);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   abstract class BaseListener implements SessionFailureListener
   {
      public void beforeReconnect(final HornetQException me)
      {
      }
   }

   class LatchClusterTopologyListener implements ClusterTopologyListener
   {
      final CountDownLatch latch;
      int liveNodes = 0;
      int backUpNodes = 0;
      List<String> liveNode = new ArrayList<String>();
      List<String> backupNode = new ArrayList<String>();

      public LatchClusterTopologyListener(CountDownLatch latch)
      {
         this.latch = latch;
      }

      public void nodeUP(String nodeID, Pair<TransportConfiguration, TransportConfiguration> connectorPair, boolean last)
      {
         if (connectorPair.a != null && !liveNode.contains(connectorPair.a.getName()))
         {
            liveNode.add(connectorPair.a.getName());
            latch.countDown();
         }
         if (connectorPair.b != null && !backupNode.contains(connectorPair.b.getName()))
         {
            backupNode.add(connectorPair.b.getName());
            latch.countDown();
         }
      }

      public void nodeDown(String nodeID)
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }
   }


}
