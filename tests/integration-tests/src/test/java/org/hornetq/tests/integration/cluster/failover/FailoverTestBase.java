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

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClusterTopologyListener;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.invm.InVMConnector;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.core.server.impl.InVMNodeManager;
import org.hornetq.tests.integration.cluster.util.SameProcessHornetQServer;
import org.hornetq.tests.integration.cluster.util.TestableServer;
import org.hornetq.tests.util.ReplicatedBackupUtils;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A FailoverTestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public abstract class FailoverTestBase extends ServiceTestBase
{
   // Constants -----------------------------------------------------

   protected static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   /*
    * Used only by tests of large messages.
    */
   protected static final int MIN_LARGE_MESSAGE = 1024;
   private static final int LARGE_MESSAGE_SIZE = MIN_LARGE_MESSAGE * 3;

   protected static final int PAGE_MAX = 2 * 1024;
   protected static final int PAGE_SIZE = 1024;

   // Attributes ----------------------------------------------------

   protected TestableServer liveServer;

   protected TestableServer backupServer;

   protected Configuration backupConfig;

   protected Configuration liveConfig;

   protected NodeManager nodeManager;

   protected boolean startBackupServer = true;

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

      liveServer.setIdentity(this.getClass().getSimpleName() + "/liveServer");

      liveServer.start();

      waitForServer(liveServer.getServer());

      if (backupServer != null)
      {
         backupServer.setIdentity(this.getClass().getSimpleName() + "/backupServer");
        if (startBackupServer)
        {
         backupServer.start();
         waitForServer(backupServer.getServer());
        }
      }
   }

   protected TestableServer createLiveServer()
   {
      return new SameProcessHornetQServer(createInVMFailoverServer(true, liveConfig, nodeManager, 1));
   }

   protected TestableServer createBackupServer()
   {
      return new SameProcessHornetQServer(createInVMFailoverServer(true, backupConfig, nodeManager, 2));
   }

   /**
    * Large message version of {@link #setBody(int, ClientMessage)}.
    * @param i
    * @param message
    * @param size
    */
   protected static void setLargeMessageBody(final int i, final ClientMessage message)
   {
      try
      {
         message.setBodyInputStream(UnitTestCase.createFakeLargeStream(LARGE_MESSAGE_SIZE));
      }
      catch (Exception e)
      {
         throw new RuntimeException(e);
      }
   }

   /**
    * Large message version of {@link #assertMessageBody(int, ClientMessage)}.
    * @param i
    * @param message
    */
   protected static void assertLargeMessageBody(final int i, final ClientMessage message)
   {
      HornetQBuffer buffer = message.getBodyBuffer();

      for (int j = 0; j < LARGE_MESSAGE_SIZE; j++)
      {
         Assert.assertTrue("expecting " + LARGE_MESSAGE_SIZE + " bytes, got " + j, buffer.readable());
         Assert.assertEquals("equal at " + j, UnitTestCase.getSamplebyte(j), buffer.readByte());
      }
   }

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
      ReplicatedBackupUtils.createClusterConnectionConf(backupConfig, backupConnector.getName(),
                                                        liveConnector.getName());
      backupServer = createBackupServer();

      liveConfig = super.createDefaultConfig();
      liveConfig.getAcceptorConfigurations().clear();
      liveConfig.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(true));
      liveConfig.setSecurityEnabled(false);
      liveConfig.setSharedStore(true);
      liveConfig.setClustered(true);
      ReplicatedBackupUtils.createClusterConnectionConf(liveConfig, liveConnector.getName());
      liveConfig.getConnectorConfigurations().put(liveConnector.getName(), liveConnector);
      liveServer = createLiveServer();
   }

   protected void createReplicatedConfigs() throws Exception
   {
      final TransportConfiguration liveConnector = getConnectorTransportConfiguration(true);
      final TransportConfiguration backupConnector = getConnectorTransportConfiguration(false);
      final TransportConfiguration backupAcceptor = getAcceptorTransportConfiguration(false);

      nodeManager = new InVMNodeManager();
      backupConfig = createDefaultConfig();
      liveConfig = createDefaultConfig();

      ReplicatedBackupUtils.configureReplicationPair(backupConfig, backupConnector, backupAcceptor, liveConfig,
                                                     liveConnector);

      backupConfig.setBindingsDirectory(backupConfig.getBindingsDirectory() + "_backup");
      backupConfig.setJournalDirectory(backupConfig.getJournalDirectory() + "_backup");
      backupConfig.setPagingDirectory(backupConfig.getPagingDirectory() + "_backup");
      backupConfig.setLargeMessagesDirectory(backupConfig.getLargeMessagesDirectory() + "_backup");
      backupConfig.setSecurityEnabled(false);

      backupServer = createBackupServer();
      liveConfig.getAcceptorConfigurations().clear();
      liveConfig.getAcceptorConfigurations().add(getAcceptorTransportConfiguration(true));


      liveServer = createLiveServer();
   }

   @Override
   protected void tearDown() throws Exception
   {
      logAndSystemOut("#test tearDown");
      stopComponent(backupServer);
      stopComponent(liveServer);

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
         throw e;
      }
      try
      {
         ServerSocket serverSocket = new ServerSocket(5446);
         serverSocket.close();
      }
      catch (IOException e)
      {
         throw e;
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

   /**
    * Waits for backup to be in the "started" state and to finish synchronization with its live.
    * @param sessionFactory
    * @param seconds
    * @throws Exception
    */
   protected void waitForBackup(ClientSessionFactoryInternal sessionFactory, int seconds) throws Exception
   {
      final HornetQServerImpl actualServer = (HornetQServerImpl)backupServer.getServer();
      if (actualServer.getConfiguration().isSharedStore())
      {
         waitForServer(actualServer);
      }
      else
      {
         waitForRemoteBackup(sessionFactory, seconds, true, actualServer);
      }
   }

   /**
    * @param sessionFactory
    * @param seconds
    * @param waitForSync
    * @param actualServer
    */
   public static void waitForRemoteBackup(ClientSessionFactoryInternal sessionFactory,
                                    int seconds,
                                    boolean waitForSync,
                                    final HornetQServer backup)
   {
      final HornetQServerImpl actualServer = (HornetQServerImpl)backup;
      final long toWait = seconds * 1000;
      final long time = System.currentTimeMillis();
      while (true)
      {
         if ((sessionFactory == null || sessionFactory.getBackupConnector() != null) &&
                  (actualServer.isRemoteBackupUpToDate() || !waitForSync))
         {
            break;
         }
         if (System.currentTimeMillis() > (time + toWait))
         {
            fail("backup server never started (" + actualServer.isStarted() + "), or never finished synchronizing (" +
                     actualServer.isRemoteBackupUpToDate() + ")");
         }
         try
         {
            Thread.sleep(100);
         }
         catch (InterruptedException e)
         {
            fail(e.getMessage());
         }
      }
   }

   protected TransportConfiguration getNettyAcceptorTransportConfiguration(final boolean live)
   {
      if (live)
      {
         return new TransportConfiguration(NETTY_ACCEPTOR_FACTORY);
      }

      Map<String, Object> server1Params = new HashMap<String, Object>();

      server1Params.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME,
                        org.hornetq.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 1);

      return new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, server1Params);
   }

   protected TransportConfiguration getNettyConnectorTransportConfiguration(final boolean live)
   {
      if (live)
      {
         return new TransportConfiguration(NETTY_CONNECTOR_FACTORY);
      }

      Map<String, Object> server1Params = new HashMap<String, Object>();

      server1Params.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME,
                        org.hornetq.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + 1);
      return new TransportConfiguration(NETTY_CONNECTOR_FACTORY, server1Params);
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

   protected void crash(final boolean waitFailure, final ClientSession... sessions) throws Exception
   {
      liveServer.crash(waitFailure, sessions);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

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

      public void nodeUP(final long uniqueEventID, String nodeID, Pair<TransportConfiguration, TransportConfiguration> connectorPair, boolean last)
      {
         if (connectorPair.getA() != null && !liveNode.contains(connectorPair.getA().getName()))
         {
            liveNode.add(connectorPair.getA().getName());
            latch.countDown();
         }
         if (connectorPair.getB() != null && !backupNode.contains(connectorPair.getB().getName()))
         {
            backupNode.add(connectorPair.getB().getName());
            latch.countDown();
         }
      }

      public void nodeDown(final long uniqueEventID, String nodeID)
      {
         //To change body of implemented methods use File | Settings | File Templates.
      }
   }


}
