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

package org.hornetq.tests.integration.cluster.bridge;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Assert;

import org.hornetq.api.config.HornetQDefaultConfiguration;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.HornetQNotConnectedException;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.remoting.impl.invm.InVMConnector;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.NodeManager;
import org.hornetq.core.server.cluster.Bridge;
import org.hornetq.core.server.cluster.impl.BridgeImpl;
import org.hornetq.core.server.impl.InVMNodeManager;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.integration.IntegrationTestLogger;

/**
 * A BridgeReconnectTest
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class BridgeReconnectTest extends BridgeTestBase
{
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private static final int NUM_MESSAGES = 100;

   Map<String, Object> server0Params;
   Map<String, Object> server1Params;
   Map<String, Object> server2Params;

   HornetQServer server0;
   HornetQServer server1;
   HornetQServer server2;
   ServerLocator locator;

   ClientSession session0;
   ClientSession session1;
   ClientSession session2;

   private TransportConfiguration server1tc;
   private Map<String, TransportConfiguration> connectors;
   private ArrayList<String> staticConnectors;

   final String bridgeName = "bridge1";
   final String testAddress = "testAddress";
   final String queueName = "queue0";
   final String forwardAddress = "forwardAddress";

   final long retryInterval = 50;
   final double retryIntervalMultiplier = 1d;
   final int confirmationWindowSize = 1024;
   int reconnectAttempts = 3;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      server0Params = new HashMap<String, Object>();
      server1Params = new HashMap<String, Object>();
      server2Params = new HashMap<String, Object>();
      connectors = new HashMap<String, TransportConfiguration>();

      server1 = createHornetQServer(1, isNetty(), server1Params);
      server1tc = new TransportConfiguration(getConnector(), server1Params, "server1tc");
      connectors.put(server1tc.getName(), server1tc);
      staticConnectors = new ArrayList<String>();
      staticConnectors.add(server1tc.getName());
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      locator = null;
      super.tearDown();
   }

   protected boolean isNetty()
   {
      return false;
   }

   /**
    * @return
    */
   private String getConnector()
   {
      if (isNetty())
      {
         return NETTY_CONNECTOR_FACTORY;
      }
      return INVM_CONNECTOR_FACTORY;
   }

   /**
    * Backups must successfully deploy its bridges on fail-over.
    * @see https://bugzilla.redhat.com/show_bug.cgi?id=900764
    */
   @Test
   public void testFailoverDeploysBridge() throws Exception
   {
      NodeManager nodeManager = new InVMNodeManager(false);
      server0 = createHornetQServer(0, server0Params, isNetty(), nodeManager);
      server2 = createBackupHornetQServer(2, server2Params, isNetty(), 0, nodeManager);

      TransportConfiguration server0tc = new TransportConfiguration(getConnector(), server0Params, "server0tc");
      TransportConfiguration server2tc = new TransportConfiguration(getConnector(), server2Params, "server2tc");

      connectors.put(server2tc.getName(), server2tc);

      server0.getConfiguration().setConnectorConfigurations(connectors);
      server1.getConfiguration().setConnectorConfigurations(connectors);
      server2.getConfiguration().setConnectorConfigurations(connectors);
      reconnectAttempts = -1;

      BridgeConfiguration bridgeConfiguration = createBridgeConfig();
      bridgeConfiguration.setQueueName(queueName);
      List<BridgeConfiguration> bridgeConfigs = new ArrayList<BridgeConfiguration>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);
      server2.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      CoreQueueConfiguration queueConfig0 = new CoreQueueConfiguration(testAddress, queueName, null, true);
      List<CoreQueueConfiguration> queueConfigs0 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs0.add(queueConfig0);
      server1.getConfiguration().setQueueConfigurations(queueConfigs0);

      CoreQueueConfiguration queueConfig1 = new CoreQueueConfiguration(forwardAddress, queueName, null, true);
      List<CoreQueueConfiguration> queueConfigs1 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs1.add(queueConfig1);
      server0.getConfiguration().setQueueConfigurations(queueConfigs1);
      server2.getConfiguration().setQueueConfigurations(queueConfigs1);

      startServers();

      waitForServerStart(server0);
      server0.stop(true);

      waitForServerStart(server2);

      locator = addServerLocator(HornetQClient.createServerLocatorWithoutHA(server0tc, server2tc));

      ClientSessionFactory csf0 = addSessionFactory(locator.createSessionFactory(server2tc));

      session0 = csf0.createSession(false, true, true);
      Map<String, Bridge> bridges = server2.getClusterManager().getBridges();
      assertTrue("backup must deploy bridge on failover", !bridges.isEmpty());
   }

   // Fail bridge and reconnecting immediately
   @Test
   public void testFailoverAndReconnectImmediately() throws Exception
   {
      NodeManager nodeManager = new InVMNodeManager(false);
      server0 = createHornetQServer(0, server0Params, isNetty(), nodeManager);
      server2 = createBackupHornetQServer(2, server2Params, isNetty(), 0, nodeManager);

      TransportConfiguration server0tc = new TransportConfiguration(getConnector(), server0Params, "server0tc");
      TransportConfiguration server2tc = new TransportConfiguration(getConnector(), server2Params, "server2tc");

      connectors.put(server2tc.getName(), server2tc);

      server0.getConfiguration().setConnectorConfigurations(connectors);
      server1.getConfiguration().setConnectorConfigurations(connectors);

      reconnectAttempts = 1;

      BridgeConfiguration bridgeConfiguration = createBridgeConfig();

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<BridgeConfiguration>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      CoreQueueConfiguration queueConfig0 = new CoreQueueConfiguration(testAddress, queueName, null, true);
      List<CoreQueueConfiguration> queueConfigs0 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigurations(queueConfigs0);

      CoreQueueConfiguration queueConfig1 = new CoreQueueConfiguration(forwardAddress, queueName, null, true);
      List<CoreQueueConfiguration> queueConfigs1 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigurations(queueConfigs1);
      server2.getConfiguration().setQueueConfigurations(queueConfigs1);

      startServers();

         BridgeReconnectTest.log.info("** failing connection");
         // Now we will simulate a failure of the bridge connection between server0 and server1
         server0.stop(true);

         waitForServerStart(server2);

         locator = addServerLocator(HornetQClient.createServerLocatorWithoutHA(server0tc, server2tc));

      ClientSessionFactory csf0 = addSessionFactory(locator.createSessionFactory(server2tc));

      session0 = csf0.createSession(false, true, true);

      ClientProducer prod0 = session0.createProducer(testAddress);

      ClientSessionFactory csf2 = addSessionFactory(locator.createSessionFactory(server2tc));

      session2 = csf2.createSession(false, true, true);

      ClientConsumer cons2 = session2.createConsumer(queueName);

         session2.start();

         final int numMessages = NUM_MESSAGES;

         SimpleString propKey = new SimpleString("propkey");

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session0.createMessage(true);
            message.putIntProperty(propKey, i);

            prod0.send(message);
         }

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage r1 = cons2.receive(1500);
            Assert.assertNotNull(r1);
            Assert.assertEquals(i, r1.getObjectProperty(propKey));
         }
         closeServers();


      assertNoMoreConnections();
   }

   private BridgeConfiguration createBridgeConfig()
   {
      return new BridgeConfiguration(bridgeName, queueName, forwardAddress, null, null,
                                     HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                     HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                     HornetQClient.DEFAULT_CONNECTION_TTL, retryInterval,
                                     HornetQClient.DEFAULT_MAX_RETRY_INTERVAL, retryIntervalMultiplier,
                                     reconnectAttempts, 0, true, confirmationWindowSize, staticConnectors, false,
                                     HornetQDefaultConfiguration.getDefaultClusterUser(), CLUSTER_PASSWORD);
   }

   // Fail bridge and attempt failover a few times before succeeding
   @Test
   public void testFailoverAndReconnectAfterAFewTries() throws Exception
   {
      NodeManager nodeManager = new InVMNodeManager(false);

      server0 = createHornetQServer(0, server0Params, isNetty(), nodeManager);
      server2 = createBackupHornetQServer(2, server2Params, isNetty(), 0, nodeManager);

      TransportConfiguration server2tc = new TransportConfiguration(getConnector(), server2Params, "server2tc");

      connectors.put(server2tc.getName(), server2tc);

      server0.getConfiguration().setConnectorConfigurations(connectors);
      server1.getConfiguration().setConnectorConfigurations(connectors);

      BridgeConfiguration bridgeConfiguration = createBridgeConfig();

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<BridgeConfiguration>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      CoreQueueConfiguration queueConfig0 = new CoreQueueConfiguration(testAddress, queueName, null, true);
      List<CoreQueueConfiguration> queueConfigs0 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigurations(queueConfigs0);

      CoreQueueConfiguration queueConfig1 = new CoreQueueConfiguration(forwardAddress, queueName, null, true);
      List<CoreQueueConfiguration> queueConfigs1 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigurations(queueConfigs1);
      server2.getConfiguration().setQueueConfigurations(queueConfigs1);

      startServers();
         // Now we will simulate a failure of the bridge connection between server0 and server1
         server0.stop(true);

         locator = addServerLocator(HornetQClient.createServerLocatorWithHA(server2tc));
         locator.setReconnectAttempts(100);
         ClientSessionFactory csf0 = addSessionFactory(locator.createSessionFactory(server2tc));
      session0 = csf0.createSession(false, true, true);

         ClientSessionFactory csf2 = addSessionFactory(locator.createSessionFactory(server2tc));
      session2 = csf2.createSession(false, true, true);

         ClientProducer prod0 = session0.createProducer(testAddress);

      ClientConsumer cons2 = session2.createConsumer(queueName);

         session2.start();

         final int numMessages = NUM_MESSAGES;

         SimpleString propKey = new SimpleString("propkey");

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session0.createMessage(false);
            message.putIntProperty(propKey, i);

            prod0.send(message);
         }

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage r1 = cons2.receive(1500);
            Assert.assertNotNull(r1);
            Assert.assertEquals(i, r1.getObjectProperty(propKey));
         }
         closeServers();

      assertNoMoreConnections();
   }

   // Fail bridge and reconnect same node, no backup specified
   @Test
   public void testReconnectSameNode() throws Exception
   {
      server0 = createHornetQServer(0, isNetty(), server0Params);

      TransportConfiguration server0tc = new TransportConfiguration(getConnector(), server0Params, "server0tc");

      server0.getConfiguration().setConnectorConfigurations(connectors);
      server1.getConfiguration().setConnectorConfigurations(connectors);

      BridgeConfiguration bridgeConfiguration = createBridgeConfig();

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<BridgeConfiguration>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      CoreQueueConfiguration queueConfig0 = new CoreQueueConfiguration(testAddress, queueName, null, true);
      List<CoreQueueConfiguration> queueConfigs0 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigurations(queueConfigs0);

      CoreQueueConfiguration queueConfig1 = new CoreQueueConfiguration(forwardAddress, queueName, null, true);
      List<CoreQueueConfiguration> queueConfigs1 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigurations(queueConfigs1);

      startServers();

         locator = addServerLocator(HornetQClient.createServerLocatorWithHA(server0tc, server1tc));
         ClientSessionFactory csf0 = locator.createSessionFactory(server0tc);
      session0 = csf0.createSession(false, true, true);

         ClientSessionFactory csf1 = locator.createSessionFactory(server1tc);
      session1 = csf1.createSession(false, true, true);

         ClientProducer prod0 = session0.createProducer(testAddress);

      ClientConsumer cons1 = session1.createConsumer(queueName);

         session1.start();

         // Now we will simulate a failure of the bridge connection between server0 and server1
         Bridge bridge = server0.getClusterManager().getBridges().get(bridgeName);
         assertNotNull(bridge);
         RemotingConnection forwardingConnection = getForwardingConnection(bridge);
         InVMConnector.failOnCreateConnection = true;
         InVMConnector.numberOfFailures = reconnectAttempts - 1;
         forwardingConnection.fail(new HornetQNotConnectedException());

         forwardingConnection = getForwardingConnection(bridge);
         forwardingConnection.fail(new HornetQNotConnectedException());

         final int numMessages = NUM_MESSAGES;

         SimpleString propKey = new SimpleString("propkey");

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session0.createMessage(false);
            message.putIntProperty(propKey, i);

            prod0.send(message);
         }

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage r1 = cons1.receive(1500);
            Assert.assertNotNull(r1);
            Assert.assertEquals(i, r1.getObjectProperty(propKey));
         }
         closeServers();

      assertNoMoreConnections();
   }

   // We test that we can pause more than client failure check period (to prompt the pinger to failing)
   // before reconnecting
   @Test
   public void testShutdownServerCleanlyAndReconnectSameNodeWithSleep() throws Exception
   {
      testShutdownServerCleanlyAndReconnectSameNode(true);
   }

   @Test
   public void testShutdownServerCleanlyAndReconnectSameNode() throws Exception
   {
      testShutdownServerCleanlyAndReconnectSameNode(false);
   }

   private void testShutdownServerCleanlyAndReconnectSameNode(final boolean sleep) throws Exception
   {
      server0 = createHornetQServer(0, isNetty(), server0Params);
      TransportConfiguration server0tc = new TransportConfiguration(getConnector(), server0Params, "server0tc");

      server0.getConfiguration().setConnectorConfigurations(connectors);
      server1.getConfiguration().setConnectorConfigurations(connectors);
      reconnectAttempts = -1;
      final long clientFailureCheckPeriod = 1000;

      BridgeConfiguration bridgeConfiguration = new BridgeConfiguration(bridgeName,
                                                                        queueName,
                                                                        forwardAddress,
                                                                        null,
                                                                        null,
                                                                        HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                                        clientFailureCheckPeriod,
                                                                        HornetQClient.DEFAULT_CONNECTION_TTL,
                                                                        retryInterval,
                                                                        HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                                                        retryIntervalMultiplier,
                                                                        reconnectAttempts,
                                                                        0,
                                                                        true,
                                                                        confirmationWindowSize,
                                                                        staticConnectors,
                                                                        false,
                                                                        HornetQDefaultConfiguration.getDefaultClusterUser(),
                                                                        CLUSTER_PASSWORD);

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<BridgeConfiguration>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      CoreQueueConfiguration queueConfig0 = new CoreQueueConfiguration(testAddress, queueName, null, true);
      List<CoreQueueConfiguration> queueConfigs0 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigurations(queueConfigs0);

      CoreQueueConfiguration queueConfig1 = new CoreQueueConfiguration(forwardAddress, queueName, null, true);
      List<CoreQueueConfiguration> queueConfigs1 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigurations(queueConfigs1);
      startServers();

      waitForServerStart(server0);
      waitForServerStart(server1);

      locator = addServerLocator(HornetQClient.createServerLocatorWithHA(server0tc, server1tc));
      ClientSessionFactory csf0 = locator.createSessionFactory(server0tc);
      session0 = csf0.createSession(false, true, true);

      ClientProducer prod0 = session0.createProducer(testAddress);

      BridgeReconnectTest.log.info("stopping server1");
      server1.stop();

      if (sleep)
      {
         Thread.sleep(2 * clientFailureCheckPeriod);
      }

      BridgeReconnectTest.log.info("restarting server1");
      server1.start();
      BridgeReconnectTest.log.info("server 1 restarted");

      ClientSessionFactory csf1 = locator.createSessionFactory(server1tc);
      session1 = csf1.createSession(false, true, true);

      ClientConsumer cons1 = session1.createConsumer(queueName);

      session1.start();

      final int numMessages = NUM_MESSAGES;

      SimpleString propKey = new SimpleString("propkey");

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session0.createMessage(false);
         message.putIntProperty(propKey, i);

         prod0.send(message);
      }

      BridgeReconnectTest.log.info("sent messages");

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage r1 = cons1.receive(30000);
         Assert.assertNotNull("received expected msg", r1);
         Assert.assertEquals("property value matches", i, r1.getObjectProperty(propKey));
         BridgeReconnectTest.log.info("got message " + r1.getObjectProperty(propKey));
      }

      BridgeReconnectTest.log.info("got messages");
      closeServers();
      assertNoMoreConnections();
   }

   /**
    * @throws Exception
    */
   private void closeServers() throws Exception
   {
      if (session0 != null)
         session0.close();
      if (session1 != null)
         session1.close();
      if (session2 != null)
         session2.close();

      if (locator != null)
      {
         locator.close();
      }

      server0.stop();
      server1.stop();
      if (server2 != null)
         server2.stop();
   }

   private void assertNoMoreConnections()
   {
      Assert.assertEquals(0, server0.getRemotingService().getConnections().size());
      Assert.assertEquals(0, server1.getRemotingService().getConnections().size());
      if (server2 != null)
         Assert.assertEquals(0, server2.getRemotingService().getConnections().size());
   }

   @Test
   public void testFailoverThenFailAgainAndReconnect() throws Exception
   {
      server0 = createHornetQServer(0, isNetty(), server0Params);

      TransportConfiguration server0tc = new TransportConfiguration(getConnector(), server0Params, "server0tc");

      server0.getConfiguration().setConnectorConfigurations(connectors);

      BridgeConfiguration bridgeConfiguration = createBridgeConfig();

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<BridgeConfiguration>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      CoreQueueConfiguration queueConfig0 = new CoreQueueConfiguration(testAddress, queueName, null, true);
      List<CoreQueueConfiguration> queueConfigs0 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigurations(queueConfigs0);

      CoreQueueConfiguration queueConfig1 = new CoreQueueConfiguration(forwardAddress, queueName, null, true);
      List<CoreQueueConfiguration> queueConfigs1 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigurations(queueConfigs1);

      startServers();

         locator = addServerLocator(HornetQClient.createServerLocatorWithHA(server0tc, server1tc));
         ClientSessionFactory csf0 = locator.createSessionFactory(server0tc);
      session0 = csf0.createSession(false, true, true);

         ClientSessionFactory csf1 = locator.createSessionFactory(server1tc);
      session1 = csf1.createSession(false, true, true);

         ClientProducer prod0 = session0.createProducer(testAddress);

      ClientConsumer cons1 = session1.createConsumer(queueName);

         session1.start();

         Bridge bridge = server0.getClusterManager().getBridges().get(bridgeName);
         RemotingConnection forwardingConnection = getForwardingConnection(bridge);
         InVMConnector.failOnCreateConnection = true;
         InVMConnector.numberOfFailures = reconnectAttempts - 1;
         forwardingConnection.fail(new HornetQNotConnectedException());

         final int numMessages = NUM_MESSAGES;

         SimpleString propKey = new SimpleString("propkey");

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session0.createMessage(false);
            message.putIntProperty(propKey, i);

            prod0.send(message);
         }
         int outOfOrder = -1;
         int supposed = -1;

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage r1 = cons1.receive(1500);
            Assert.assertNotNull(r1);
            if (outOfOrder == -1 && i != r1.getIntProperty(propKey).intValue())
            {
               outOfOrder = r1.getIntProperty(propKey).intValue();
               supposed = i;
            }
         }
         if (outOfOrder != -1)
         {
            fail("Message " + outOfOrder + " was received out of order, it was supposed to be " + supposed);
         }

         log.info("=========== second failure, sending message");


         // Fail again - should reconnect
         forwardingConnection = ((BridgeImpl)bridge).getForwardingConnection();
         InVMConnector.failOnCreateConnection = true;
         InVMConnector.numberOfFailures = reconnectAttempts - 1;
         forwardingConnection.fail(new HornetQException(HornetQExceptionType.UNBLOCKED));

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session0.createMessage(false);
            message.putIntProperty(propKey, i);

            prod0.send(message);
         }

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage r1 = cons1.receive(1500);
            Assert.assertNotNull("Didn't receive message", r1);
            if (outOfOrder == -1 && i != r1.getIntProperty(propKey).intValue())
            {
               outOfOrder = r1.getIntProperty(propKey).intValue();
               supposed = i;
            }
         }


      if (outOfOrder != -1)
         {
            fail("Message " + outOfOrder + " was received out of order, it was supposed to be " + supposed);
         }
         closeServers();

      assertNoMoreConnections();
   }

   private void startServers() throws Exception
   {
      if (server2 != null)
         server2.start();
      server1.start();
      server0.start();
   }

   private RemotingConnection getForwardingConnection(final Bridge bridge) throws Exception
   {
      long start = System.currentTimeMillis();

      do
      {
         RemotingConnection forwardingConnection = ((BridgeImpl)bridge).getForwardingConnection();

         if (forwardingConnection != null)
         {
            return forwardingConnection;
         }

         Thread.sleep(10);
      }
      while (System.currentTimeMillis() - start < 50000);

      throw new IllegalStateException("Failed to get forwarding connection");
   }

}
