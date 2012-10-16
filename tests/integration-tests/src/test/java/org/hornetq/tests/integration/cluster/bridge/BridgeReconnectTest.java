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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

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
import org.hornetq.core.config.impl.ConfigurationImpl;
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
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * Created 20 Jan 2009 19:20:41
 *
 *
 */
public class BridgeReconnectTest extends BridgeTestBase
{
   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   private final int NUM_MESSAGES = 100;
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

   // Fail bridge and reconnecting immediately
   public void testFailoverAndReconnectImmediately() throws Exception
   {
      NodeManager nodeManager = new InVMNodeManager();
      Map<String, Object> server0Params = new HashMap<String, Object>();
      HornetQServer server0 = createHornetQServer(0, server0Params, isNetty(), nodeManager);

      Map<String, Object> server1Params = new HashMap<String, Object>();
      HornetQServer server1 = createHornetQServer(1, isNetty(), server1Params);

      Map<String, Object> server2Params = new HashMap<String, Object>();
      HornetQServer service2 = createBackupHornetQServer(2, server2Params, isNetty(), 0, nodeManager);

      TransportConfiguration server0tc = new TransportConfiguration(getConnector(), server0Params, "server0tc");

      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();

      TransportConfiguration server1tc = new TransportConfiguration(getConnector(), server1Params, "server1tc");

      TransportConfiguration server2tc = new TransportConfiguration(getConnector(), server2Params, "server2tc");

      connectors.put(server1tc.getName(), server1tc);

      connectors.put(server2tc.getName(), server2tc);

      server0.getConfiguration().setConnectorConfigurations(connectors);
      server1.getConfiguration().setConnectorConfigurations(connectors);

      final String bridgeName = "bridge1";
      final String testAddress = "testAddress";
      final String queueName0 = "queue0";
      final String forwardAddress = "forwardAddress";

      final long retryInterval = 50;
      final double retryIntervalMultiplier = 1d;
      final int reconnectAttempts = 1;
      final int confirmationWindowSize = 1024;

      ArrayList<String> staticConnectors = new ArrayList<String>();
      staticConnectors.add(server1tc.getName());

      BridgeConfiguration bridgeConfiguration = new BridgeConfiguration(bridgeName,
                                                                        queueName0,
                                                                        forwardAddress,
                                                                        null,
                                                                        null,
                                                                        HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                                        HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                                                        HornetQClient.DEFAULT_CONNECTION_TTL,
                                                                        retryInterval,
                                                                        HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                                                        retryIntervalMultiplier,
                                                                        reconnectAttempts,
                                                                        true,
                                                                        confirmationWindowSize,
                                                                        staticConnectors,
                                                                        false,
                                                                        HornetQDefaultConfiguration.DEFAULT_CLUSTER_USER,
                                                                        HornetQDefaultConfiguration.DEFAULT_CLUSTER_PASSWORD);

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<BridgeConfiguration>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      CoreQueueConfiguration queueConfig0 = new CoreQueueConfiguration(testAddress, queueName0, null, true);
      List<CoreQueueConfiguration> queueConfigs0 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigurations(queueConfigs0);

      CoreQueueConfiguration queueConfig1 = new CoreQueueConfiguration(forwardAddress, queueName0, null, true);
      List<CoreQueueConfiguration> queueConfigs1 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigurations(queueConfigs1);
      service2.getConfiguration().setQueueConfigurations(queueConfigs1);

      ServerLocator locator = null;

      try
      {
         service2.start();
         server1.start();
         server0.start();

         BridgeReconnectTest.log.info("** failing connection");
         // Now we will simulate a failure of the bridge connection between server0 and server1
         server0.stop(true);

         waitForServerStart(service2);

         locator = addServerLocator(HornetQClient.createServerLocatorWithoutHA(server0tc, server2tc));

         ClientSessionFactory csf0 = addSessionFactory(locator.createSessionFactory(server2tc));

         ClientSession session0 = csf0.createSession(false, true, true);

         ClientProducer prod0 = session0.createProducer(testAddress);

         ClientSessionFactory csf2 = addSessionFactory(locator.createSessionFactory(server2tc));

         ClientSession session2 = csf2.createSession(false, true, true);

         ClientConsumer cons2 = session2.createConsumer(queueName0);

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

         session0.close();
         session2.close();
      }
      finally
      {
         if (locator != null)
         {
            locator.close();
         }

         server0.stop();
         server1.stop();
         service2.stop();
      }

      Assert.assertEquals(0, server0.getRemotingService().getConnections().size());
      Assert.assertEquals(0, server1.getRemotingService().getConnections().size());
      Assert.assertEquals(0, service2.getRemotingService().getConnections().size());
   }

   // Fail bridge and attempt failover a few times before succeeding
   public void testFailoverAndReconnectAfterAFewTries() throws Exception
   {
      NodeManager nodeManager = new InVMNodeManager();

      Map<String, Object> server0Params = new HashMap<String, Object>();
      HornetQServer server0 = createHornetQServer(0, server0Params, isNetty(), nodeManager);

      Map<String, Object> server1Params = new HashMap<String, Object>();
      HornetQServer server1 = createHornetQServer(1, isNetty(), server1Params);

      Map<String, Object> server2Params = new HashMap<String, Object>();
      HornetQServer service2 = createBackupHornetQServer(2, server2Params, isNetty(), 0, nodeManager);

      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();

      TransportConfiguration server1tc = new TransportConfiguration(getConnector(), server1Params, "server1tc");

      TransportConfiguration server2tc = new TransportConfiguration(getConnector(), server2Params, "server2tc");

      connectors.put(server1tc.getName(), server1tc);

      connectors.put(server2tc.getName(), server2tc);

      server0.getConfiguration().setConnectorConfigurations(connectors);
      server1.getConfiguration().setConnectorConfigurations(connectors);

      final String bridgeName = "bridge1";
      final String testAddress = "testAddress";
      final String queueName0 = "queue0";
      final String forwardAddress = "forwardAddress";

      final long retryInterval = 50;
      final double retryIntervalMultiplier = 1d;
      final int reconnectAttempts = 3;
      final int confirmationWindowSize = 1024;

      ArrayList<String> staticConnectors = new ArrayList<String>();
      staticConnectors.add(server1tc.getName());

      BridgeConfiguration bridgeConfiguration = new BridgeConfiguration(bridgeName,
                                                                        queueName0,
                                                                        forwardAddress,
                                                                        null,
                                                                        null,
                                                                        HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                                        HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                                                        HornetQClient.DEFAULT_CONNECTION_TTL,
                                                                        retryInterval,
                                                                        HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                                                        retryIntervalMultiplier,
                                                                        reconnectAttempts,
                                                                        true,
                                                                        confirmationWindowSize,
                                                                        staticConnectors,
                                                                        false,
                                                                        HornetQDefaultConfiguration.DEFAULT_CLUSTER_USER,
                                                                        HornetQDefaultConfiguration.DEFAULT_CLUSTER_PASSWORD);

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<BridgeConfiguration>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      CoreQueueConfiguration queueConfig0 = new CoreQueueConfiguration(testAddress, queueName0, null, true);
      List<CoreQueueConfiguration> queueConfigs0 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigurations(queueConfigs0);

      CoreQueueConfiguration queueConfig1 = new CoreQueueConfiguration(forwardAddress, queueName0, null, true);
      List<CoreQueueConfiguration> queueConfigs1 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigurations(queueConfigs1);
      service2.getConfiguration().setQueueConfigurations(queueConfigs1);

      ServerLocator locator = null;
      try
      {
         service2.start();
         server1.start();
         server0.start();
         // Now we will simulate a failure of the bridge connection between server0 and server1
         server0.stop(true);

         locator = addServerLocator(HornetQClient.createServerLocatorWithHA(server2tc));
         locator.setReconnectAttempts(100);
         ClientSessionFactory csf0 = addSessionFactory(locator.createSessionFactory(server2tc));
         ClientSession session0 = csf0.createSession(false, true, true);

         ClientSessionFactory csf2 = addSessionFactory(locator.createSessionFactory(server2tc));
         ClientSession session2 = csf2.createSession(false, true, true);

         ClientProducer prod0 = session0.createProducer(testAddress);

         ClientConsumer cons2 = session2.createConsumer(queueName0);

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

         session0.close();
         session2.close();
      }
      finally
      {
         if (locator != null)
         {
            locator.close();
         }

         server0.stop();
         server1.stop();
         service2.stop();
      }

      Assert.assertEquals(0, server0.getRemotingService().getConnections().size());
      Assert.assertEquals(0, server1.getRemotingService().getConnections().size());
      Assert.assertEquals(0, service2.getRemotingService().getConnections().size());
   }

   // Fail bridge and reconnect same node, no backup specified
   public void testReconnectSameNode() throws Exception
   {
      Map<String, Object> server0Params = new HashMap<String, Object>();
      HornetQServer server0 = createHornetQServer(0, isNetty(), server0Params);

      Map<String, Object> server1Params = new HashMap<String, Object>();
      HornetQServer server1 = createHornetQServer(1, isNetty(), server1Params);

      TransportConfiguration server0tc = new TransportConfiguration(getConnector(), server0Params, "server0tc");

      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();

      TransportConfiguration server1tc = new TransportConfiguration(getConnector(), server1Params, "server1tc");

      connectors.put(server1tc.getName(), server1tc);

      server0.getConfiguration().setConnectorConfigurations(connectors);
      server1.getConfiguration().setConnectorConfigurations(connectors);

      final String bridgeName = "bridge1";
      final String testAddress = "testAddress";
      final String queueName0 = "queue0";
      final String forwardAddress = "forwardAddress";

      final long retryInterval = 50;
      final double retryIntervalMultiplier = 1d;
      final int reconnectAttempts = 3;
      final int confirmationWindowSize = 1024;

      ArrayList<String> staticConnectors = new ArrayList<String>();
      staticConnectors.add(server1tc.getName());

      BridgeConfiguration bridgeConfiguration = new BridgeConfiguration(bridgeName,
                                                                        queueName0,
                                                                        forwardAddress,
                                                                        null,
                                                                        null,
                                                                        HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                                        HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                                                        HornetQClient.DEFAULT_CONNECTION_TTL,
                                                                        retryInterval,
                                                                        HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                                                        retryIntervalMultiplier,
                                                                        reconnectAttempts,
                                                                        true,
                                                                        confirmationWindowSize,
                                                                        staticConnectors,
                                                                        false,
                                                                        HornetQDefaultConfiguration.DEFAULT_CLUSTER_USER,
                                                                        HornetQDefaultConfiguration.DEFAULT_CLUSTER_PASSWORD);

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<BridgeConfiguration>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      CoreQueueConfiguration queueConfig0 = new CoreQueueConfiguration(testAddress, queueName0, null, true);
      List<CoreQueueConfiguration> queueConfigs0 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigurations(queueConfigs0);

      CoreQueueConfiguration queueConfig1 = new CoreQueueConfiguration(forwardAddress, queueName0, null, true);
      List<CoreQueueConfiguration> queueConfigs1 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigurations(queueConfigs1);

      ServerLocator locator = null;
      try
      {
         server1.start();
         server0.start();

         locator = addServerLocator(HornetQClient.createServerLocatorWithHA(server0tc, server1tc));
         ClientSessionFactory csf0 = locator.createSessionFactory(server0tc);
         ClientSession session0 = csf0.createSession(false, true, true);

         ClientSessionFactory csf1 = locator.createSessionFactory(server1tc);
         ClientSession session1 = csf1.createSession(false, true, true);

         ClientProducer prod0 = session0.createProducer(testAddress);

         ClientConsumer cons1 = session1.createConsumer(queueName0);

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

         session0.close();
         session1.close();
      }
      finally
      {
         if (locator != null)
         {
            locator.close();
         }

         server0.stop();
         server1.stop();
      }

      Assert.assertEquals(0, server0.getRemotingService().getConnections().size());
      Assert.assertEquals(0, server1.getRemotingService().getConnections().size());
   }

   // We test that we can pause more than client failure check period (to prompt the pinger to failing)
   // before reconnecting
   public void testShutdownServerCleanlyAndReconnectSameNodeWithSleep() throws Exception
   {
      testShutdownServerCleanlyAndReconnectSameNode(true);
   }

   public void testShutdownServerCleanlyAndReconnectSameNode() throws Exception
   {
      testShutdownServerCleanlyAndReconnectSameNode(false);
   }

   private void testShutdownServerCleanlyAndReconnectSameNode(final boolean sleep) throws Exception
   {
      Map<String, Object> server0Params = new HashMap<String, Object>();
      HornetQServer server0 = createHornetQServer(0, isNetty(), server0Params);

      Map<String, Object> server1Params = new HashMap<String, Object>();
      HornetQServer server1 = createHornetQServer(1, isNetty(), server1Params);

      TransportConfiguration server0tc = new TransportConfiguration(getConnector(), server0Params, "server0tc");

      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();

      TransportConfiguration server1tc = new TransportConfiguration(getConnector(), server1Params, "server1tc");

      connectors.put(server1tc.getName(), server1tc);

      server0.getConfiguration().setConnectorConfigurations(connectors);
      server1.getConfiguration().setConnectorConfigurations(connectors);

      final String bridgeName = "bridge1";
      final String testAddress = "testAddress";
      final String queueName0 = "queue0";
      final String forwardAddress = "forwardAddress";

      final long retryInterval = 50;
      final double retryIntervalMultiplier = 1d;
      final int reconnectAttempts = -1;
      final int confirmationWindowSize = 1024;
      final long clientFailureCheckPeriod = 1000;

      ArrayList<String> staticConnectors = new ArrayList<String>();
      staticConnectors.add(server1tc.getName());

      BridgeConfiguration bridgeConfiguration = new BridgeConfiguration(bridgeName,
                                                                        queueName0,
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
                                                                        true,
                                                                        confirmationWindowSize,
                                                                        staticConnectors,
                                                                        false,
                                                                        HornetQDefaultConfiguration.DEFAULT_CLUSTER_USER,
                                                                        HornetQDefaultConfiguration.DEFAULT_CLUSTER_PASSWORD);

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<BridgeConfiguration>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      CoreQueueConfiguration queueConfig0 = new CoreQueueConfiguration(testAddress, queueName0, null, true);
      List<CoreQueueConfiguration> queueConfigs0 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigurations(queueConfigs0);

      CoreQueueConfiguration queueConfig1 = new CoreQueueConfiguration(forwardAddress, queueName0, null, true);
      List<CoreQueueConfiguration> queueConfigs1 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigurations(queueConfigs1);

      ServerLocator locator = null;
      try
      {
         server1.start();
         server0.start();

         waitForServerStart(server0);
         waitForServerStart(server1);

         locator = addServerLocator(HornetQClient.createServerLocatorWithHA(server0tc, server1tc));
         ClientSessionFactory csf0 = locator.createSessionFactory(server0tc);
         ClientSession session0 = csf0.createSession(false, true, true);

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
         ClientSession session1 = csf1.createSession(false, true, true);

         ClientConsumer cons1 = session1.createConsumer(queueName0);

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

         session0.close();
         session1.close();
      }
      finally
      {
         if (locator != null)
         {
            locator.close();
         }

         server0.stop();
         server1.stop();
      }

      Assert.assertEquals(0, server0.getRemotingService().getConnections().size());
      Assert.assertEquals(0, server1.getRemotingService().getConnections().size());
   }

   public void testFailoverThenFailAgainAndReconnect() throws Exception
   {
      Map<String, Object> server0Params = new HashMap<String, Object>();
      HornetQServer server0 = createHornetQServer(0, isNetty(), server0Params);

      Map<String, Object> server1Params = new HashMap<String, Object>();
      HornetQServer server1 = createHornetQServer(1, isNetty(), server1Params);

      TransportConfiguration server0tc = new TransportConfiguration(getConnector(), server0Params, "server0tc");

      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();

      TransportConfiguration server1tc = new TransportConfiguration(getConnector(), server1Params, "server1tc");

      connectors.put(server1tc.getName(), server1tc);

      server0.getConfiguration().setConnectorConfigurations(connectors);

      final String bridgeName = "bridge1";
      final String testAddress = "testAddress";
      final String queueName0 = "queue0";
      final String forwardAddress = "forwardAddress";

      final long retryInterval = 50;
      final double retryIntervalMultiplier = 1d;
      final int reconnectAttempts = 3;
      final int confirmationWindowSize = 1024;

      ArrayList<String> staticConnectors = new ArrayList<String>();
      staticConnectors.add(server1tc.getName());

      BridgeConfiguration bridgeConfiguration = new BridgeConfiguration(bridgeName,
                                                                        queueName0,
                                                                        forwardAddress,
                                                                        null,
                                                                        null,
                                                                        HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                                                        HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                                                        HornetQClient.DEFAULT_CONNECTION_TTL,
                                                                        retryInterval,
                                                                        HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                                                        retryIntervalMultiplier,
                                                                        reconnectAttempts,
                                                                        true,
                                                                        confirmationWindowSize,
                                                                        staticConnectors,
                                                                        false,
                                                                        HornetQDefaultConfiguration.DEFAULT_CLUSTER_USER,
                                                                        HornetQDefaultConfiguration.DEFAULT_CLUSTER_PASSWORD);

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<BridgeConfiguration>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      CoreQueueConfiguration queueConfig0 = new CoreQueueConfiguration(testAddress, queueName0, null, true);
      List<CoreQueueConfiguration> queueConfigs0 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigurations(queueConfigs0);

      CoreQueueConfiguration queueConfig1 = new CoreQueueConfiguration(forwardAddress, queueName0, null, true);
      List<CoreQueueConfiguration> queueConfigs1 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigurations(queueConfigs1);

      ServerLocator locator = null;
      try
      {
         server1.start();
         server0.start();

         locator = addServerLocator(HornetQClient.createServerLocatorWithHA(server0tc, server1tc));
         ClientSessionFactory csf0 = locator.createSessionFactory(server0tc);
         ClientSession session0 = csf0.createSession(false, true, true);

         ClientSessionFactory csf1 = locator.createSessionFactory(server1tc);
         ClientSession session1 = csf1.createSession(false, true, true);

         ClientProducer prod0 = session0.createProducer(testAddress);

         ClientConsumer cons1 = session1.createConsumer(queueName0);

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


         session0.close();
         session1.close();

         if (outOfOrder != -1)
         {
            fail("Message " + outOfOrder + " was received out of order, it was supposed to be " + supposed);
         }
      }
      finally
      {
         if (locator != null)
         {
            locator.close();
         }

         server0.stop();
         server1.stop();
      }

      Assert.assertEquals(0, server0.getRemotingService().getConnections().size());
      Assert.assertEquals(0, server1.getRemotingService().getConnections().size());
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
