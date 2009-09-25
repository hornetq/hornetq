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

import static org.hornetq.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.cluster.BridgeConfiguration;
import org.hornetq.core.config.cluster.QueueConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.cluster.Bridge;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.Pair;
import org.hornetq.utils.SimpleString;

/**
 * A BridgeStartTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 14 Jan 2009 14:05:01
 *
 *
 */
public class BridgeStartTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(BridgeStartTest.class);

   public void testStartStop() throws Exception
   {
      Map<String, Object> server0Params = new HashMap<String, Object>();
      HornetQServer server0 = createClusteredServerWithParams(0, true, server0Params);

      Map<String, Object> server1Params = new HashMap<String, Object>();
      server1Params.put(SERVER_ID_PROP_NAME, 1);
      HornetQServer server1 = createClusteredServerWithParams(1, true, server1Params);

      final String testAddress = "testAddress";
      final String queueName0 = "queue0";
      final String forwardAddress = "forwardAddress";
      final String queueName1 = "queue1";

      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      TransportConfiguration server0tc = new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                    server0Params);
      TransportConfiguration server1tc = new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                    server1Params);
      connectors.put(server1tc.getName(), server1tc);

      server0.getConfiguration().setConnectorConfigurations(connectors);

      Pair<String, String> connectorPair = new Pair<String, String>(server1tc.getName(), null);

      final String bridgeName = "bridge1";

      BridgeConfiguration bridgeConfiguration = new BridgeConfiguration(bridgeName,
                                                                        queueName0,
                                                                        forwardAddress,
                                                                        null,
                                                                        null,
                                                                        1000,
                                                                        1d,
                                                                        0,
                                                                        true,
                                                                        true,
                                                                        connectorPair);

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<BridgeConfiguration>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      QueueConfiguration queueConfig0 = new QueueConfiguration(testAddress, queueName0, null, true);
      List<QueueConfiguration> queueConfigs0 = new ArrayList<QueueConfiguration>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigurations(queueConfigs0);

      QueueConfiguration queueConfig1 = new QueueConfiguration(forwardAddress, queueName1, null, true);
      List<QueueConfiguration> queueConfigs1 = new ArrayList<QueueConfiguration>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigurations(queueConfigs1);

      server1.start();
      server0.start();

      ClientSessionFactory sf0 = new ClientSessionFactoryImpl(server0tc);

      ClientSessionFactory sf1 = new ClientSessionFactoryImpl(server1tc);

      ClientSession session0 = sf0.createSession(false, true, true);

      ClientSession session1 = sf1.createSession(false, true, true);

      ClientProducer producer0 = session0.createProducer(new SimpleString(testAddress));

      ClientConsumer consumer1 = session1.createConsumer(queueName1);

      session1.start();

      final int numMessages = 10;

      final SimpleString propKey = new SimpleString("testkey");

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session0.createClientMessage(false);

         message.putIntProperty(propKey, i);

         producer0.send(message);
      }

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer1.receive(200);

         assertNotNull(message);

         assertEquals((Integer)i, (Integer)message.getProperty(propKey));

         message.acknowledge();
      }

      assertNull(consumer1.receive(200));

      Bridge bridge = server0.getClusterManager().getBridges().get(bridgeName);

      bridge.stop();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session0.createClientMessage(false);

         message.putIntProperty(propKey, i);

         producer0.send(message);
      }

      assertNull(consumer1.receive(500));

      bridge.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer1.receive(1000);

         assertNotNull(message);

         assertEquals((Integer)i, (Integer)message.getProperty(propKey));

         message.acknowledge();
      }

      assertNull(consumer1.receive(200));

      session0.close();

      session1.close();

      sf0.close();

      sf1.close();

      server0.stop();

      server1.stop();
   }

   public void testTargetServerUpAndDown() throws Exception
   {
      // This test needs to use real files, since it requires duplicate detection, since when the target server is
      // shutdown, messages will get resent when it is started, so the dup id cache needs
      // to be persisted

      Map<String, Object> server0Params = new HashMap<String, Object>();
      HornetQServer server0 = createClusteredServerWithParams(0, true, server0Params);

      Map<String, Object> server1Params = new HashMap<String, Object>();
      server1Params.put(SERVER_ID_PROP_NAME, 1);
      HornetQServer server1 = createClusteredServerWithParams(1, true, server1Params);

      final String testAddress = "testAddress";
      final String queueName0 = "queue0";
      final String forwardAddress = "forwardAddress";
      final String queueName1 = "queue1";

      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      TransportConfiguration server0tc = new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                    server0Params);
      TransportConfiguration server1tc = new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                    server1Params);
      connectors.put(server1tc.getName(), server1tc);

      server0.getConfiguration().setConnectorConfigurations(connectors);

      Pair<String, String> connectorPair = new Pair<String, String>(server1tc.getName(), null);

      final String bridgeName = "bridge1";

      BridgeConfiguration bridgeConfiguration = new BridgeConfiguration(bridgeName,
                                                                        queueName0,
                                                                        forwardAddress,
                                                                        null,
                                                                        null,
                                                                        500,
                                                                        1d,
                                                                        -1,
                                                                        true,
                                                                        true,
                                                                        connectorPair);

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<BridgeConfiguration>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      QueueConfiguration queueConfig0 = new QueueConfiguration(testAddress, queueName0, null, true);
      List<QueueConfiguration> queueConfigs0 = new ArrayList<QueueConfiguration>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigurations(queueConfigs0);

      QueueConfiguration queueConfig1 = new QueueConfiguration(forwardAddress, queueName1, null, true);
      List<QueueConfiguration> queueConfigs1 = new ArrayList<QueueConfiguration>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigurations(queueConfigs1);

      try
      {
         // Don't start server 1 yet

         server0.start();

         ClientSessionFactory sf0 = new ClientSessionFactoryImpl(server0tc);

         ClientSession session0 = sf0.createSession(false, true, true);

         ClientProducer producer0 = session0.createProducer(new SimpleString(testAddress));

         final int numMessages = 10;

         final SimpleString propKey = new SimpleString("testkey");

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session0.createClientMessage(false);

            message.putIntProperty(propKey, i);

            producer0.send(message);
         }

         // Wait a bit
         Thread.sleep(1000);

         server1.start();

         ClientSessionFactory sf1 = new ClientSessionFactoryImpl(server1tc);

         ClientSession session1 = sf1.createSession(false, true, true);

         ClientConsumer consumer1 = session1.createConsumer(queueName1);

         session1.start();

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = consumer1.receive(1000);

            assertNotNull(message);

            assertEquals((Integer)i, (Integer)message.getProperty(propKey));

            message.acknowledge();
         }

         assertNull(consumer1.receive(200));

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session0.createClientMessage(false);

            message.putIntProperty(propKey, i);

            producer0.send(message);
         }

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = consumer1.receive(1000);

            assertNotNull(message);

            assertEquals((Integer)i, (Integer)message.getProperty(propKey));

            message.acknowledge();
         }

         assertNull(consumer1.receive(200));

         session1.close();

         sf1.close();

         log.info("stopping server 1");
         
         server1.stop();
         
         log.info("stopped server 1");

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session0.createClientMessage(false);

            message.putIntProperty(propKey, i);

            producer0.send(message);
         }
         
         log.info("sent some more messages");

         server1.start();
         
         log.info("started server1");

         sf1 = new ClientSessionFactoryImpl(server1tc);

         session1 = sf1.createSession(false, true, true);

         consumer1 = session1.createConsumer(queueName1);

         session1.start();
         
         log.info("started session");

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = consumer1.receive(1000);

            assertNotNull(message);

            assertEquals((Integer)i, (Integer)message.getProperty(propKey));

            message.acknowledge();
         }

         assertNull(consumer1.receive(200));

         session1.close();

         sf1.close();

         session0.close();

         sf0.close();
      }
      finally
      {
         server0.stop();

         server1.stop();
      }
   }

   public void testTargetServerNotAvailableNoReconnectTries() throws Exception
   {
      Map<String, Object> server0Params = new HashMap<String, Object>();
      HornetQServer server0 = createClusteredServerWithParams(0, false, server0Params);

      Map<String, Object> server1Params = new HashMap<String, Object>();
      server1Params.put(SERVER_ID_PROP_NAME, 1);
      HornetQServer server1 = createClusteredServerWithParams(1, false, server1Params);

      final String testAddress = "testAddress";
      final String queueName0 = "queue0";
      final String forwardAddress = "forwardAddress";
      final String queueName1 = "queue1";

      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      TransportConfiguration server0tc = new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                    server0Params);
      TransportConfiguration server1tc = new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                    server1Params);
      connectors.put(server1tc.getName(), server1tc);

      server0.getConfiguration().setConnectorConfigurations(connectors);

      Pair<String, String> connectorPair = new Pair<String, String>(server1tc.getName(), null);

      final String bridgeName = "bridge1";

      BridgeConfiguration bridgeConfiguration = new BridgeConfiguration(bridgeName,
                                                                        queueName0,
                                                                        forwardAddress,
                                                                        null,
                                                                        null,
                                                                        1000,
                                                                        1d,
                                                                        0,
                                                                        false,
                                                                        false,
                                                                        connectorPair);

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<BridgeConfiguration>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      QueueConfiguration queueConfig0 = new QueueConfiguration(testAddress, queueName0, null, true);
      List<QueueConfiguration> queueConfigs0 = new ArrayList<QueueConfiguration>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigurations(queueConfigs0);

      QueueConfiguration queueConfig1 = new QueueConfiguration(forwardAddress, queueName1, null, true);
      List<QueueConfiguration> queueConfigs1 = new ArrayList<QueueConfiguration>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigurations(queueConfigs1);

      // Don't start server 1 yet

      server0.start();

      ClientSessionFactory sf0 = new ClientSessionFactoryImpl(server0tc);

      ClientSession session0 = sf0.createSession(false, true, true);

      ClientProducer producer0 = session0.createProducer(new SimpleString(testAddress));

      final int numMessages = 10;

      final SimpleString propKey = new SimpleString("testkey");

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session0.createClientMessage(false);

         message.putIntProperty(propKey, i);

         producer0.send(message);
      }

      // Wait a bit
      Thread.sleep(1000);

      // JMSBridge should be stopped since retries = 0

      server1.start();

      ClientSessionFactory sf1 = new ClientSessionFactoryImpl(server1tc);

      ClientSession session1 = sf1.createSession(false, true, true);

      ClientConsumer consumer1 = session1.createConsumer(queueName1);

      session1.start();

      // Won't be received since the bridge was deactivated
      assertNull(consumer1.receive(200));

      // Now start the bridge manually

      Bridge bridge = server0.getClusterManager().getBridges().get(bridgeName);

      bridge.start();

      // Messages should now be received

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer1.receive(1000);

         assertNotNull(message);

         assertEquals((Integer)i, (Integer)message.getProperty(propKey));

         message.acknowledge();
      }

      assertNull(consumer1.receive(200));

      session1.close();

      sf1.close();

      session0.close();

      sf0.close();

      server0.stop();

      server1.stop();
   }

   public void testManualStopStart() throws Exception
   {
      Map<String, Object> server0Params = new HashMap<String, Object>();
      HornetQServer server0 = createClusteredServerWithParams(0, false, server0Params);

      Map<String, Object> server1Params = new HashMap<String, Object>();
      server1Params.put(SERVER_ID_PROP_NAME, 1);
      HornetQServer server1 = createClusteredServerWithParams(1, false, server1Params);

      final String testAddress = "testAddress";
      final String queueName0 = "queue0";
      final String forwardAddress = "forwardAddress";
      final String queueName1 = "queue1";

      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      TransportConfiguration server0tc = new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                    server0Params);
      TransportConfiguration server1tc = new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                    server1Params);
      connectors.put(server1tc.getName(), server1tc);

      server0.getConfiguration().setConnectorConfigurations(connectors);

      Pair<String, String> connectorPair = new Pair<String, String>(server1tc.getName(), null);

      final String bridgeName = "bridge1";

      BridgeConfiguration bridgeConfiguration = new BridgeConfiguration(bridgeName,
                                                                        queueName0,
                                                                        forwardAddress,
                                                                        null,
                                                                        null,
                                                                        1000,
                                                                        1d,
                                                                        1,
                                                                        false,
                                                                        false,
                                                                        connectorPair);

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<BridgeConfiguration>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      QueueConfiguration queueConfig0 = new QueueConfiguration(testAddress, queueName0, null, true);
      List<QueueConfiguration> queueConfigs0 = new ArrayList<QueueConfiguration>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigurations(queueConfigs0);

      QueueConfiguration queueConfig1 = new QueueConfiguration(forwardAddress, queueName1, null, true);
      List<QueueConfiguration> queueConfigs1 = new ArrayList<QueueConfiguration>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigurations(queueConfigs1);

      server1.start();

      server0.start();

      ClientSessionFactory sf0 = new ClientSessionFactoryImpl(server0tc);

      ClientSession session0 = sf0.createSession(false, true, true);

      ClientProducer producer0 = session0.createProducer(new SimpleString(testAddress));

      final int numMessages = 10;

      final SimpleString propKey = new SimpleString("testkey");

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session0.createClientMessage(false);

         message.putIntProperty(propKey, i);

         producer0.send(message);
      }

      ClientSessionFactory sf1 = new ClientSessionFactoryImpl(server1tc);

      ClientSession session1 = sf1.createSession(false, true, true);

      ClientConsumer consumer1 = session1.createConsumer(queueName1);

      session1.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer1.receive(1000);

         assertNotNull(message);

         assertEquals((Integer)i, (Integer)message.getProperty(propKey));

         message.acknowledge();
      }

      assertNull(consumer1.receive(200));

      // Now stop the bridge manually

      Bridge bridge = server0.getClusterManager().getBridges().get(bridgeName);

      bridge.stop();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session0.createClientMessage(false);

         message.putIntProperty(propKey, i);

         producer0.send(message);
      }

      assertNull(consumer1.receive(200));

      bridge.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer1.receive(1000);

         assertNotNull(message);

         assertEquals((Integer)i, (Integer)message.getProperty(propKey));

         message.acknowledge();
      }

      assertNull(consumer1.receive(200));

      bridge.stop();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session0.createClientMessage(false);

         message.putIntProperty(propKey, i);

         producer0.send(message);
      }

      assertNull(consumer1.receive(200));

      bridge.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer1.receive(1000);

         assertNotNull(message);

         assertEquals((Integer)i, (Integer)message.getProperty(propKey));

         message.acknowledge();
      }

      assertNull(consumer1.receive(200));

      session1.close();

      sf1.close();

      session0.close();

      sf0.close();

      server0.stop();

      server1.stop();
   }
   
   protected void setUp() throws Exception
   {
      super.setUp();
      clearData();
   }

   protected void tearDown() throws Exception
   {
      clearData();
      super.tearDown();
   }

}
