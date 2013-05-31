/*
 * Copyright 2013 Red Hat, Inc.
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
import java.util.List;

import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.cluster.impl.BridgeImpl;
import org.hornetq.tests.integration.cluster.util.MultiServerTestBase;

/**
 * @author Clebert Suconic
 */
public class BridgeFailoverTest extends MultiServerTestBase
{

   public void testSimpleConnectOnMultipleNodes() throws Exception
   {
      String ORIGINAL_QUEUE = "noCluster.originalQueue";
      String TARGET_QUEUE = "noCluster.targetQueue";

      List<String> connectors = new ArrayList<String>();
      connectors.add("target-4");
      connectors.add("backup-4");

      BridgeConfiguration bridgeConfiguration = new BridgeConfiguration("Bridge-for-test",
                                                                        ORIGINAL_QUEUE,
                                                                        TARGET_QUEUE,
                                 null,
                                 null,
                                 100 * 1024,
                                 HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                 HornetQClient.DEFAULT_CONNECTION_TTL,
                                 100,
                                 HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                 1.0,
                                 -1,
                                 true,
                                 1,
                                 connectors,
                                 true,
                                 null,
                                 null);

      
      servers[2].getConfiguration().getBridgeConfigurations().add(bridgeConfiguration);

      for (HornetQServer server: servers)
      {
         server.getConfiguration().getQueueConfigurations().add(new CoreQueueConfiguration(ORIGINAL_QUEUE, ORIGINAL_QUEUE, null, true));
         server.getConfiguration().getQueueConfigurations().add(new CoreQueueConfiguration(TARGET_QUEUE, TARGET_QUEUE, null, true));
      }

      startServers();

      // The server where the bridge source is configured at
      ServerLocator locator = createLocator(false, 2);

      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, false);
      ClientProducer producer = session.createProducer(ORIGINAL_QUEUE);

      for (int i = 0 ; i < 100; i++)
      {
         ClientMessage msg = session.createMessage(true);
         msg.putIntProperty("i", i);
         producer.send(msg);
      }
      session.commit();


      ServerLocator locatorConsumer = createLocator(false, 4);
      ClientSessionFactory factoryConsumer = locatorConsumer.createSessionFactory();
      ClientSession sessionConsumer = factoryConsumer.createSession(false, false);
      ClientConsumer consumer = sessionConsumer.createConsumer(TARGET_QUEUE);

      sessionConsumer.start();

      for (int i = 0 ; i < 100; i++)
      {
         ClientMessage message = consumer.receive(10000);
         assertNotNull(message);
         message.acknowledge();
      }

      sessionConsumer.commit();
   }

   public void testFailoverOnBridgeNoRetryOnSameNode() throws Exception
   {
      internalTestFailoverOnBridge(0);
   }

   public void testFailoverOnBridgeForeverRetryOnSameNode() throws Exception
   {
      internalTestFailoverOnBridge(-1);
   }

   public void internalTestFailoverOnBridge(int retriesSameNode) throws Exception
   {
      String ORIGINAL_QUEUE = "noCluster.originalQueue";
      String TARGET_QUEUE = "noCluster.targetQueue";

      List<String> connectors = new ArrayList<String>();
      connectors.add("target-4");
      connectors.add("backup-4");

      BridgeConfiguration bridgeConfiguration = new BridgeConfiguration("Bridge-for-test",
                                                                        ORIGINAL_QUEUE,
                                                                        TARGET_QUEUE,
                                 null,
                                 null,
                                 100 * 1024,
                                 HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                 HornetQClient.DEFAULT_CONNECTION_TTL,
                                 100,
                                 HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                 1.0,
                                 -1,
                                 true,
                                 1,
                                 connectors,
                                 true,
                                 null,
                                 null);

      servers[2].getConfiguration().getBridgeConfigurations().add(bridgeConfiguration);
      
      for (HornetQServer server: servers)
      {
         server.getConfiguration().getQueueConfigurations().add(new CoreQueueConfiguration(ORIGINAL_QUEUE, ORIGINAL_QUEUE, null, true));
         server.getConfiguration().getQueueConfigurations().add(new CoreQueueConfiguration(TARGET_QUEUE, TARGET_QUEUE, null, true));
      }

      startServers();

      BridgeImpl bridge = (BridgeImpl)servers[2].getClusterManager().getBridges().get("Bridge-for-test");
      assertNotNull(bridge);

      // The server where the bridge source is configured at
      ServerLocator locatorProducer = createLocator(false, 2);

      ClientSessionFactory factory = locatorProducer.createSessionFactory();
      ClientSession session = factory.createSession(false, false);
      ClientProducer producer = session.createProducer(ORIGINAL_QUEUE);

      for (int i = 0 ; i < 100; i++)
      {
         ClientMessage msg = session.createMessage(true);
         msg.putIntProperty("i", i);
         producer.send(msg);
      }
      session.commit();


      ServerLocator locatorConsumer = createLocator(false, 4);
      ClientSessionFactory factoryConsumer = locatorConsumer.createSessionFactory();
      ClientSession sessionConsumer = factoryConsumer.createSession(false, false);
      ClientConsumer consumer = sessionConsumer.createConsumer(TARGET_QUEUE);

      sessionConsumer.start();

      for (int i = 0 ; i < 100; i++)
      {
         ClientMessage message = consumer.receive(10000);
         assertNotNull(message);
         message.acknowledge();
      }

      // We rollback as we will receive them again
      sessionConsumer.rollback();

      factoryConsumer.close();
      sessionConsumer.close();


      crashAndWaitForFailure(servers[4], locatorConsumer);

      locatorConsumer.close();

      waitForServer(backupServers[4]);

      for (int i = 100 ; i < 200; i++)
      {
         ClientMessage msg = session.createMessage(true);
         msg.putIntProperty("i", i);
         producer.send(msg);
      }

      session.commit();


      locatorConsumer = createLocator(false, 9);
      factoryConsumer = locatorConsumer.createSessionFactory();
      sessionConsumer = factoryConsumer.createSession();

      consumer = sessionConsumer.createConsumer(TARGET_QUEUE);

      sessionConsumer.start();

      for (int i = 0 ; i < 200; i++)
      {
         ClientMessage message = consumer.receive(10000);
         assertNotNull(message);
         message.acknowledge();
      }

      sessionConsumer.commit();

   }

   public void testInitialConnectionNodeAlreadyDown() throws Exception
   {
      String ORIGINAL_QUEUE = "noCluster.originalQueue";
      String TARGET_QUEUE = "noCluster.targetQueue";

      List<String> connectors = new ArrayList<String>();
      connectors.add("target-4");
      connectors.add("backup-4");

      BridgeConfiguration bridgeConfiguration = new BridgeConfiguration("Bridge-for-test",
                                                                        ORIGINAL_QUEUE,
                                                                        TARGET_QUEUE,
                                 null,
                                 null,
                                 100 * 1024,
                                 HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                 HornetQClient.DEFAULT_CONNECTION_TTL,
                                 100,
                                 HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                 1.0,
                                 -1,
                                 true,
                                 1,
                                 connectors,
                                 true,
                                 null,
                                 null);
      
      
      
      servers[2].getConfiguration().getBridgeConfigurations().add(bridgeConfiguration);

      for (HornetQServer server: servers)
      {
         server.getConfiguration().getQueueConfigurations().add(new CoreQueueConfiguration(ORIGINAL_QUEUE, ORIGINAL_QUEUE, null, true));
         server.getConfiguration().getQueueConfigurations().add(new CoreQueueConfiguration(TARGET_QUEUE, TARGET_QUEUE, null, true));
      }

      startBackups(0,1,3,4);
      startServers(0,1,3,4);

      System.err.println("**** Crashing server 4");
      crashAndWaitForFailure(servers[4], createLocator(false, 4));
      waitForBackupInitialize(backupServers[4]);

      startBackups(2);
      startServers(2);


      // The server where the bridge source is configured at
      ServerLocator locator = createLocator(false, 2); // connecting to the backup

      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(false, false);
      ClientProducer producer = session.createProducer(ORIGINAL_QUEUE);

      for (int i = 0 ; i < 100; i++)
      {
         ClientMessage msg = session.createMessage(true);
         msg.putIntProperty("i", i);
         producer.send(msg);
      }
      session.commit();

      ServerLocator locatorConsumer = createLocator(false, 9);
      ClientSessionFactory factoryConsumer = locatorConsumer.createSessionFactory();
      ClientSession sessionConsumer = factoryConsumer.createSession(false, false);
      ClientConsumer consumer = sessionConsumer.createConsumer(TARGET_QUEUE);

      sessionConsumer.start();

      for (int i = 0 ; i < 100; i++)
      {
         ClientMessage message = consumer.receive(10000);
         assertNotNull(message);
         message.acknowledge();
      }

      sessionConsumer.commit();
   }
}
