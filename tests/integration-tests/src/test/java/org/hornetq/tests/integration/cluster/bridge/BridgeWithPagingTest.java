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

package org.hornetq.tests.integration.cluster.bridge;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.core.config.BridgeConfiguration;
import org.hornetq.core.config.CoreQueueConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.invm.InVMConnector;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.cluster.Bridge;
import org.hornetq.core.server.cluster.impl.BridgeImpl;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.spi.core.protocol.RemotingConnection;

/**
 * A BridgeWithPagingTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class BridgeWithPagingTest extends BridgeTestBase
{
   private static final Logger log = Logger.getLogger(BridgeWithPagingTest.class);

   protected boolean isNetty()
   {
      return false;
   }

   private String getConnector()
   {
      if (isNetty())
      {
         return NettyConnectorFactory.class.getName();
      }
      else
      {
         return InVMConnectorFactory.class.getName();
      }
   }

   public void testFoo() throws Exception
   {
      
   }
   
   // https://jira.jboss.org/browse/HORNETQ-382
   public void _testReconnectWithPaging() throws Exception
   {
      final byte[] content = new byte[2048]; // 2 kiB
      for (int i=0; i < content.length; ++i) {
          content[i] = (byte) i;
      }
      
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
      final int confirmationWindowSize = 1024; // 1 kiB


      ArrayList<String> staticConnectors = new ArrayList<String>();
      staticConnectors.add(server1tc.getName());
      BridgeConfiguration bridgeConfiguration = new BridgeConfiguration(bridgeName,
                                                                        queueName0,
                                                                        forwardAddress,
                                                                        null,
                                                                        null,
                                                                        retryInterval,
                                                                        retryIntervalMultiplier,
                                                                        reconnectAttempts,
                                                                        false,
                                                                        confirmationWindowSize,
                                                                        HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
            staticConnectors,
                                                                        false,
                                                                        ConfigurationImpl.DEFAULT_CLUSTER_USER,
                                                                        ConfigurationImpl.DEFAULT_CLUSTER_PASSWORD);

      List<BridgeConfiguration> bridgeConfigs = new ArrayList<BridgeConfiguration>();
      bridgeConfigs.add(bridgeConfiguration);
      server0.getConfiguration().setBridgeConfigurations(bridgeConfigs);

      CoreQueueConfiguration queueConfig0 = new CoreQueueConfiguration(testAddress, queueName0, null, true);
      List<CoreQueueConfiguration> queueConfigs0 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs0.add(queueConfig0);
      server0.getConfiguration().setQueueConfigurations(queueConfigs0);
      
      AddressSettings addressSettings = new AddressSettings();
      addressSettings.setRedeliveryDelay(0);
      addressSettings.setMaxSizeBytes(10485760); // 1 MiB
      addressSettings.setPageSizeBytes(1048576); // 100 kiB
      addressSettings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      
      server0.getConfiguration().getAddressesSettings().put("#", addressSettings);

      CoreQueueConfiguration queueConfig1 = new CoreQueueConfiguration(forwardAddress, queueName0, null, true);
      List<CoreQueueConfiguration> queueConfigs1 = new ArrayList<CoreQueueConfiguration>();
      queueConfigs1.add(queueConfig1);
      server1.getConfiguration().setQueueConfigurations(queueConfigs1);
      
      server1.getConfiguration().getAddressesSettings().put("#", addressSettings);

      server1.start();
      server0.start();
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(server0tc, server1tc);
      ClientSessionFactory csf0 = locator.createSessionFactory(server0tc);
      ClientSession session0 = csf0.createSession(false, true, true);

      ClientSessionFactory csf1 = locator.createSessionFactory(server1tc);
      //csf1.setAckBatchSize(20480); // 20 kiB
      ClientSession session1 = csf1.createSession(false, true, true);

      ClientProducer prod0 = session0.createProducer(testAddress);

      ClientConsumer cons1 = session1.createConsumer(queueName0);

      session1.start();

      // Now we will simulate a failure of the bridge connection between server0 and server1
      Bridge bridge = server0.getClusterManager().getBridges().get(bridgeName);
      final RemotingConnection forwardingConnection = getForwardingConnection(bridge);
      InVMConnector.failOnCreateConnection = true;
      InVMConnector.numberOfFailures = Integer.MAX_VALUE;
      
      Thread t = new Thread()
      {
         @Override
         public void run()
         {
            System.out.println("failing...");
            forwardingConnection.fail(new HornetQException(HornetQException.NOT_CONNECTED));
            System.out.println("reconnected!!!");
         }
      };
      t.start();
      
      final int numMessages = 5000;

      SimpleString propKey = new SimpleString("propkey");

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session0.createMessage(false);
         message.putIntProperty(propKey, i);
         message.getBodyBuffer().writeBytes(content);
         //message.setPriority((byte)3);
         prod0.send(message);
         System.out.println(">>>> " + i);
      }
      
      InVMConnector.failOnCreateConnection = false;
      
      Thread.sleep(200);

      for (int i = 0; i < numMessages; i++)
      {
         System.out.println("<<< " + i);
         ClientMessage r1 = cons1.receive(1500);
         Assert.assertNotNull("did not receive message " + i, r1);
         Assert.assertEquals(i, r1.getObjectProperty(propKey));
         r1.acknowledge();
      }

      session0.close();
      session1.close();

      server0.stop();
      server1.stop();

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
