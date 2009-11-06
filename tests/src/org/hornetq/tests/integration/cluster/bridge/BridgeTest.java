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

import org.hornetq.core.buffers.ChannelBuffers;
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
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.Pair;
import org.hornetq.utils.SimpleString;

/**
 * A JMSBridgeTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 14 Jan 2009 14:05:01
 *
 *
 */
public class BridgeTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(BridgeTest.class);

   public void testSimpleBridge() throws Exception
   {
      internaltestSimpleBridge(false, false);
   }

   public void testSimpleBridgeFiles() throws Exception
   {
      internaltestSimpleBridge(false, true);
   }

   // Commented out by Clebert - I'm investigating this failure.. so I've set as disabled
   public void disabled_testSimpleBridgeLargeMessageNullPersistence() throws Exception
   {
      internaltestSimpleBridge(true, false);
   }

   public void testSimpleBridgeLargeMessageFiles() throws Exception
   {
      internaltestSimpleBridge(true, true);
   }

   public void internaltestSimpleBridge(boolean largeMessage, boolean useFiles) throws Exception
   {
      HornetQServer server0 = null;
      HornetQServer server1 = null;

      try
      {

         Map<String, Object> server0Params = new HashMap<String, Object>();
         server0 = createClusteredServerWithParams(0, useFiles, server0Params);

         Map<String, Object> server1Params = new HashMap<String, Object>();
         server1Params.put(SERVER_ID_PROP_NAME, 1);
         server1 = createClusteredServerWithParams(1, useFiles, server1Params);

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

         BridgeConfiguration bridgeConfiguration = new BridgeConfiguration("bridge1",
                                                                           queueName0,
                                                                           forwardAddress,
                                                                           null,
                                                                           null,
                                                                           1000,
                                                                           1d,
                                                                           -1,
                                                                           true,
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

            if (largeMessage)
            {
               message.setBodyInputStream(createFakeLargeStream(1024 * 1024));
            }

            message.putIntProperty(propKey, i);

            producer0.send(message);
         }

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = consumer1.receive(200);

            assertNotNull(message);

            assertEquals((Integer)i, (Integer)message.getObjectProperty(propKey));

            if (largeMessage)
            {
               readMessages(message);
            }

            message.acknowledge();
         }

         assertNull(consumer1.receive(200));

         session0.close();

         session1.close();

         sf0.close();

         sf1.close();

      }
      finally
      {
         try
         {
            server0.stop();
         }
         catch (Throwable ignored)
         {
         }

         try
         {
            server1.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   /**
    * @param message
    */
   private void readMessages(ClientMessage message)
   {
      byte byteRead[] = new byte[1024];

      for (int j = 0; j < 1024; j++)
      {
         message.getBody().readBytes(byteRead);
      }
   }

   public void testWithFilter() throws Exception
   {
      internalTestWithFilter(false, false);
   }

   public void testWithFilterFiles() throws Exception
   {
      internalTestWithFilter(false, true);
   }

   // Commented out by Clebert - I'm investigating this failure.. so I've set as disabled
   public void disabled_testWithFilterLargeMessages() throws Exception
   {
      internalTestWithFilter(true, false);
   }

   public void testWithFilterLargeMessagesFiles() throws Exception
   {
      internalTestWithFilter(true, true);
   }

   public void internalTestWithFilter(final boolean largeMessage, final boolean useFiles) throws Exception
   {
      HornetQServer server0 = null;
      HornetQServer server1 = null;

      try
      {

         Map<String, Object> server0Params = new HashMap<String, Object>();
         server0 = createClusteredServerWithParams(0, useFiles, server0Params);

         Map<String, Object> server1Params = new HashMap<String, Object>();
         server1Params.put(SERVER_ID_PROP_NAME, 1);
         server1 = createClusteredServerWithParams(1, useFiles, server1Params);

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

         final String filterString = "animal='goat'";

         BridgeConfiguration bridgeConfiguration = new BridgeConfiguration("bridge1",
                                                                           queueName0,
                                                                           forwardAddress,
                                                                           filterString,
                                                                           null,
                                                                           1000,
                                                                           1d,
                                                                           -1,
                                                                           true,
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

         ClientSessionFactory sf1 = new ClientSessionFactoryImpl(server1tc);

         ClientSession session0 = sf0.createSession(false, true, true);

         ClientSession session1 = sf1.createSession(false, true, true);

         ClientProducer producer0 = session0.createProducer(new SimpleString(testAddress));

         ClientConsumer consumer1 = session1.createConsumer(queueName1);

         session1.start();

         final int numMessages = 10;

         final SimpleString propKey = new SimpleString("testkey");

         final SimpleString selectorKey = new SimpleString("animal");

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session0.createClientMessage(false);

            message.putIntProperty(propKey, i);

            message.putStringProperty(selectorKey, new SimpleString("monkey"));
            
            if (largeMessage)
            {
               message.setBodyInputStream(createFakeLargeStream(1024 * 1024));
            }

            producer0.send(message);
         }

         assertNull(consumer1.receive(200));

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session0.createClientMessage(false);

            message.putIntProperty(propKey, i);

            message.putStringProperty(selectorKey, new SimpleString("goat"));

            if (largeMessage)
            {
               message.setBodyInputStream(createFakeLargeStream(1024 * 1024));
            }

            producer0.send(message);
         }

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = consumer1.receive(200);

            assertNotNull(message);

            assertEquals((Integer)i, (Integer)message.getObjectProperty(propKey));

            message.acknowledge();
            
            if (largeMessage)
            {
               readMessages(message);
            }
         }

         assertNull(consumer1.receive(200));

         session0.close();

         session1.close();

         sf0.close();

         sf1.close();
      }

      finally
      {
         try
         {
            server0.stop();
         }
         catch (Throwable ignored)
         {
         }

         try
         {
            server1.stop();
         }
         catch (Throwable ignored)
         {
         }

      }

   }
   
   public void testWithTransformer() throws Exception
   {
      internaltestWithTransformer(false);
   }

   public void testWithTransformerFiles() throws Exception
   {
      internaltestWithTransformer(true);
   }

   public void internaltestWithTransformer(boolean useFiles) throws Exception
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

      BridgeConfiguration bridgeConfiguration = new BridgeConfiguration("bridge1",
                                                                        queueName0,
                                                                        forwardAddress,
                                                                        null,
                                                                        SimpleTransformer.class.getName(),
                                                                        1000,
                                                                        1d,
                                                                        -1,
                                                                        true,
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

      ClientSessionFactory sf1 = new ClientSessionFactoryImpl(server1tc);

      ClientSession session0 = sf0.createSession(false, true, true);

      ClientSession session1 = sf1.createSession(false, true, true);

      ClientProducer producer0 = session0.createProducer(new SimpleString(testAddress));

      ClientConsumer consumer1 = session1.createConsumer(queueName1);

      session1.start();

      final int numMessages = 10;

      final SimpleString propKey = new SimpleString("wibble");

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session0.createClientMessage(false);

         message.putStringProperty(propKey, new SimpleString("bing"));

         message.getBody().writeString("doo be doo be doo be doo");
         
         producer0.send(message);
      }

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer1.receive(200);

         assertNotNull(message);

         SimpleString val = (SimpleString)message.getObjectProperty(propKey);

         assertEquals(new SimpleString("bong"), val);

         String sval = message.getBody().readString();

         assertEquals("dee be dee be dee be dee", sval);

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
   

   // https://jira.jboss.org/jira/browse/HORNETQ-182
   public void disabled_testBridgeWithPaging() throws Exception
   {
      HornetQServer server0 = null;
      HornetQServer server1 = null;

      final int PAGE_MAX = 100 * 1024;

      final int PAGE_SIZE = 10 * 1024;

      try
      {

         Map<String, Object> server0Params = new HashMap<String, Object>();
         server0 = createClusteredServerWithParams(0, true, PAGE_SIZE, PAGE_MAX, server0Params);

         Map<String, Object> server1Params = new HashMap<String, Object>();
         server1Params.put(SERVER_ID_PROP_NAME, 1);
         server1 = createClusteredServerWithParams(1, true, server1Params);

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

         BridgeConfiguration bridgeConfiguration = new BridgeConfiguration("bridge1",
                                                                           queueName0,
                                                                           forwardAddress,
                                                                           null,
                                                                           null,
                                                                           1000,
                                                                           1d,
                                                                           -1,
                                                                           true,
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

         ClientSessionFactory sf1 = new ClientSessionFactoryImpl(server1tc);

         ClientSession session0 = sf0.createSession(false, true, true);

         ClientSession session1 = sf1.createSession(false, true, true);

         ClientProducer producer0 = session0.createProducer(new SimpleString(testAddress));

         ClientConsumer consumer1 = session1.createConsumer(queueName1);

         session1.start();

         final int numMessages = 500;

         final SimpleString propKey = new SimpleString("testkey");

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session0.createClientMessage(false);
            
            message.setBody(ChannelBuffers.wrappedBuffer(new byte[1024]));

            message.putIntProperty(propKey, i);

            producer0.send(message);
         }

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = consumer1.receive(200);

            assertNotNull(message);

            assertEquals((Integer)i, (Integer)message.getObjectProperty(propKey));

            message.acknowledge();
         }

         assertNull(consumer1.receive(200));

         session0.close();

         session1.close();

         sf0.close();

         sf1.close();

      }
      finally
      {
         try
         {
            server0.stop();
         }
         catch (Throwable ignored)
         {
         }

         try
         {
            server1.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   
   protected void setUp() throws Exception
   {
      super.setUp();
      clearData();
   }
   
   protected void tearDown() throws Exception
   {
      clearData();
      super.setUp();
   }

}
