/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat
 * Middleware LLC, and individual contributors by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of individual
 * contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.integration.cluster.distribution;

import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;
import static org.jboss.messaging.core.config.impl.ConfigurationImpl.DEFAULT_USE_DUPLICATE_DETECTION;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.MessageFlowConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.tests.util.ServiceTestBase;
import org.jboss.messaging.util.Pair;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A MaxHopsTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 24 Nov 2008 14:26:45
 *
 *
 */
public class MaxHopsTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(MaxHopsTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   public void testFoo()
   {      
   }
   
   
//
//   public void testHops() throws Exception
//   {
//      testHops(0, false);
//      testHops(1, false);
//      testHops(2, false);
//      testHops(3, false);
//      testHops(4, true);
//      testHops(5, true);
//      testHops(6, true);
//      testHops(-1, true);
//   }
//
//   public void testHopsFanout() throws Exception
//   {
//      testHopsFanout(0, false);
//      testHopsFanout(1, false);
//      testHopsFanout(2, true);
//      testHopsFanout(3, true);
//      testHopsFanout(4, true);
//      testHopsFanout(-1, true);
//   }
//
//   private void testHops(final int maxHops, final boolean shouldReceive) throws Exception
//   {
//      Map<String, Object> service0Params = new HashMap<String, Object>();
//      MessagingService service0 = createClusteredServiceWithParams(0, false, service0Params);
//
//      Map<String, Object> service1Params = new HashMap<String, Object>();
//      service1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
//      MessagingService service1 = createClusteredServiceWithParams(1, false, service1Params);
//
//      Map<String, Object> service2Params = new HashMap<String, Object>();
//      service2Params.put(TransportConstants.SERVER_ID_PROP_NAME, 2);
//      MessagingService service2 = createClusteredServiceWithParams(2, false, service2Params);
//
//      Map<String, Object> service3Params = new HashMap<String, Object>();
//      service3Params.put(TransportConstants.SERVER_ID_PROP_NAME, 3);
//      MessagingService service3 = createClusteredServiceWithParams(3, false, service3Params);
//
//      Map<String, Object> service4Params = new HashMap<String, Object>();
//      service4Params.put(TransportConstants.SERVER_ID_PROP_NAME, 4);
//      MessagingService service4 = createClusteredServiceWithParams(4, false, service4Params);
//
//      Map<String, TransportConfiguration> connectors0 = new HashMap<String, TransportConfiguration>();
//      TransportConfiguration server1tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
//                                                                    service1Params,
//                                                                    "connector1");
//      connectors0.put(server1tc.getName(), server1tc);
//      service0.getServer().getConfiguration().setConnectorConfigurations(connectors0);
//
//      Map<String, TransportConfiguration> connectors1 = new HashMap<String, TransportConfiguration>();
//      TransportConfiguration server2tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
//                                                                    service2Params,
//                                                                    "connector1");
//      connectors1.put(server2tc.getName(), server2tc);
//      service1.getServer().getConfiguration().setConnectorConfigurations(connectors1);
//
//      Map<String, TransportConfiguration> connectors2 = new HashMap<String, TransportConfiguration>();
//      TransportConfiguration server3tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
//                                                                    service3Params,
//                                                                    "connector1");
//      connectors2.put(server3tc.getName(), server3tc);
//      service2.getServer().getConfiguration().setConnectorConfigurations(connectors2);
//
//      Map<String, TransportConfiguration> connectors3 = new HashMap<String, TransportConfiguration>();
//      TransportConfiguration server4tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
//                                                                    service4Params,
//                                                                    "connector1");
//      connectors3.put(server4tc.getName(), server4tc);
//      service3.getServer().getConfiguration().setConnectorConfigurations(connectors3);
//
//      List<Pair<String, String>> connectorNames = new ArrayList<Pair<String, String>>();
//      connectorNames.add(new Pair<String, String>("connector1", null));
//
//      final SimpleString testAddress = new SimpleString("testaddress");
//
//      MessageFlowConfiguration ofconfig = new MessageFlowConfiguration("outflow1",
//                                                                       testAddress.toString(),
//                                                                       null,
//                                                                       false,
//                                                                       1,
//                                                                       -1,
//                                                                       null,
//                                                                       DEFAULT_RETRY_INTERVAL,
//                                                                       DEFAULT_RETRY_INTERVAL_MULTIPLIER,
//                                                                       0,
//                                                                       0,
//                                                                       DEFAULT_USE_DUPLICATE_DETECTION,
//                                                                       maxHops,
//                                                                       connectorNames);
//
//      Set<MessageFlowConfiguration> ofconfigs = new HashSet<MessageFlowConfiguration>();
//      ofconfigs.add(ofconfig);
//
//      service0.getServer().getConfiguration().setMessageFlowConfigurations(ofconfigs);
//      service1.getServer().getConfiguration().setMessageFlowConfigurations(ofconfigs);
//      service2.getServer().getConfiguration().setMessageFlowConfigurations(ofconfigs);
//      service3.getServer().getConfiguration().setMessageFlowConfigurations(ofconfigs);
//
//      service4.start();
//      service3.start();
//      service2.start();
//      service1.start();
//      service0.start();
//
//      log.info("started service");
//
//      TransportConfiguration server0tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
//                                                                    service0Params);
//
//      ClientSessionFactory csf0 = new ClientSessionFactoryImpl(server0tc);
//      ClientSession session0 = csf0.createSession(false, true, true);
//
//      ClientSessionFactory csf4 = new ClientSessionFactoryImpl(server4tc);
//      ClientSession session4 = csf4.createSession(false, true, true);
//      session4.createQueue(testAddress, testAddress, null, false, true);
//
//      ClientProducer prod0 = session0.createProducer(testAddress);
//
//      ClientConsumer cons4 = session4.createConsumer(testAddress);
//
//      session4.start();
//
//      final int numMessages = 10;
//
//      final SimpleString propKey = new SimpleString("testkey");
//
//      for (int i = 0; i < numMessages; i++)
//      {
//         ClientMessage message = session0.createClientMessage(true);
//         message.putIntProperty(propKey, i);
//         message.getBody().flip();
//
//         prod0.send(message);
//      }
//
//      log.info("sent messages");
//
//      if (shouldReceive)
//      {
//         for (int i = 0; i < numMessages; i++)
//         {
//            ClientMessage rmessage = cons4.receive(5000);
//            assertNotNull(rmessage);
//            assertEquals(i, rmessage.getProperty(propKey));
//         }
//      }
//
//      ClientMessage rmessage = cons4.receive(1000);
//
//      assertNull(rmessage);
//
//      service0.stop();
//      service1.stop();
//      service2.stop();
//      service3.stop();
//      service4.stop();
//
//      assertEquals(0, service0.getServer().getRemotingService().getConnections().size());
//      assertEquals(0, service1.getServer().getRemotingService().getConnections().size());
//      assertEquals(0, service2.getServer().getRemotingService().getConnections().size());
//      assertEquals(0, service3.getServer().getRemotingService().getConnections().size());
//      assertEquals(0, service4.getServer().getRemotingService().getConnections().size());
//   }
//
//   private void testHopsFanout(final int maxHops, final boolean shouldReceive) throws Exception
//   {
//      Map<String, Object> service0Params = new HashMap<String, Object>();
//      MessagingService service0 = createClusteredServiceWithParams(0, false, service0Params);
//
//      Map<String, Object> service1Params = new HashMap<String, Object>();
//      service1Params.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
//      MessagingService service1 = createClusteredServiceWithParams(1, false, service1Params);
//
//      Map<String, Object> service2Params = new HashMap<String, Object>();
//      service2Params.put(TransportConstants.SERVER_ID_PROP_NAME, 2);
//      MessagingService service2 = createClusteredServiceWithParams(2, false, service2Params);
//
//      Map<String, Object> service3Params = new HashMap<String, Object>();
//      service3Params.put(TransportConstants.SERVER_ID_PROP_NAME, 3);
//      MessagingService service3 = createClusteredServiceWithParams(3, false, service3Params);
//
//      Map<String, Object> service4Params = new HashMap<String, Object>();
//      service4Params.put(TransportConstants.SERVER_ID_PROP_NAME, 4);
//      MessagingService service4 = createClusteredServiceWithParams(4, false, service4Params);
//
//      Map<String, TransportConfiguration> connectors0 = new HashMap<String, TransportConfiguration>();
//      TransportConfiguration server1tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
//                                                                    service1Params,
//                                                                    "connector1");
//      connectors0.put(server1tc.getName(), server1tc);
//      service0.getServer().getConfiguration().setConnectorConfigurations(connectors0);
//
//      Map<String, TransportConfiguration> connectors1 = new HashMap<String, TransportConfiguration>();
//      TransportConfiguration server2tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
//                                                                    service2Params,
//                                                                    "connector1");
//
//      TransportConfiguration server3tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
//                                                                    service3Params,
//                                                                    "connector2");
//
//      TransportConfiguration server4tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
//                                                                    service4Params,
//                                                                    "connector3");
//      connectors1.put(server2tc.getName(), server2tc);
//      connectors1.put(server3tc.getName(), server3tc);
//      connectors1.put(server4tc.getName(), server4tc);
//
//      service1.getServer().getConfiguration().setConnectorConfigurations(connectors1);
//
//      List<Pair<String, String>> connectorNames1 = new ArrayList<Pair<String, String>>();
//      connectorNames1.add(new Pair<String, String>("connector1", null));
//
//      final SimpleString testAddress = new SimpleString("testaddress");
//
//      MessageFlowConfiguration ofconfig1 = new MessageFlowConfiguration("outflow1",
//                                                                        testAddress.toString(),
//                                                                        null,
//                                                                        false,
//                                                                        1,
//                                                                        -1,
//                                                                        null,
//                                                                        DEFAULT_RETRY_INTERVAL,
//                                                                        DEFAULT_RETRY_INTERVAL_MULTIPLIER,
//                                                                        0,
//                                                                        0,
//                                                                        DEFAULT_USE_DUPLICATE_DETECTION,
//                                                                        maxHops,
//                                                                        connectorNames1);
//
//      Set<MessageFlowConfiguration> ofconfigs1 = new HashSet<MessageFlowConfiguration>();
//      ofconfigs1.add(ofconfig1);
//
//      service0.getServer().getConfiguration().setMessageFlowConfigurations(ofconfigs1);
//
//      List<Pair<String, String>> connectorNames2 = new ArrayList<Pair<String, String>>();
//      connectorNames2.add(new Pair<String, String>("connector1", null));
//      connectorNames2.add(new Pair<String, String>("connector2", null));
//      connectorNames2.add(new Pair<String, String>("connector3", null));
//
//      MessageFlowConfiguration ofconfig2 = new MessageFlowConfiguration("outflow2",
//                                                                        testAddress.toString(),
//                                                                        null,
//                                                                        false,
//                                                                        1,
//                                                                        -1,
//                                                                        null,
//                                                                        DEFAULT_RETRY_INTERVAL,
//                                                                        DEFAULT_RETRY_INTERVAL_MULTIPLIER,
//                                                                        0,
//                                                                        0,
//                                                                        DEFAULT_USE_DUPLICATE_DETECTION,
//                                                                        maxHops,
//                                                                        connectorNames2);
//
//      Set<MessageFlowConfiguration> ofconfigs2 = new HashSet<MessageFlowConfiguration>();
//      ofconfigs2.add(ofconfig2);
//
//      service1.getServer().getConfiguration().setMessageFlowConfigurations(ofconfigs2);
//
//      service4.start();
//      service3.start();
//      service2.start();
//      service1.start();
//      service0.start();
//
//      log.info("started service");
//
//      TransportConfiguration server0tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
//                                                                    service0Params);
//
//      ClientSessionFactory csf0 = new ClientSessionFactoryImpl(server0tc);
//      ClientSession session0 = csf0.createSession(false, true, true);
//
//      ClientSessionFactory csf2 = new ClientSessionFactoryImpl(server2tc);
//      ClientSession session2 = csf2.createSession(false, true, true);
//      session2.createQueue(testAddress, testAddress, null, false, true);
//
//      ClientSessionFactory csf3 = new ClientSessionFactoryImpl(server3tc);
//      ClientSession session3 = csf3.createSession(false, true, true);
//      session3.createQueue(testAddress, testAddress, null, false, true);
//
//      ClientSessionFactory csf4 = new ClientSessionFactoryImpl(server4tc);
//      ClientSession session4 = csf4.createSession(false, true, true);
//      session4.createQueue(testAddress, testAddress, null, false, true);
//
//      ClientProducer prod0 = session0.createProducer(testAddress);
//
//      ClientConsumer cons2 = session2.createConsumer(testAddress);
//      ClientConsumer cons3 = session3.createConsumer(testAddress);
//      ClientConsumer cons4 = session4.createConsumer(testAddress);
//
//      session2.start();
//      session3.start();
//      session4.start();
//
//      final int numMessages = 1;
//
//      final SimpleString propKey = new SimpleString("testkey");
//
//      for (int i = 0; i < numMessages; i++)
//      {
//         ClientMessage message = session0.createClientMessage(true);
//         message.putIntProperty(propKey, i);
//         message.getBody().flip();
//
//         prod0.send(message);
//      }
//
//      log.info("sent messages");
//
//      if (shouldReceive)
//      {
//         for (int i = 0; i < numMessages; i++)
//         {
//            ClientMessage rmessage = cons2.receive(5000);
//            assertNotNull(rmessage);
//            assertEquals(i, rmessage.getProperty(propKey));
//
//            rmessage = cons3.receive(5000);
//            assertNotNull(rmessage);
//            assertEquals(i, rmessage.getProperty(propKey));
//
//            rmessage = cons4.receive(5000);
//            assertNotNull(rmessage);
//            assertEquals(i, rmessage.getProperty(propKey));
//         }
//      }
//
//      ClientMessage rmessage = cons2.receive(1000);
//      rmessage = cons3.receive(1000);
//      rmessage = cons4.receive(1000);
//
//      assertNull(rmessage);
//
//      service0.stop();
//      service1.stop();
//      service2.stop();
//      service3.stop();
//      service4.stop();
//
//      assertEquals(0, service0.getServer().getRemotingService().getConnections().size());
//      assertEquals(0, service1.getServer().getRemotingService().getConnections().size());
//      assertEquals(0, service2.getServer().getRemotingService().getConnections().size());
//      assertEquals(0, service3.getServer().getRemotingService().getConnections().size());
//      assertEquals(0, service4.getServer().getRemotingService().getConnections().size());
//   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.clearData();
   }

   @Override
   protected void tearDown() throws Exception
   {
      assertEquals(0, InVMRegistry.instance.size());
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
