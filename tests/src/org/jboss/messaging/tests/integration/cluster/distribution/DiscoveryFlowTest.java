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

import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_AFTER_FAILOVER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_RETRIES_BEFORE_FAILOVER;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER;

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
import org.jboss.messaging.core.config.cluster.BroadcastGroupConfiguration;
import org.jboss.messaging.core.config.cluster.DiscoveryGroupConfiguration;
import org.jboss.messaging.core.config.cluster.MessageFlowConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.util.Pair;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A DiscoveryFlowTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 24 Nov 2008 14:25:45
 *
 *
 */
public class DiscoveryFlowTest extends MessageFlowTestBase
{
   private static final Logger log = Logger.getLogger(DiscoveryFlowTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   public void testDiscoveryOutflow() throws Exception
   {
      Map<String, Object> service0Params = new HashMap<String, Object>();
      MessagingService service0 = createMessagingService(0, service0Params);

      final String groupAddress = "230.1.2.3";

      final int groupPort = 8765;

      final long broadcastPeriod = 250;

      final String bcGroupName = "bc1";

      final String localBindAddress = "localhost";

      final int localBindPort = 5432;

      Map<String, Object> service1Params = new HashMap<String, Object>();
      MessagingService service1 = createMessagingService(1, service1Params);
      TransportConfiguration server1tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                    service1Params);
      Map<String, TransportConfiguration> server1Connectors = new HashMap<String, TransportConfiguration>();
      server1Connectors.put(server1tc.getName(), server1tc);
      service1.getServer().getConfiguration().setConnectorConfigurations(server1Connectors);
      List<Pair<String, String>> connectorNames1 = new ArrayList<Pair<String, String>>();
      connectorNames1.add(new Pair<String, String>(server1tc.getName(), null));
      BroadcastGroupConfiguration bcConfig1 = new BroadcastGroupConfiguration(bcGroupName,
                                                                              localBindAddress,
                                                                              localBindPort,
                                                                              groupAddress,
                                                                              groupPort,
                                                                              broadcastPeriod,
                                                                              connectorNames1);
      Set<BroadcastGroupConfiguration> bcConfigs1 = new HashSet<BroadcastGroupConfiguration>();
      bcConfigs1.add(bcConfig1);
      service1.getServer().getConfiguration().setBroadcastGroupConfigurations(bcConfigs1);
      service1.start();

      Map<String, Object> service2Params = new HashMap<String, Object>();
      MessagingService service2 = createMessagingService(2, service2Params);
      TransportConfiguration server2tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                    service2Params);
      Map<String, TransportConfiguration> server2Connectors = new HashMap<String, TransportConfiguration>();
      server2Connectors.put(server2tc.getName(), server2tc);
      service2.getServer().getConfiguration().setConnectorConfigurations(server2Connectors);
      List<Pair<String, String>> connectorNames2 = new ArrayList<Pair<String, String>>();
      connectorNames2.add(new Pair<String, String>(server2tc.getName(), null));
      BroadcastGroupConfiguration bcConfig2 = new BroadcastGroupConfiguration(bcGroupName,
                                                                              localBindAddress,
                                                                              localBindPort,
                                                                              groupAddress,
                                                                              groupPort,
                                                                              broadcastPeriod,
                                                                              connectorNames2);
      Set<BroadcastGroupConfiguration> bcConfigs2 = new HashSet<BroadcastGroupConfiguration>();
      bcConfigs2.add(bcConfig2);
      service2.getServer().getConfiguration().setBroadcastGroupConfigurations(bcConfigs2);
      service2.start();

      Map<String, Object> service3Params = new HashMap<String, Object>();
      MessagingService service3 = createMessagingService(3, service3Params);
      TransportConfiguration server3tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                    service3Params);
      Map<String, TransportConfiguration> server3Connectors = new HashMap<String, TransportConfiguration>();
      server3Connectors.put(server3tc.getName(), server3tc);
      service3.getServer().getConfiguration().setConnectorConfigurations(server3Connectors);
      List<Pair<String, String>> connectorNames3 = new ArrayList<Pair<String, String>>();
      connectorNames3.add(new Pair<String, String>(server3tc.getName(), null));
      BroadcastGroupConfiguration bcConfig3 = new BroadcastGroupConfiguration(bcGroupName,
                                                                              localBindAddress,
                                                                              localBindPort,
                                                                              groupAddress,
                                                                              groupPort,
                                                                              broadcastPeriod,
                                                                              connectorNames3);
      Set<BroadcastGroupConfiguration> bcConfigs3 = new HashSet<BroadcastGroupConfiguration>();
      bcConfigs3.add(bcConfig3);
      service3.getServer().getConfiguration().setBroadcastGroupConfigurations(bcConfigs3);
      service3.start();

      Map<String, Object> service4Params = new HashMap<String, Object>();
      MessagingService service4 = createMessagingService(4, service4Params);
      TransportConfiguration server4tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                    service4Params);
      Map<String, TransportConfiguration> server4Connectors = new HashMap<String, TransportConfiguration>();
      server4Connectors.put(server4tc.getName(), server4tc);
      service4.getServer().getConfiguration().setConnectorConfigurations(server4Connectors);
      List<Pair<String, String>> connectorNames4 = new ArrayList<Pair<String, String>>();
      connectorNames4.add(new Pair<String, String>(server4tc.getName(), null));
      BroadcastGroupConfiguration bcConfig4 = new BroadcastGroupConfiguration(bcGroupName,
                                                                              localBindAddress,
                                                                              localBindPort,
                                                                              groupAddress,
                                                                              groupPort,
                                                                              broadcastPeriod,
                                                                              connectorNames4);
      Set<BroadcastGroupConfiguration> bcConfigs4 = new HashSet<BroadcastGroupConfiguration>();
      bcConfigs4.add(bcConfig4);
      service4.getServer().getConfiguration().setBroadcastGroupConfigurations(bcConfigs4);
      service4.start();

      final SimpleString testAddress = new SimpleString("testaddress");

      final long discoveryTimeout = 500;
      
      final String discoveryGroupName = "dcGroup";

      DiscoveryGroupConfiguration dcConfig = new DiscoveryGroupConfiguration(discoveryGroupName,
                                                                             groupAddress,
                                                                             groupPort,
                                                                             discoveryTimeout);
      
      
      Map<String, DiscoveryGroupConfiguration> dcConfigs = new HashMap<String, DiscoveryGroupConfiguration>();
      dcConfigs.put(dcConfig.getName(), dcConfig);
      service0.getServer().getConfiguration().setDiscoveryGroupConfigurations(dcConfigs);

      MessageFlowConfiguration ofconfig = new MessageFlowConfiguration("outflow1",
                                                                       testAddress.toString(),
                                                                       null,
                                                                       true,
                                                                       1,
                                                                       -1,
                                                                       null,
                                                                       DEFAULT_RETRY_INTERVAL,
                                                                       DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                                                       DEFAULT_MAX_RETRIES_BEFORE_FAILOVER,
                                                                       DEFAULT_MAX_RETRIES_AFTER_FAILOVER,
                                                                       discoveryGroupName);
      Set<MessageFlowConfiguration> ofconfigs = new HashSet<MessageFlowConfiguration>();
      ofconfigs.add(ofconfig);
      service0.getServer().getConfiguration().setMessageFlowConfigurations(ofconfigs);

      service0.start();
      
      Thread.sleep(1000);

      TransportConfiguration server0tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                    service0Params);

      ClientSessionFactory csf0 = new ClientSessionFactoryImpl(server0tc);
      ClientSession session0 = csf0.createSession(false, true, true);

      ClientSessionFactory csf1 = new ClientSessionFactoryImpl(server1tc);
      ClientSession session1 = csf1.createSession(false, true, true);

      ClientSessionFactory csf2 = new ClientSessionFactoryImpl(server2tc);
      ClientSession session2 = csf2.createSession(false, true, true);

      ClientSessionFactory csf3 = new ClientSessionFactoryImpl(server3tc);
      ClientSession session3 = csf3.createSession(false, true, true);

      ClientSessionFactory csf4 = new ClientSessionFactoryImpl(server4tc);
      ClientSession session4 = csf4.createSession(false, true, true);

      session0.createQueue(testAddress, testAddress, null, false, false, true);
      session1.createQueue(testAddress, testAddress, null, false, false, true);
      session2.createQueue(testAddress, testAddress, null, false, false, true);
      session3.createQueue(testAddress, testAddress, null, false, false, true);
      session4.createQueue(testAddress, testAddress, null, false, false, true);

      ClientProducer prod0 = session0.createProducer(testAddress);

      ClientConsumer cons0 = session0.createConsumer(testAddress);
      ClientConsumer cons1 = session1.createConsumer(testAddress);
      ClientConsumer cons2 = session2.createConsumer(testAddress);
      ClientConsumer cons3 = session3.createConsumer(testAddress);
      ClientConsumer cons4 = session4.createConsumer(testAddress);

      session0.start();

      session1.start();
      session2.start();
      session3.start();
      session4.start();

      final int numMessages = 100;

      final SimpleString propKey = new SimpleString("testkey");

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session0.createClientMessage(false);
         message.putIntProperty(propKey, i);
         message.getBody().flip();

         prod0.send(message);
      }
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage rmessage0 = cons0.receive(1000);
         assertNotNull(rmessage0);
         assertEquals(i, rmessage0.getProperty(propKey));

         ClientMessage rmessage1 = cons1.receive(1000);
         assertNotNull(rmessage1);
         assertEquals(i, rmessage1.getProperty(propKey));

         ClientMessage rmessage2 = cons2.receive(1000);
         assertNotNull(rmessage2);
         assertEquals(i, rmessage2.getProperty(propKey));

         ClientMessage rmessage3 = cons3.receive(1000);
         assertNotNull(rmessage3);
         assertEquals(i, rmessage3.getProperty(propKey));

         ClientMessage rmessage4 = cons4.receive(1000);
         assertNotNull(rmessage4);
         assertEquals(i, rmessage4.getProperty(propKey));
      }
      
      session0.close();
      session1.close();
      session2.close();
      session3.close();
      session4.close();
      
      service0.stop();
      service1.stop();
      service2.stop();
      service3.stop();
      service4.stop();
      
      assertEquals(0, service0.getServer().getRemotingService().getConnections().size());
      assertEquals(0, service1.getServer().getRemotingService().getConnections().size());
      assertEquals(0, service2.getServer().getRemotingService().getConnections().size());
      assertEquals(0, service3.getServer().getRemotingService().getConnections().size());
      assertEquals(0, service4.getServer().getRemotingService().getConnections().size());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
   }

   @Override
   protected void tearDown() throws Exception
   {
      assertEquals(0, InVMRegistry.instance.size());
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
