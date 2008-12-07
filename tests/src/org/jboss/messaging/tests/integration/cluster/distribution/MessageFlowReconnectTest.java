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
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnector;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.cluster.Forwarder;
import org.jboss.messaging.core.server.cluster.MessageFlow;
import org.jboss.messaging.core.server.cluster.impl.ForwarderImpl;
import org.jboss.messaging.util.Pair;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A MessageFlowReconnectTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 7 Dec 2008 11:48:30
 *
 *
 */
public class MessageFlowReconnectTest extends MessageFlowTestBase
{
   private static final Logger log = Logger.getLogger(MessageFlowReconnectTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAutomaticReconnectBeforeFailover() throws Exception
   {
      Map<String, Object> service0Params = new HashMap<String, Object>();
      MessagingService service0 = createMessagingService(0, service0Params);

      Map<String, Object> service1Params = new HashMap<String, Object>();
      MessagingService service1 = createMessagingService(1, service1Params);
      service1.start();

      Map<String, Object> service2Params = new HashMap<String, Object>();
      MessagingService service2 = createMessagingService(2, service2Params);
      service2.start();

      TransportConfiguration server0tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                    service0Params,
                                                                    "server0tc");

      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();

      TransportConfiguration server1tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                    service1Params,
                                                                    "server1tc");

      TransportConfiguration server2tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                    service2Params,
                                                                    "server2tc");

      connectors.put(server1tc.getName(), server1tc);
      
      connectors.put(server2tc.getName(), server2tc);
      
      service0.getServer().getConfiguration().setConnectorConfigurations(connectors);

      List<Pair<String, String>> connectorNames = new ArrayList<Pair<String, String>>();
      connectorNames.add(new Pair<String, String>(server1tc.getName(), server2tc.getName()));

      final SimpleString address1 = new SimpleString("testaddress");

      final long retryInterval = 50;
      final double retryIntervalMultiplier = 1d;
      final int retriesBeforeFailover = 3;
      final int maxRetriesAfterFailover = -1;
      
      final String flowName = "flow1";
      
      MessageFlowConfiguration ofconfig1 = new MessageFlowConfiguration(flowName,
                                                                        address1.toString(),
                                                                        null,
                                                                        true,
                                                                        1,
                                                                        -1,
                                                                        null,
                                                                        retryInterval,
                                                                        retryIntervalMultiplier,
                                                                        retriesBeforeFailover,
                                                                        maxRetriesAfterFailover,
                                                                        connectorNames);

      Set<MessageFlowConfiguration> ofconfigs = new HashSet<MessageFlowConfiguration>();
      ofconfigs.add(ofconfig1);

      service0.getServer().getConfiguration().setMessageFlowConfigurations(ofconfigs);

      service0.start();

      ClientSessionFactory csf0 = new ClientSessionFactoryImpl(server0tc);
      ClientSession session0 = csf0.createSession(false, true, true);

      ClientSessionFactory csf1 = new ClientSessionFactoryImpl(server1tc);
      ClientSession session1 = csf1.createSession(false, true, true);

      session0.createQueue(address1, address1, null, false, false, false);
      session1.createQueue(address1, address1, null, false, false, false);
      ClientProducer prod0 = session0.createProducer(address1);

      ClientConsumer cons1 = session1.createConsumer(address1);

      session1.start();

      ClientMessage message = session0.createClientMessage(false);
      SimpleString propKey = new SimpleString("propkey");
      SimpleString propVal = new SimpleString("propval");
      message.putStringProperty(propKey, propVal);
      message.getBody().flip();
      
      //Now we will simulate a failure of the message flow connection between server1 and server2
      //And prevent reconnection for a few tries, then it will reconnect without failing over
      MessageFlow flow = service0.getServer().getClusterManager().getMessageFlows().get(flowName);
      Forwarder forwarder = flow.getForwarders().iterator().next();
      RemotingConnection forwardingConnection = ((ForwarderImpl)forwarder).getForwardingConnection();
      InVMConnector.failOnCreateConnection = true;
      InVMConnector.numberOfFailures = retriesBeforeFailover - 1;
      forwardingConnection.fail(new MessagingException(MessagingException.NOT_CONNECTED));

      prod0.send(message);

      ClientMessage r1 = cons1.receive(1000);
      assertNotNull(r1);
      assertEquals(propVal, r1.getProperty(propKey));

      session0.close();
      session1.close();

      service0.stop();
      service1.stop();
      service2.stop();

      assertEquals(0, service0.getServer().getRemotingService().getConnections().size());
      assertEquals(0, service1.getServer().getRemotingService().getConnections().size());
      assertEquals(0, service2.getServer().getRemotingService().getConnections().size());
   }
   
   public void testAutomaticReconnectTryThenFailover() throws Exception
   {
      Map<String, Object> service0Params = new HashMap<String, Object>();
      MessagingService service0 = createMessagingService(0, service0Params);

      Map<String, Object> service1Params = new HashMap<String, Object>();
      MessagingService service1 = createMessagingService(1, service1Params);
            
      Map<String, Object> service2Params = new HashMap<String, Object>();
      MessagingService service2 = createMessagingService(2, service2Params, true);
      
      TransportConfiguration server0tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                    service0Params,
                                                                    "server0tc");

      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();

      TransportConfiguration server1tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                    service1Params,
                                                                    "server1tc");

      TransportConfiguration server2tc = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                    service2Params,
                                                                    "server2tc");
      
      connectors.put(server1tc.getName(), server1tc);
      
      connectors.put(server2tc.getName(), server2tc);
      
      service1.getServer().getConfiguration().setConnectorConfigurations(connectors);
      
      service1.getServer().getConfiguration().setBackupConnectorName(server2tc.getName());
      
      service2.getServer().getConfiguration().setBackup(true);
      
      service1.start();
      
      service2.start();
      
      log.info("Started service1 and service2");
                 
      service0.getServer().getConfiguration().setConnectorConfigurations(connectors);

      List<Pair<String, String>> connectorNames = new ArrayList<Pair<String, String>>();
      connectorNames.add(new Pair<String, String>(server1tc.getName(), server2tc.getName()));

      final SimpleString address1 = new SimpleString("testaddress");

      final long retryInterval = 50;
      final double retryIntervalMultiplier = 1d;
      final int retriesBeforeFailover = 3;
      final int maxRetriesAfterFailover = -1;
      
      final String flowName = "flow1";
      
      MessageFlowConfiguration ofconfig1 = new MessageFlowConfiguration(flowName,
                                                                        address1.toString(),
                                                                        null,
                                                                        true,
                                                                        1,
                                                                        -1,
                                                                        null,
                                                                        retryInterval,
                                                                        retryIntervalMultiplier,
                                                                        retriesBeforeFailover,
                                                                        maxRetriesAfterFailover,
                                                                        connectorNames);

      Set<MessageFlowConfiguration> ofconfigs = new HashSet<MessageFlowConfiguration>();
      ofconfigs.add(ofconfig1);

      service0.getServer().getConfiguration().setMessageFlowConfigurations(ofconfigs);

      service0.start();
      
      log.info("started service0");

      ClientSessionFactory csf0 = new ClientSessionFactoryImpl(server0tc);
      ClientSession session0 = csf0.createSession(false, true, true);

      ClientSessionFactory csf2 = new ClientSessionFactoryImpl(server2tc);
      ClientSession session2 = csf2.createSession(false, true, true);

      session0.createQueue(address1, address1, null, false, false, false);
      session2.createQueue(address1, address1, null, false, false, false);
      ClientProducer prod0 = session0.createProducer(address1);

      ClientConsumer cons1 = session2.createConsumer(address1);

      session2.start();

      ClientMessage message = session0.createClientMessage(false);
      SimpleString propKey = new SimpleString("propkey");
      SimpleString propVal = new SimpleString("propval");
      message.putStringProperty(propKey, propVal);
      message.getBody().flip();
      
      //Now we will simulate a failure of the message flow connection between server1 and server2
      //And prevent reconnection for a few tries, then it will failover
      MessageFlow flow = service0.getServer().getClusterManager().getMessageFlows().get(flowName);
      Forwarder forwarder = flow.getForwarders().iterator().next();
      RemotingConnection forwardingConnection = ((ForwarderImpl)forwarder).getForwardingConnection();
      InVMConnector.failOnCreateConnection = true;
      InVMConnector.numberOfFailures = retriesBeforeFailover;
      forwardingConnection.fail(new MessagingException(MessagingException.NOT_CONNECTED));

      prod0.send(message);

      ClientMessage r1 = cons1.receive(2000);
      assertNotNull(r1);
      assertEquals(propVal, r1.getProperty(propKey));

      session0.close();
      session2.close();

      service0.stop();
      service1.stop();
      service2.stop();

      assertEquals(0, service0.getServer().getRemotingService().getConnections().size());
      assertEquals(0, service1.getServer().getRemotingService().getConnections().size());
      assertEquals(0, service2.getServer().getRemotingService().getConnections().size());
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
      InVMConnector.resetFailures();
      
      assertEquals(0, InVMRegistry.instance.size());
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
