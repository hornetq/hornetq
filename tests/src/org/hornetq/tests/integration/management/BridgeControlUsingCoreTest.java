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

package org.hornetq.tests.integration.management;

import static org.hornetq.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME;
import static org.hornetq.tests.util.RandomUtil.randomBoolean;
import static org.hornetq.tests.util.RandomUtil.randomDouble;
import static org.hornetq.tests.util.RandomUtil.randomPositiveInt;
import static org.hornetq.tests.util.RandomUtil.randomPositiveLong;
import static org.hornetq.tests.util.RandomUtil.randomString;

import java.util.HashMap;
import java.util.Map;

import javax.management.MBeanServerFactory;

import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.cluster.BridgeConfiguration;
import org.hornetq.core.config.cluster.QueueConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.management.ObjectNameBuilder;
import org.hornetq.core.management.ResourceNames;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.utils.Pair;

/**
 * A BridgeControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 11 dec. 2008 17:38:58
 *
 */
public class BridgeControlUsingCoreTest extends ManagementTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private HornetQServer server_0;

   private BridgeConfiguration bridgeConfig;

   private HornetQServer server_1;
   
   private ClientSession session;

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAttributes() throws Exception
   {
      checkResource(ObjectNameBuilder.DEFAULT.getBridgeObjectName(bridgeConfig.getName()));
      CoreMessagingProxy proxy = createProxy(bridgeConfig.getName());

      assertEquals(bridgeConfig.getName(), (String)proxy.retrieveAttributeValue("name"));
      assertEquals(bridgeConfig.getDiscoveryGroupName(), (String)proxy.retrieveAttributeValue("discoveryGroupName"));
      assertEquals(bridgeConfig.getQueueName(), (String)proxy.retrieveAttributeValue("queueName"));
      assertEquals(bridgeConfig.getForwardingAddress(), (String)proxy.retrieveAttributeValue("forwardingAddress"));
      assertEquals(bridgeConfig.getFilterString(), (String)proxy.retrieveAttributeValue("filterString"));
      assertEquals(bridgeConfig.getRetryInterval(), ((Long)proxy.retrieveAttributeValue("retryInterval")).longValue());
      assertEquals(bridgeConfig.getRetryIntervalMultiplier(), (Double)proxy.retrieveAttributeValue("retryIntervalMultiplier"));
      assertEquals(bridgeConfig.getReconnectAttempts(), ((Integer)proxy.retrieveAttributeValue("reconnectAttempts")).intValue());
      assertEquals(bridgeConfig.isFailoverOnServerShutdown(), ((Boolean)proxy.retrieveAttributeValue("failoverOnServerShutdown")).booleanValue());
      assertEquals(bridgeConfig.isUseDuplicateDetection(), ((Boolean)proxy.retrieveAttributeValue("useDuplicateDetection")).booleanValue());

      Object[] data = (Object[])proxy.retrieveAttributeValue("connectorPair");
      assertEquals(bridgeConfig.getConnectorPair().a, data[0]);
      assertEquals(bridgeConfig.getConnectorPair().b, data[1]);

      assertTrue((Boolean)proxy.retrieveAttributeValue("started"));
   }

   public void testStartStop() throws Exception
   {
      checkResource(ObjectNameBuilder.DEFAULT.getBridgeObjectName(bridgeConfig.getName()));
      CoreMessagingProxy proxy = createProxy(bridgeConfig.getName());

      // started by the server
      assertTrue((Boolean)proxy.retrieveAttributeValue("Started"));

      proxy.invokeOperation("stop");      
      assertFalse((Boolean)proxy.retrieveAttributeValue("Started"));

      proxy.invokeOperation("start");      
      assertTrue((Boolean)proxy.retrieveAttributeValue("Started"));
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Map<String, Object> acceptorParams = new HashMap<String, Object>();
      acceptorParams.put(SERVER_ID_PROP_NAME, 1);
      TransportConfiguration acceptorConfig = new TransportConfiguration(InVMAcceptorFactory.class.getName(),
                                                                         acceptorParams,
                                                                         randomString());

      TransportConfiguration connectorConfig = new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                          acceptorParams,
                                                                          randomString());

      QueueConfiguration sourceQueueConfig = new QueueConfiguration(randomString(), randomString(), null, false);
      QueueConfiguration targetQueueConfig = new QueueConfiguration(randomString(), randomString(), null, false);
      Pair<String, String> connectorPair = new Pair<String, String>(connectorConfig.getName(), null);
      bridgeConfig = new BridgeConfiguration(randomString(),
                                             sourceQueueConfig.getName(),
                                             targetQueueConfig.getAddress(),
                                             null,
                                             null,
                                             randomPositiveLong(),
                                             randomDouble(),
                                             randomPositiveInt(),
                                             randomBoolean(),
                                             randomBoolean(),
                                             connectorPair);

      Configuration conf_1 = new ConfigurationImpl();
      conf_1.setSecurityEnabled(false);
      conf_1.setJMXManagementEnabled(true);
      conf_1.setClustered(true);
      conf_1.getAcceptorConfigurations().add(acceptorConfig);
      conf_1.getQueueConfigurations().add(targetQueueConfig);

      Configuration conf_0 = new ConfigurationImpl();
      conf_0.setSecurityEnabled(false);
      conf_0.setJMXManagementEnabled(true);
      conf_0.setClustered(true);
      conf_0.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      conf_0.getConnectorConfigurations().put(connectorConfig.getName(), connectorConfig);
      conf_0.getQueueConfigurations().add(sourceQueueConfig);
      conf_0.getBridgeConfigurations().add(bridgeConfig);

      server_1 = HornetQ.newHornetQServer(conf_1, MBeanServerFactory.createMBeanServer(), false);
      server_1.start();

      server_0 = HornetQ.newHornetQServer(conf_0, mbeanServer, false);
      server_0.start();
      
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      session = sf.createSession(false, true, true);
      session.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      session.close();
      server_0.stop();
      server_1.stop();

      session = null;
      
      server_0 = null;
      
      server_1 = null;
      
      
      super.tearDown();
   }
   
   protected CoreMessagingProxy createProxy(final String name) throws Exception
   {
      CoreMessagingProxy proxy = new CoreMessagingProxy(session,
                                                       ResourceNames.CORE_BRIDGE + name);
      
      return proxy;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}