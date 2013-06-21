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

package org.hornetq.tests.util;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSContext;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.naming.NamingException;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.unit.util.InVMNamingContext;
import org.junit.After;
import org.junit.Before;

/**
 * A JMSBaseTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class JMSTestBase extends ServiceTestBase
{

   protected HornetQServer server;

   protected JMSServerManagerImpl jmsServer;

   protected MBeanServer mbeanServer;

   protected ConnectionFactory cf;
   protected Connection conn;
   private final Set<JMSContext> contextSet = new HashSet<JMSContext>();

   protected InVMNamingContext namingContext;



   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // TestCase overrides -------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected boolean useSecurity()
   {
      return false;
   }

   protected boolean useJMX()
   {
      return true;
   }

   protected boolean usePersistence()
   {
      return false;
   }

   protected final JMSContext addContext(JMSContext context0)
   {
      contextSet.add(context0);
      return context0;
   }

   protected final JMSContext createContext()
   {
      return addContext(cf.createContext());
   }

   /**
    * @throws Exception
    * @throws NamingException
    */
   protected Queue createQueue(final String name) throws Exception, NamingException
   {
      return createQueue(false, name);
   }

   protected Topic createTopic(final String name) throws Exception, NamingException
   {
      return createTopic(false, name);
   }

   /**
    * @throws Exception
    * @throws NamingException
    */
   protected Queue createQueue(final boolean storeConfig, final String name) throws Exception, NamingException
   {
      jmsServer.createQueue(storeConfig, name, null, true, "/jms/" + name);

      return (Queue)namingContext.lookup("/jms/" + name);
   }

   protected Topic createTopic(final boolean storeConfig, final String name) throws Exception, NamingException
   {
      jmsServer.createTopic(storeConfig, name, "/jms/" + name);

      return (Topic)namingContext.lookup("/jms/" + name);
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      mbeanServer = MBeanServerFactory.createMBeanServer();

      Configuration conf = createDefaultConfig(true);
      conf.setSecurityEnabled(useSecurity());
      conf.getConnectorConfigurations().put("invm", new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      server = HornetQServers.newHornetQServer(conf, mbeanServer, usePersistence());
      addServer(server);
      jmsServer = new JMSServerManagerImpl(server);
      namingContext = new InVMNamingContext();
      jmsServer.setContext(namingContext);
      jmsServer.start();

      registerConnectionFactory();
   }

   @Override
   protected Configuration createDefaultConfig(boolean netty) throws Exception
   {
      Configuration conf = super.createDefaultConfig(netty);

      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);

      return conf;
   }

   protected void restartServer() throws Exception
   {
      namingContext = new InVMNamingContext();
      jmsServer.setContext(namingContext);
      jmsServer.start();
      jmsServer.activated();
      registerConnectionFactory();
   }

   protected void killServer() throws Exception
   {
      jmsServer.stop();
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      try
      {
         for (JMSContext jmsContext: contextSet) {
            jmsContext.close();
         }
      }
      catch (RuntimeException ignored)
      {
         // no-op
      }
      finally
      {
         contextSet.clear();
      }
      try
      {
         if (conn != null)
            conn.close();
      }
      catch (Exception e)
      {
         // no-op
      }
      namingContext.close();
      jmsServer.stop();
      server = null;

      jmsServer = null;

      namingContext = null;

      MBeanServerFactory.releaseMBeanServer(mbeanServer);

      mbeanServer = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   protected void registerConnectionFactory() throws Exception
   {
      List<TransportConfiguration> connectorConfigs = new ArrayList<TransportConfiguration>();
      connectorConfigs.add(new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      createCF(connectorConfigs, "/cf");

      cf = (ConnectionFactory)namingContext.lookup("/cf");
   }

   /**
    * @param connectorConfigs
    * @param jndiBindings
    * @throws Exception
    */
   protected void createCF(final List<TransportConfiguration> connectorConfigs, final String... jndiBindings) throws Exception
   {
      int retryInterval = 1000;
      double retryIntervalMultiplier = 1.0;
      int reconnectAttempts = -1;
      int callTimeout = 30000;

      jmsServer.createConnectionFactory("ManualReconnectionToSingleServerTest",
                                        false,
                                        JMSFactoryType.CF,
                                        registerConnectors(server, connectorConfigs),
                                        null,
                                        HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                        HornetQClient.DEFAULT_CONNECTION_TTL,
                                        callTimeout,
                                        HornetQClient.DEFAULT_CALL_FAILOVER_TIMEOUT,
                                        HornetQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT,
                                        HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                        HornetQClient.DEFAULT_COMPRESS_LARGE_MESSAGES,
                                        HornetQClient.DEFAULT_CONSUMER_WINDOW_SIZE,
                                        HornetQClient.DEFAULT_CONSUMER_MAX_RATE,
                                        HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                                        HornetQClient.DEFAULT_PRODUCER_WINDOW_SIZE,
                                        HornetQClient.DEFAULT_PRODUCER_MAX_RATE,
                                        HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                        HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND,
                                        HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND,
                                        HornetQClient.DEFAULT_AUTO_GROUP,
                                        HornetQClient.DEFAULT_PRE_ACKNOWLEDGE,
                                        HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                        HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                                        HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                                        HornetQClient.DEFAULT_USE_GLOBAL_POOLS,
                                        HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                                        HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                                        retryInterval,
                                        retryIntervalMultiplier,
                                        HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                        reconnectAttempts,
                                        HornetQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION,
                                        null,
                                        jndiBindings);
   }

}
