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

import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_RETRY_INTERVAL;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_WINDOW_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_THREAD_POOL_MAX_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_USE_GLOBAL_POOLS;

import java.util.ArrayList;
import java.util.List;

import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.naming.NamingException;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.integration.transports.netty.NettyAcceptorFactory;
import org.hornetq.integration.transports.netty.NettyConnectorFactory;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.unit.util.InVMContext;
import org.hornetq.utils.Pair;

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
   
   protected ConnectionFactory cf;

   protected InVMContext context;

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
   
   /**
    * @throws Exception
    * @throws NamingException
    */
   protected Queue createQueue(String name) throws Exception, NamingException
   {
      jmsServer.createQueue(name, "/jms/" + name, null, true);
      
      return (Queue)context.lookup("/jms/" + name);
   }
   

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = createDefaultConfig(false);
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);

      conf.getAcceptorConfigurations().add(new TransportConfiguration(NettyAcceptorFactory.class.getName()));

      server = HornetQ.newHornetQServer(conf, usePersistence());

      jmsServer = new JMSServerManagerImpl(server);
      context = new InVMContext();
      jmsServer.setContext(context);
      jmsServer.start();
      jmsServer.activated();

      registerConnectionFactory();
   }

   protected void restartServer() throws Exception
   {
      jmsServer.start();
      jmsServer.activated();
      context = new InVMContext();
      jmsServer.setContext(context);
      registerConnectionFactory();
   }

   protected void killServer() throws Exception
   {
      jmsServer.stop();
   }

   @Override
   protected void tearDown() throws Exception
   {

      jmsServer.stop();

      server.stop();

      context.close();

      server = null;

      jmsServer = null;

      context = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private void registerConnectionFactory() throws Exception
   {
      List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();
      connectorConfigs.add(new Pair<TransportConfiguration, TransportConfiguration>(new TransportConfiguration(NettyConnectorFactory.class.getName()),
                                                                                    null));

      List<String> jndiBindings = new ArrayList<String>();
      jndiBindings.add("/cf");

      createCF(connectorConfigs, jndiBindings);
      
      cf = (ConnectionFactory)context.lookup("/cf");
      
   }

   /**
    * @param connectorConfigs
    * @param jndiBindings
    * @throws Exception
    */
   protected void createCF(List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                         List<String> jndiBindings) throws Exception
   {
      int retryInterval = 1000;
      double retryIntervalMultiplier = 1.0;
      int reconnectAttempts = -1;
      boolean failoverOnServerShutdown = true;
      int callTimeout = 30000;

      jmsServer.createConnectionFactory("ManualReconnectionToSingleServerTest",
                                            connectorConfigs,
                                            null,
                                            DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                            DEFAULT_CONNECTION_TTL,
                                            callTimeout,                                            
                                            DEFAULT_CACHE_LARGE_MESSAGE_CLIENT,
                                            DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                            DEFAULT_CONSUMER_WINDOW_SIZE,
                                            DEFAULT_CONSUMER_MAX_RATE,
                                            DEFAULT_PRODUCER_WINDOW_SIZE,
                                            DEFAULT_PRODUCER_MAX_RATE,
                                            DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                            DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                                            DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                            DEFAULT_AUTO_GROUP,
                                            DEFAULT_PRE_ACKNOWLEDGE,
                                            DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                            DEFAULT_ACK_BATCH_SIZE,
                                            DEFAULT_ACK_BATCH_SIZE,
                                            DEFAULT_USE_GLOBAL_POOLS,
                                            DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                                            DEFAULT_THREAD_POOL_MAX_SIZE,
                                            retryInterval,
                                            retryIntervalMultiplier,
                                            DEFAULT_MAX_RETRY_INTERVAL,
                                            reconnectAttempts,
                                            failoverOnServerShutdown,
                                            jndiBindings);
   }

}
