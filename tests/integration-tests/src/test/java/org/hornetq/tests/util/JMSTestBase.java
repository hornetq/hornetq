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
import java.util.Random;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSException;
import javax.jms.JMSProducer;
import javax.jms.JMSRuntimeException;
import javax.jms.Message;
import javax.jms.Queue;
import javax.jms.Topic;
import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;
import javax.naming.NamingException;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;
import org.hornetq.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.unit.util.InVMNamingContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

/**
 * A JMSBaseTest
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class JMSTestBase extends ServiceTestBase
{

   protected HornetQServer server;

   protected JMSServerManagerImpl jmsServer;

   protected MBeanServer mbeanServer;

   protected ConnectionFactory cf;
   protected Connection conn;
   private final Set<JMSContext> contextSet = new HashSet<JMSContext>();
   private final Random random = new Random();
   protected InVMNamingContext namingContext;

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

   protected final JMSContext createContext(int sessionMode)
   {
      return addContext(cf.createContext(null, null, sessionMode));
   }

   /**
    * @throws Exception
    * @throws NamingException
    */
   protected Queue createQueue(final String queueName) throws Exception, NamingException
   {
      return createQueue(false, queueName);
   }

   protected Topic createTopic(final String topicName) throws Exception, NamingException
   {
      return createTopic(false, topicName);
   }

   /**
    * @throws Exception
    * @throws NamingException
    */
   protected Queue createQueue(final boolean storeConfig, final String queueName) throws Exception, NamingException
   {
      jmsServer.createQueue(storeConfig, queueName, null, true, "/jms/" + queueName);

      return (Queue)namingContext.lookup("/jms/" + queueName);
   }

   protected Topic createTopic(final boolean storeConfig, final String topicName) throws Exception, NamingException
   {
      jmsServer.createTopic(storeConfig, topicName, "/jms/" + topicName);

      return (Topic)namingContext.lookup("/jms/" + topicName);
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
         for (JMSContext jmsContext : contextSet)
         {
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
      cf = null;
      jmsServer = null;

      namingContext = null;

      MBeanServerFactory.releaseMBeanServer(mbeanServer);

      mbeanServer = null;

      super.tearDown();
   }

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
   protected void
            createCF(final List<TransportConfiguration> connectorConfigs, final String... jndiBindings)
                                                                                                       throws Exception
   {
      final int retryInterval = 1000;
      final double retryIntervalMultiplier = 1.0;
      final int reconnectAttempts = -1;
      final int callTimeout = 30000;
      final boolean ha = false;
      List<String> connectorNames = registerConnectors(server, connectorConfigs);

      ConnectionFactoryConfiguration configuration =
               new ConnectionFactoryConfigurationImpl(name.getMethodName(), ha, connectorNames);
      configuration.setRetryInterval(retryInterval);
      configuration.setRetryIntervalMultiplier(retryIntervalMultiplier);
      configuration.setCallTimeout(callTimeout);
      configuration.setReconnectAttempts(reconnectAttempts);
      testCaseCfExtraConfig(configuration);
      jmsServer.createConnectionFactory(false, configuration, jndiBindings);
   }

   /**
    * Allows test-cases to set their own options to the {@link ConnectionFactoryConfiguration}
    * @param configuration
    */
   protected void testCaseCfExtraConfig(ConnectionFactoryConfiguration configuration)
   {
      // no-op

   }

   protected final void sendMessages(JMSContext context, JMSProducer producer, Queue queue, final int total)
   {
      try
      {
         for (int j = 0; j < total; j++)
         {
            StringBuilder sb = new StringBuilder();
            for (int m = 0; m < 200; m++)
            {
               sb.append(random.nextLong());
            }
            Message msg = context.createTextMessage(sb.toString());
            msg.setIntProperty("counter", j);
            producer.send(queue, msg);
         }
      }
      catch (JMSException cause)
      {
         throw new JMSRuntimeException(cause.getMessage(), cause.getErrorCode(), cause);
      }
   }

   protected final void receiveMessages(JMSConsumer consumer, final int start, final int msgCount, final boolean ack)

   {
      try
      {
         for (int i = start; i < msgCount; i++)
         {
            Message message = consumer.receive(100);
            Assert.assertNotNull("Expecting a message " + i, message);
            final int actual = message.getIntProperty("counter");
            Assert.assertEquals("expected=" + i + ". Got: property['counter']=" + actual, i, actual);
            if (ack)
               message.acknowledge();
         }
      }
      catch (JMSException cause)
      {
         throw new JMSRuntimeException(cause.getMessage(), cause.getErrorCode(), cause);
      }
   }
}
