/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.integration.jms.server.management;

import static org.jboss.messaging.tests.util.RandomUtil.randomBoolean;
import static org.jboss.messaging.tests.util.RandomUtil.randomDouble;
import static org.jboss.messaging.tests.util.RandomUtil.randomInt;
import static org.jboss.messaging.tests.util.RandomUtil.randomLong;
import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.Topic;

import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.DiscoveryGroupConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.management.ObjectNames;
import org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.jms.server.JMSServerManager;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.jms.server.management.JMSServerControlMBean;
import org.jboss.messaging.tests.integration.management.ManagementControlHelper;
import org.jboss.messaging.tests.integration.management.ManagementTestBase;
import org.jboss.messaging.tests.unit.util.InVMContext;
import org.jboss.messaging.tests.util.RandomUtil;

/**
 * A JMSServerControlTest
 *
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 * 
 * Created 14 nov. 2008 13:35:10
 *
 *
 */
public class JMSServerControlTest extends ManagementTestBase
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(JMSServerControlTest.class);

   // Attributes ----------------------------------------------------

   protected InVMContext context;

   private JMSServerManagerImpl serverManager;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetVersion() throws Exception
   {
      JMSServerControlMBean control = createManagementControl();
      String version = control.getVersion();
      assertEquals(serverManager.getVersion(), version);
   }

   public void testCreateQueue() throws Exception
   {
      String queueJNDIBinding = randomString();
      String queueName = randomString();

      checkNoBinding(context, queueJNDIBinding);
      checkNoResource(ObjectNames.getJMSQueueObjectName(queueName));

      JMSServerControlMBean control = createManagementControl();
      control.createQueue(queueName, queueJNDIBinding);

      Object o = checkBinding(context, queueJNDIBinding);
      assertTrue(o instanceof Queue);
      Queue queue = (Queue)o;
      assertEquals(queueName, queue.getQueueName());
      checkResource(ObjectNames.getJMSQueueObjectName(queueName));

   }

   public void testDestroyQueue() throws Exception
   {
      String queueJNDIBinding = randomString();
      String queueName = randomString();

      checkNoBinding(context, queueJNDIBinding);
      checkNoResource(ObjectNames.getJMSQueueObjectName(queueName));

      JMSServerControlMBean control = createManagementControl();
      control.createQueue(queueName, queueJNDIBinding);

      checkBinding(context, queueJNDIBinding);
      checkResource(ObjectNames.getJMSQueueObjectName(queueName));

      control.destroyQueue(queueName);

      checkNoBinding(context, queueJNDIBinding);
      checkNoResource(ObjectNames.getJMSQueueObjectName(queueName));
   }

   public void testCreateTopic() throws Exception
   {
      String topicJNDIBinding = randomString();
      String topicName = randomString();

      checkNoBinding(context, topicJNDIBinding);
      checkNoResource(ObjectNames.getJMSTopicObjectName(topicName));

      JMSServerControlMBean control = createManagementControl();
      control.createTopic(topicName, topicJNDIBinding);

      Object o = checkBinding(context, topicJNDIBinding);
      assertTrue(o instanceof Topic);
      Topic topic = (Topic)o;
      assertEquals(topicName, topic.getTopicName());
      checkResource(ObjectNames.getJMSTopicObjectName(topicName));
   }

   public void testDestroyTopic() throws Exception
   {
      String topicJNDIBinding = randomString();
      String topicName = randomString();

      checkNoBinding(context, topicJNDIBinding);
      checkNoResource(ObjectNames.getJMSTopicObjectName(topicName));

      JMSServerControlMBean control = createManagementControl();
      control.createTopic(topicName, topicJNDIBinding);

      checkBinding(context, topicJNDIBinding);
      checkResource(ObjectNames.getJMSTopicObjectName(topicName));

      control.destroyTopic(topicName);

      checkNoBinding(context, topicJNDIBinding);
      checkNoResource(ObjectNames.getJMSTopicObjectName(topicName));
   }

   public void testCreateConnectionFactory_1() throws Exception
   {
      doCreateConnectionFactory(new ConnectionFactoryCreator()
      {
         public void createConnectionFactory(JMSServerControlMBean control, Object[] bindings) throws Exception
         {
            String cfName = randomString();
            TransportConfiguration tcLive = new TransportConfiguration(InVMConnectorFactory.class.getName());

            control.createConnectionFactory(cfName, tcLive.getFactoryClassName(), tcLive.getParams(), bindings);
         }         
      });
   }

   public void testCreateConnectionFactory_2() throws Exception
   {
      doCreateConnectionFactory(new ConnectionFactoryCreator()
      {
         public void createConnectionFactory(JMSServerControlMBean control, Object[] bindings) throws Exception
         {
            String cfName = randomString();
            String clientID = randomString();
            TransportConfiguration tcLive = new TransportConfiguration(InVMConnectorFactory.class.getName());

            control.createConnectionFactory(cfName, tcLive.getFactoryClassName(), tcLive.getParams(), clientID, bindings);
         }         
      });
   }

   public void testCreateConnectionFactory_3() throws Exception
   {
      doCreateConnectionFactory(new ConnectionFactoryCreator()
      {
         public void createConnectionFactory(JMSServerControlMBean control, Object[] bindings) throws Exception
         {
            String cfName = randomString();
            TransportConfiguration tcLive = new TransportConfiguration(InVMConnectorFactory.class.getName());
            TransportConfiguration tcBackup = new TransportConfiguration(InVMConnectorFactory.class.getName());

            control.createConnectionFactory(cfName, 
                                            tcLive.getFactoryClassName(),
                                            tcLive.getParams(),
                                            tcBackup.getFactoryClassName(),
                                            tcBackup.getParams(),
                                            bindings);
         }         
      });
   }
   
   public void testCreateConnectionFactory_4() throws Exception
   {
      doCreateConnectionFactory(new ConnectionFactoryCreator()
      {
         public void createConnectionFactory(JMSServerControlMBean control, Object[] bindings) throws Exception
         {
            String cfName = randomString();
            String clientID = randomString();
            TransportConfiguration tcLive = new TransportConfiguration(InVMConnectorFactory.class.getName());
            TransportConfiguration tcBackup = new TransportConfiguration(InVMConnectorFactory.class.getName());

            control.createConnectionFactory(cfName, 
                                            tcLive.getFactoryClassName(),
                                            tcLive.getParams(),
                                            tcBackup.getFactoryClassName(),
                                            tcBackup.getParams(),
                                            clientID,
                                            bindings);
         }         
      });
   }
   
   public void testCreateConnectionFactory_5() throws Exception
   {
      doCreateConnectionFactory(new ConnectionFactoryCreator()
      {
         public void createConnectionFactory(JMSServerControlMBean control, Object[] bindings) throws Exception
         {
            String cfName = randomString();
            TransportConfiguration tcLive = new TransportConfiguration(InVMConnectorFactory.class.getName());
            TransportConfiguration tcBackup = new TransportConfiguration(InVMConnectorFactory.class.getName());

            control.createConnectionFactory(cfName, 
                                            new Object[] {tcLive.getFactoryClassName()},
                                            new Object[] {tcLive.getParams()},
                                            new Object[] {tcBackup.getFactoryClassName()},
                                            new Object[] {tcBackup.getParams()},
                                            bindings);
         }         
      });
   }
      
   public void testCreateConnectionFactory_6() throws Exception
   {
      doCreateConnectionFactory(new ConnectionFactoryCreator()
      {
         public void createConnectionFactory(JMSServerControlMBean control, Object[] bindings) throws Exception
         {
            String cfName = randomString();
            String clientID = randomString();
            TransportConfiguration tcLive = new TransportConfiguration(InVMConnectorFactory.class.getName());
            TransportConfiguration tcBackup = new TransportConfiguration(InVMConnectorFactory.class.getName());

            control.createConnectionFactory(cfName, 
                                            new Object[] {tcLive.getFactoryClassName()},
                                            new Object[] {tcLive.getParams()},
                                            new Object[] {tcBackup.getFactoryClassName()},
                                            new Map[] {tcBackup.getParams()},
                                            clientID,
                                            bindings);
         }         
      });
   }
   
   public void testCreateConnectionFactory_7() throws Exception
   {
      doCreateConnectionFactory(new ConnectionFactoryCreator()
      {
         public void createConnectionFactory(JMSServerControlMBean control, Object[] bindings) throws Exception
         {
            String cfName = randomString();
            String clientID = randomString();
            TransportConfiguration tcLive = new TransportConfiguration(InVMConnectorFactory.class.getName());
            TransportConfiguration tcBackup = new TransportConfiguration(InVMConnectorFactory.class.getName());

            control.createConnectionFactory(cfName, 
                                            new Object[] {tcLive.getFactoryClassName()},
                                            new Object[] {tcLive.getParams()},
                                            new Object[] {tcBackup.getFactoryClassName()},
                                            new Object[] {tcBackup.getParams()},
                                            clientID,
                                            ClientSessionFactoryImpl.DEFAULT_PING_PERIOD,
                                            ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL,
                                            ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,
                                            ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS,
                                            ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                            ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE,
                                            ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE,
                                            ClientSessionFactoryImpl.DEFAULT_PRODUCER_WINDOW_SIZE,
                                            ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE,
                                            ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                            ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND,
                                            ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND,
                                            ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP,
                                            ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE,
                                            ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                            ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                            ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                            ClientSessionFactoryImpl.DEFAULT_USE_GLOBAL_POOLS,
                                            ClientSessionFactoryImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                                            ClientSessionFactoryImpl.DEFAULT_THREAD_POOL_MAX_SIZE,
                                            ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL,
                                            ClientSessionFactoryImpl.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                            ClientSessionFactoryImpl.DEFAULT_RECONNECT_ATTEMPTS,
                                            ClientSessionFactoryImpl.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN,
                                            bindings);
         }         
      });
   }

   public void _testCreateConnectionFactoryWithDiscoveryGroup() throws Exception
   {
      String[] cfJNDIBindings = new String[] {randomString(), randomString(), randomString()};
      String cfName = randomString();
      String clientID = randomString();

      // restart the server with a discovery group configuration
      serverManager.stop();
      startMessagingServer(8765);

      for (String cfJNDIBinding : cfJNDIBindings)
      {
         checkNoBinding(context, cfJNDIBinding);            
      }

      JMSServerControlMBean control = createManagementControl();

      control.createConnectionFactory(cfName, 
                                      "231.7.7.7",
                                      8765,
                                      clientID,
                                      cfJNDIBindings);

      for (String cfJNDIBinding : cfJNDIBindings)
      {
         Object o = checkBinding(context, cfJNDIBinding);
         assertTrue(o instanceof ConnectionFactory);
         ConnectionFactory cf = (ConnectionFactory)o;
         Connection connection = cf.createConnection();
         connection.close();
      }
   }
   
   public void testDestroyConnectionFactory() throws Exception
   {
      String[] cfJNDIBindings = new String[] {randomString(), randomString(), randomString()};
      String cfName = randomString();

      for (String cfJNDIBinding : cfJNDIBindings)
      {
         checkNoBinding(context, cfJNDIBinding);            
      }

      JMSServerControlMBean control = createManagementControl();

      TransportConfiguration tcLive = new TransportConfiguration(InVMConnectorFactory.class.getName());

      control.createConnectionFactory(cfName, tcLive.getFactoryClassName(), tcLive.getParams(), cfJNDIBindings);

      for (String cfJNDIBinding : cfJNDIBindings)
      {
         Object o = checkBinding(context, cfJNDIBinding);
         assertTrue(o instanceof ConnectionFactory);
         ConnectionFactory cf = (ConnectionFactory)o;
         Connection connection = cf.createConnection();
         connection.close();
      }
      
      control.destroyConnectionFactory(cfName);
      
      for (String cfJNDIBinding : cfJNDIBindings)
      {
         checkNoBinding(context, cfJNDIBinding);            
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
      MessagingServer server = Messaging.newMessagingServer(conf, mbeanServer, false);      

      context = new InVMContext();
      serverManager = new JMSServerManagerImpl(server);      
      serverManager.setContext(context);
      serverManager.start();
      serverManager.activated();
   }

   @Override
   protected void tearDown() throws Exception
   {
      serverManager.stop();

      super.tearDown();
   }

   protected JMSServerControlMBean createManagementControl() throws Exception
   {
      return ManagementControlHelper.createJMSServerControl(mbeanServer);
   }

   // Private -------------------------------------------------------

   private void doCreateConnectionFactory(ConnectionFactoryCreator creator) throws Exception
   {
      Object[] cfJNDIBindings = new Object[] { randomString(), randomString(), randomString() };
            
      String cfName = randomString();

      for (Object cfJNDIBinding : cfJNDIBindings)
      {
         checkNoBinding(context, cfJNDIBinding.toString());
      }
      checkNoResource(ObjectNames.getConnectionFactoryObjectName(cfName));

      JMSServerControlMBean control = createManagementControl();
     creator.createConnectionFactory(control, cfJNDIBindings);
      
      TransportConfiguration tcLive = new TransportConfiguration(InVMConnectorFactory.class.getName());
      
      control.createConnectionFactory(cfName, tcLive.getFactoryClassName(), tcLive.getParams(), cfJNDIBindings);

      for (Object cfJNDIBinding : cfJNDIBindings)
      {
         Object o = checkBinding(context, cfJNDIBinding.toString());
         assertTrue(o instanceof ConnectionFactory);
         ConnectionFactory cf = (ConnectionFactory)o;
         Connection connection = cf.createConnection();
         connection.close();
      }
      checkResource(ObjectNames.getConnectionFactoryObjectName(cfName));
   }

   private JMSServerManager startMessagingServer(int discoveryPort) throws Exception
   {
      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getDiscoveryGroupConfigurations()
          .put("discovery",
               new DiscoveryGroupConfiguration("discovery",
                                               "231.7.7.7",
                                               discoveryPort,
                                               ConfigurationImpl.DEFAULT_BROADCAST_REFRESH_TIMEOUT));
      MessagingServer server = Messaging.newMessagingServer(conf, mbeanServer, false);
      
      context = new InVMContext();
      JMSServerManagerImpl serverManager = new JMSServerManagerImpl(server);
      serverManager.setContext(context);
      serverManager.start();
      serverManager.activated();

      return serverManager;
   }
   
   // Inner classes -------------------------------------------------

   interface ConnectionFactoryCreator{
      void createConnectionFactory(JMSServerControlMBean control, Object[] bindings) throws Exception;
   }
   


}