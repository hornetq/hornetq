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

package org.hornetq.tests.integration.jms.server.management;

import static org.hornetq.tests.util.RandomUtil.randomString;

import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.Topic;

import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.cluster.DiscoveryGroupConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.management.ObjectNameBuilder;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.jms.server.management.JMSServerControl;
import org.hornetq.tests.integration.management.ManagementControlHelper;
import org.hornetq.tests.integration.management.ManagementTestBase;
import org.hornetq.tests.unit.util.InVMContext;

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
   private HornetQServer server;
   
   private JMSServerManagerImpl serverManager;

   // Static --------------------------------------------------------

   private static String toCSV(Object[] objects)
   {
      String str = "";
      for (int i = 0; i < objects.length; i++)
      {
         if (i > 0)
         {
            str += ", ";
         }
         str += objects[i];
      }
      return str;
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testGetVersion() throws Exception
   {
      JMSServerControl control = createManagementControl();
      String version = control.getVersion();
      assertEquals(serverManager.getVersion(), version);
   }

   public void testCreateQueue() throws Exception
   {
      String queueJNDIBinding = randomString();
      String queueName = randomString();

      checkNoBinding(context, queueJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      JMSServerControl control = createManagementControl();
      control.createQueue(queueName, queueJNDIBinding);

      Object o = checkBinding(context, queueJNDIBinding);
      assertTrue(o instanceof Queue);
      Queue queue = (Queue)o;
      assertEquals(queueName, queue.getQueueName());
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

   }

   public void testDestroyQueue() throws Exception
   {
      String queueJNDIBinding = randomString();
      String queueName = randomString();

      checkNoBinding(context, queueJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      JMSServerControl control = createManagementControl();
      control.createQueue(queueName, queueJNDIBinding);

      checkBinding(context, queueJNDIBinding);
      checkResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));

      control.destroyQueue(queueName);

      checkNoBinding(context, queueJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSQueueObjectName(queueName));
   }
   
   public void testGetQueueNames() throws Exception
   {
      String queueJNDIBinding = randomString();
      String queueName = randomString();

      JMSServerControl control = createManagementControl();
      assertEquals(0, control.getQueueNames().length);

      control.createQueue(queueName, queueJNDIBinding);

      String[] names = control.getQueueNames();
      assertEquals(1, names.length);
      assertEquals(queueName, names[0]);

      control.destroyQueue(queueName);

      assertEquals(0, control.getQueueNames().length);
   }

   public void testCreateTopic() throws Exception
   {
      String topicJNDIBinding = randomString();
      String topicName = randomString();

      checkNoBinding(context, topicJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));

      JMSServerControl control = createManagementControl();
      control.createTopic(topicName, topicJNDIBinding);

      Object o = checkBinding(context, topicJNDIBinding);
      assertTrue(o instanceof Topic);
      Topic topic = (Topic)o;
      assertEquals(topicName, topic.getTopicName());
      checkResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));
   }

   public void testDestroyTopic() throws Exception
   {
      String topicJNDIBinding = randomString();
      String topicName = randomString();

      checkNoBinding(context, topicJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));

      JMSServerControl control = createManagementControl();
      control.createTopic(topicName, topicJNDIBinding);

      checkBinding(context, topicJNDIBinding);
      checkResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));

      control.destroyTopic(topicName);

      checkNoBinding(context, topicJNDIBinding);
      checkNoResource(ObjectNameBuilder.DEFAULT.getJMSTopicObjectName(topicName));
   }
   
   public void testGetTopicNames() throws Exception
   {
      String topicJNDIBinding = randomString();
      String topicName = randomString();

      JMSServerControl control = createManagementControl();
      assertEquals(0, control.getTopicNames().length);

      control.createTopic(topicName, topicJNDIBinding);

      String[] names = control.getTopicNames();
      assertEquals(1, names.length);
      assertEquals(topicName, names[0]);

      control.destroyTopic(topicName);

      assertEquals(0, control.getTopicNames().length);
   }

   public void testCreateConnectionFactory_1() throws Exception
   {
      doCreateConnectionFactory(new ConnectionFactoryCreator()
      {
         public void createConnectionFactory(JMSServerControl control, String cfName, Object[] bindings) throws Exception
         {
            TransportConfiguration tcLive = new TransportConfiguration(InVMConnectorFactory.class.getName());

            control.createConnectionFactory(cfName, tcLive.getFactoryClassName(), tcLive.getParams(), bindings);
         }
      });
   }

   public void testCreateConnectionFactory_1b() throws Exception
   {
      doCreateConnectionFactory(new ConnectionFactoryCreator()
      {
         public void createConnectionFactory(JMSServerControl control, String cfName, Object[] bindings) throws Exception
         {
            String jndiBindings = toCSV(bindings);
            String params = "\"key1\"=1, \"key2\"=false, \"key3\"=\"value3\"";

            control.createConnectionFactory(cfName, InVMConnectorFactory.class.getName(), params, jndiBindings);
         }
      });
   }

   public void testCreateConnectionFactory_2() throws Exception
   {
      doCreateConnectionFactory(new ConnectionFactoryCreator()
      {
         public void createConnectionFactory(JMSServerControl control, String cfName, Object[] bindings) throws Exception
         {
            String clientID = randomString();
            TransportConfiguration tcLive = new TransportConfiguration(InVMConnectorFactory.class.getName());

            control.createConnectionFactory(cfName,
                                            tcLive.getFactoryClassName(),
                                            tcLive.getParams(),
                                            clientID,
                                            bindings);
         }
      });
   }

   public void testCreateConnectionFactory_2b() throws Exception
   {
      doCreateConnectionFactory(new ConnectionFactoryCreator()
      {
         public void createConnectionFactory(JMSServerControl control, String cfName, Object[] bindings) throws Exception
         {
            String clientID = randomString();
            String jndiBindings = toCSV(bindings);
            String params = "\"key1\"=1, \"key2\"=false, \"key3\"=\"value3\"";

            control.createConnectionFactory(cfName,
                                            InVMConnectorFactory.class.getName(),
                                            params,
                                            clientID,
                                            jndiBindings);
         }
      });
   }

   public void testCreateConnectionFactory_3() throws Exception
   {
      doCreateConnectionFactory(new ConnectionFactoryCreator()
      {
         public void createConnectionFactory(JMSServerControl control, String cfName, Object[] bindings) throws Exception
         {
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

   public void testCreateConnectionFactory_3b() throws Exception
   {
      doCreateConnectionFactory(new ConnectionFactoryCreator()
      {
         public void createConnectionFactory(JMSServerControl control, String cfName, Object[] bindings) throws Exception
         {
            String jndiBindings = toCSV(bindings);
            String params = "\"key1\"=1, \"key2\"=false, \"key3\"=\"value3\"";

            control.createConnectionFactory(cfName,
                                            InVMConnectorFactory.class.getName(),
                                            params,
                                            InVMConnectorFactory.class.getName(),
                                            params,
                                            jndiBindings);
         }
      });
   }

   // with 2 live servers & no backups
   public void testCreateConnectionFactory_3c() throws Exception
   {
      doCreateConnectionFactory(new ConnectionFactoryCreator()
      {
         public void createConnectionFactory(JMSServerControl control, String cfName, Object[] bindings) throws Exception
         {
            String jndiBindings = toCSV(bindings);
            String params = String.format("{%s=%s}, {%s=%s}",
                                          TransportConstants.SERVER_ID_PROP_NAME,
                                          0,
                                          TransportConstants.SERVER_ID_PROP_NAME,
                                          1);
            
            control.createConnectionFactory(cfName, 
                                            InVMConnectorFactory.class.getName() + ", " + InVMConnectorFactory.class.getName(),
                                            params,
                                            "",
                                            "",
                                            jndiBindings);
         }
      });
   }

   public void testCreateConnectionFactory_4() throws Exception
   {
      doCreateConnectionFactory(new ConnectionFactoryCreator()
      {
         public void createConnectionFactory(JMSServerControl control, String cfName, Object[] bindings) throws Exception
         {
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

   // with 1 live and 1 backup 
   public void testCreateConnectionFactory_4b() throws Exception
   {
      doCreateConnectionFactory(new ConnectionFactoryCreator()
      {
         public void createConnectionFactory(JMSServerControl control, String cfName, Object[] bindings) throws Exception
         {
            String clientID = randomString();
            String jndiBindings = toCSV(bindings);

            control.createConnectionFactory(cfName,
                                            InVMConnectorFactory.class.getName(),
                                            TransportConstants.SERVER_ID_PROP_NAME + "=0",
                                            InVMConnectorFactory.class.getName(),
                                            TransportConstants.SERVER_ID_PROP_NAME + "=1",
                                            clientID,
                                            jndiBindings);
         }
      });
   }

   public void testCreateConnectionFactory_5() throws Exception
   {
      doCreateConnectionFactory(new ConnectionFactoryCreator()
      {
         public void createConnectionFactory(JMSServerControl control, String cfName, Object[] bindings) throws Exception
         {
            TransportConfiguration tcLive = new TransportConfiguration(InVMConnectorFactory.class.getName());
            TransportConfiguration tcBackup = new TransportConfiguration(InVMConnectorFactory.class.getName());

            control.createConnectionFactory(cfName,
                                            new Object[] { tcLive.getFactoryClassName() },
                                            new Object[] { tcLive.getParams() },
                                            new Object[] { tcBackup.getFactoryClassName() },
                                            new Object[] { tcBackup.getParams() },
                                            bindings);
         }
      });
   }

   public void testCreateConnectionFactory_6() throws Exception
   {
      doCreateConnectionFactory(new ConnectionFactoryCreator()
      {
         public void createConnectionFactory(JMSServerControl control, String cfName, Object[] bindings) throws Exception
         {
            String clientID = randomString();
            TransportConfiguration tcLive = new TransportConfiguration(InVMConnectorFactory.class.getName());
            TransportConfiguration tcBackup = new TransportConfiguration(InVMConnectorFactory.class.getName());

            control.createConnectionFactory(cfName,
                                            new Object[] { tcLive.getFactoryClassName() },
                                            new Object[] { tcLive.getParams() },
                                            new Object[] { tcBackup.getFactoryClassName() },
                                            new Map[] { tcBackup.getParams() },
                                            clientID,
                                            bindings);
         }
      });
   }

   public void testCreateConnectionFactory_7() throws Exception
   {
      doCreateConnectionFactory(new ConnectionFactoryCreator()
      {
         public void createConnectionFactory(JMSServerControl control, String cfName, Object[] bindings) throws Exception
         {
            String clientID = randomString();
            TransportConfiguration tcLive = new TransportConfiguration(InVMConnectorFactory.class.getName());
            TransportConfiguration tcBackup = new TransportConfiguration(InVMConnectorFactory.class.getName());

            control.createConnectionFactory(cfName,
                                            new Object[] { tcLive.getFactoryClassName() },
                                            new Object[] { tcLive.getParams() },
                                            new Object[] { tcBackup.getFactoryClassName() },
                                            new Object[] { tcBackup.getParams() },
                                            clientID,
                                            ClientSessionFactoryImpl.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                            ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL,
                                            ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,                                            
                                            ClientSessionFactoryImpl.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT,
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
                                            ClientSessionFactoryImpl.DEFAULT_MAX_RETRY_INTERVAL,
                                            ClientSessionFactoryImpl.DEFAULT_RECONNECT_ATTEMPTS,
                                            ClientSessionFactoryImpl.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN,
                                            bindings);
         }
      });
   }
   
   public void testCreateConnectionFactory_7b() throws Exception
   {
      doCreateConnectionFactory(new ConnectionFactoryCreator()
      {
         public void createConnectionFactory(JMSServerControl control, String cfName, Object[] bindings) throws Exception
         {
            String clientID = randomString();
            String jndiBindings = toCSV(bindings);

            control.createConnectionFactory(cfName,
                                            InVMConnectorFactory.class.getName(),
                                            "",
                                            InVMConnectorFactory.class.getName(),
                                            TransportConstants.SERVER_ID_PROP_NAME + "=1",
                                            clientID,
                                            ClientSessionFactoryImpl.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                            ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL,
                                            ClientSessionFactoryImpl.DEFAULT_CALL_TIMEOUT,                                            
                                            ClientSessionFactoryImpl.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT,
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
                                            ClientSessionFactoryImpl.DEFAULT_MAX_RETRY_INTERVAL,
                                            ClientSessionFactoryImpl.DEFAULT_RECONNECT_ATTEMPTS,
                                            ClientSessionFactoryImpl.DEFAULT_FAILOVER_ON_SERVER_SHUTDOWN,
                                            jndiBindings);
         }
      });
   }

   public void _testCreateConnectionFactoryWithDiscoveryGroup() throws Exception
   {
      String[] cfJNDIBindings = new String[] { randomString(), randomString(), randomString() };
      String cfName = randomString();
      String clientID = randomString();

      // restart the server with a discovery group configuration
      serverManager.stop();
      startHornetQServer(8765);

      for (String cfJNDIBinding : cfJNDIBindings)
      {
         checkNoBinding(context, cfJNDIBinding);
      }

      JMSServerControl control = createManagementControl();

      control.createConnectionFactory(cfName, "231.7.7.7", 8765, clientID, cfJNDIBindings);

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
      String[] cfJNDIBindings = new String[] { randomString(), randomString(), randomString() };
      String cfName = randomString();

      for (String cfJNDIBinding : cfJNDIBindings)
      {
         checkNoBinding(context, cfJNDIBinding);
      }

      JMSServerControl control = createManagementControl();

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
   
   public void testGetConnectionFactoryNames() throws Exception
   {
      String cfBinding = randomString();
      String cfName = randomString();

      JMSServerControl control = createManagementControl();
      assertEquals(0, control.getConnectionFactoryNames().length);
      
      TransportConfiguration tcLive = new TransportConfiguration(InVMConnectorFactory.class.getName());
      control.createConnectionFactory(cfName, tcLive.getFactoryClassName(), tcLive.getParams(), new String[] {cfBinding});

      String[] cfNames = control.getConnectionFactoryNames();
      assertEquals(1, cfNames.length);
      assertEquals(cfName, cfNames[0]);
      
      control.destroyConnectionFactory(cfName);
      assertEquals(0, control.getConnectionFactoryNames().length);
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
      server = HornetQ.newHornetQServer(conf, mbeanServer, false);

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
      
      server.stop();
      
      serverManager = null;
      
      server = null;

      super.tearDown();
   }

   protected JMSServerControl createManagementControl() throws Exception
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
      checkNoResource(ObjectNameBuilder.DEFAULT.getConnectionFactoryObjectName(cfName));

      JMSServerControl control = createManagementControl();
      creator.createConnectionFactory(control, cfName, cfJNDIBindings);

      for (Object cfJNDIBinding : cfJNDIBindings)
      {
         Object o = checkBinding(context, cfJNDIBinding.toString());
         assertTrue(o instanceof ConnectionFactory);
         ConnectionFactory cf = (ConnectionFactory)o;
         Connection connection = cf.createConnection();
         connection.close();
      }
      checkResource(ObjectNameBuilder.DEFAULT.getConnectionFactoryObjectName(cfName));
   }

   private JMSServerManager startHornetQServer(int discoveryPort) throws Exception
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
      HornetQServer server = HornetQ.newHornetQServer(conf, mbeanServer, false);

      context = new InVMContext();
      JMSServerManagerImpl serverManager = new JMSServerManagerImpl(server);
      serverManager.setContext(context);
      serverManager.start();
      serverManager.activated();

      return serverManager;
   }

   // Inner classes -------------------------------------------------

   interface ConnectionFactoryCreator
   {
      void createConnectionFactory(JMSServerControl control, String cfName, Object[] bindings) throws Exception;
   }

}