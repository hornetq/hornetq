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
package org.hornetq.tests.integration.jms;

import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_WINDOW_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_THREAD_POOL_MAX_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_USE_GLOBAL_POOLS;

import java.util.ArrayList;
import java.util.List;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.integration.transports.netty.NettyAcceptorFactory;
import org.hornetq.integration.transports.netty.NettyConnectorFactory;
import org.hornetq.jms.HornetQTopic;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.unit.util.InVMContext;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.Pair;

/**
 * 
 * A FloodServerTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class FloodServerTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(FloodServerTest.class);

   private HornetQServer server;

   private JMSServerManagerImpl serverManager;

   private InVMContext initialContext;

   private final String topicName = "my-topic";

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // TestCase overrides -------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setJMXManagementEnabled(true);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(NettyAcceptorFactory.class.getName()));
      server = HornetQ.newHornetQServer(conf, false);
      server.start();

      serverManager = new JMSServerManagerImpl(server);
      initialContext = new InVMContext();
      serverManager.setContext(initialContext);
      serverManager.start();
      serverManager.activated();

      serverManager.createTopic(topicName, topicName);
      registerConnectionFactory();
   }

   @Override
   protected void tearDown() throws Exception
   {
      
      serverManager.stop();
      
      server.stop();

      server = null;

      serverManager = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private void registerConnectionFactory() throws Exception
   {
      int retryInterval = 1000;
      double retryIntervalMultiplier = 1.0;
      int reconnectAttempts = -1;
      boolean failoverOnServerShutdown = true;
      long callTimeout = 30000;

      List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();
      connectorConfigs.add(new Pair<TransportConfiguration, TransportConfiguration>(new TransportConfiguration(NettyConnectorFactory.class.getName()),
                                                                                    null));

      List<String> jndiBindings = new ArrayList<String>();
      jndiBindings.add("/cf");

      serverManager.createConnectionFactory("ManualReconnectionToSingleServerTest",
                                            connectorConfigs,
                                            null,
                                            1000,
                                            DEFAULT_CONNECTION_TTL,
                                            callTimeout,                                           
                                            DEFAULT_CACHE_LARGE_MESSAGE_CLIENT,
                                            DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                            DEFAULT_CONSUMER_WINDOW_SIZE,
                                            DEFAULT_CONSUMER_MAX_RATE,
                                            DEFAULT_PRODUCER_WINDOW_SIZE,
                                            DEFAULT_PRODUCER_MAX_RATE,
                                            false,
                                            false,
                                            false,
                                            DEFAULT_AUTO_GROUP,
                                            false,
                                            DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                            DEFAULT_ACK_BATCH_SIZE,
                                            DEFAULT_ACK_BATCH_SIZE,
                                            DEFAULT_USE_GLOBAL_POOLS,
                                            DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                                            DEFAULT_THREAD_POOL_MAX_SIZE,
                                            retryInterval,
                                            retryIntervalMultiplier,
                                            1000,
                                            reconnectAttempts,
                                            failoverOnServerShutdown,
                                            jndiBindings);
   }

      
    public void testFoo()
    {
    }

   public void _testFlood() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/cf");

      final int numProducers = 20;

      final int numConsumers = 20;

      final int numMessages = 10000;

      ProducerThread[] producers = new ProducerThread[numProducers];

      for (int i = 0; i < numProducers; i++)
      {
         producers[i] = new ProducerThread(cf, numMessages);
      }

      ConsumerThread[] consumers = new ConsumerThread[numConsumers];

      for (int i = 0; i < numConsumers; i++)
      {
         consumers[i] = new ConsumerThread(cf, numMessages);
      }

      for (int i = 0; i < numConsumers; i++)
      {
         consumers[i].start();
      }

      for (int i = 0; i < numProducers; i++)
      {
         producers[i].start();
      }

      for (int i = 0; i < numConsumers; i++)
      {
         consumers[i].join();
      }

      for (int i = 0; i < numProducers; i++)
      {
         producers[i].join();
      }

   }

   class ProducerThread extends Thread
   {
      private Connection connection;

      private Session session;

      private MessageProducer producer;

      private int numMessages;

      ProducerThread(ConnectionFactory cf, int numMessages) throws Exception
      {
         connection = cf.createConnection();

         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         producer = session.createProducer(new HornetQTopic("my-topic"));

         producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         this.numMessages = numMessages;
      }

      public void run()
      {
         try
         {
            byte[] bytes = new byte[1000];

            BytesMessage message = session.createBytesMessage();

            message.writeBytes(bytes);

            for (int i = 0; i < numMessages; i++)
            {
               producer.send(message);

//               if (i % 1000 == 0)
//               {
//                  log.info("Producer " + this + " sent " + i);
//               }
            }

            connection.close();
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }
   }

   class ConsumerThread extends Thread
   {
      private Connection connection;

      private Session session;

      private MessageConsumer consumer;

      private int numMessages;

      ConsumerThread(ConnectionFactory cf, int numMessages) throws Exception
      {
         connection = cf.createConnection();

         connection.start();

         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         consumer = session.createConsumer(new HornetQTopic("my-topic"));

         this.numMessages = numMessages;
      }

      public void run()
      {
         try
         {
            for (int i = 0; i < numMessages; i++)
            {
               Message msg = consumer.receive();

               if (msg == null)
               {
                  log.error("message is null");
                  break;
               }

//               if (i % 1000 == 0)
//               {
//                  log.info("Consumer " + this + " received " + i);
//               }
            }

            connection.close();
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }
      }
   }

}
