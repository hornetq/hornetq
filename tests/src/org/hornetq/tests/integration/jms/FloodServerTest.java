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

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.jms.server.impl.JMSFactoryType;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.unit.util.InVMContext;
import org.hornetq.tests.util.UnitTestCase;

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
      server = HornetQServers.newHornetQServer(conf, false);
      server.start();

      serverManager = new JMSServerManagerImpl(server);
      initialContext = new InVMContext();
      serverManager.setContext(initialContext);
      serverManager.start();
      serverManager.activated();

      serverManager.createTopic(false, topicName, topicName);
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

      serverManager.createConnectionFactory("ManualReconnectionToSingleServerTest",
                                            connectorConfigs,
                                            null,
                                            1000,
                                            HornetQClient.DEFAULT_CONNECTION_TTL,
                                            callTimeout,
                                            HornetQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT,
                                            HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                            HornetQClient.DEFAULT_CONSUMER_WINDOW_SIZE,
                                            HornetQClient.DEFAULT_CONSUMER_MAX_RATE,
                                            HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                                            HornetQClient.DEFAULT_PRODUCER_WINDOW_SIZE,
                                            HornetQClient.DEFAULT_PRODUCER_MAX_RATE,
                                            false,
                                            false,
                                            false,
                                            HornetQClient.DEFAULT_AUTO_GROUP,
                                            false,
                                            HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                            HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                                            HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                                            HornetQClient.DEFAULT_USE_GLOBAL_POOLS,
                                            HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                                            HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                                            retryInterval,
                                            retryIntervalMultiplier,
                                            1000,
                                            reconnectAttempts,
                                            HornetQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION,
                                            failoverOnServerShutdown,
                                            null,
                                            JMSFactoryType.CF,
                                            "/cf");
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
      private final Connection connection;

      private final Session session;

      private final MessageProducer producer;

      private final int numMessages;

      ProducerThread(final ConnectionFactory cf, final int numMessages) throws Exception
      {
         connection = cf.createConnection();

         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         producer = session.createProducer(HornetQJMSClient.createTopic("my-topic"));

         producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         this.numMessages = numMessages;
      }

      @Override
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

               // if (i % 1000 == 0)
               // {
               // log.info("Producer " + this + " sent " + i);
               // }
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
      private final Connection connection;

      private final Session session;

      private final MessageConsumer consumer;

      private final int numMessages;

      ConsumerThread(final ConnectionFactory cf, final int numMessages) throws Exception
      {
         connection = cf.createConnection();

         connection.start();

         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         consumer = session.createConsumer(HornetQJMSClient.createTopic("my-topic"));

         this.numMessages = numMessages;
      }

      @Override
      public void run()
      {
         try
         {
            for (int i = 0; i < numMessages; i++)
            {
               Message msg = consumer.receive();

               if (msg == null)
               {
                  FloodServerTest.log.error("message is null");
                  break;
               }

               // if (i % 1000 == 0)
               // {
               // log.info("Consumer " + this + " received " + i);
               // }
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
