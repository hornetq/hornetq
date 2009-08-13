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
package org.jboss.messaging.tests.integration.jms;

import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_WINDOW_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_THREAD_POOL_MAX_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_USE_GLOBAL_POOLS;

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
import javax.jms.Topic;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.integration.transports.netty.NettyAcceptorFactory;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.tests.unit.util.InVMContext;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.Pair;

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

   private MessagingServer server;

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
      server = Messaging.newMessagingServer(conf, false);
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
                                            DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                            DEFAULT_CONNECTION_TTL,
                                            callTimeout,
                                            DEFAULT_MAX_CONNECTIONS,
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
                                            reconnectAttempts,
                                            failoverOnServerShutdown,
                                            jndiBindings);
   }

   
   public void testFoo()
   {      
   }
   
   public void _testFlood() throws Exception
   {      
      Connection connection = null;
      
      try
      {
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/cf");
         
         connection = cf.createConnection();
         
         final int numProducers = 20;
         
         final int numConsumers = 20;
         
         final int numMessages = 100000;
         
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
      finally
      {
         if (connection != null)
         {
            connection.close();
         }
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
         
         producer = session.createProducer(new JBossTopic("my-topic"));
         
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
               
               if (i % 1000 == 0)
               {
                  log.info("Producer " + this + " sent " + i);
               }
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
         
         consumer = session.createConsumer(new JBossTopic("my-topic"));
         
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
               
               if (i % 1000 == 0)
               {
                  log.info("Consumer " + this + " received " + i);
               }
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
