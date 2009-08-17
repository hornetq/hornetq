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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_PERSISTENT_SEND;
import static org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_PERSISTENT_SEND;
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
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.Context;

import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.integration.transports.netty.NettyAcceptorFactory;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;
import org.jboss.messaging.jms.server.impl.JMSServerManagerImpl;
import org.jboss.messaging.tests.unit.util.InVMContext;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.Pair;

/**
 * Connection tests. Contains all connection tests, except tests relating to closing a connection,
 * which go to ConnectionClosedTest.
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ManualReconnectionToSingleServerTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ManualReconnectionToSingleServerTest.class);

   private Connection connection;

   private Session session;

   private MessageProducer producer;

   private MessageConsumer consumer;

   private CountDownLatch exceptionLatch = new CountDownLatch(1);

   private boolean afterRestart = false;

   private boolean receivedMessagesAfterRestart = false;
   
   private int callTimeout;

   private MessageListener listener = new MessageListener()
   {
      public void onMessage(Message msg)
      {
         if (afterRestart)
         {
            receivedMessagesAfterRestart = true;
         }
         System.out.println(msg);
      }
   };

   private ExceptionListener exceptionListener = new ExceptionListener()
   {
      public void onException(JMSException e)
      {
         exceptionLatch.countDown();
         disconnect();
         connect();
      }
   };

   private MessagingServer server;

   private JMSServerManagerImpl serverManager;

   private InVMContext context;

   private final String topicName = "my-topic";

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // TestCase overrides -------------------------------------------

   // Public --------------------------------------------------------

   public void testExceptionListener() throws Exception
   {
      long start = System.currentTimeMillis();
      
      connect();

      int num = 10;
      for (int i = 0; i < num; i++)
      {
         try
         {
            Message message = session.createTextMessage((new Date()).toString());
            producer.send(message);
            Thread.sleep(500);
         }
         catch (Exception e)
         {
            e.printStackTrace();
         }

         if (i == num / 2)
         {
            killServer();
            Thread.sleep(5000);
            restartServer();
            afterRestart = true;
         }
      }

      boolean gotException = exceptionLatch.await(10, SECONDS);
      assertTrue(gotException);

      assertTrue(receivedMessagesAfterRestart);
      connection.close();
      
      long end = System.currentTimeMillis();
      
      log.info("That took " + (end - start));
      
      //Make sure it doesn't pass by just timing out on blocking send
      assertTrue(end - start < callTimeout);

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
      conf.getAcceptorConfigurations().add(new TransportConfiguration(NettyAcceptorFactory.class.getName()));
      server = Messaging.newMessagingServer(conf, false);
      server.start();

      serverManager = new JMSServerManagerImpl(server);
      context = new InVMContext();
      serverManager.setContext(context);
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
      
      connection = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private void restartServer() throws Exception
   {
      serverManager.start();
      serverManager.activated();
      context = new InVMContext();
      serverManager.setContext(context);      
      serverManager.createTopic(topicName, topicName);
      registerConnectionFactory();
   }

   private void killServer() throws Exception
   {
      serverManager.stop();
   }

   private void registerConnectionFactory() throws Exception
   {
      int retryInterval = 1000;
      double retryIntervalMultiplier = 1.0;
      int reconnectAttempts = -1;
      boolean failoverOnServerShutdown = true;
      callTimeout = 30000;

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
                                            reconnectAttempts,
                                            failoverOnServerShutdown,
                                            jndiBindings);
   }

   protected void disconnect()
   {
      if (connection == null)
      {
         return;
      }

      try
      {
         connection.setExceptionListener(null);
         connection.close();
         connection = null;
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }

   protected void connect()
   {
      try
      {
         Context initialContext = context;
         Topic topic = (Topic)initialContext.lookup(topicName);
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/cf");
         connection = cf.createConnection();
         connection.setExceptionListener(exceptionListener);
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         producer = session.createProducer(topic);
         consumer = session.createConsumer(topic);
         consumer.setMessageListener(listener);
         connection.start();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
   }
}
