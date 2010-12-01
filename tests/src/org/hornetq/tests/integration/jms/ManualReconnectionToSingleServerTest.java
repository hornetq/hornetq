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

import static java.util.concurrent.TimeUnit.SECONDS;

import java.util.ArrayList;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.Context;

import junit.framework.Assert;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.config.ConnectionFactoryConfiguration;
import org.hornetq.jms.server.config.JMSConfiguration;
import org.hornetq.jms.server.config.impl.ConnectionFactoryConfigurationImpl;
import org.hornetq.jms.server.config.impl.JMSConfigurationImpl;
import org.hornetq.jms.server.config.impl.JMSQueueConfigurationImpl;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.tests.unit.util.InVMContext;
import org.hornetq.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ManualReconnectionToSingleServerTest extends UnitTestCase
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ManualReconnectionToSingleServerTest.class);

   private Connection connection;

   private MessageConsumer consumer;

   private CountDownLatch exceptionLatch = new CountDownLatch(1);

   private CountDownLatch reconnectionLatch = new CountDownLatch(1);

   private CountDownLatch allMessagesReceived = new CountDownLatch(1);

   private JMSServerManager serverManager;

   private InVMContext context;

   private final String queueName = "ManualReconnectionToSingleServerTest.queue";

   private final int num = 20;

   private final ExceptionListener exceptionListener = new ExceptionListener()
   {
      public void onException(final JMSException e)
      {
         exceptionLatch.countDown();
         disconnect();
         connect();
         reconnectionLatch.countDown();
      }
   };

   private Listener listener;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // TestCase overrides -------------------------------------------

   // Public --------------------------------------------------------

   public void testExceptionListener() throws Exception
   {
      connect();

      ConnectionFactory cf = (ConnectionFactory)context.lookup("/cf");
      Destination dest = (Destination)context.lookup(queueName);
      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = sess.createProducer(dest);

      for (int i = 0; i < num; i++)
      {
         Message message = sess.createTextMessage(new Date().toString());
         message.setIntProperty("counter", i + 1);
         prod.send(message);

         if (i == num / 2)
         {
            conn.close();
            serverManager.stop();
            Thread.sleep(5000);
            serverManager.start();
            cf = (ConnectionFactory)context.lookup("/cf");
            dest = (Destination)context.lookup(queueName);
            conn = cf.createConnection();
            sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            prod = sess.createProducer(dest);
         }
      }

      conn.close();

      boolean gotException = exceptionLatch.await(10, SECONDS);
      Assert.assertTrue(gotException);

      boolean clientReconnected = reconnectionLatch.await(10, SECONDS);

      Assert.assertTrue("client did not reconnect after server was restarted", clientReconnected);

      boolean gotAllMessages = allMessagesReceived.await(10, SECONDS);
      Assert.assertTrue(gotAllMessages);

      connection.close();

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
      HornetQServer server = HornetQServers.newHornetQServer(conf, false);

      JMSConfiguration configuration = new JMSConfigurationImpl();
      context = new InVMContext();
      configuration.setContext(context);
      configuration.getQueueConfigurations().add(new JMSQueueConfigurationImpl(queueName, null, true, queueName));

      ArrayList<TransportConfiguration> configs = new ArrayList<TransportConfiguration>();
      configs.add(new TransportConfiguration(NettyConnectorFactory.class.getName()));
      ConnectionFactoryConfiguration cfConfig = new ConnectionFactoryConfigurationImpl("cf",
            false,
            registerConnectors(server, configs), "/cf");
      cfConfig.setRetryInterval(1000);
      cfConfig.setRetryIntervalMultiplier(1.0);
      cfConfig.setReconnectAttempts(-1);
      configuration.getConnectionFactoryConfigurations().add(cfConfig);
      serverManager = new JMSServerManagerImpl(server, configuration);
      serverManager.start();

      listener = new Listener();

      exceptionLatch = new CountDownLatch(1);
      reconnectionLatch = new CountDownLatch(1);
      allMessagesReceived = new CountDownLatch(1);
   }

   @Override
   protected void tearDown() throws Exception
   {
      serverManager.stop();

      serverManager = null;

      connection = null;

      super.tearDown();

      System.gc();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   protected void disconnect()
   {
      ManualReconnectionToSingleServerTest.log.info("calling disconnect");
      if (connection == null)
      {
         ManualReconnectionToSingleServerTest.log.info("connection is null");
         return;
      }

      try
      {
         connection.setExceptionListener(null);
         ManualReconnectionToSingleServerTest.log.info("closing the connection");
         connection.close();
         connection = null;
         ManualReconnectionToSingleServerTest.log.info("connection closed");
      }
      catch (Exception e)
      {
         ManualReconnectionToSingleServerTest.log.info("** got exception");
         e.printStackTrace();
      }
   }

   protected void connect()
   {
      try
      {
         if (context == null)
         {
            return;
         }
         Context initialContext = context;
         Queue queue;
         ConnectionFactory cf;
         while (true)
         {
            try
            {
               queue = (Queue)initialContext.lookup(queueName);
               cf = (ConnectionFactory)initialContext.lookup("/cf");
               break;
            }
            catch (Exception e)
            {
               // retry until server is up
               Thread.sleep(100);
            }
         }
         connection = cf.createConnection();
         connection.setExceptionListener(exceptionListener);
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         consumer = session.createConsumer(queue);
         consumer.setMessageListener(listener);
         connection.start();
      }
      catch (Exception e)
      {
         if (connection != null)
         {
            try
            {
               connection.close();
            }
            catch (JMSException e1)
            {
               e1.printStackTrace();
            }
         }
      }
   }

   private class Listener implements MessageListener
   {
      private int count = 0;

      public void onMessage(final Message msg)
      {
         count++;

         try
         {
            int counter = msg.getIntProperty("counter");
         }
         catch (JMSException e)
         {
            e.printStackTrace();
         }
         if (count == num)
         {
            allMessagesReceived.countDown();
         }
      }
   };

}
