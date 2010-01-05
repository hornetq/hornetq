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

package org.hornetq.tests.integration.jms.divert;

import java.util.ArrayList;
import java.util.List;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.Assert;

import org.hornetq.Pair;
import org.hornetq.core.client.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.cluster.DivertConfiguration;
import org.hornetq.jms.HornetQQueue;
import org.hornetq.tests.util.JMSTestBase;

/**
 * A DivertAndACKClientTest
 * 
 * https://jira.jboss.org/jira/browse/HORNETQ-165
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class DivertAndACKClientTest extends JMSTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testAutoACK() throws Exception
   {
      HornetQQueue queueSource = (HornetQQueue)createQueue("Source");
      HornetQQueue queueTarget = (HornetQQueue)createQueue("Dest");

      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      final MessageProducer producer = session.createProducer(queueSource);

      final TextMessage message = session.createTextMessage("message text");
      producer.send(message);

      connection.start();

      final MessageConsumer consumer = session.createConsumer(queueTarget);
      TextMessage receivedMessage = (TextMessage)consumer.receive(1000);

      Assert.assertNotNull(receivedMessage);

      connection.close();
   }

   public void testClientACK() throws Exception
   {
      HornetQQueue queueSource = (HornetQQueue)createQueue("Source");
      HornetQQueue queueTarget = (HornetQQueue)createQueue("Dest");

      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

      final MessageProducer producer = session.createProducer(queueSource);

      final TextMessage message = session.createTextMessage("message text");
      producer.send(message);

      connection.start();

      final MessageConsumer consumer = session.createConsumer(queueTarget);
      TextMessage receivedMessage = (TextMessage)consumer.receive(1000);
      Assert.assertNotNull(receivedMessage);
      receivedMessage.acknowledge();

      connection.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected boolean usePersistence()
   {
      return true;
   }

   @Override
   protected Configuration createDefaultConfig(final boolean netty)
   {
      Configuration config = super.createDefaultConfig(netty);

      DivertConfiguration divert = new DivertConfiguration("local-divert",
                                                           "some-name",
                                                           "jms.queue.Source",
                                                           "jms.queue.Dest",
                                                           true,
                                                           null,
                                                           null);

      ArrayList<DivertConfiguration> divertList = new ArrayList<DivertConfiguration>();
      divertList.add(divert);

      config.setDivertConfigurations(divertList);

      return config;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   @Override
   protected void createCF(final List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs,
                           final List<String> jndiBindings) throws Exception
   {
      int retryInterval = 1000;
      double retryIntervalMultiplier = 1.0;
      int reconnectAttempts = -1;
      boolean failoverOnServerShutdown = true;
      int callTimeout = 30000;

      jmsServer.createConnectionFactory("ManualReconnectionToSingleServerTest",
                                        connectorConfigs,
                                        null,
                                        ClientSessionFactoryImpl.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                        ClientSessionFactoryImpl.DEFAULT_CONNECTION_TTL,
                                        callTimeout,
                                        ClientSessionFactoryImpl.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT,
                                        ClientSessionFactoryImpl.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                        ClientSessionFactoryImpl.DEFAULT_CONSUMER_WINDOW_SIZE,
                                        ClientSessionFactoryImpl.DEFAULT_CONSUMER_MAX_RATE,
                                        ClientSessionFactoryImpl.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                                        ClientSessionFactoryImpl.DEFAULT_PRODUCER_WINDOW_SIZE,
                                        ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE,
                                        true, // this test needs to block on ACK
                                        ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_DURABLE_SEND,
                                        ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_NON_DURABLE_SEND,
                                        ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP,
                                        ClientSessionFactoryImpl.DEFAULT_PRE_ACKNOWLEDGE,
                                        ClientSessionFactoryImpl.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                        ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                        ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE,
                                        ClientSessionFactoryImpl.DEFAULT_USE_GLOBAL_POOLS,
                                        ClientSessionFactoryImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                                        ClientSessionFactoryImpl.DEFAULT_THREAD_POOL_MAX_SIZE,
                                        retryInterval,
                                        retryIntervalMultiplier,
                                        ClientSessionFactoryImpl.DEFAULT_MAX_RETRY_INTERVAL,
                                        reconnectAttempts,
                                        failoverOnServerShutdown,
                                        null,
                                        jndiBindings);
   }

}
