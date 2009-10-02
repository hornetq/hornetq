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

import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
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

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.cluster.DivertConfiguration;
import org.hornetq.jms.HornetQQueue;
import org.hornetq.tests.util.JMSTestBase;
import org.hornetq.utils.Pair;

/**
 * A DivertAndACKClientTest
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

      assertNotNull(receivedMessage);

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
      assertNotNull(receivedMessage);
      receivedMessage.acknowledge();

      connection.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected boolean usePersistence()
   {
      return true;
   }
   

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
                                              false, // TODO: set this to true, and the test will fail
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
