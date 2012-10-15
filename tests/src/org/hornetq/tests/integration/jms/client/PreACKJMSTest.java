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

package org.hornetq.tests.integration.jms.client;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import junit.framework.Assert;

import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.JMSTestBase;
import org.hornetq.tests.util.RandomUtil;

/**
 * A PreACKJMSTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class PreACKJMSTest extends JMSTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private Queue queue;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testPreACKAuto() throws Exception
   {
      internalTestPreACK(Session.AUTO_ACKNOWLEDGE);
   }

   public void testPreACKClientACK() throws Exception
   {
      internalTestPreACK(Session.CLIENT_ACKNOWLEDGE);
   }

   public void testPreACKDupsOK() throws Exception
   {
      internalTestPreACK(Session.DUPS_OK_ACKNOWLEDGE);
   }

   public void internalTestPreACK(final int sessionType) throws Exception
   {
      Connection conn = cf.createConnection();
      try
      {
         Session sess = conn.createSession(false, sessionType);

         MessageProducer prod = sess.createProducer(queue);

         TextMessage msg1 = sess.createTextMessage("hello");

         prod.send(msg1);

         conn.start();

         MessageConsumer cons = sess.createConsumer(queue);

         TextMessage msg2 = (TextMessage)cons.receive(1000);

         Assert.assertNotNull(msg2);

         Assert.assertEquals(msg1.getText(), msg2.getText());

         conn.close();

         conn = cf.createConnection();

         conn.start();

         sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

         cons = sess.createConsumer(queue);

         msg2 = (TextMessage)cons.receiveNoWait();

         Assert.assertNull("ConnectionFactory is on PreACK mode, the message shouldn't be received", msg2);
      }
      finally
      {
         try
         {
            conn.close();
         }
         catch (Throwable igonred)
         {
         }
      }

   }

   public void disabled_testPreACKTransactional() throws Exception
   {
      Connection conn = cf.createConnection();
      try
      {
         Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);

         MessageProducer prod = sess.createProducer(queue);

         TextMessage msg1 = sess.createTextMessage("hello");

         prod.send(msg1);

         sess.commit();

         conn.start();

         MessageConsumer cons = sess.createConsumer(queue);

         TextMessage msg2 = (TextMessage)cons.receive(1000);

         Assert.assertNotNull(msg2);

         Assert.assertEquals(msg1.getText(), msg2.getText());

         sess.rollback();

         conn.close();

         conn = cf.createConnection();

         conn.start();

         sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

         cons = sess.createConsumer(queue);

         msg2 = (TextMessage)cons.receive(10);

         Assert.assertNotNull("ConnectionFactory is on PreACK mode but it is transacted", msg2);
      }
      finally
      {
         try
         {
            conn.close();
         }
         catch (Throwable igonred)
         {
         }
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      queue = createQueue("queue1");
   }

   @Override
   protected void tearDown() throws Exception
   {
      queue = null;
      super.tearDown();
   }

   @Override
   protected void createCF(final List<TransportConfiguration> connectorConfigs,
                           final String ... jndiBindings) throws Exception
   {
      int retryInterval = 1000;
      double retryIntervalMultiplier = 1.0;
      int reconnectAttempts = -1;
      int callTimeout = 30000;

      ArrayList<String> connectors = registerConnectors(server, connectorConfigs);

      jmsServer.createConnectionFactory("ManualReconnectionToSingleServerTest",
                                       false,
                                       JMSFactoryType.CF,
                                       connectors,
                                        null,
                                        HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                        HornetQClient.DEFAULT_CONNECTION_TTL,
                                        callTimeout,
                                        HornetQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT,
                                        HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                        HornetQClient.DEFAULT_COMPRESS_LARGE_MESSAGES,
                                        HornetQClient.DEFAULT_CONSUMER_WINDOW_SIZE,
                                        HornetQClient.DEFAULT_CONSUMER_MAX_RATE,
                                        HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                                        HornetQClient.DEFAULT_PRODUCER_WINDOW_SIZE,
                                        HornetQClient.DEFAULT_PRODUCER_MAX_RATE,
                                        HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                        HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND,
                                        HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND,
                                        HornetQClient.DEFAULT_AUTO_GROUP,
                                        true,
                                        HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                        HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                                        HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                                        HornetQClient.DEFAULT_USE_GLOBAL_POOLS,
                                        HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                                        HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                                        retryInterval,
                                        retryIntervalMultiplier,
                                        HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                        reconnectAttempts,
                                        HornetQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION,
                                        null,
                                        jndiBindings);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
