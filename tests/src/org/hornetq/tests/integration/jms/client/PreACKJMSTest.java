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

import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_ACK_BATCH_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_AUTO_GROUP;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_BLOCK_ON_ACKNOWLEDGE;
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
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_MAX_RATE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_PRODUCER_WINDOW_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_THREAD_POOL_MAX_SIZE;
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_USE_GLOBAL_POOLS;

import java.util.List;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.tests.util.JMSTestBase;
import org.hornetq.utils.Pair;

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

   public void internalTestPreACK(int sessionType) throws Exception
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

         assertNotNull(msg2);

         assertEquals(msg1.getText(), msg2.getText());

         conn.close();

         conn = cf.createConnection();

         conn.start();

         sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

         cons = sess.createConsumer(queue);

         msg2 = (TextMessage)cons.receive(10);

         assertNull("ConnectionFactory is on PreACK mode, the message shouldn't be received", msg2);
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

         assertNotNull(msg2);

         assertEquals(msg1.getText(), msg2.getText());
         
         sess.rollback();

         conn.close();

         conn = cf.createConnection();

         conn.start();

         sess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

         cons = sess.createConsumer(queue);

         msg2 = (TextMessage)cons.receive(10);

         assertNotNull("ConnectionFactory is on PreACK mode but it is transacted", msg2);
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
                                        DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                        DEFAULT_CONNECTION_TTL,
                                        callTimeout,                                        
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
                                        true,
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

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
