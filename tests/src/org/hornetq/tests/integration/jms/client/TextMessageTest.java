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
import static org.hornetq.core.client.impl.ClientSessionFactoryImpl.DEFAULT_CONFIRMATION_WINDOW_SIZE;
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

import java.util.List;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.tests.util.JMSTestBase;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.utils.Pair;

/**
 * 
 * A TextMessageTest
 *
 * @author Tim Fox
 *
 *
 */
public class TextMessageTest extends JMSTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private Queue queue;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------
   
   public void testSendReceiveNullBody() throws Exception
   {
      Connection conn = cf.createConnection();
      
      try
      {
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sess.createProducer(queue);
         
         conn.start();
         
         MessageConsumer cons = sess.createConsumer(queue);

         TextMessage msg1 = sess.createTextMessage(null);
         prod.send(msg1);                 
         TextMessage received1 = (TextMessage)cons.receive(1000);
         assertNotNull(received1);
         assertNull(received1.getText());   
         
         TextMessage msg2 = sess.createTextMessage();
         msg2.setText(null);
         prod.send(msg2);                 
         TextMessage received2 = (TextMessage)cons.receive(1000);
         assertNotNull(received2);
         assertNull(received2.getText());
         
         TextMessage msg3 = sess.createTextMessage();
         prod.send(msg3);                 
         TextMessage received3 = (TextMessage)cons.receive(1000);
         assertNotNull(received3);
         assertNull(received3.getText());
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
   
   public void testSendReceiveWithBody0() throws Exception
   {
      testSendReceiveWithBody(0);
   }
   
   public void testSendReceiveWithBody1() throws Exception
   {
      testSendReceiveWithBody(1);
   }
   
   public void testSendReceiveWithBody9() throws Exception
   {
      testSendReceiveWithBody(9);
   }
   
   public void testSendReceiveWithBody20() throws Exception
   {
      testSendReceiveWithBody(20);
   }
   
   public void testSendReceiveWithBody10000() throws Exception
   {
      testSendReceiveWithBody(10000);
   }
   
   public void testSendReceiveWithBody0xffff() throws Exception
   {
      testSendReceiveWithBody(0xffff);
   }
   
   public void testSendReceiveWithBody0xffffplus1() throws Exception
   {
      testSendReceiveWithBody(0xffff + 1);
   }
   
   public void testSendReceiveWithBody0xfffftimes2() throws Exception
   {
      testSendReceiveWithBody(2 * 0xffff);
   }
   
   private void testSendReceiveWithBody(int bodyLength) throws Exception
   {
      Connection conn = cf.createConnection();
      
      try
      {
         char[] chrs = new char[bodyLength];
         
         for (int i = 0; i < bodyLength; i++)
         {
            chrs[i] = RandomUtil.randomChar();
         }
         String str = new String(chrs);
         
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sess.createProducer(queue);
         
         conn.start();
         
         MessageConsumer cons = sess.createConsumer(queue);

         TextMessage msg1 = sess.createTextMessage(str);
         prod.send(msg1);                 
         TextMessage received1 = (TextMessage)cons.receive(1000);
         assertNotNull(received1);
         assertEquals(str, received1.getText());   
         
         TextMessage msg2 = sess.createTextMessage();
         msg2.setText(str);
         prod.send(msg2);                 
         TextMessage received2 = (TextMessage)cons.receive(1000);
         assertNotNull(received2);
         assertEquals(str, received2.getText());
         
         //Now resend it
         prod.send(received2);
         TextMessage received3 = (TextMessage)cons.receive(1000);
         assertNotNull(received3);
         
         //And resend again
         
         prod.send(received3);
         TextMessage received4 = (TextMessage)cons.receive(1000);
         assertNotNull(received4);
         
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
                                        DEFAULT_CONFIRMATION_WINDOW_SIZE,
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
                                        DEFAULT_MAX_RETRY_INTERVAL,
                                        reconnectAttempts,
                                        failoverOnServerShutdown,
                                        jndiBindings);
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
