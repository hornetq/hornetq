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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.jms.client.HornetQSession;
import org.hornetq.jms.server.impl.JMSFactoryType;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.util.JMSTestBase;

/**
 * 
 * A SessionClosedOnRemotingConnectionFailureTest
 *
 * @author Tim Fox
 *
 *
 */
public class SessionClosedOnRemotingConnectionFailureTest extends JMSTestBase
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(SessionClosedOnRemotingConnectionFailureTest.class);

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testSessionClosedOnRemotingConnectionFailure() throws Exception
   {
      List<Pair<TransportConfiguration, TransportConfiguration>> connectorConfigs = new ArrayList<Pair<TransportConfiguration, TransportConfiguration>>();
      connectorConfigs.add(new Pair<TransportConfiguration, TransportConfiguration>(new TransportConfiguration(NettyConnectorFactory.class.getName()),
                                                                                    null));


      jmsServer.createConnectionFactory("cffoo",
                                        connectorConfigs,
                                        null,
                                        HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                        HornetQClient.DEFAULT_CONNECTION_TTL,
                                        HornetQClient.DEFAULT_CALL_TIMEOUT,
                                        HornetQClient.DEFAULT_CACHE_LARGE_MESSAGE_CLIENT,
                                        HornetQClient.DEFAULT_MIN_LARGE_MESSAGE_SIZE,
                                        HornetQClient.DEFAULT_CONSUMER_WINDOW_SIZE,
                                        HornetQClient.DEFAULT_CONSUMER_MAX_RATE,
                                        HornetQClient.DEFAULT_CONFIRMATION_WINDOW_SIZE,
                                        HornetQClient.DEFAULT_PRODUCER_WINDOW_SIZE,
                                        HornetQClient.DEFAULT_PRODUCER_MAX_RATE,
                                        HornetQClient.DEFAULT_BLOCK_ON_ACKNOWLEDGE,
                                        HornetQClient.DEFAULT_BLOCK_ON_DURABLE_SEND,
                                        HornetQClient.DEFAULT_BLOCK_ON_NON_DURABLE_SEND,
                                        HornetQClient.DEFAULT_AUTO_GROUP,
                                        HornetQClient.DEFAULT_PRE_ACKNOWLEDGE,
                                        HornetQClient.DEFAULT_CONNECTION_LOAD_BALANCING_POLICY_CLASS_NAME,
                                        HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                                        HornetQClient.DEFAULT_ACK_BATCH_SIZE,
                                        HornetQClient.DEFAULT_USE_GLOBAL_POOLS,
                                        HornetQClient.DEFAULT_SCHEDULED_THREAD_POOL_MAX_SIZE,
                                        HornetQClient.DEFAULT_THREAD_POOL_MAX_SIZE,
                                        HornetQClient.DEFAULT_RETRY_INTERVAL,
                                        HornetQClient.DEFAULT_RETRY_INTERVAL_MULTIPLIER,
                                        HornetQClient.DEFAULT_MAX_RETRY_INTERVAL,
                                        0,
                                        HornetQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION,
                                        false,
                                        null,
                                        JMSFactoryType.CF,
                                        "/cffoo");

      cf = (ConnectionFactory)context.lookup("/cffoo");

      Connection conn = cf.createConnection();

      Queue queue = createQueue("testQueue");

      try
      {
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = session.createProducer(queue);

         MessageConsumer cons = session.createConsumer(queue);

         conn.start();

         prod.send(session.createMessage());

         Assert.assertNotNull(cons.receive());

         // Now fail the underlying connection

         RemotingConnection connection = ((ClientSessionInternal)((HornetQSession)session).getCoreSession()).getConnection();

         connection.fail(new HornetQException(HornetQException.NOT_CONNECTED));

         // Now try and use the producer

         try
         {
            prod.send(session.createMessage());

            Assert.fail("Should throw exception");
         }
         catch (JMSException e)
         {
            // assertEquals(HornetQException.OBJECT_CLOSED, e.getCode());
         }

         try
         {
            cons.receive();

            Assert.fail("Should throw exception");
         }
         catch (JMSException e)
         {
            // assertEquals(HornetQException.OBJECT_CLOSED, e.getCode());
         }

         session.close();

         conn.close();
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

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
