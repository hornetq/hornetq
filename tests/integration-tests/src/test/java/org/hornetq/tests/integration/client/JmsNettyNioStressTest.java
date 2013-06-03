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

package org.hornetq.tests.integration.client;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.junit.Assert;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.api.jms.JMSFactoryType;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.client.HornetQDestination;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * -- https://issues.jboss.org/browse/HORNETQ-746
 * Stress test using netty with NIO and many JMS clients concurrently, to try
 * and induce a deadlock.
 * <p>
 * A large number of JMS clients are started concurrently. Some produce to queue
 * 1 over one connection, others consume from queue 1 and produce to queue 2
 * over a second connection, and others consume from queue 2 over a third
 * connection.
 * <p>
 * Each operation is done in a JMS transaction, sending/consuming one message
 * per transaction.
 * <p>
 * The server is set up with netty, with only one NIO worker and 1 hornetq
 * server worker. This increases the chance for the deadlock to occur.
 * <p>
 * If the deadlock occurs, all threads will block/die. A simple transaction
 * counting strategy is used to verify that the count has reached the expected
 * value.
 * @author Carl Heymann
 */
public class JmsNettyNioStressTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Remove this method to re-enable those tests
   @Test
   public void testStressSendNetty() throws Exception
   {
      doTestStressSend(true);
   }

   public void doTestStressSend(final boolean netty) throws Exception
   {
      // first set up the server
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.PORT_PROP_NAME, 5445);
      params.put(TransportConstants.HOST_PROP_NAME, "localhost");
      params.put(TransportConstants.USE_NIO_PROP_NAME, true);
      // minimize threads to maximize possibility for deadlock
      params.put(TransportConstants.NIO_REMOTING_THREADS_PROPNAME, 1);
      params.put(TransportConstants.BATCH_DELAY, 50);
      Configuration config = createDefaultConfig(params, ServiceTestBase.NETTY_ACCEPTOR_FACTORY);
      HornetQServer server = createServer(true, config);
      server.getConfiguration().setThreadPoolMaxSize(2);
      server.start();

      // now the client side
      Map<String, Object> connectionParams = new HashMap<String, Object>();
      connectionParams.put(TransportConstants.PORT_PROP_NAME, 5445);
      connectionParams.put(TransportConstants.HOST_PROP_NAME, "localhost");
      connectionParams.put(TransportConstants.USE_NIO_PROP_NAME, true);
      connectionParams.put(TransportConstants.BATCH_DELAY, 50);
      connectionParams.put(TransportConstants.NIO_REMOTING_THREADS_PROPNAME, 6);
      final TransportConfiguration transpConf = new TransportConfiguration(NettyConnectorFactory.class.getName(),
                                                                           connectionParams);
      final ServerLocator locator = createNonHALocator(netty);

      // each thread will do this number of transactions
      final int numberOfMessages = 100;

      // these must all be the same
      final int numProducers = 30;
      final int numConsumerProducers = 30;
      final int numConsumers = 30;

      // each produce, consume+produce and consume increments this counter
      final AtomicInteger totalCount = new AtomicInteger(0);

      // the total we expect if all producers, consumer-producers and
      // consumers complete normally
      int totalExpectedCount = (numProducers + numConsumerProducers + numConsumerProducers) * numberOfMessages;

      // each group gets a separate connection
      final Connection connectionProducer;
      final Connection connectionConsumerProducer;
      final Connection connectionConsumer;

      // create the 2 queues used in the test
      ClientSessionFactory sf = locator.createSessionFactory(transpConf);
      ClientSession session = sf.createTransactedSession();
      session.createQueue("jms.queue.queue", "jms.queue.queue");
      session.createQueue("jms.queue.queue2", "jms.queue.queue2");
      session.commit();
      sf.close();
      session.close();
      locator.close();

      // create and start JMS connections
      HornetQConnectionFactory cf = HornetQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF, transpConf);
      connectionProducer = cf.createConnection();
      connectionProducer.start();

      connectionConsumerProducer = cf.createConnection();
      connectionConsumerProducer.start();

      connectionConsumer = cf.createConnection();
      connectionConsumer.start();

      // these threads produce messages on the the first queue
      for (int i = 0; i < numProducers; i++)
      {
         new Thread()
         {
            @Override
            public void run()
            {

               Session session = null;
               try
               {
                  session = connectionProducer.createSession(true, Session.SESSION_TRANSACTED);
                  MessageProducer messageProducer = session.createProducer(HornetQDestination.createQueue("queue"));
                  messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);

                  for (int i = 0; i < numberOfMessages; i++)
                  {
                     BytesMessage message = session.createBytesMessage();
                     message.writeBytes(new byte[3000]);
                     message.setStringProperty("Service", "LoadShedService");
                     message.setStringProperty("Action", "testAction");

                     messageProducer.send(message);
                     session.commit();

                     totalCount.incrementAndGet();
                  }
               }
               catch (Exception e)
               {
                  throw new RuntimeException(e);
               }
               finally
               {
                  if (session != null)
                  {
                     try
                     {
                        session.close();
                     }
                     catch (Exception e)
                     {
                        e.printStackTrace();
                     }
                  }
               }
            }
         }.start();
      }

      // these threads just consume from the one and produce on a second queue
      for (int i = 0; i < numConsumerProducers; i++)
      {
         new Thread()
         {
            @Override
            public void run()
            {
               Session session = null;
               try
               {
                  session = connectionConsumerProducer.createSession(true, Session.SESSION_TRANSACTED);
                  MessageConsumer consumer = session.createConsumer(HornetQDestination.createQueue("queue"));
                  MessageProducer messageProducer = session.createProducer(HornetQDestination.createQueue("queue2"));
                  messageProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
                  for (int i = 0; i < numberOfMessages; i++)
                  {
                     BytesMessage message = (BytesMessage)consumer.receive(5000);
                     if (message == null)
                     {
                        return;
                     }
                     message = session.createBytesMessage();
                     message.writeBytes(new byte[3000]);
                     message.setStringProperty("Service", "LoadShedService");
                     message.setStringProperty("Action", "testAction");
                     messageProducer.send(message);
                     session.commit();

                     totalCount.incrementAndGet();
                  }
               }
               catch (Exception e)
               {
                  throw new RuntimeException(e);
               }
               finally
               {
                  if (session != null)
                  {
                     try
                     {
                        session.close();
                     }
                     catch (Exception e)
                     {
                        e.printStackTrace();
                     }
                  }
               }
            }
         }.start();
      }

      // these threads consume from the second queue
      for (int i = 0; i < numConsumers; i++)
      {
         new Thread()
         {
            @Override
            public void run()
            {
               Session session = null;
               try
               {
                  session = connectionConsumer.createSession(true, Session.SESSION_TRANSACTED);
                  MessageConsumer consumer = session.createConsumer(HornetQDestination.createQueue("queue2"));
                  for (int i = 0; i < numberOfMessages; i++)
                  {
                     BytesMessage message = (BytesMessage)consumer.receive(5000);
                     if (message == null)
                     {
                        return;
                     }
                     session.commit();

                     totalCount.incrementAndGet();
                  }
               }
               catch (Exception e)
               {
                  throw new RuntimeException(e);
               }
               finally
               {
                  if (session != null)
                  {
                     try
                     {
                        session.close();
                     }
                     catch (Exception e)
                     {
                        e.printStackTrace();
                     }
                  }
               }
            }
         }.start();
      }

      // check that the overall transaction count reaches the expected number,
      // which would indicate that the system didn't stall
      int timeoutCounter = 0;
      int maxSecondsToWait = 60;
      while (timeoutCounter < maxSecondsToWait && totalCount.get() < totalExpectedCount)
      {
         timeoutCounter++;
         Thread.sleep(1000);
         System.out.println("Not done yet.. " + (maxSecondsToWait - timeoutCounter) + "; " + totalCount.get());
      }
      System.out.println("Done.." + totalCount.get() + ", expected " + totalExpectedCount);
      Assert.assertEquals("Possible deadlock", totalExpectedCount, totalCount.get());
      System.out.println("After assert");

      // attempt cleaning up (this is not in a finally, still needs some work)
      connectionProducer.close();
      connectionConsumerProducer.close();
      connectionConsumer.close();

      server.stop();
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}