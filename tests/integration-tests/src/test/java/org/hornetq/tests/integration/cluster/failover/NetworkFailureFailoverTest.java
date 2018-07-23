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

package org.hornetq.tests.integration.cluster.failover;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.client.SessionFailureListener;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.impl.wireformat.SessionSendMessage;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.util.NetUtil;
import org.hornetq.tests.util.NetUtilResource;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

public class NetworkFailureFailoverTest extends FailoverTestBase
{
   @Rule
   public NetUtilResource netUtilResource = new NetUtilResource();

   @BeforeClass
   public static void start()
   {
      NetUtil.assumeSudo();
   }

   // 192.0.2.0 is reserved for documentation, so I'm pretty sure this won't exist on any system. (It shouldn't at least)
   private static final String LIVE_IP = "192.0.2.0";

   @BeforeClass
   public static void setupDevices() throws Exception
   {
      NetUtil.netUp(LIVE_IP);
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live)
   {
      return getNettyAcceptorTransportConfiguration(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live)
   {
      return getNettyConnectorTransportConfiguration(live);
   }

   protected ClientSession createSession(ClientSessionFactory sf1,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks,
                                         int ackBatchSize) throws Exception
   {
      return addClientSession(sf1.createSession(autoCommitSends, autoCommitAcks, ackBatchSize));
   }

   protected ClientSession
   createSession(ClientSessionFactory sf1, boolean autoCommitSends, boolean autoCommitAcks) throws Exception
   {
      return addClientSession(sf1.createSession(autoCommitSends, autoCommitAcks));
   }

   protected ClientSession createSession(ClientSessionFactory sf1) throws Exception
   {
      return addClientSession(sf1.createSession());
   }

   protected ClientSession createSession(ClientSessionFactory sf1,
                                         boolean xa,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks) throws Exception
   {
      return addClientSession(sf1.createSession(xa, autoCommitSends, autoCommitAcks));
   }


   protected TransportConfiguration getNettyAcceptorTransportConfiguration(final boolean live)
   {
      Map<String, Object> server1Params = new HashMap<String, Object>();

      if (live)
      {
         server1Params.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME, org.hornetq.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT);
         server1Params.put(TransportConstants.HOST_PROP_NAME, LIVE_IP);
      }
      else
      {
         server1Params.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME, org.hornetq.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT);
         server1Params.put(TransportConstants.HOST_PROP_NAME, "localhost");
      }


      return new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, server1Params);
   }

   protected TransportConfiguration getNettyConnectorTransportConfiguration(final boolean live)
   {
      Map<String, Object> server1Params = new HashMap<String, Object>();

      if (live)
      {
         server1Params.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME, org.hornetq.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT);
         server1Params.put(TransportConstants.HOST_PROP_NAME, LIVE_IP);
      }
      else
      {
         server1Params.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME, org.hornetq.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT);
         server1Params.put(TransportConstants.HOST_PROP_NAME, "localhost");
      }

      return new TransportConfiguration(NETTY_CONNECTOR_FACTORY, server1Params);
   }


   @Test
   public void testFailoverAfterNetFailure() throws Exception
   {
      final AtomicInteger sentMessages = new AtomicInteger(0);
      final AtomicInteger blockedAt = new AtomicInteger(0);

      Assert.assertTrue(NetUtil.checkIP(LIVE_IP));
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.HOST_PROP_NAME, LIVE_IP);
      TransportConfiguration tc = createTransportConfiguration(true, false, params);

      final AtomicInteger countSent = new AtomicInteger(0);

      liveServer.addInterceptor(new Interceptor()
      {
         @Override
         public boolean intercept(Packet packet, RemotingConnection connection) throws HornetQException
         {
            //System.out.println("Received " + packet);
            if (packet instanceof SessionSendMessage)
            {

               if (countSent.incrementAndGet() == 500)
               {
                  try
                  {
                     NetUtil.netDown(LIVE_IP);
                     System.out.println("Blocking traffic");
                     Thread.sleep(3000); // this is important to let stuff to block
                     blockedAt.set(sentMessages.get());
                  }
                  catch (Exception e)
                  {
                     e.printStackTrace();
                  }
                  new Thread()
                  {
                     public void run()
                     {
                        try
                        {
                           System.err.println("Stopping server");
                           // liveServer.stop();
                           liveServer.crash(true, false);
                        }
                        catch (Exception e)
                        {
                           e.printStackTrace();
                        }
                     }
                  }.start();
               }
            }
            return true;
         }
      });

      ServerLocator locator = addServerLocator(HornetQClient.createServerLocatorWithHA(tc));

      locator.setBlockOnNonDurableSend(false);
      locator.setBlockOnDurableSend(false);
      locator.setBlockOnAcknowledge(false);
      locator.setReconnectAttempts(-1);
      locator.setConfirmationWindowSize(-1);
      locator.setProducerWindowSize(-1);
      locator.setClientFailureCheckPeriod(100);
      locator.setConnectionTTL(1000);
      ClientSessionFactoryInternal sfProducer = createSessionFactoryAndWaitForTopology(locator, 2);
      sfProducer.addFailureListener(new SessionFailureListener()
      {
         @Override
         public void beforeReconnect(HornetQException exception)
         {
            new Exception("producer before reconnect", exception).printStackTrace();
         }

         @Override
         public void connectionFailed(HornetQException exception, boolean failedOver)
         {

         }
      });

      ClientSession sessionProducer = createSession(sfProducer, true, true, 0);

      sessionProducer.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = sessionProducer.createProducer(FailoverTestBase.ADDRESS);

      final int numMessages = 10000;
      final CountDownLatch latchReceived = new CountDownLatch(numMessages);


      ClientSessionFactoryInternal sfConsumer = createSessionFactoryAndWaitForTopology(locator, 2);

      final ClientSession sessionConsumer = createSession(sfConsumer, true, true, 0);
      final ClientConsumer consumer = sessionConsumer.createConsumer(FailoverTestBase.ADDRESS);

      sessionConsumer.start();

      final AtomicBoolean running = new AtomicBoolean(true);
      final


      Thread t = new Thread()
      {
         public void run()
         {
            int received = 0;
            int errors = 0;
            while (running.get() && received < numMessages)
            {
               try
               {
                  ClientMessage msgReceived = consumer.receive(5000);
                  if (msgReceived != null)
                  {
                     latchReceived.countDown();
                     msgReceived.acknowledge();
                     if (received++ % 100 == 0)
                     {
                        System.out.println("Received " + received);
                        sessionConsumer.commit();
                     }
                  }
               }
               catch (Throwable e)
               {
                  errors++;
                  if (errors > 10)
                  {
                     break;
                  }
                  e.printStackTrace();
               }
            }
         }
      };

      t.start();

      for (sentMessages.set(0); sentMessages.get() < numMessages; sentMessages.incrementAndGet())
      {
         do
         {
            try
            {
               if (sentMessages.get() % 100 == 0)
               {
                  System.out.println("Sent " + sentMessages.get());
               }
               producer.send(createMessage(sessionProducer, sentMessages.get(), true));
               break;
            }
            catch (Exception e)
            {
               new Exception("Exception on ending", e).printStackTrace();
            }
         }
         while (true);
      }

      // these may never be received. doing the count down where we blocked.
      for (int i = 0; i < blockedAt.get(); i++)
      {
         latchReceived.countDown();
      }

      Assert.assertTrue(latchReceived.await(1, TimeUnit.MINUTES));

      running.set(false);

      t.join();
   }

}
