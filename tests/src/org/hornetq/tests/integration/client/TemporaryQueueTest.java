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

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.protocol.core.PacketImpl;
import org.hornetq.core.protocol.core.RemotingConnectionImpl;
import org.hornetq.core.remoting.CloseListener;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.server.impl.RemotingServiceImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A TemporaryQueueTest
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class TemporaryQueueTest extends ServiceTestBase
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(TemporaryQueueTest.class);

   private static final long CONNECTION_TTL = 2000;

   // Attributes ----------------------------------------------------

   private HornetQServer server;

   private ClientSession session;

   private ClientSessionFactory sf;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testConsumeFromTemporaryQueue() throws Exception
   {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();

      session.createTemporaryQueue(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage msg = session.createMessage(false);

      producer.send(msg);

      session.start();
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = consumer.receive(500);
      Assert.assertNotNull(message);
      message.acknowledge();

      consumer.close();
      session.deleteQueue(queue);

      session.close();
   }
   
   public void testPaginStoreIsRemovedWhenQueueIsDeleted() throws Exception
   {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();

      session.createTemporaryQueue(address, queue);

      ClientProducer producer = session.createProducer(address);
      ClientMessage msg = session.createMessage(false);

      producer.send(msg);

      session.start();
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = consumer.receive(500);
      Assert.assertNotNull(message);
      message.acknowledge();

      SimpleString[] storeNames = server.getPostOffice().getPagingManager().getStoreNames();
      assertTrue(Arrays.asList(storeNames).contains(address));      

      consumer.close();
      session.deleteQueue(queue);

      storeNames = server.getPostOffice().getPagingManager().getStoreNames();
      assertFalse(Arrays.asList(storeNames).contains(address));

      session.close();
   }
   
   public void testConsumeFromTemporaryQueueCreatedByOtherSession() throws Exception
   {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();

      session.createTemporaryQueue(address, queue);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createMessage(false));

      ClientSession session2 = sf.createSession(false, true, true);
      session2.start();

      ClientConsumer consumer = session2.createConsumer(queue);
      ClientMessage message = consumer.receive(500);
      Assert.assertNotNull(message);

      session2.close();
      session.close();
   }

   public void testDeleteTemporaryQueueAfterConnectionIsClosed() throws Exception
   {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();

      session.createTemporaryQueue(address, queue);
      RemotingConnectionImpl conn = (RemotingConnectionImpl)server.getRemotingService()
                                                                  .getConnections()
                                                                  .iterator()
                                                                  .next();

      final CountDownLatch latch = new CountDownLatch(1);
      conn.addCloseListener(new CloseListener()
      {
         public void connectionClosed()
         {
            latch.countDown();
         }
      });
      session.close();
      // wait for the closing listeners to be fired
      Assert.assertTrue("connection close listeners not fired", latch.await(2 * TemporaryQueueTest.CONNECTION_TTL,
                                                                            TimeUnit.MILLISECONDS));
      session = sf.createSession(false, true, true);
      session.start();

      try
      {
         session.createConsumer(queue);
         Assert.fail("temp queue must not exist after the remoting connection is closed");
      }
      catch (HornetQException e)
      {
         Assert.assertEquals(HornetQException.QUEUE_DOES_NOT_EXIST, e.getCode());
      }

      session.close();
   }

   /**
    * @see org.hornetq.core.server.impl.ServerSessionImpl#doHandleCreateQueue(org.hornetq.core.remoting.impl.wireformat.CreateQueueMessage) 
    */
   public void testDeleteTemporaryQueueAfterConnectionIsClosed_2() throws Exception
   {
      SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();

      session.createTemporaryQueue(address, queue);
      Assert.assertEquals(1, server.getConnectionCount());

      // we create a second session. the temp queue must be present
      // even after we closed the session which created it
      ClientSession session2 = sf.createSession(false, true, true);

      session.close();

      // let some time for the server to clean the connections
      // Thread.sleep(1000);

      session2.start();

      ClientConsumer consumer = session2.createConsumer(queue);

      session2.close();
   }

   public void testDeleteTemporaryQueueWhenClientCrash() throws Exception
   {
      session.close();

      final SimpleString queue = RandomUtil.randomSimpleString();
      SimpleString address = RandomUtil.randomSimpleString();

      // server must received at least one ping from the client to pass
      // so that the server connection TTL is configured with the client value
      final CountDownLatch pingOnServerLatch = new CountDownLatch(1);
      server.getRemotingService().addInterceptor(new Interceptor()
      {

         public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
         {
            if (packet.getType() == PacketImpl.PING)
            {
               pingOnServerLatch.countDown();
            }
            return true;
         }
      });

      sf = HornetQClient.createClientSessionFactory(new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY));
      sf.setConnectionTTL(TemporaryQueueTest.CONNECTION_TTL);
      session = sf.createSession(false, true, true);

      session.createTemporaryQueue(address, queue);
      Assert.assertTrue("server has not received any ping from the client",
                        pingOnServerLatch.await(2 * RemotingServiceImpl.CONNECTION_TTL_CHECK_INTERVAL,
                                                TimeUnit.MILLISECONDS));
      Assert.assertEquals(1, server.getConnectionCount());

      RemotingConnection remotingConnection = server.getRemotingService().getConnections().iterator().next();
      final CountDownLatch serverCloseLatch = new CountDownLatch(1);
      remotingConnection.addCloseListener(new CloseListener()
      {
         public void connectionClosed()
         {
            serverCloseLatch.countDown();
         }
      });

      ((ClientSessionInternal)session).getConnection().fail(new HornetQException(HornetQException.INTERNAL_ERROR,
                                                                                 "simulate a client failure"));

      // let some time for the server to clean the connections
      Assert.assertTrue("server has not closed the connection",
                        serverCloseLatch.await(2 * RemotingServiceImpl.CONNECTION_TTL_CHECK_INTERVAL +
                                               2 *
                                               TemporaryQueueTest.CONNECTION_TTL, TimeUnit.MILLISECONDS));
      Assert.assertEquals(0, server.getConnectionCount());

      session.close();

      sf.close();
      sf = HornetQClient.createClientSessionFactory(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      session = sf.createSession(false, true, true);
      session.start();

      UnitTestCase.expectHornetQException("temp queue must not exist after the server detected the client crash",
                                          HornetQException.QUEUE_DOES_NOT_EXIST,
                                          new HornetQAction()
                                          {
                                             public void run() throws HornetQException
                                             {
                                                session.createConsumer(queue);
                                             }
                                          });

      session.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration configuration = createDefaultConfig();
      configuration.setSecurityEnabled(false);
      server = createServer(false, configuration);
      server.start();

      sf = HornetQClient.createClientSessionFactory(new TransportConfiguration(ServiceTestBase.INVM_CONNECTOR_FACTORY));
      sf.setConnectionTTL(TemporaryQueueTest.CONNECTION_TTL);
      session = sf.createSession(false, true, true);
   }

   @Override
   protected void tearDown() throws Exception
   {

      sf.close();

      session.close();

      server.stop();

      session = null;

      server = null;

      sf = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
