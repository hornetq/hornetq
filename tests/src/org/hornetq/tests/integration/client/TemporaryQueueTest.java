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

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.exception.MessagingException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.CloseListener;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.RemotingConnectionImpl;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.MessagingServer;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.SimpleString;

import static org.hornetq.tests.util.RandomUtil.randomSimpleString;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

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

   private MessagingServer server;

   private ClientSession session;

   private ClientSessionFactory sf;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testConsumeFromTemporaryQueue() throws Exception
   {
      SimpleString queue = randomSimpleString();
      SimpleString address = randomSimpleString();

      session.createTemporaryQueue(address, queue);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));

      session.start();
      ClientConsumer consumer = session.createConsumer(queue);
      ClientMessage message = consumer.receive(500);
      assertNotNull(message);
      message.acknowledge();

      consumer.close();
      session.deleteQueue(queue);

      session.close();
   }

   public void testConsumeFromTemporaryQueueCreatedByOtherSession() throws Exception
   {
      SimpleString queue = randomSimpleString();
      SimpleString address = randomSimpleString();

      session.createTemporaryQueue(address, queue);

      ClientProducer producer = session.createProducer(address);
      producer.send(session.createClientMessage(false));

      ClientSession session2 = sf.createSession(false, true, true);
      session2.start();

      ClientConsumer consumer = session2.createConsumer(queue);
      ClientMessage message = consumer.receive(500);
      assertNotNull(message);

      session2.close();
      session.close();
   }

   public void testDeleteTemporaryQueueAfterConnectionIsClosed() throws Exception
   {
      SimpleString queue = randomSimpleString();
      SimpleString address = randomSimpleString();

      session.createTemporaryQueue(address, queue);
      RemotingConnectionImpl conn = (RemotingConnectionImpl) server.getRemotingService().getConnections().iterator().next();

      final CountDownLatch latch = new CountDownLatch(1);
      conn.addCloseListener(new CloseListener()
      {
         public void connectionClosed()
         {
            latch.countDown();
         }
      });
      session.close();
      //wait for the closing listeners to be fired
      assertTrue("connection close listeners not fired", latch.await(2 * CONNECTION_TTL, TimeUnit.MILLISECONDS));
      session = sf.createSession(false, true, true);
      session.start();
      
      try
      {
         session.createConsumer(queue);
         fail("temp queue must not exist after the remoting connection is closed");
      }
      catch (MessagingException e)
      {
         assertEquals(MessagingException.QUEUE_DOES_NOT_EXIST, e.getCode());
      }

      session.close();
   }
   
   /**
    * @see org.hornetq.core.server.impl.ServerSessionImpl#doHandleCreateQueue(org.hornetq.core.remoting.impl.wireformat.CreateQueueMessage) 
    */
   public void testDeleteTemporaryQueueAfterConnectionIsClosed_2() throws Exception
   {
      SimpleString queue = randomSimpleString();
      SimpleString address = randomSimpleString();

      session.createTemporaryQueue(address, queue);
      assertEquals(1, server.getConnectionCount());
      
      // we create a second session. the temp queue must be present
      // even after we closed the session which created it
      ClientSession session2 = sf.createSession(false, true, true);

      session.close();

      // let some time for the server to clean the connections
      //Thread.sleep(1000);

      session2.start();
      
      ClientConsumer consumer = session2.createConsumer(queue);

      session2.close();
   }

   public void testDeleteTemporaryQueueWhenClientCrash() throws Exception
   {
      final SimpleString queue = randomSimpleString();
      SimpleString address = randomSimpleString();

      session.createTemporaryQueue(address, queue);

      assertEquals(1, server.getConnectionCount());

      RemotingConnection remotingConnection = server
                                                     .getRemotingService()
                                                     .getConnections()
                                                     .iterator()
                                                     .next();
      final CountDownLatch latch = new CountDownLatch(1);
      remotingConnection.addCloseListener(new CloseListener()
      {
         public void connectionClosed()
         {
            latch.countDown();
         }
      });
      
      ((ClientSessionInternal)session).getConnection().fail(new MessagingException(MessagingException.INTERNAL_ERROR, "simulate a client failure"));


      // let some time for the server to clean the connections
      latch.await(2 * CONNECTION_TTL + 1, TimeUnit.MILLISECONDS);
      Thread.sleep(5000);
      assertEquals(0, server.getConnectionCount());
      
      session.close();

      sf.close();
      sf = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()));
      session = sf.createSession(false, true, true);
      session.start();
      
      expectMessagingException("temp queue must not exist after the server detected the client crash", MessagingException.QUEUE_DOES_NOT_EXIST, new MessagingAction()
      {
         public void run() throws MessagingException
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
      server = createServer(false, configuration );
      server.start();

      sf = new ClientSessionFactoryImpl(new TransportConfiguration(INVM_CONNECTOR_FACTORY));
      sf.setConnectionTTL(CONNECTION_TTL);
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
