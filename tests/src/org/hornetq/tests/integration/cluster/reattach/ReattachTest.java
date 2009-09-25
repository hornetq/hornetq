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

package org.hornetq.tests.integration.cluster.reattach;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.exception.HornetQException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.FailureListener;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.invm.InVMConnector;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

/**
 * 
 * A ReattachTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 4 Nov 2008 16:54:50
 *
 *
 */
public class ReattachTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(ReattachTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   private HornetQServer service;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /*
    * Test failure on connection, but server is still up so should immediately reconnect
    */
   public void testImmediateReattach() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = 1;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));
      
      sf.setRetryInterval(retryInterval);
      sf.setRetryIntervalMultiplier(retryMultiplier);
      sf.setReconnectAttempts(reconnectAttempts);
      sf.setUseReattach(true);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false);

      final int numIterations = 100;

      for (int j = 0; j < numIterations; j++)
      {
         ClientProducer producer = session.createProducer(ADDRESS);

         final int numMessages = 1000;

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session.createClientMessage(HornetQTextMessage.TYPE,
                                                                false,
                                                                0,
                                                                System.currentTimeMillis(),
                                                                (byte)1);
            message.putIntProperty(new SimpleString("count"), i);
            message.getBody().writeString("aardvarks");
            producer.send(message);
         }

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

         conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

         session.start();

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = consumer.receive(500);

            assertNotNull(message);

            assertEquals("aardvarks", message.getBody().readString());

            assertEquals(i, message.getProperty(new SimpleString("count")));

            message.acknowledge();
         }

         ClientMessage message = consumer.receiveImmediate();

         assertNull(message);

         producer.close();

         consumer.close();
      }

      session.close();

      sf.close();
   }

   /*
    * Test failure on connection, simulate failure to create connection for a while, then 
    * allow connection to be recreated
    */
   public void testDelayedReattach() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = -1;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));
      
      sf.setRetryInterval(retryInterval);
      sf.setRetryIntervalMultiplier(retryMultiplier);
      sf.setReconnectAttempts(reconnectAttempts);
      sf.setUseReattach(true);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      InVMConnector.failOnCreateConnection = true;

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      Thread t = new Thread()
      {
         public void run()
         {
            try
            {
               Thread.sleep(retryInterval * 3);
            }
            catch (InterruptedException ignore)
            {
            }

            InVMConnector.failOnCreateConnection = false;
         }
      };

      t.start();

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(500);

         assertNotNull(message);

         assertEquals("aardvarks", message.getBody().readString());

         assertEquals(i, message.getProperty(new SimpleString("count")));

         message.acknowledge();
      }

      ClientMessage message = consumer.receiveImmediate();

      assertNull(message);

      session.close();

      sf.close();

      t.join();
   }

   // Test an async (e.g. pinger) failure coming in while a connection manager is already reconnecting
   public void testAsyncFailureWhileReattaching() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = -1;

      final long asyncFailDelay = 2000;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));
      
      sf.setRetryInterval(retryInterval);
      sf.setRetryIntervalMultiplier(retryMultiplier);
      sf.setReconnectAttempts(reconnectAttempts);
      sf.setUseReattach(true);

      ClientSession session = sf.createSession(false, true, true);
           
      ClientSession session2 = sf.createSession(false, true, true);
      
      class MyFailureListener implements FailureListener
      {
         volatile boolean failed;
         
         public void connectionFailed(HornetQException me)
         {
            failed = true;
         }
      }
      
      MyFailureListener listener = new MyFailureListener();
      
      session2.addFailureListener(listener);

      session.createQueue(ADDRESS, ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      InVMConnector.numberOfFailures = 10;
      InVMConnector.failOnCreateConnection = true;

      //We need to fail on different connections.
      
      //We fail on one connection then the connection manager tries to reconnect all connections
      //Then we fail the other, and the connection  manager is then called while the reconnection is occurring
      //We can't use the same connection since RemotingConnectionImpl only allows one fail to be in process
      //at same time
      
      final RemotingConnection conn = ((ClientSessionInternal)session).getConnection();
      
      final RemotingConnection conn2 = ((ClientSessionInternal)session2).getConnection();
      
      assertTrue(conn != conn2);

      Thread t = new Thread()
      {
         public void run()
         {
            try
            {
               Thread.sleep(asyncFailDelay);
            }
            catch (InterruptedException ignore)
            {
            }
            
            log.info("calling fail async");

            conn2.fail(new HornetQException(HornetQException.NOT_CONNECTED, "Did not receive pong from server"));
         }
      };

      t.start();
      
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));
      
      assertTrue(listener.failed);
      
      session.start();            

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(500);

         assertNotNull(message);

         assertEquals("aardvarks", message.getBody().readString());

         assertEquals(i, message.getProperty(new SimpleString("count")));

         message.acknowledge();
      }

      ClientMessage message = consumer.receiveImmediate();

      assertNull(message);

      session.close();
      
      session2.close();

      sf.close();

      t.join();
   }

   public void testReattachAttemptsFailsToReconnect() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = 3;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));
      
      sf.setRetryInterval(retryInterval);
      sf.setRetryIntervalMultiplier(retryMultiplier);
      sf.setReconnectAttempts(reconnectAttempts);
      sf.setUseReattach(true);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      InVMConnector.failOnCreateConnection = true;

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Sleep for longer than max retries so should fail to reconnect

      Thread t = new Thread()
      {
         public void run()
         {
            try
            {
               Thread.sleep(retryInterval * (reconnectAttempts + 1));
            }
            catch (InterruptedException ignore)
            {
            }

            InVMConnector.failOnCreateConnection = false;
         }
      };

      t.start();

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      session.start();

      // Should be null since failed to reconnect
      ClientMessage message = consumer.receive(500);

      assertNull(message);

      session.close();

      sf.close();

      t.join();
   }

   public void testReattachAttemptsSucceedsInReconnecting() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = 10;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));
      
      sf.setRetryInterval(retryInterval);
      sf.setRetryIntervalMultiplier(retryMultiplier);
      sf.setReconnectAttempts(reconnectAttempts);
      sf.setUseReattach(true);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      InVMConnector.failOnCreateConnection = true;
      InVMConnector.numberOfFailures = reconnectAttempts - 1;

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(500);

         assertNotNull(message);

         assertEquals("aardvarks", message.getBody().readString());

         assertEquals(i, message.getProperty(new SimpleString("count")));

         message.acknowledge();
      }

      ClientMessage message = consumer.receiveImmediate();

      assertNull(message);

      session.close();

      sf.close();
   }

   public void testRetryInterval() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = -1;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));
      
      sf.setRetryInterval(retryInterval);
      sf.setRetryIntervalMultiplier(retryMultiplier);
      sf.setReconnectAttempts(reconnectAttempts);
      sf.setUseReattach(true);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      InVMConnector.failOnCreateConnection = true;

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      long start = System.currentTimeMillis();

      Thread t = new Thread()
      {
         public void run()
         {
            try
            {
               Thread.sleep(retryInterval / 2);
            }
            catch (InterruptedException ignore)
            {
            }
            InVMConnector.failOnCreateConnection = false;
         }
      };

      t.start();

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(500);

         assertNotNull(message);

         assertEquals("aardvarks", message.getBody().readString());

         assertEquals(i, message.getProperty(new SimpleString("count")));

         message.acknowledge();
      }

      ClientMessage message = consumer.receiveImmediate();

      assertNull(message);

      long end = System.currentTimeMillis();

      assertTrue((end - start) >= retryInterval);

      session.close();

      sf.close();

      t.join();
   }

   public void testExponentialBackoff() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 4d;

      final int reconnectAttempts = -1;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));
      
      sf.setRetryInterval(retryInterval);
      sf.setRetryIntervalMultiplier(retryMultiplier);
      sf.setReconnectAttempts(reconnectAttempts);
      sf.setUseReattach(true);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().writeString("aardvarks");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      InVMConnector.failOnCreateConnection = true;

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      long start = System.currentTimeMillis();

      Thread t = new Thread()
      {
         public void run()
         {
            try
            {
               Thread.sleep(retryInterval * 2);
            }
            catch (InterruptedException ignore)
            {
            }

            InVMConnector.failOnCreateConnection = false;
         }
      };

      t.start();

      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(500);

         assertNotNull(message);

         assertEquals("aardvarks", message.getBody().readString());

         assertEquals(i, message.getProperty(new SimpleString("count")));

         message.acknowledge();
      }

      ClientMessage message = consumer.receiveImmediate();

      assertNull(message);

      long end = System.currentTimeMillis();

      assertTrue((end - start) >= retryInterval * (1 + retryMultiplier));

      session.close();

      sf.close();

      t.join();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory"));
      service = HornetQ.newHornetQServer(liveConf, false);
      service.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      InVMConnector.resetFailures();

      service.stop();

      assertEquals(0, InVMRegistry.instance.size());
      
      service = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
