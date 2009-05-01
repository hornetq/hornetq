/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat
 * Middleware LLC, and individual contributors by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of individual
 * contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.tests.integration.cluster.failover;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryInternal;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.FailureListener;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnector;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

/**
 * 
 * A ReconnectTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 4 Nov 2008 16:54:50
 *
 *
 */
public class ReconnectTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(ReconnectTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   private MessagingServer service;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /*
    * Test failure on connection, but server is still up so should immediately reconnect
    */
   public void testImmediateReconnect() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = 1;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));
      
      sf.setRetryInterval(retryInterval);
      sf.setRetryIntervalMultiplier(retryMultiplier);
      sf.setReconnectAttempts(reconnectAttempts);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false);

      final int numIterations = 100;

      for (int j = 0; j < numIterations; j++)
      {
         ClientProducer producer = session.createProducer(ADDRESS);

         final int numMessages = 1000;

         for (int i = 0; i < numMessages; i++)
         {
            ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE,
                                                                false,
                                                                0,
                                                                System.currentTimeMillis(),
                                                                (byte)1);
            message.putIntProperty(new SimpleString("count"), i);
            message.getBody().writeString("aardvarks");
            producer.send(message);
         }

         ClientConsumer consumer = session.createConsumer(ADDRESS);

         RemotingConnection conn = ((ClientSessionImpl)session).getConnection();

         conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

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
   public void testDelayedReconnect() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = -1;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));
      
      sf.setRetryInterval(retryInterval);
      sf.setRetryIntervalMultiplier(retryMultiplier);
      sf.setReconnectAttempts(reconnectAttempts);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE,
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

      RemotingConnection conn = ((ClientSessionImpl)session).getConnection();

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

      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

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
   public void testAsyncFailureWhileReconnecting() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = -1;

      final long asyncFailDelay = 2000;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));
      
      sf.setRetryInterval(retryInterval);
      sf.setRetryIntervalMultiplier(retryMultiplier);
      sf.setReconnectAttempts(reconnectAttempts);

      ClientSession session = sf.createSession(false, true, true);
           
      ClientSession session2 = sf.createSession(false, true, true);
      
      class MyFailureListener implements FailureListener
      {
         volatile boolean failed;
         
         public boolean connectionFailed(MessagingException me)
         {
            failed = true;
            
            return true;
         }
      }
      
      MyFailureListener listener = new MyFailureListener();
      
      session2.addFailureListener(listener);

      session.createQueue(ADDRESS, ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE,
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
      
      final RemotingConnection conn = ((ClientSessionImpl)session).getConnection();
      
      final RemotingConnection conn2 = ((ClientSessionImpl)session2).getConnection();
      
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

            conn2.fail(new MessagingException(MessagingException.NOT_CONNECTED, "Did not receive pong from server"));
         }
      };

      t.start();
      
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));
      
      assertFalse(listener.failed);
      
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

   public void testReconnectAttemptsFailsToReconnect() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = 3;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));
      
      sf.setRetryInterval(retryInterval);
      sf.setRetryIntervalMultiplier(retryMultiplier);
      sf.setReconnectAttempts(reconnectAttempts);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE,
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

      RemotingConnection conn = ((ClientSessionImpl)session).getConnection();

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

      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

      session.start();

      // Should be null since failed to reconnect
      ClientMessage message = consumer.receive(500);

      assertNull(message);

      session.close();

      sf.close();

      t.join();
   }

   public void testReconnectAttemptsSucceedsInReconnecting() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = 10;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));
      
      sf.setRetryInterval(retryInterval);
      sf.setRetryIntervalMultiplier(retryMultiplier);
      sf.setReconnectAttempts(reconnectAttempts);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE,
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

      RemotingConnection conn = ((ClientSessionImpl)session).getConnection();

      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

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

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));
      
      sf.setRetryInterval(retryInterval);
      sf.setRetryIntervalMultiplier(retryMultiplier);
      sf.setReconnectAttempts(reconnectAttempts);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE,
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

      RemotingConnection conn = ((ClientSessionImpl)session).getConnection();

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

      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

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

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));
      
      sf.setRetryInterval(retryInterval);
      sf.setRetryIntervalMultiplier(retryMultiplier);
      sf.setReconnectAttempts(reconnectAttempts);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE,
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

      RemotingConnection conn = ((ClientSessionImpl)session).getConnection();

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

      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

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
              .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      service = Messaging.newMessagingServer(liveConf, false);
      service.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      InVMConnector.resetFailures();

      service.stop();

      assertEquals(0, InVMRegistry.instance.size());

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
