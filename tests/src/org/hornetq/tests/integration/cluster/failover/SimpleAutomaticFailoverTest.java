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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ClientSessionImpl;
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
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.remoting.spi.ConnectionLifeCycleListener;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

/**
 * 
 * A SimpleAutomaticFailoverTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SimpleAutomaticFailoverTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(SimpleAutomaticFailoverTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   private HornetQServer liveService;

   private HornetQServer backupService;

   private final Map<String, Object> backupParams = new HashMap<String, Object>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testReplication() throws Exception
   {
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));

      sf.setProducerWindowSize(32 * 1024);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

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

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().readString());
         assertEquals(i, message2.getProperty(new SimpleString("count")));

         message2.acknowledge();
      }

      ClientMessage message3 = consumer.receive(250);

      assertNull(message3);

      session.close();
   }

   public void testFailoverSameConnectionFactory() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));

      sf.setProducerWindowSize(32 * 1024);

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

      RemotingConnection conn1 = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn1.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < numMessages / 2; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().readString());

         assertEquals(i, message2.getProperty(new SimpleString("count")));

         message2.acknowledge();
      }

      session.close();

      session = sf.createSession(false, true, true);

      consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = numMessages / 2; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().readString());

         assertEquals(i, message2.getProperty(new SimpleString("count")));

         message2.acknowledge();
      }

      ClientMessage message3 = consumer.receive(250);

      session.close();

      assertNull(message3);

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testFailoverChangeConnectionFactory() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));

      sf.setProducerWindowSize(32 * 1024);

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

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < numMessages / 2; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().readString());

         assertEquals(i, message2.getProperty(new SimpleString("count")));

         message2.acknowledge();
      }

      session.close();

      sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                   backupParams));

      session = sf.createSession(false, true, true);

      consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = numMessages / 2; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().readString());

         assertEquals(i, message2.getProperty(new SimpleString("count")));

         message2.acknowledge();
      }

      ClientMessage message3 = consumer.receive(250);

      assertNull(message3);

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testNoMessagesLeftAfterFailoverNewSession() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));

      sf.setProducerWindowSize(32 * 1024);

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

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().readString());

         assertEquals(i, message2.getProperty(new SimpleString("count")));

         message2.acknowledge();
      }

      session.close();

      sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                   backupParams));

      session = sf.createSession(false, true, true);

      consumer = session.createConsumer(ADDRESS);

      ClientMessage message3 = consumer.receive(250);

      assertNull(message3);

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testCreateMoreSessionsAfterFailover() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));

      sf.setProducerWindowSize(32 * 1024);

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

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().readString());

         assertEquals(i, message2.getProperty(new SimpleString("count")));

         message2.acknowledge();
      }

      sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                   backupParams));

      ClientSession session1 = sf.createSession(false, true, true);

      ClientSession session2 = sf.createSession(false, true, true);

      ClientSession session3 = sf.createSession(false, true, true);

      session.close();

      session1.close();

      session2.close();

      session3.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testFailoverMultipleSessions() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));

      sf.setProducerWindowSize(32 * 1024);

      final int numSessions = ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS * 2;

      List<ClientSession> sessions = new ArrayList<ClientSession>();

      List<ClientConsumer> consumers = new ArrayList<ClientConsumer>();

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession sess = sf.createSession(false, true, true);

         SimpleString queueName = new SimpleString("subscription" + i);

         sess.createQueue(ADDRESS, queueName, null, false);

         ClientConsumer consumer = sess.createConsumer(queueName);

         sess.start();

         sessions.add(sess);

         consumers.add(consumer);
      }

      ClientSession session = sf.createSession(false, true, true);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

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

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      for (int i = 0; i < numSessions; i++)
      {
         ClientConsumer cons = consumers.get(i);

         for (int j = 0; j < numMessages; j++)
         {
            ClientMessage message2 = cons.receive();

            assertEquals("aardvarks", message2.getBody().readString());

            assertEquals(j, message2.getProperty(new SimpleString("count")));

            message2.acknowledge();
         }

         ClientMessage message3 = cons.receive(250);

         assertNull(message3);
      }

      session.close();

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession sess = sessions.get(i);

         sess.close();
      }

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testAllConnectionsReturned() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));

      sf.setProducerWindowSize(32 * 1024);

      final int numSessions = ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS * 2;

      List<ClientSession> sessions = new ArrayList<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession sess = sf.createSession(false, true, true);

         sessions.add(sess);
      }

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession sess = sessions.get(i);

         sess.close();
      }

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testAllConnectionsReturnedAfterFailover() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));

      sf.setProducerWindowSize(32 * 1024);

      final int numSessions = ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS * 2;

      List<ClientSession> sessions = new ArrayList<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession sess = sf.createSession(false, true, true);

         sessions.add(sess);
      }

      RemotingConnection conn = ((ClientSessionInternal)sessions.get(0)).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession sess = sessions.get(i);

         sess.close();
      }

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testFailureAfterFailover() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 1d;

      final int reconnectAttempts = 0;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));
      
      
      sf.setFailoverOnServerShutdown(true);
      sf.setRetryInterval(retryInterval);
      sf.setRetryIntervalMultiplier(retryMultiplier);
      sf.setReconnectAttempts(reconnectAttempts);

      sf.setProducerWindowSize(32 * 1024);

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

      RemotingConnection conn = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      // Consume half of them

      for (int i = 0; i < numMessages / 2; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().readString());

         assertEquals(i, message2.getProperty(new SimpleString("count")));

         message2.acknowledge();
      }

      RemotingConnection conn2 = ((ClientSessionInternal)session).getConnection();

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements FailureListener
      {
         public void connectionFailed(final HornetQException me)
         {
            latch.countDown();
         }
      }

      conn2.addFailureListener(new MyListener());

      assertFalse(conn == conn2);

      InVMConnector.failOnCreateConnection = true;
      conn2.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      try
      {
         session.createQueue(new SimpleString("blah"), new SimpleString("blah"), null, false);

         fail("Should throw exception");
      }
      catch (HornetQException me)
      {
         assertEquals(HornetQException.NOT_CONNECTED, me.getCode());
      }

      session.close();

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   public void testFailoverOnCreateSession() throws Exception
   {
      stopServers();

      for (int j = 0; j < 10; j++)
      {
         startServers();

         ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                        new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                   backupParams));
         
         
         sf.setFailoverOnServerShutdown(true);
         sf.setRetryInterval(100);
         sf.setRetryIntervalMultiplier(1);
         sf.setReconnectAttempts(-1);
         sf.setProducerWindowSize(32 * 1024);

         for (int i = 0; i < 10; i++)
         {
            // We test failing on the 0th connection created, then the first, then the second etc, to make sure they are
            // all failed over ok
            if (i == j)
            {
               InVMConnector.numberOfFailures = 1;
               InVMConnector.failOnCreateConnection = true;
            }

            ClientSession session = sf.createSession(false, true, true);

            session.createQueue(ADDRESS, ADDRESS, null, false);

            ClientProducer producer = session.createProducer(ADDRESS);

            final int numMessages = 10;

            for (int k = 0; k < numMessages; k++)
            {
               ClientMessage message = session.createClientMessage(HornetQTextMessage.TYPE,
                                                                   false,
                                                                   0,
                                                                   System.currentTimeMillis(),
                                                                   (byte)1);
               message.putIntProperty(new SimpleString("count"), k);
               message.getBody().writeString("aardvarks");
               producer.send(message);
            }

            ClientConsumer consumer = session.createConsumer(ADDRESS);

            session.start();

            for (int k = 0; k < numMessages; k++)
            {
               ClientMessage message2 = consumer.receive();

               assertEquals("aardvarks", message2.getBody().readString());

               assertEquals(k, message2.getProperty(new SimpleString("count")));

               message2.acknowledge();
            }

            consumer.close();

            session.deleteQueue(ADDRESS);

            session.close();
         }

         assertEquals(0, sf.numSessions());

         assertEquals(0, sf.numConnections());

         sf.close();

         stopServers();
      }
   }

   public void testFailoverWithNotifications() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));

      sf.setProducerWindowSize(32 * 1024);

      ClientSession sess = sf.createSession(false, true, true);

      sess.createQueue("hornetq.notifications", "notifqueue", false);

      ClientConsumer cons = sess.createConsumer("notifqueue");

      sess.start();

      sess.createQueue("blah", "blah", false);
      sess.createQueue("blah", "blah2", false);

      ClientMessage msg = cons.receive(1000);
      assertNotNull(msg);
      msg.acknowledge();

      msg = cons.receive(1000);
      assertNotNull(msg);
      msg.acknowledge();

      sess.stop();

      sess.createQueue("blah", "blah3", false);
      sess.createQueue("blah", "blah4", false);

      RemotingConnection conn = ((ClientSessionInternal)sess).getConnection();

      // Simulate failure on connection
      conn.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      sess.start();

      msg = cons.receive(1000);
      assertNotNull(msg);
      msg.acknowledge();

      msg = cons.receive(1000);
      assertNotNull(msg);
      msg.acknowledge();

      sess.close();

   }

   /*
    * When a real connection fails due to the server actually dying, the backup server will receive 
    * a connection exception on the server side, since the live server has died taking the replicating
    * connection with it.
    * We cannot just fail the connection on the server side when this happens since this will cause the session
    * on the backup to be closed, so clients won't be able to re-attach.
    * This test verifies that server session is not closed on server side connection failure.
    */
   public void testFailoverFailBothOnClientAndServerSide() throws Exception
   {
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));

      sf.setProducerWindowSize(32 * 1024);

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

      RemotingConnection conn1 = ((ClientSessionInternal)session).getConnection();

      // Simulate failure on connection
      // We fail on the replicating connection and the client connection

      HornetQException me = new HornetQException(HornetQException.NOT_CONNECTED);
      
      //Note we call the remoting service impl handler which is what would happen in event
      //of real connection failure
      
      RemotingConnection serverSideReplicatingConnection = backupService.getRemotingService()
                                                                        .getServerSideReplicatingConnection();
      
            
      ((ConnectionLifeCycleListener)backupService.getRemotingService()).connectionException(serverSideReplicatingConnection.getID(), me);

      conn1.fail(new HornetQException(HornetQException.NOT_CONNECTED));

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < numMessages / 2; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().readString());

         assertEquals(i, message2.getProperty(new SimpleString("count")));

         message2.acknowledge();
      }

      session.close();

      session = sf.createSession(false, true, true);

      consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = numMessages / 2; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().readString());

         assertEquals(i, message2.getProperty(new SimpleString("count")));

         message2.acknowledge();
      }

      ClientMessage message3 = consumer.receive(250);

      session.close();

      assertNull(message3);

      assertEquals(0, sf.numSessions());

      assertEquals(0, sf.numConnections());
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void startServers() throws Exception
   {
      Configuration backupConf = new ConfigurationImpl();
      backupConf.setSecurityEnabled(false);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory",
                                                backupParams));
      backupConf.setBackup(true);
      backupService = HornetQ.newHornetQServer(backupConf, false);
      backupService.start();

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory"));
      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      TransportConfiguration backupTC = new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                   backupParams,
                                                                   "backup-connector");
      connectors.put(backupTC.getName(), backupTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());
      liveService = HornetQ.newHornetQServer(liveConf, false);
      liveService.start();
   }

   protected void stopServers() throws Exception
   {
      if (backupService.isStarted())
      {
         backupService.stop();
      }

      if (liveService.isStarted())
      {
         liveService.stop();
      }

      assertEquals(0, InVMRegistry.instance.size());
   }

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      startServers();
   }

   @Override
   protected void tearDown() throws Exception
   {
      stopServers();

      InVMConnector.resetFailures();
      
      liveService = null;
      
      backupService = null;

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
