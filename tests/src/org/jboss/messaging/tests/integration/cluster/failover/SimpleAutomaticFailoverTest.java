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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
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
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

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

   private MessagingServer liveService;

   private MessagingServer backupService;

   private final Map<String, Object> backupParams = new HashMap<String, Object>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testReplication() throws Exception
   {
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));

      sf.setSendWindowSize(32 * 1024);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 100;

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
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));

      sf.setSendWindowSize(32 * 1024);

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

      RemotingConnection conn1 = ((ClientSessionImpl)session).getConnection();

      // Simulate failure on connection
      conn1.fail(new MessagingException(MessagingException.NOT_CONNECTED));

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
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));

      sf.setSendWindowSize(32 * 1024);

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

      RemotingConnection conn = ((ClientSessionImpl)session).getConnection();

      // Simulate failure on connection
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

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

      sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
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
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));

      sf.setSendWindowSize(32 * 1024);

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

      RemotingConnection conn = ((ClientSessionImpl)session).getConnection();

      // Simulate failure on connection
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

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

      sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
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
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));

      sf.setSendWindowSize(32 * 1024);

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

      RemotingConnection conn = ((ClientSessionImpl)session).getConnection();

      // Simulate failure on connection
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().readString());

         assertEquals(i, message2.getProperty(new SimpleString("count")));

         message2.acknowledge();
      }

      sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
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
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));

      sf.setSendWindowSize(32 * 1024);

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
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().writeString("aardvarks");
         producer.send(message);
      }

      RemotingConnection conn = ((ClientSessionImpl)session).getConnection();

      // Simulate failure on connection
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

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
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));

      sf.setSendWindowSize(32 * 1024);

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
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));

      sf.setSendWindowSize(32 * 1024);

      final int numSessions = ClientSessionFactoryImpl.DEFAULT_MAX_CONNECTIONS * 2;

      List<ClientSession> sessions = new ArrayList<ClientSession>();

      for (int i = 0; i < numSessions; i++)
      {
         ClientSession sess = sf.createSession(false, true, true);

         sessions.add(sess);
      }

      RemotingConnection conn = ((ClientSessionImpl)sessions.get(0)).getConnection();

      // Simulate failure on connection
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

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

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams),
                                                                     true,
                                                                     retryInterval,
                                                                     retryMultiplier,
                                                                     reconnectAttempts);

      sf.setSendWindowSize(32 * 1024);

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

      RemotingConnection conn = ((ClientSessionImpl)session).getConnection();

      // Simulate failure on connection
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

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

      RemotingConnection conn2 = ((ClientSessionImpl)session).getConnection();

      final CountDownLatch latch = new CountDownLatch(1);

      class MyListener implements FailureListener
      {
         public boolean connectionFailed(final MessagingException me)
         {
            latch.countDown();

            return true;
         }
      }

      conn2.addFailureListener(new MyListener());

      assertFalse(conn == conn2);

      InVMConnector.failOnCreateConnection = true;
      conn2.fail(new MessagingException(MessagingException.NOT_CONNECTED));

      boolean ok = latch.await(1000, TimeUnit.MILLISECONDS);

      assertTrue(ok);

      try
      {
         session.createQueue(new SimpleString("blah"), new SimpleString("blah"), null, false);

         fail("Should throw exception");
      }
      catch (MessagingException me)
      {
         assertEquals(MessagingException.NOT_CONNECTED, me.getCode());
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

         ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                        new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                   backupParams),
                                                                        true,
                                                                        100,
                                                                        1,
                                                                        -1);

         sf.setSendWindowSize(32 * 1024);

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
               ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE,
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
      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams));

      sf.setSendWindowSize(32 * 1024);

      ClientSession sess = sf.createSession(false, true, true);

      sess.createQueue("jbm.notifications", "notifqueue", false);

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

      RemotingConnection conn = ((ClientSessionImpl)sess).getConnection();

      // Simulate failure on connection
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

      sess.start();

      msg = cons.receive(1000);
      assertNotNull(msg);
      msg.acknowledge();

      msg = cons.receive(1000);
      assertNotNull(msg);
      msg.acknowledge();

      sess.close();

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void startServers() throws Exception
   {
      Configuration backupConf = new ConfigurationImpl();
      backupConf.setSecurityEnabled(false);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory",
                                                backupParams));
      backupConf.setBackup(true);
      backupService = Messaging.newNullStorageMessagingServer(backupConf);
      backupService.start();

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      TransportConfiguration backupTC = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                   backupParams,
                                                                   "backup-connector");
      connectors.put(backupTC.getName(), backupTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());
      liveService = Messaging.newNullStorageMessagingServer(liveConf);
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

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
