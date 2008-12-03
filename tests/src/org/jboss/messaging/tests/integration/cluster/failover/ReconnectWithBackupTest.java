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

import java.util.HashMap;
import java.util.Map;

import junit.framework.TestCase;

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
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.RemotingConnectionImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A ReconnectWithBackupTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 4 Nov 2008 16:54:50
 *
 *
 */
public class ReconnectWithBackupTest extends TestCase
{
   private static final Logger log = Logger.getLogger(ReconnectWithBackupTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   private MessagingService liveService;

   private MessagingService backupService;

   private final Map<String, Object> backupParams = new HashMap<String, Object>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /*
    * Test retrying reconnect on current node before failover
    */
   public void testRetryBeforeFailoverSuccess() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 1d;

      final int maxRetriesBeforeFailover = 3;

      final int maxRetriesAfterFailover = 0;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams),
                                                                     retryInterval,
                                                                     retryMultiplier,
                                                                     maxRetriesBeforeFailover,
                                                                     maxRetriesAfterFailover);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false, false, true);

      final int numIterations = 100;
      
      // We reconnect in a loop a few times
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
            message.getBody().putString("aardvarks");
            message.getBody().flip();
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

            assertEquals("aardvarks", message.getBody().getString());

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

   public void testFailoverThenFailAgainThenSuccessAfterRetry() throws Exception
   {
      final long retryInterval = 500;

      final double retryMultiplier = 1d;

      final int maxRetriesBeforeFailover = 0;

      final int maxRetriesAfterFailover = 3;

      ClientSessionFactoryInternal sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                     new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                backupParams),
                                                                     retryInterval,
                                                                     retryMultiplier,
                                                                     maxRetriesBeforeFailover,
                                                                     maxRetriesAfterFailover);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false, false, true);

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
         message.getBody().putString("aardvarks");
         message.getBody().flip();
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

         assertEquals("aardvarks", message.getBody().getString());

         assertEquals(i, message.getProperty(new SimpleString("count")));

         message.acknowledge();
      }

      ClientMessage message = consumer.receiveImmediate();

      assertNull(message);

      session.stop();

      final int numIterations = 10;
      
      for (int j = 0; j < numIterations; j++)
      {

         // Send some more messages

         for (int i = numMessages; i < numMessages * 2; i++)
         {
            message = session.createClientMessage(JBossTextMessage.TYPE, false, 0, System.currentTimeMillis(), (byte)1);
            message.putIntProperty(new SimpleString("count"), i);
            message.getBody().putString("aardvarks");
            message.getBody().flip();
            producer.send(message);
         }

         // Now fail again - should reconnect to the backup node

         conn = ((ClientSessionImpl)session).getConnection();

         conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));

         session.start();

         for (int i = numMessages; i < numMessages * 2; i++)
         {
            message = consumer.receive(500);

            assertNotNull(message);

            assertEquals("aardvarks", message.getBody().getString());

            assertEquals(i, message.getProperty(new SimpleString("count")));

            message.acknowledge();
         }

         message = consumer.receiveImmediate();

         assertNull(message);
      }

      session.close();

      sf.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      Configuration backupConf = new ConfigurationImpl();
      backupConf.setSecurityEnabled(false);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory",
                                                backupParams));
      backupConf.setBackup(true);
      backupService = MessagingServiceImpl.newNullStorageMessagingServer(backupConf);
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
      liveService = MessagingServiceImpl.newNullStorageMessagingServer(liveConf);
      liveService.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      assertEquals(0, backupService.getServer().getRemotingService().getConnections().size());

      backupService.stop();

      assertEquals(0, liveService.getServer().getRemotingService().getConnections().size());

      liveService.stop();

      assertEquals(0, InVMRegistry.instance.size());
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
