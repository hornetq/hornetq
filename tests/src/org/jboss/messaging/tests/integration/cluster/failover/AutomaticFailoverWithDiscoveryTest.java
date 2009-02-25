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

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.cluster.BroadcastGroupConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.Pair;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A AutomaticFailoverWithDiscoveryTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 8 Dec 2008 14:52:21
 *
 *
 */
public class AutomaticFailoverWithDiscoveryTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(AutomaticFailoverWithDiscoveryTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   private MessagingService liveService;

   private MessagingService backupService;

   private final Map<String, Object> backupParams = new HashMap<String, Object>();
   
   private final String groupAddress = "230.1.2.3";

   private final int groupPort = 8765;


   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testFailover() throws Exception
   {
      ClientSessionFactoryImpl sf = new ClientSessionFactoryImpl(groupAddress, groupPort);

      ClientSession session = sf.createSession(false, true, true);
      
      log.info("Created session");

      session.createQueue(ADDRESS, ADDRESS, null, false, false);

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

      RemotingConnection conn1 = ((ClientSessionImpl)session).getConnection();

      // Simulate failure on connection
      conn1.fail(new MessagingException(MessagingException.NOT_CONNECTED));

      ClientConsumer consumer = session.createConsumer(ADDRESS);

      session.start();

      for (int i = 0; i < numMessages / 2; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().getString());

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

         assertEquals("aardvarks", message2.getBody().getString());

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

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      
      Configuration backupConf = new ConfigurationImpl();
      backupConf.setSecurityEnabled(false);
      backupConf.setClustered(true);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory",
                                                backupParams));
      backupConf.setBackup(true);
      backupService = Messaging.newNullStorageMessagingService(backupConf);
      backupService.start();

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setSecurityEnabled(false);
      TransportConfiguration liveTC = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory");
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      TransportConfiguration backupTC = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                   backupParams);
      connectors.put(backupTC.getName(), backupTC);
      connectors.put(liveTC.getName(), liveTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());
      liveConf.setClustered(true);
      
      List<Pair<String, String>> connectorNames = new ArrayList<Pair<String, String>>();
      connectorNames.add(new Pair<String, String>(liveTC.getName(), backupTC.getName()));
      
      final long broadcastPeriod = 250;

      final String bcGroupName = "bc1";

      final String localBindAddress = "localhost";

      final int localBindPort = 5432;
      
      BroadcastGroupConfiguration bcConfig1 = new BroadcastGroupConfiguration(bcGroupName,
                                                                              localBindAddress,
                                                                              localBindPort,
                                                                              groupAddress,
                                                                              groupPort,
                                                                              broadcastPeriod,
                                                                              connectorNames);
      
      List<BroadcastGroupConfiguration> bcConfigs1 = new ArrayList<BroadcastGroupConfiguration>();
      bcConfigs1.add(bcConfig1);
      liveConf.setBroadcastGroupConfigurations(bcConfigs1);
      
      liveService = Messaging.newNullStorageMessagingService(liveConf);
      liveService.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      backupService.stop();

      liveService.stop();

      assertEquals(0, InVMRegistry.instance.size());
      
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
