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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.remoting.spi.Connection;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.util.SimpleString;

/**
 * 
 * A SplitBrainTest
 * 
 * Verify that split brain can occur
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 6 Nov 2008 11:27:17
 *
 *
 */
public class SplitBrainTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(FailBackupServerTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   private MessagingService liveService;

   private MessagingService backupService;

   private final Map<String, Object> backupParams = new HashMap<String, Object>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testDemonstrateSplitBrain() throws Exception
   {
      ClientSessionFactoryInternal sf1 = new ClientSessionFactoryImpl(new TransportConfiguration(InVMConnectorFactory.class.getName()),
                                                                      new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                                                 backupParams));
      
      sf1.setBlockOnNonPersistentSend(true);

      ClientSession session = sf1.createSession(false, true, true);

      session.createQueue(ADDRESS, ADDRESS, null, false, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 10;
      
      int sendCount = 0;
      
      int consumeCount = 0;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE,
                                                              false,
                                                              0,
                                                              System.currentTimeMillis(),
                                                              (byte)1);
         message.putIntProperty(new SimpleString("count"), sendCount++);
         message.getBody().putString("aardvarks");
         message.getBody().flip();
         producer.send(message);
      }
      
      //Now fail the replicating connections
      Set<RemotingConnection> conns = liveService.getServer().getRemotingService().getConnections();
      for (RemotingConnection conn : conns)
      {
         RemotingConnection replicatingConn = conn.getReplicatingConnection();
         Connection tcConn = replicatingConn.getTransportConnection();
         tcConn.fail(new MessagingException(MessagingException.INTERNAL_ERROR, "blah"));
      }
      
      Thread.sleep(2000);
      
      //Fail client connection      
      ((ClientSessionImpl)session).getConnection().fail(new MessagingException(MessagingException.NOT_CONNECTED, "simulated failure b/w client and live node"));

      ClientConsumer consumer1 = session.createConsumer(ADDRESS);

      session.start();
      
      Set<Integer> deliveredMessageIDs = new HashSet<Integer>();
           
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer1.receive(1000);

         assertNotNull(message);

         assertEquals("aardvarks", message.getBody().getString());
         
         int count = (Integer)message.getProperty(new SimpleString("count"));
         
         log.info("got message " + count);

         assertEquals(consumeCount++, count);
       
         deliveredMessageIDs.add(count);

         message.acknowledge();
      }
      
      session.close();
      
      sf1.close();
      
      //Now try and connect to live node - even though we failed over
      
      ClientSessionFactoryInternal sf2 = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));

      session = sf2.createSession(false, true, true);
      
      producer = session.createProducer(ADDRESS);

      consumer1 = session.createConsumer(ADDRESS);
         
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE,
                                                              false,
                                                              0,
                                                              System.currentTimeMillis(),
                                                              (byte)1);
         message.putIntProperty(new SimpleString("count"), i + numMessages);
         message.getBody().putString("aardvarks");
         message.getBody().flip();
         producer.send(message);
      }
      
      session.start();
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer1.receive(1000);
         
         assertNotNull(message);

         assertEquals("aardvarks", message.getBody().getString());
         
         int count = (Integer)message.getProperty(new SimpleString("count"));
         
         log.info("got message on live after failover " + count);
         
         //Assert that this has been consumed before!!
         assertTrue(deliveredMessageIDs.contains(count));
 
         message.acknowledge();
      }
      
      session.close();
      
      sf2.close();
      
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
      backupService = Messaging.newNullStorageMessagingService(backupConf);
      backupService.start();

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      TransportConfiguration backupTC = new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                   backupParams, "backup-connector");
      connectors.put(backupTC.getName(), backupTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());
      liveService = Messaging.newNullStorageMessagingService(liveConf);
      liveService.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      backupService.stop();

      liveService.stop();

      assertEquals(0, InVMRegistry.instance.size());
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
