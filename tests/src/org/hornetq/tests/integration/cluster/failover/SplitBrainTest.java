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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
import org.hornetq.core.exception.MessagingException;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.RemotingConnection;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.remoting.impl.invm.InVMRegistry;
import org.hornetq.core.remoting.impl.invm.TransportConstants;
import org.hornetq.core.remoting.spi.Connection;
import org.hornetq.core.server.Messaging;
import org.hornetq.core.server.MessagingServer;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

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
   private static final Logger log = Logger.getLogger(SplitBrainTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   private MessagingServer liveServer;

   private MessagingServer backupServer;

   private Map<String, Object> backupParams = new HashMap<String, Object>();

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

      session.createQueue(ADDRESS, ADDRESS, null, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      final int numMessages = 10;
      
      int sendCount = 0;
      
      int consumeCount = 0;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(HornetQTextMessage.TYPE,
                                                              false,
                                                              0,
                                                              System.currentTimeMillis(),
                                                              (byte)1);
         message.putIntProperty(new SimpleString("count"), sendCount++);
         message.getBody().writeString("aardvarks");
         producer.send(message);
      }
      
      //Now fail the replicating connections
      Set<RemotingConnection> conns = liveServer.getRemotingService().getConnections();
      for (RemotingConnection conn : conns)
      {
         RemotingConnection replicatingConnection = liveServer.getReplicatingChannel().getConnection();
         Connection tcConn = replicatingConnection.getTransportConnection();
         tcConn.fail(new MessagingException(MessagingException.INTERNAL_ERROR, "blah"));
      }
      
      Thread.sleep(2000);
      
      //Fail client connection      
      ((ClientSessionInternal)session).getConnection().fail(new MessagingException(MessagingException.NOT_CONNECTED, "simulated failure b/w client and live node"));

      ClientConsumer consumer1 = session.createConsumer(ADDRESS);

      session.start();
      
      Set<Integer> deliveredMessageIDs = new HashSet<Integer>();
           
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer1.receive(1000);

         assertNotNull(message);

         assertEquals("aardvarks", message.getBody().readString());
         
         int count = (Integer)message.getProperty(new SimpleString("count"));
         
         assertEquals(consumeCount++, count);
       
         deliveredMessageIDs.add(count);

         message.acknowledge();
      }
      
      session.close();
      
      sf1.close();
      
      //Now try and connect to live node - even though we failed over
      
      ClientSessionFactoryInternal sf2 = new ClientSessionFactoryImpl(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory"));

      session = sf2.createSession(false, true, true);
      
      producer = session.createProducer(ADDRESS);

      consumer1 = session.createConsumer(ADDRESS);
         
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(HornetQTextMessage.TYPE,
                                                              false,
                                                              0,
                                                              System.currentTimeMillis(),
                                                              (byte)1);
         message.putIntProperty(new SimpleString("count"), i + numMessages);
         message.getBody().writeString("aardvarks");
         producer.send(message);
      }
      
      session.start();
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer1.receive(1000);
         
         assertNotNull(message);

         assertEquals("aardvarks", message.getBody().readString());
         
         int count = (Integer)message.getProperty(new SimpleString("count"));
         
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
      super.setUp();
      
      Configuration backupConf = new ConfigurationImpl();
      backupConf.setSecurityEnabled(false);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory",
                                                backupParams));
      backupConf.setBackup(true);
      backupServer = Messaging.newMessagingServer(backupConf, false);
      backupServer.start();

      Configuration liveConf = new ConfigurationImpl();
      liveConf.setSecurityEnabled(false);
      liveConf.getAcceptorConfigurations()
              .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory"));
      Map<String, TransportConfiguration> connectors = new HashMap<String, TransportConfiguration>();
      TransportConfiguration backupTC = new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMConnectorFactory",
                                                                   backupParams, "backup-connector");
      connectors.put(backupTC.getName(), backupTC);
      liveConf.setConnectorConfigurations(connectors);
      liveConf.setBackupConnectorName(backupTC.getName());
      liveServer = Messaging.newMessagingServer(liveConf, false);
      liveServer.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      backupServer.stop();

      liveServer.stop();

      assertEquals(0, InVMRegistry.instance.size());
      
      backupServer = null;
      
      liveServer = null;
      
      backupParams = null;
      
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
