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
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

/**
 * 
 * A ActivationTimeoutTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 4 Nov 2008 16:54:50
 *
 *
 */
public class ActivationTimeoutTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(SimpleAutomaticFailoverTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final long ACTIVATION_TIMEOUT = 5000;
   
   private static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");

   private MessagingServer liveService;

   private MessagingServer backupService;

   private final Map<String, Object> backupParams = new HashMap<String, Object>();

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testTimeoutAfterConsumerFailsToReattach() throws Exception
   {            
      ClientSessionFactoryInternal sf1 = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                      new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                 backupParams));
      
      ClientSessionFactoryInternal sf2 = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                      new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                 backupParams));

      sf1.setSendWindowSize(32 * 1024);
      sf2.setSendWindowSize(32 * 1024);

      ClientSession session1 = sf1.createSession(false, true, true);

      session1.createQueue(ADDRESS, ADDRESS, null, false);

      ClientProducer producer = session1.createProducer(ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createClientMessage(JBossTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().writeString("aardvarks");
         producer.send(message);
      }
      
      ClientConsumer consumer1 = session1.createConsumer(ADDRESS);
      
      ClientSession session2 = sf2.createSession(false, true, true);
      
      //Create another consumer so we have two consumers on the queue
      ClientConsumer consumer2 = session2.createConsumer(ADDRESS);
      
      long start = System.currentTimeMillis();

      RemotingConnection conn1 = ((ClientSessionImpl)session1).getConnection();

      // Now we fail ONLY the connections on sf1, not on sf2      
      conn1.fail(new MessagingException(MessagingException.NOT_CONNECTED));

      session1.start();

      //The messages should not be delivered until after activationTimeout ms, since
      //session 2 didn't reattach
             
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer1.receive(2 * ACTIVATION_TIMEOUT);
         
         assertNotNull(message);
         
         if (i == 0)
         {
            long now = System.currentTimeMillis();
            
            assertTrue(now - start >= ACTIVATION_TIMEOUT);
         }
         
         assertEquals("aardvarks", message.getBody().readString());

         assertEquals(i, message.getProperty(new SimpleString("count")));

         message.acknowledge();
      }
      
      ClientMessage message = consumer1.receive(1000);
      
      assertNull(message);
      
      session1.close();
      
      RemotingConnection conn2 = ((ClientSessionImpl)session2).getConnection();
     
      conn2.fail(new MessagingException(MessagingException.NOT_CONNECTED));
      
      session2.close();
   }
   
   public void testTimeoutAfterAllConsumerFailsToReattach() throws Exception
   {            
      ClientSessionFactoryInternal sf1 = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                      new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                 backupParams));
      
      ClientSessionFactoryInternal sf2 = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                                                                      new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory",
                                                                                                 backupParams));

      sf1.setSendWindowSize(32 * 1024);
      sf2.setSendWindowSize(32 * 1024);

      ClientSession session1 = sf1.createSession(false, true, true);

      session1.createQueue(ADDRESS, ADDRESS, null, false);

      ClientProducer producer = session1.createProducer(ADDRESS);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session1.createClientMessage(JBossTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().writeString("aardvarks");
         producer.send(message);
      }
      
      ClientSession session2 = sf2.createSession(false, true, true);
      
      ClientConsumer consumer1 = session2.createConsumer(ADDRESS);
      
      ClientConsumer consumer2 = session2.createConsumer(ADDRESS);
      
      long start = System.currentTimeMillis();

      RemotingConnection conn1 = ((ClientSessionImpl)session1).getConnection();

      // Now we fail ONLY the connections on sf1, not on sf2      
      conn1.fail(new MessagingException(MessagingException.NOT_CONNECTED));

      session1.start();

      //The messages should not be delivered until after activationTimeout ms, since
      //session 2 didn't reattach
              
      //We now create a new consumer but it shouldn't receive the messages until after the timeout
      
      ClientConsumer consumer3 = session1.createConsumer(ADDRESS);
               
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer3.receive(2 * ACTIVATION_TIMEOUT);
         
         assertNotNull(message);
         
         if (i == 0)
         {
            long now = System.currentTimeMillis();
            
            assertTrue(now - start >= ACTIVATION_TIMEOUT);
         }
         
         assertEquals("aardvarks", message.getBody().readString());

         assertEquals(i, message.getProperty(new SimpleString("count")));

         message.acknowledge();
      }
      
      ClientMessage message = consumer3.receive(1000);
      
      assertNull(message);
      
      session1.close();
      
      RemotingConnection conn2 = ((ClientSessionImpl)session2).getConnection();
     
      conn2.fail(new MessagingException(MessagingException.NOT_CONNECTED));
      
      session2.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();
      
      Configuration backupConf = new ConfigurationImpl();
      backupConf.setSecurityEnabled(false);
      backupConf.setQueueActivationTimeout(ACTIVATION_TIMEOUT);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations()
                .add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory",
                                                backupParams));
      backupConf.setBackup(true);
      backupService = Messaging.newMessagingServer(backupConf, false);
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
      liveService = Messaging.newMessagingServer(liveConf, false);
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
