/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */ 

package org.jboss.messaging.tests.integration.cluster;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.client.impl.ClientSessionImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.RemotingConnection;
import org.jboss.messaging.core.remoting.impl.ConnectionRegistryImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMRegistry;
import org.jboss.messaging.core.remoting.impl.invm.TransportConstants;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.util.SimpleString;

public class ReplicationTest extends TestCase
{
   private static final Logger log = Logger.getLogger(ReplicationTest.class);
      
   // Constants -----------------------------------------------------
  
   // Attributes ----------------------------------------------------
   
   private static final SimpleString ADDRESS = new SimpleString("FailoverTestAddress");
   
   private MessagingService liveService;
   
   private MessagingService backupService;
   
   private Map<String, Object> backupParams = new HashMap<String, Object>();
      
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testReplication() throws Exception
   {                                     
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));

      ClientSession session = sf.createSession(false, true, true, -1, false);
        
      session.createQueue(ADDRESS, ADDRESS, null, false, false);
            
      ClientProducer producer = session.createProducer(ADDRESS);     
      
      final int numMessages = 1000;
      
      log.info("starting");
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, false, 0,
               System.currentTimeMillis(), (byte) 1);  
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().putString("aardvarks");
         message.getBody().flip();  
         producer.send(message);
      }
      
      log.info("Sent messages");
                                
      ClientConsumer consumer = session.createConsumer(ADDRESS);
      
      session.start();
       
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();
         
        // log.info("Got message " + message2);

         assertEquals("aardvarks", message2.getBody().getString());
         assertEquals(i, message2.getProperty(new SimpleString("count")));
         
         session.acknowledge();
      }
      
      log.info("done");
      
      ClientMessage message3 = consumer.receive(500);
      
      assertNull(message3);
      
      log.info("Got all messages");
      
      session.close();
   }
   
      
   public void testFailoverSameConnectionFactory() throws Exception
   {                              
      ClientSessionFactory sf =
         new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                  new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory", backupParams));

      ClientSession session = sf.createSession(false, true, true, -1, false);
                  
      session.createQueue(ADDRESS, ADDRESS, null, false, false);
       
      ClientProducer producer = session.createProducer(ADDRESS);     
      
      final int numMessages = 1000;
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, false, 0,
               System.currentTimeMillis(), (byte) 1);         
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().putString("aardvarks");
         message.getBody().flip();  
         producer.send(message);
      }
      
      RemotingConnection conn = ((ClientSessionImpl)session).getConnection();
      
      //Simulate failure on connection
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));
                      
      ClientConsumer consumer = session.createConsumer(ADDRESS);
      
      session.start();
      
      for (int i = 0; i < numMessages / 2; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().getString());
         
         assertEquals(i, message2.getProperty(new SimpleString("count")));
         
         session.acknowledge();
         
         //log.info("got message " + message2.getProperty(new SimpleString("blah")));
      }

      session.close();
                  
      session = sf.createSession(false, true, true, -1, false);
      
      consumer = session.createConsumer(ADDRESS);
      
      session.start();
      
      for (int i = numMessages / 2 ; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().getString());
         
         assertEquals(i, message2.getProperty(new SimpleString("count")));
         
         session.acknowledge();
         
        //log.info("got message " + message2.getProperty(new SimpleString("blah")));
      }
      
      ClientMessage message3 = consumer.receive(500);
      
      session.close();
      
      assertNull(message3);
   }
   
   public void testFailoverChangeConnectionFactory() throws Exception
   {                                     
      ClientSessionFactory sf =
         new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                  new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory", backupParams));

      ClientSession session = sf.createSession(false, true, true, -1, false);
                  
      session.createQueue(ADDRESS, ADDRESS, null, false, false);
       
      ClientProducer producer = session.createProducer(ADDRESS);     
      
      final int numMessages = 1000;
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, false, 0,
               System.currentTimeMillis(), (byte) 1);         
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().putString("aardvarks");
         message.getBody().flip();  
         producer.send(message);
     //    log.info("sent " + i);
      }
      
      RemotingConnection conn = ((ClientSessionImpl)session).getConnection();
      
      //Simulate failure on connection
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));
                      
      ClientConsumer consumer = session.createConsumer(ADDRESS);
      
      session.start();
            
      for (int i = 0; i < numMessages / 2; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().getString());
         
         assertEquals(i, message2.getProperty(new SimpleString("count")));
         
         session.acknowledge();
         
         //log.info("got message " + message2.getProperty(new SimpleString("blah")));
      }

      session.close();
                  
      sf =
         new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory", backupParams));                  
      
      log.info("** creating new one");
      
      session = sf.createSession(false, true, true, -1, false);
      
      consumer = session.createConsumer(ADDRESS);
      
      session.start();
      
      for (int i = numMessages / 2 ; i < numMessages; i++)      
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("aardvarks", message2.getBody().getString());
         
         assertEquals(i, message2.getProperty(new SimpleString("count")));
         
         session.acknowledge();
         
        //log.info("got message " + message2.getProperty(new SimpleString("blah")));
      }
      
      ClientMessage message3 = consumer.receive(500);
      
      assertNull(message3);
      
      session.close();      
   }
   
   public void testFailoverMultipleSessions() throws Exception
   {                                     
      ClientSessionFactory sf =
         new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                  new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory", backupParams));

      final int numSessions = 10;
      
      List<ClientSession> sessions = new ArrayList<ClientSession>();
      
      List<ClientConsumer> consumers = new ArrayList<ClientConsumer>();
      
      for (int i = 0; i < numSessions; i++)
      {
         ClientSession sess = sf.createSession(false, true, true, -1, false);
         
         SimpleString queueName = new SimpleString("subscription" + i);
         
         sess.createQueue(ADDRESS, queueName, null, false, false);
         
         ClientConsumer consumer = sess.createConsumer(queueName);
                          
         sess.start();
         
         sessions.add(sess);
         
         consumers.add(consumer);
      }
      
      log.info("Created consumers");
            
      ClientSession session = sf.createSession(false, true, true, -1, false);
                  
      ClientProducer producer = session.createProducer(ADDRESS);     
      
      final int numMessages = 100;
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, false, 0,
               System.currentTimeMillis(), (byte) 1);         
         message.putIntProperty(new SimpleString("count"), i);
         message.getBody().putString("aardvarks");
         message.getBody().flip();  
         producer.send(message);
     //    log.info("sent " + i);
      }
                  
      RemotingConnection conn = ((ClientSessionImpl)session).getConnection();
      
      //Simulate failure on connection
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));
      
      for (int i = 0; i < numSessions; i++)
      {      
         ClientConsumer cons = consumers.get(i);
         
         ClientSession sess = sessions.get(i);
         
         for (int j = 0; j < numMessages; j++)
         {
            ClientMessage message2 = cons.receive();
   
            assertEquals("aardvarks", message2.getBody().getString());
            
            //log.info("got message " + i + ":" + message2.getProperty(new SimpleString("count")));
            
            assertEquals(j, message2.getProperty(new SimpleString("count")));
            
            sess.acknowledge();
         }
      }
      
      session.close();
      
      for (int i = 0; i < numSessions; i++)
      {      
         ClientSession sess = sessions.get(i);
         
         sess.close();
      }
   }
   
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------
   
   protected void setUp() throws Exception
   {
      Configuration backupConf = new ConfigurationImpl();      
      backupConf.setSecurityEnabled(false);        
      backupConf.setPacketConfirmationBatchSize(1);
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations().add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory", backupParams));
      backupConf.setBackup(true);                  
      backupService = MessagingServiceImpl.newNullStorageMessagingServer(backupConf);              
      backupService.start();
            
      Configuration liveConf = new ConfigurationImpl();      
      liveConf.setSecurityEnabled(false);    
      liveConf.setPacketConfirmationBatchSize(1);
      liveConf.getAcceptorConfigurations().add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      liveConf.setBackupConnectorConfiguration(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory", backupParams));
      liveService = MessagingServiceImpl.newNullStorageMessagingServer(liveConf);              
      liveService.start();
   }
   
   protected void tearDown() throws Exception
   {                
      assertEquals(0, ConnectionRegistryImpl.instance.size());
      
      assertEquals(0, backupService.getServer().getRemotingService().getConnections().size());
      
      backupService.stop();
      
      assertEquals(0, liveService.getServer().getRemotingService().getConnections().size());
      
      liveService.stop();
      
      assertEquals(0, InVMRegistry.instance.size());
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
