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

import java.util.HashMap;
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
   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testReplication() throws Exception
   {             
      final SimpleString QUEUE = new SimpleString("CoreClientTestQueue");
      
      Configuration backupConf = new ConfigurationImpl();      
      backupConf.setSecurityEnabled(false);        
      backupConf.setPacketConfirmationBatchSize(1);
      Map<String, Object> backupParams = new HashMap<String, Object>();
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations().add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory", backupParams));
      backupConf.setBackup(true);                  
      MessagingService backupService = MessagingServiceImpl.newNullStorageMessagingServer(backupConf);              
      backupService.start();
            
      Configuration liveConf = new ConfigurationImpl();      
      liveConf.setSecurityEnabled(false);    
      liveConf.setPacketConfirmationBatchSize(1);
      liveConf.getAcceptorConfigurations().add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      liveConf.setBackupConnectorConfiguration(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory", backupParams));
      MessagingService liveService = MessagingServiceImpl.newNullStorageMessagingServer(liveConf);              
      liveService.start();
            
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"));

      ClientSession session = sf.createSession(false, true, true, -1, false);
        
      session.createQueue(QUEUE, QUEUE, null, false, false);
      
      
      ClientProducer producer = session.createProducer(QUEUE);     
      
      final int numMessages = 1000;
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, false, 0,
               System.currentTimeMillis(), (byte) 1);         
         message.getBody().putString("testINVMCoreClient");
         message.getBody().flip();  
         producer.send(message);
      }
                                
      ClientConsumer consumer = session.createConsumer(QUEUE);
      
      session.start();
       
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("testINVMCoreClient", message2.getBody().getString());
         
         session.acknowledge();
      }
      
      session.close();
      
      liveService.stop();
      backupService.stop();
   }
   
   public void testFailover() throws Exception
   {             
      final SimpleString QUEUE = new SimpleString("CoreClientTestQueue");
      
      Configuration backupConf = new ConfigurationImpl();      
      backupConf.setSecurityEnabled(false);        
      backupConf.setPacketConfirmationBatchSize(1);
      Map<String, Object> backupParams = new HashMap<String, Object>();
      backupParams.put(TransportConstants.SERVER_ID_PROP_NAME, 1);
      backupConf.getAcceptorConfigurations().add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory", backupParams));
      backupConf.setBackup(true);                  
      MessagingService backupService = MessagingServiceImpl.newNullStorageMessagingServer(backupConf);              
      backupService.start();
            
      Configuration liveConf = new ConfigurationImpl();      
      liveConf.setSecurityEnabled(false);    
      liveConf.setPacketConfirmationBatchSize(1);
      liveConf.getAcceptorConfigurations().add(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory"));
      liveConf.setBackupConnectorConfiguration(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory", backupParams));
      MessagingService liveService = MessagingServiceImpl.newNullStorageMessagingServer(liveConf);              
      liveService.start();
            
      ClientSessionFactory sf =
         new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory"),
                  new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory", backupParams));

      ClientSession session = sf.createSession(false, true, true, -1, false);
                  
      session.createQueue(QUEUE, QUEUE, null, false, false);
       
      ClientProducer producer = session.createProducer(QUEUE);     
      
      final int numMessages = 10;
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, false, 0,
               System.currentTimeMillis(), (byte) 1);         
         message.putIntProperty(new SimpleString("blah"), i);
         message.getBody().putString("testINVMCoreClient");
         message.getBody().flip();  
         producer.send(message);
      }
      
      RemotingConnection conn = ((ClientSessionImpl)session).getConnection();
      
      //Simulate failure on connection
      conn.fail(new MessagingException(MessagingException.NOT_CONNECTED));
                      
      ClientConsumer consumer = session.createConsumer(QUEUE);
      
      session.start();
      
      for (int i = 0; i < numMessages / 2; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("testINVMCoreClient", message2.getBody().getString());
         
         session.acknowledge();
         
         log.info("got message " + message2.getProperty(new SimpleString("blah")));
      }

      session.close();
            
      sf =
         new ClientSessionFactoryImpl(new TransportConfiguration("org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory", backupParams));
            
      session = sf.createSession(false, true, true, -1, false);
      
      consumer = session.createConsumer(QUEUE);
      
      session.start();
      
      for (int i = 0; i < numMessages / 2; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("testINVMCoreClient", message2.getBody().getString());
         
         session.acknowledge();
         
         log.info("got message " + message2.getProperty(new SimpleString("blah")));
      }
      
      ClientMessage message3 = consumer.receive(1000);
      
      assertNull(message3);
      
      liveService.stop();
      backupService.stop();
      
  //    todo - do we need to failover connection factories too?????
               
               
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
