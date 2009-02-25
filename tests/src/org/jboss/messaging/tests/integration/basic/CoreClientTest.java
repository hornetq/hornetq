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

package org.jboss.messaging.tests.integration.basic;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.tests.util.UnitTestCase;
import org.jboss.messaging.utils.SimpleString;

public class CoreClientTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(CoreClientTest.class);
      
   // Constants -----------------------------------------------------

  
   // Attributes ----------------------------------------------------

   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCoreClient() throws Exception
   {
      testCoreClient("org.jboss.messaging.integration.transports.mina.MinaAcceptorFactory", "org.jboss.messaging.integration.transports.mina.MinaConnectorFactory");
      testCoreClient("org.jboss.messaging.integration.transports.netty.NettyAcceptorFactory", "org.jboss.messaging.integration.transports.netty.NettyConnectorFactory");
      testCoreClient("org.jboss.messaging.core.remoting.impl.invm.InVMAcceptorFactory", "org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory");
   }
   
   private void testCoreClient(final String acceptorFactoryClassName, final String connectorFactoryClassName) throws Exception
   {             
      final SimpleString QUEUE = new SimpleString("CoreClientTestQueue");
      
      Configuration conf = new ConfigurationImpl();
      
      conf.setSecurityEnabled(false);   
      
      conf.getAcceptorConfigurations().add(new TransportConfiguration(acceptorFactoryClassName));
            
      MessagingService messagingService = Messaging.newNullStorageMessagingService(conf);   
           
      messagingService.start();
      
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(connectorFactoryClassName));

      ClientSession session = sf.createSession(false, true, true);
      
      session.createQueue(QUEUE, QUEUE, null, false, false);
      
      ClientProducer producer = session.createProducer(QUEUE);     
      
      final int numMessages = 10000;
      
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
         
         message2.acknowledge();
      }
      
      session.close();
      
      messagingService.stop();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
