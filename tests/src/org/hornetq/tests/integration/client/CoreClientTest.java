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

package org.hornetq.tests.integration.client;

import org.hornetq.core.buffers.ChannelBuffers;
import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.Messaging;
import org.hornetq.core.server.MessagingServer;
import org.hornetq.jms.client.JBossTextMessage;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

public class CoreClientTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(CoreClientTest.class);
      
   // Constants -----------------------------------------------------

  
   // Attributes ----------------------------------------------------

   
   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCoreClientNetty() throws Exception
   {
      testCoreClient("org.hornetq.integration.transports.netty.NettyAcceptorFactory", "org.hornetq.integration.transports.netty.NettyConnectorFactory");
   }
   
   public void testCoreClientInVM() throws Exception
   {
      testCoreClient("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory", "org.hornetq.core.remoting.impl.invm.InVMConnectorFactory");
   }
   
   private void testCoreClient(final String acceptorFactoryClassName, final String connectorFactoryClassName) throws Exception
   {             
      final SimpleString QUEUE = new SimpleString("CoreClientTestQueue");
      
      Configuration conf = new ConfigurationImpl();
      
      conf.setSecurityEnabled(false);   
      
      conf.getAcceptorConfigurations().add(new TransportConfiguration(acceptorFactoryClassName));
            
      MessagingServer server = Messaging.newMessagingServer(conf, false);   
           
      server.start();
      
      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(connectorFactoryClassName));
      sf.setConsumerWindowSize(0);

      ClientSession session = sf.createSession(false, true, true);
      
      session.createQueue(QUEUE, QUEUE, null, false);
      
      ClientProducer producer = session.createProducer(QUEUE);     
       
      final int numMessages = 10000;
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, false, 0,
               System.currentTimeMillis(), (byte) 1);         
         message.setBody(ChannelBuffers.buffer(3000));
         
         message.getBody().writeString("testINVMCoreClient");
         producer.send(message);
      }
      
      ClientConsumer consumer = session.createConsumer(QUEUE);
      
      session.start();
      
      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         assertEquals("testINVMCoreClient", message2.getBody().readString());
         
         message2.acknowledge();
      }
      
      session.close();
      
      server.stop();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
