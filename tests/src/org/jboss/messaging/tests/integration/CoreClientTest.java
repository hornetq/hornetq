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

package org.jboss.messaging.tests.integration;

import junit.framework.TestCase;
import org.jboss.messaging.core.client.*;
import org.jboss.messaging.core.client.impl.ClientConnectionFactoryImpl;
import org.jboss.messaging.core.client.impl.ConnectionParamsImpl;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.util.SimpleString;

public class CoreClientTest extends TestCase
{
   // Constants -----------------------------------------------------

   private final SimpleString QUEUE = new SimpleString("CoreClientTestQueue");
   // Attributes ----------------------------------------------------

   private ConfigurationImpl conf;
   private MessagingService messagingService;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      conf = new ConfigurationImpl();
      conf.setSecurityEnabled(false);
      conf.setTransport(TransportType.TCP);
      conf.setHost("localhost");      
      messagingService = MessagingServiceImpl.newNullStorageMessagingServer(conf);
      messagingService.start();
   }
   
   @Override
   protected void tearDown() throws Exception
   {
      messagingService.stop();
      
      super.tearDown();
   }
   
   
   public void testCoreClient() throws Exception
   {
      Location location = new LocationImpl(TransportType.TCP, "localhost", ConfigurationImpl.DEFAULT_PORT);
            
      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(location);
      ClientConnection conn = cf.createConnection();
      
      ClientSession session = conn.createClientSession(false, true, true, -1, false, false);
      session.createQueue(QUEUE, QUEUE, null, false, false);
      
      ClientProducer producer = session.createProducer(QUEUE);

      ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, false, 0,
            System.currentTimeMillis(), (byte) 1);
      message.getBody().putString("testINVMCoreClient");
      producer.send(message);

      ClientConsumer consumer = session.createConsumer(QUEUE);
      conn.start();
      
      message = consumer.receive(1000);
      
      assertEquals("testINVMCoreClient", message.getBody().getString());
      
      conn.close();
   }

   public void testCoreClientMultipleConnections() throws Exception
   {
      Location location = new LocationImpl(TransportType.TCP, "localhost", ConfigurationImpl.DEFAULT_PORT);
      ConnectionParams connectionParams = new ConnectionParamsImpl();
      connectionParams.setCallTimeout(500000);
      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(location, connectionParams);
      ClientConnection conn = cf.createConnection();

      ClientConnectionFactory cf2 = new ClientConnectionFactoryImpl(location, connectionParams);
      ClientConnection conn2 = cf2.createConnection();

      ClientSession session = conn.createClientSession(false, true, true, -1, false, false);
      session.createQueue(QUEUE, QUEUE, null, false, false);

      ClientProducer producer = session.createProducer(QUEUE);

      ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, false, 0,
            System.currentTimeMillis(), (byte) 1);
      message.getBody().putString("testINVMCoreClient");
      producer.send(message);

      ClientConsumer consumer = session.createConsumer(QUEUE);
      conn.start();

      message = consumer.receive(1000);

      assertEquals("testINVMCoreClient", message.getBody().getString());

      conn.close();
   }
   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
