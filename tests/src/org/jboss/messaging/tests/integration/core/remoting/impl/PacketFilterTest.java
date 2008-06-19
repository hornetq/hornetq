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

package org.jboss.messaging.tests.integration.core.remoting.impl;

import junit.framework.TestCase;
import org.jboss.messaging.core.client.*;
import org.jboss.messaging.core.client.impl.ClientConnectionFactoryImpl;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.server.MessagingService;
import org.jboss.messaging.core.server.impl.MessagingServiceImpl;
import org.jboss.messaging.jms.client.JBossTextMessage;
import org.jboss.messaging.util.SimpleString;

import java.util.UUID;

/**
 * 
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *
 */
public class PacketFilterTest  extends TestCase
{
   Logger log = Logger.getLogger(PacketFilterTest.class);

   private MessagingService messagingService;
   
   private static final SimpleString QUEUE1 = new SimpleString("queue1");


   public PacketFilterTest(String name)
   {
      super(name);
   }

   protected void setUp() throws Exception
   {
      ConfigurationImpl config = new ConfigurationImpl();
      config.setTransport(TransportType.TCP);
      config.setHost("localhost");
      config.setSecurityEnabled(false);
      messagingService = MessagingServiceImpl.newNullStorageMessagingServer(config);
      messagingService.start();
   }

   protected void tearDown() throws Exception
   {
      if (messagingService != null)
      {
         messagingService.stop();
         messagingService = null;
      }
   }

   public void testFilter() throws Throwable
   {
      DummyInterceptor interceptorA = null;
      DummyInterceptorB interceptorB = null;

      Location location = new LocationImpl(TransportType.TCP, "localhost", ConfigurationImpl.DEFAULT_PORT);
      
      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(location);
      ClientConnection conn = null;
      try
      {         
         // Deploy using the API
         interceptorA = new DummyInterceptor();
         messagingService.getServer().getRemotingService().addInterceptor(interceptorA);
                  
         interceptorA.sendException=true;
         try
         {
            conn = cf.createConnection();
            fail("Exception expected");
         }
         catch (Exception e)
         {
            conn = null;
         }
         
         interceptorA.sendException=false;
         
         conn = cf.createConnection();
         conn.createClientSession(false, true, true, -1, false, false);
         conn.close();
         conn = null;
                  
         assertEquals(0, DummyInterceptorB.getCounter());
         assertTrue(interceptorA.getCounter() > 0);
         
         interceptorA.clearCounter();
         DummyInterceptorB.clearCounter();
         interceptorB = new DummyInterceptorB();
         messagingService.getServer().getRemotingService().addInterceptor(interceptorB);
         conn = cf.createConnection();
         conn.createClientSession(false, true, true, -1, false, false);
         conn.close();
         conn = null;
         
         assertTrue(DummyInterceptorB.getCounter() > 0);
         assertTrue(interceptorA.getCounter() > 0);
         
         interceptorA.clearCounter();
         DummyInterceptorB.clearCounter();
   
         messagingService.getServer().getRemotingService().removeInterceptor(interceptorA);
   
         conn = cf.createConnection();
         conn.createClientSession(false, true, true, -1, false, false);
         conn.close();
         conn = null;
         
         assertTrue(DummyInterceptorB.getCounter() > 0);
         assertTrue(interceptorA.getCounter() == 0);
         
         log.info("Undeploying server");
         messagingService.getServer().getRemotingService().removeInterceptor(interceptorB);
         interceptorB = null;
         interceptorA.clearCounter();
         DummyInterceptorB.clearCounter();
         
         conn = cf.createConnection();
         conn.createClientSession(false, true, true, -1, false, false);
         conn.close();
         conn = null;
         
         assertEquals(0, interceptorA.getCounter());
         assertEquals(0, DummyInterceptorB.getCounter());

         interceptorA = null;
      }
      finally
      {
         if (conn != null)
         {
            try{conn.close();} catch (Exception ignored){}
         }
         if (interceptorA != null)
         {
            messagingService.getServer().getRemotingService().removeInterceptor(interceptorA);
         }
         if (interceptorB != null)
         {
            try{messagingService.getServer().getRemotingService().removeInterceptor(interceptorB);} catch (Exception ignored){}
         }
      }
   }

   public void testReceiveMessages() throws Throwable
   {
      
      DummyInterceptor interceptor = null;
      ClientConnection conn = null;
        
      try
      {         
         interceptor = new DummyInterceptor();
         messagingService.getServer().getRemotingService().addInterceptor(interceptor);
 
         interceptor.sendException = false;

         Location location = new LocationImpl(TransportType.TCP, "localhost", ConfigurationImpl.DEFAULT_PORT);
         
         ClientConnectionFactory cf = new ClientConnectionFactoryImpl(location);
         conn = cf.createConnection();
         conn.start();
         ClientSession session = conn.createClientSession(false, true, true, -1, false, false);
         
         session.createQueue(QUEUE1, QUEUE1, null, false, true);
         
         ClientProducer producer = session.createProducer(QUEUE1);
         String msg = "msg " + UUID.randomUUID().toString();
         
         interceptor.changeMessage = true;
         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE, true, 0, System.currentTimeMillis(), (byte) 1);
         message.getBody().putString(msg);
         producer.send(message);
         
         ClientConsumer consumer = session.createConsumer(QUEUE1);
         Message m = consumer.receive(100000);
         assertEquals(m.getProperty(new SimpleString("DummyInterceptor")), new SimpleString("was here"));
                  
         assertNotNull(m);
         
         assertEquals(msg, m.getBody().getString());
      }
      finally
      {
         try
         {
            if (conn != null)
            {
               conn.close();
            }
         }
         catch (Exception ignored)
         {
         }

         try
         {
            if (interceptor != null)
            {
               messagingService.getServer().getRemotingService().removeInterceptor(interceptor);
            }
         }
         catch (Exception ignored)
         {
         }
      }
   }
}
