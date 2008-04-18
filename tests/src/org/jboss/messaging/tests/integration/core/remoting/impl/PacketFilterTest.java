/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.tests.integration.core.remoting.impl;

import java.util.UUID;

import org.jboss.messaging.core.server.impl.MessagingServerImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.tests.unit.core.remoting.impl.ConfigurationHelper;
import static org.jboss.messaging.core.remoting.TransportType.INVM;
import org.jboss.messaging.core.client.*;
import org.jboss.messaging.core.client.impl.ClientConnectionFactoryImpl;
import org.jboss.messaging.core.client.impl.LocationImpl;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.jms.client.JBossTextMessage;
import junit.framework.TestCase;

public class PacketFilterTest  extends TestCase
{
   Logger log = Logger.getLogger(PacketFilterTest.class);

   private MessagingServerImpl server;


   public PacketFilterTest(String name)
   {
      super(name);
   }

   protected void setUp() throws Exception
   {
      server = new MessagingServerImpl(ConfigurationHelper.newConfiguration(INVM, null, 0));
      server.start();
   }

   protected void tearDown() throws Exception
   {
      if(server != null)
      {
         server.stop();
         server = null;
      }
   }

   public void testFilter() throws Throwable
   {
      DummyInterceptor interceptorA = null;
      DummyInterceptorB interceptorB = null;

      ClientConnectionFactory cf = new ClientConnectionFactoryImpl(0, new LocationImpl(INVM));
      ClientConnection conn = null;
      try
      {
         
         // Deploy using the API
         interceptorA = new DummyInterceptor();
         server.getRemotingService().addInterceptor(interceptorA);
         
         
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
         server.getRemotingService().addInterceptor(interceptorB);
         conn = cf.createConnection();
         conn.createClientSession(false, true, true, -1, false, false);
         conn.close();
         conn = null;
         
         assertTrue(DummyInterceptorB.getCounter() > 0);
         assertTrue(interceptorA.getCounter() > 0);
         
         interceptorA.clearCounter();
         DummyInterceptorB.clearCounter();
   
         server.getRemotingService().removeInterceptor(interceptorA);
   
         conn = cf.createConnection();
         conn.createClientSession(false, true, true, -1, false, false);
         conn.close();
         conn = null;
         
         assertTrue(DummyInterceptorB.getCounter() > 0);
         assertTrue(interceptorA.getCounter() == 0);

         
         log.info("Undeploying server");
         server.getRemotingService().removeInterceptor(interceptorB);
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
            server.getRemotingService().removeInterceptor(interceptorA);
         }
         if (interceptorB != null)
         {
            try{server.getRemotingService().removeInterceptor(interceptorB);} catch (Exception ignored){}
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
         server.getRemotingService().addInterceptor(interceptor);
         server.getPostOffice().addBinding("queue1", "queue1", null, false, false);
         
         interceptor.sendException=false;


         ClientConnectionFactory cf = new ClientConnectionFactoryImpl(0, new LocationImpl(INVM));
         conn = cf.createConnection();
         conn.start();
         ClientSession session = conn.createClientSession(false, true, true, -1, false, false);
         ClientProducer producer = session.createProducer("queue1");
         String msg = "msg " + UUID.randomUUID().toString();
         
         interceptor.changeMessage = true;
         MessageImpl message = new MessageImpl(JBossTextMessage.TYPE, true, 0, System.currentTimeMillis(), (byte) 1);
         message.setPayload(msg.getBytes());
         producer.send(message);
         
         ClientConsumer consumer = session.createConsumer("queue1", null, false, false, true);
         Message jmsMsg = consumer.receive(100000);
         assertEquals(jmsMsg.getHeader("DummyInterceptor"), "was here");
         
         
         assertNotNull(jmsMsg);
         
         assertEquals(msg, new String(jmsMsg.getPayload()));
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
               server.getRemotingService().removeInterceptor(interceptor);
            }
         }
         catch (Exception ignored)
         {
         }
      }
   }
}
