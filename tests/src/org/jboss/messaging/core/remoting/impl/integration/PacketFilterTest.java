/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.messaging.core.remoting.impl.integration;

import java.util.UUID;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.jboss.kernel.spi.deployment.KernelDeployment;
import org.jboss.test.messaging.jms.JMSTestCase;

public class PacketFilterTest  extends JMSTestCase
{

   
   public PacketFilterTest(String name)
   {
      super(name);
      // TODO Auto-generated constructor stub
   }
   
   
   public void testFilter() throws Throwable
   {
      DummyInterceptor interceptorA = null;
      KernelDeployment packetFilterDeploymentB = null;
      Connection conn = null;
        
      try
      {
         
         // Deploy using the API
         interceptorA = new DummyInterceptor();
         servers.get(0).getMessagingServer().getRemotingService().addInterceptor(interceptorA);
         
         
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
         conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         conn.close();
         conn = null;
         
         
         assertEquals(0, DummyInterceptorB.getCounter());
         assertTrue(interceptorA.getCounter() > 0);
         
         interceptorA.clearCounter();
         DummyInterceptorB.clearCounter();
         
   
         // deploy using MC
         packetFilterDeploymentB = servers.get(0).deployXML("packetFilterDeploymentB", 
               "<?xml version=\"1.0\" encoding=\"UTF-8\"?><deployment " +
               "xmlns=\"urn:jboss:bean-deployer:2.0\"><bean name=\"DummyInterceptionTestB\" " +
               "class=\"org.jboss.messaging.core.remoting.impl.integration.DummyInterceptorB\"/></deployment>");
   
         conn = cf.createConnection();
         conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         conn.close();
         conn = null;
         
         assertTrue(DummyInterceptorB.getCounter() > 0);
         assertTrue(interceptorA.getCounter() > 0);
         
         interceptorA.clearCounter();
         DummyInterceptorB.clearCounter();
   
         servers.get(0).getMessagingServer().getRemotingService().removeInterceptor(interceptorA);
   
         conn = cf.createConnection();
         conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         conn.close();
         conn = null;
         
         assertTrue(DummyInterceptorB.getCounter() > 0);
         assertTrue(interceptorA.getCounter() == 0);
         
         
         log.info("Undeploying server");
         servers.get(0).undeploy(packetFilterDeploymentB);
         packetFilterDeploymentB = null;
         interceptorA.clearCounter();
         DummyInterceptorB.clearCounter();
         
         conn = cf.createConnection();
         conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
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
            servers.get(0).getMessagingServer().getRemotingService().removeInterceptor(interceptorA);
         }
         if (packetFilterDeploymentB != null)
         {
            try{servers.get(0).undeploy(packetFilterDeploymentB);} catch (Exception ignored){}
         }
      }
   }

   public void testReceiveMessages() throws Throwable
   {
      
      DummyInterceptor interceptor = null;
      Connection conn = null;
        
      try
      {
         
         interceptor = new DummyInterceptor();
         servers.get(0).getMessagingServer().getRemotingService().addInterceptor(interceptor);
         
         
         interceptor.sendException=false;

         conn = cf.createConnection();
         conn.start();
         
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(queue1);
         String msg = "msg " + UUID.randomUUID().toString();
         
         interceptor.changeMessage = true;

         producer.send(session.createTextMessage(msg));
         
         MessageConsumer consumer = session.createConsumer(queue1);
         TextMessage jmsMsg = (TextMessage)consumer.receive(100000);
         assertEquals(jmsMsg.getStringProperty("DummyInterceptor"), "was here");
         
         
         assertNotNull(jmsMsg);
         
         assertEquals(msg, jmsMsg.getText());
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
               servers.get(0).getMessagingServer().getRemotingService().removeInterceptor(interceptor);
            }
         }
         catch (Exception ignored)
         {
         }
      }
   }
}
