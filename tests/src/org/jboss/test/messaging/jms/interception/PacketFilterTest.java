/*
 * JBoss, the OpenSource J2EE webOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */

package org.jboss.test.messaging.jms.interception;

import javax.jms.Connection;
import javax.jms.Session;

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
      KernelDeployment packetFilterDeployment = servers.get(0).deployXML("packetFilterDeployment", "<?xml version=\"1.0\" encoding=\"UTF-8\"?><deployment xmlns=\"urn:jboss:bean-deployer:2.0\"><bean name=\"DummyInterceptionTest\" class=\"org.jboss.test.messaging.jms.interception.DummyInterceptor\"/></deployment>");
      
      
      DummyInterceptor.status=false;
      try
      {
         Connection conn = cf.createConnection();
         fail("Exception expected");
      }
      catch (Exception e)
      {
      }
      
      DummyInterceptor.status=true;
      
      Connection conn = cf.createConnection();
      conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      conn.close();
      
      
      assertEquals(0, DummyInterceptorB.getCounter());
      assertTrue(DummyInterceptor.getCounter() > 0);
      
      DummyInterceptor.clearCounter();
      DummyInterceptorB.clearCounter();
      

      KernelDeployment packetFilterDeploymentB = servers.get(0).deployXML("packetFilterDeploymentB", "<?xml version=\"1.0\" encoding=\"UTF-8\"?><deployment xmlns=\"urn:jboss:bean-deployer:2.0\"><bean name=\"DummyInterceptionTestB\" class=\"org.jboss.test.messaging.jms.interception.DummyInterceptorB\"/></deployment>");

      conn = cf.createConnection();
      conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      conn.close();
      
      assertTrue(DummyInterceptorB.getCounter() > 0);
      assertTrue(DummyInterceptor.getCounter() > 0);
      
      DummyInterceptor.clearCounter();
      DummyInterceptorB.clearCounter();

      servers.get(0).undeploy(packetFilterDeployment);

      conn = cf.createConnection();
      conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      conn.close();
      
      assertTrue(DummyInterceptorB.getCounter() > 0);
      assertTrue(DummyInterceptor.getCounter() == 0);
      
      
      log.info("Undeploying server");
      servers.get(0).undeploy(packetFilterDeploymentB);
      DummyInterceptor.clearCounter();
      DummyInterceptorB.clearCounter();
      
      conn = cf.createConnection();
      conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      conn.close();
      
      assertEquals(0, DummyInterceptor.getCounter());
      assertEquals(0, DummyInterceptorB.getCounter());
   }

}
