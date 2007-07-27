/*
* JBoss, Home of Professional Open Source
* Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.test.thirdparty.jbosssx;

import java.security.Principal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.security.auth.Subject;

import org.jboss.security.SecurityAssociation;
import org.jboss.security.SimplePrincipal;
import org.jboss.test.messaging.jms.JMSTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.container.MockJBossSecurityManager;

/**
 * Set of tests to insure consistent behavior relative to the JBoss AS security infrastructure.
 * This is just a safety layer, full fledged security tests should be present in the integration
 * test suite.
 *
 * Tests contained by this class are supposed to run only in local environment.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class SecurityAssociationTest extends JMSTestCase
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   public SecurityAssociationTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   /**
    * Test for http://jira.jboss.org/jira/browse/JBMESSAGING-807
    */
   public void testSecurityAssociation() throws Exception
   {
      if(ServerManagement.isRemote())
      {
         fail("This test is supposed to be run in a local configuration");
      }

      Principal nabopolassar = new SimplePrincipal("nabopolassar");
      Set principals = new HashSet();
      principals.add(nabopolassar);
      Subject subject =
         new Subject(false, principals, Collections.EMPTY_SET, Collections.EMPTY_SET);
      Principal nebuchadrezzar = new SimplePrincipal("nebuchadrezzar");

      SecurityAssociation.pushSubjectContext(subject, nebuchadrezzar, "xexe");

      Connection conn = null;

      try
      {
         conn = cf.createConnection();
         conn.start();

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = session.createProducer(queue1);
         MessageConsumer cons = session.createConsumer(queue1);

         TextMessage m = session.createTextMessage("floccinaucinihilipilification");

         prod.send(m);

         TextMessage rm = (TextMessage)cons.receive(5000);

         assertEquals("floccinaucinihilipilification", rm.getText());

         SecurityAssociation.SubjectContext context = SecurityAssociation.popSubjectContext();

         Subject s = context.getSubject();
         assertNotNull(s);
         Set ps = s.getPrincipals();
         assertNotNull(ps);
         assertEquals(1, ps.size());
         Principal p = (Principal)ps.iterator().next();
         assertTrue(p instanceof SimplePrincipal);
         assertEquals("nabopolassar", ((SimplePrincipal)p).getName());

         p = context.getPrincipal();
         assertNotNull(p);
         assertTrue(p instanceof SimplePrincipal);
         assertEquals("nebuchadrezzar", ((SimplePrincipal)p).getName());

         Object o = context.getCredential();
         assertNotNull(o);
         assertEquals("xexe", o);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   /**
    * Test for http://jira.jboss.org/jira/browse/JBMESSAGING-824
    *
    * Send a message to a queue that requires write permissions, and make sure the thread local
    * SecurityContext stack is correctly cleaned up after that. We're using a test security
    * manager that simulates a JBoss JaasSecurityManager.
    *
    */
   public void testGuestAuthorizedSend() throws Exception
   {
      if(ServerManagement.isRemote())
      {
         fail("This test is supposed to be run in a local configuration");
      }

      MockJBossSecurityManager sm =
         (MockJBossSecurityManager)ic.lookup(MockJBossSecurityManager.TEST_SECURITY_DOMAIN);
      assertTrue(sm.isSimulateJBossJaasSecurityManager());

      Principal nabopolassar = new SimplePrincipal("nabopolassar");
      Set principals = new HashSet();
      principals.add(nabopolassar);
      Subject subject =
         new Subject(false, principals, Collections.EMPTY_SET, Collections.EMPTY_SET);
      Principal nebuchadrezzar = new SimplePrincipal("nebuchadrezzar");

      SecurityAssociation.pushSubjectContext(subject, nebuchadrezzar, "xexe");

      Connection conn = null;

      try
      {
         conn = cf.createConnection();
         conn.start();

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = session.createProducer(queue2);
         MessageConsumer cons = session.createConsumer(queue2);

         TextMessage m = session.createTextMessage("floccinaucinihilipilification");

         prod.send(m);

         TextMessage rm = (TextMessage)cons.receive(5000);

         assertEquals("floccinaucinihilipilification", rm.getText());

         SecurityAssociation.SubjectContext context = SecurityAssociation.popSubjectContext();

         Subject s = context.getSubject();
         assertNotNull(s);
         Set ps = s.getPrincipals();
         assertNotNull(ps);
         assertEquals(1, ps.size());
         Principal p = (Principal)ps.iterator().next();
         assertTrue(p instanceof SimplePrincipal);
         assertEquals("nabopolassar", ((SimplePrincipal)p).getName());

         p = context.getPrincipal();
         assertNotNull(p);
         assertTrue(p instanceof SimplePrincipal);
         assertEquals("nebuchadrezzar", ((SimplePrincipal)p).getName());

         Object o = context.getCredential();
         assertNotNull(o);
         assertEquals("xexe", o);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   /**
    * Test for http://jira.jboss.org/jira/browse/JBMESSAGING-824
    *
    * Send a message to a queue that requires write permissions, and make sure the thread local
    * SecurityContext stack is correctly cleaned up after that. We're using a test security
    * manager that simulates a JBoss JaasSecurityManager.
    */
   public void testAuthorizedSend() throws Exception
   {
      if(ServerManagement.isRemote())
      {
         fail("This test is supposed to be run in a local configuration");
      }

      MockJBossSecurityManager sm =
         (MockJBossSecurityManager)ic.lookup(MockJBossSecurityManager.TEST_SECURITY_DOMAIN);
      assertTrue(sm.isSimulateJBossJaasSecurityManager());

      Principal nabopolassar = new SimplePrincipal("nabopolassar");
      Set principals = new HashSet();
      principals.add(nabopolassar);
      Subject subject =
         new Subject(false, principals, Collections.EMPTY_SET, Collections.EMPTY_SET);
      Principal nebuchadrezzar = new SimplePrincipal("nebuchadrezzar");

      SecurityAssociation.pushSubjectContext(subject, nebuchadrezzar, "xexe");

      Connection conn = null;

      try
      {
         conn = cf.createConnection("john", "needle");
         conn.start();

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = session.createProducer(queue2);
         MessageConsumer cons = session.createConsumer(queue2);

         TextMessage m = session.createTextMessage("floccinaucinihilipilification");

         prod.send(m);

         TextMessage rm = (TextMessage)cons.receive(5000);

         assertEquals("floccinaucinihilipilification", rm.getText());

         SecurityAssociation.SubjectContext context = SecurityAssociation.popSubjectContext();

         Subject s = context.getSubject();
         assertNotNull(s);
         Set ps = s.getPrincipals();
         assertNotNull(ps);
         assertEquals(1, ps.size());
         Principal p = (Principal)ps.iterator().next();
         assertTrue(p instanceof SimplePrincipal);
         assertEquals("nabopolassar", ((SimplePrincipal)p).getName());

         p = context.getPrincipal();
         assertNotNull(p);
         assertTrue(p instanceof SimplePrincipal);
         assertEquals("nebuchadrezzar", ((SimplePrincipal)p).getName());

         Object o = context.getCredential();
         assertNotNull(o);
         assertEquals("xexe", o);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         fail("This test is supposed to be run in a local configuration");
      }

      super.setUp();
   
      final String secureQueueConfig =
         "<security>" +
            "<role name=\"publisher\" read=\"true\" write=\"true\" create=\"false\"/>" +
            "<role name=\"guest\" read=\"true\" write=\"true\" create=\"false\"/>" +
         "</security>";
      ServerManagement.configureSecurityForDestination("Queue2", secureQueueConfig);

      // make MockSecurityManager simulate JaasSecurityManager behavior. This is the whole point
      // of this test, to catch JBoss AS integreation failure before the integration test suite
      // does. However, this MUST NOT be a replacement for integration tests, it's just an
      // additional safety layer.

      MockJBossSecurityManager sm =
         (MockJBossSecurityManager)ic.lookup(MockJBossSecurityManager.TEST_SECURITY_DOMAIN);

      sm.setSimulateJBossJaasSecurityManager(true);

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {   
   	super.tearDown();
   	
      MockJBossSecurityManager sm =
         (MockJBossSecurityManager)ic.lookup(MockJBossSecurityManager.TEST_SECURITY_DOMAIN);

      sm.setSimulateJBossJaasSecurityManager(false);

      ServerManagement.configureSecurityForDestination("Queue2", null);
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}
