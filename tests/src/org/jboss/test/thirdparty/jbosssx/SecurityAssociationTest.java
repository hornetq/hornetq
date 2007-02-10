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

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.security.SecurityAssociation;
import org.jboss.security.SimplePrincipal;

import javax.naming.InitialContext;
import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;
import javax.security.auth.Subject;
import java.security.Principal;
import java.util.Set;
import java.util.HashSet;
import java.util.Collections;

/**
 * Set of tests to insure consistent behavior relative to the JBoss AS security infrastructure.
 * This is just a safety layer, full fledged security tests should be present in the integration
 * test suite.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class SecurityAssociationTest extends MessagingTestCase
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private InitialContext ic;

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
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      Queue queue = (Queue)ic.lookup("/queue/TestQueue");

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

         MessageProducer prod = session.createProducer(queue);
         MessageConsumer cons = session.createConsumer(queue);

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
      super.setUp();

      ServerManagement.start("all");

      ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      ServerManagement.deployQueue("TestQueue");

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("TestQueue");

      ic.close();

      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}
