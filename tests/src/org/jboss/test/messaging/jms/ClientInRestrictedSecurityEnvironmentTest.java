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
package org.jboss.test.messaging.jms;

import java.net.SocketPermission;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.misc.ConfigurableSecurityManager;

/**
 * This test runs the JMS client in a restricted security environments.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 */
public class ClientInRestrictedSecurityEnvironmentTest extends JMSTestCase
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private SecurityManager oldSM;
   private ConfigurableSecurityManager configurableSecurityManager;

   // Constructors ---------------------------------------------------------------------------------

   public ClientInRestrictedSecurityEnvironmentTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   /**
    * Test case for http://jira.jboss.org/jira/browse/JBMESSAGING-806
    */
   public void testGetSystemProperties() throws Exception
   {
      // TODO (ovidiu) Will be uncommented in 1.0.1.SP5 and 1.2.0.CR1
      //      See http://jira.jboss.org/jira/browse/JBMESSAGING-806

//      if (ServerManagement.isRemote())
//      {
//         // don't run in a remote configuration, so we won't have to open server sockets and
//         // interfere with those permissions (or lack of)
//         return;
//      }
//
//      // make sure our security manager disallows getProperty()
//      configurableSecurityManager.dissalow(new PropertyPermission("does not matter", "read"));
//
//      ConnectionFactory cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");
//      Queue queue = (Queue)ic.lookup("/queue/TestQueue");
//
//      Connection conn = null;
//
//      try
//      {
//         conn = cf.createConnection();
//
//         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//
//         MessageProducer p = s.createProducer(queue);
//         MessageConsumer c = s.createConsumer(queue);
//
//         conn.start();
//
//         p.send(s.createTextMessage("payload"));
//
//         TextMessage m = (TextMessage)c.receive();
//
//         assertEquals("payload", m.getText());
//
//      }
//      finally
//      {
//         if (conn != null)
//         {
//            conn.close();
//         }
//      }
   }

   /**
    * This test would make no sense on the 1.0 branch, since we won't backport the bisocket support
    * there.
    */
   public void testSendReceiveWithSecurityManager() throws Exception
   {
      if (ServerManagement.isLocal())
      {
         return;
      }

      // make sure our security manager disallows "listen" and "accept" on a socket
      configurableSecurityManager.disallow(SocketPermission.class, "listen");
      configurableSecurityManager.disallow(SocketPermission.class, "accept");

      Connection conn = null;

      try
      {
         conn = cf.createConnection();

         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer p = s.createProducer(queue1);
         MessageConsumer c = s.createConsumer(queue1);

         conn.start();

         p.send(s.createTextMessage("payload"));

         TextMessage m = (TextMessage)c.receive();

         assertEquals("payload", m.getText());
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

   // MessagingTestCase overrides ------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();
      
      // install our own security manager

      configurableSecurityManager = new ConfigurableSecurityManager();

      oldSM = System.getSecurityManager();
      System.setSecurityManager(configurableSecurityManager);

      log.info("SecurityManager is now " + System.getSecurityManager());
      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
   	super.tearDown();
   	
      configurableSecurityManager.clear();
      configurableSecurityManager = null;

      System.setSecurityManager(oldSM);

      super.tearDown();
   }

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}