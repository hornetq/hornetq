/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.util;

import org.jboss.logging.Logger;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.jms.client.JBossConnectionFactory;

import javax.naming.InitialContext;
import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.MessageProducer;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class ServiceContainerTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ServiceContainerTest.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public ServiceContainerTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testServiceContainer() throws Exception
   {
      ServerManagement.start("all");

      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      ConnectionFactory cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");

      log.info("connection factory is " + cf);

      ServerManagement.deployQueue("Queue");
      Queue queue = (Queue)ic.lookup("/queue/Queue");

      ic.close();

      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = sess.createProducer(queue);
      prod.send(sess.createMessage());
      conn.close();

      ServerManagement.stop();

      ServerManagement.start("all");

      // Now try it again

      ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");

      // problem is that we have the same cf as before even though we started and stopped the
      // servicecontainer

      log.info("Connection factory is now " + cf);

      ServerManagement.deployQueue("Queue");
      queue = (Queue)ic.lookup("/queue/Queue");

      conn = cf.createConnection();
      sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      prod = sess.createProducer(queue);
      prod.send(sess.createMessage());

      conn.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}


