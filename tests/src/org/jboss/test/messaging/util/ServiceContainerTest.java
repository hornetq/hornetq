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
   
   private ConnectionFactory cf;
   private Queue queue;

   // Constructors --------------------------------------------------

   public ServiceContainerTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void test1() throws Exception
   {
     
      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = sess.createProducer(queue);
      for (int i = 0; i < 100; i++)
      {
         prod.send(sess.createMessage());
      }
      conn.close();      
   }
   
   public void test2() throws Exception
   {
     
      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = sess.createProducer(queue);
      for (int i = 0; i < 100; i++)
      {
         prod.send(sess.createMessage());
      }
      conn.close();      
   }
   
   public void test3() throws Exception
   {
     
      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = sess.createProducer(queue);
      for (int i = 0; i < 100; i++)
      {
         prod.send(sess.createMessage());
      }
      conn.close();      
   }
   
   public void test4() throws Exception
   {
     
      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = sess.createProducer(queue);
      for (int i = 0; i < 100; i++)
      {
         prod.send(sess.createMessage());
      }
      conn.close();      
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();
      
      ServerManagement.start("all");

      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");

      ServerManagement.deployQueue("Queue");
      queue = (Queue)ic.lookup("/queue/Queue");

      ic.close();
   }

   protected void tearDown() throws Exception
   {
      ServerManagement.stop();
      
      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}


