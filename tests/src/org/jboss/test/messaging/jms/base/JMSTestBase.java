/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */


package org.jboss.test.messaging.jms.base;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.jms.client.JBossConnectionFactory;

import javax.jms.Queue;
import javax.jms.Topic;
import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.naming.InitialContext;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JMSTestBase extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected ConnectionFactory connFactory;
   protected Connection conn;
   protected Session session;
   protected MessageProducer queueProd;
   protected MessageConsumer queueCons;

   protected Queue queue;
   protected Topic topic;


   // Constructors --------------------------------------------------

   public JMSTestBase(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.startInVMServer("all");

      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      connFactory = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");

      conn = connFactory.createConnection();
      session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      ServerManagement.deployQueue("Queue");
      queue = (Queue)ic.lookup("/queue/Queue");

      ServerManagement.deployTopic("Topic");
      topic = (Topic)ic.lookup("/topic/Topic");

      queueProd = session.createProducer(queue);
      queueCons = session.createConsumer(queue);

      conn.start();

      ic.close();
   }

   public void tearDown() throws Exception
   {
      conn.close();
      ServerManagement.undeployQueue("Queue");
      ServerManagement.undeployQueue("Queue");
      ServerManagement.stopInVMServer();

      super.tearDown();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
