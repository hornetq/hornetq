/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

import javax.naming.InitialContext;
import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.management.ObjectName;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConsumerClosedTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   public static final int NUMBER_OF_MESSAGES = 10;

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   InitialContext ic;

   // Constructors --------------------------------------------------

   public ConsumerClosedTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------


   public void testMessagesSentDuringClose() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      Queue queue = (Queue)ic.lookup("/queue/ConsumerClosedTestQueue");

      Connection c = cf.createConnection();
      c.start();

      Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer p = s.createProducer(queue);

      for(int i = 0; i < NUMBER_OF_MESSAGES; i++)
      {
         p.send(s.createTextMessage("message" + i));
      }

      log.debug("all messages sent");

      MessageConsumer cons = s.createConsumer(queue);
      cons.close();

      log.debug("consumer closed");
      
      //Need to sleep - cancelling back to queue is asynch and can take some time
      Thread.sleep(500);

      // make sure that all messages are in queue
      ObjectName on =
         new ObjectName("jboss.messaging.destination:service=Queue,name=ConsumerClosedTestQueue");
      Integer count = (Integer)ServerManagement.getAttribute(on, "MessageCount");
      assertEquals(NUMBER_OF_MESSAGES, count.intValue());

      //Thread.sleep(900000000);


   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("all");

      ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      ServerManagement.deployQueue("ConsumerClosedTestQueue");

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("ConsumerClosedTestQueue");

      ic.close();

      super.tearDown();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
