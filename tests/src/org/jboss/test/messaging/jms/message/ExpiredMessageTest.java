/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.message;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.InitialContext;

import org.jboss.logging.Logger;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class ExpiredMessageTest extends MessagingTestCase
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(ExpiredMessageTest.class);

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private InitialContext ic;
   private ConnectionFactory cf;
   private Queue queue;

   // Constructors ---------------------------------------------------------------------------------

   public ExpiredMessageTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testSimpleExpiration() throws Exception
   {
      Connection conn = cf.createConnection();

      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer prod = session.createProducer(queue);
      prod.setTimeToLive(1);

      Message m = session.createTextMessage("This message will die");

      prod.send(m);

      // wait for the message to die

      Thread.sleep(1000);

      MessageConsumer cons = session.createConsumer(queue);

      conn.start();

      assertNull(cons.receive(3000));
      
      conn.close();
   }
   
   public void testManyExpiredMessagesAtOnce() throws Exception
   {
      Connection conn = cf.createConnection();
      
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prod = session.createProducer(queue);
      prod.setTimeToLive(1);
      
      Message m = session.createTextMessage("This message will die");
      
      final int MESSAGE_COUNT = 2000;

      log.info("Going to send " + MESSAGE_COUNT + " messages");

      for (int i = 0; i < MESSAGE_COUNT; i++)
      {
         prod.send(m);
         if ((i + 1) % 1000 == 0)
         {
            log.info("Sent " + (i + 1) + " messages out of " + MESSAGE_COUNT);
         }
      }
      
      Thread.sleep(1000);
      
      log.info("Creating consumer");

      MessageConsumer cons = session.createConsumer(queue);
      conn.start();
      
      final int TIMEOUT = 3000;
      log.info("Trying to receive a message, timeout is " + TIMEOUT + " ms");

      assertNull(cons.receive(TIMEOUT));
      
      log.info("Done");
      
      conn.close();
   }

  

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("all");

      ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      ServerManagement.deployQueue("expiredMessageTestQueue");
      ServerManagement.deployQueue("ExpiryQueue");

      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

      queue = (Queue)ic.lookup("/queue/expiredMessageTestQueue");

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("expiredMessageTestQueue");
      ServerManagement.undeployQueue("ExpiryQueue");
      
      ic.close();

      ServerManagement.stop();

      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}
