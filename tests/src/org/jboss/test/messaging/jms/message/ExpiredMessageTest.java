/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.message;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.logging.Logger;

import javax.naming.InitialContext;
import javax.jms.ConnectionFactory;
import javax.jms.Queue;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.XAConnectionFactory;
import javax.jms.XAConnection;
import javax.jms.TextMessage;
import javax.management.ObjectName;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
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

   // Constructors ---------------------------------------------------------------------------------

   public ExpiredMessageTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testSimpleExpiration() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      Queue queue = (Queue)ic.lookup("/queue/expiredMessageTestQueue");

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
   }

   public void testExpirationTransfer() throws Exception
   {

      ServerManagement.deployQueue("expiredTarget");

      Object originalValue = ServerManagement.getAttribute(ServerManagement.getServerPeerObjectName(), "DefaultExpiryQueue");

      ServerManagement.setAttribute(ServerManagement.getServerPeerObjectName(), "DefaultExpiryQueue", "jboss.messaging.destination:service=Queue,name=expiredTarget");

      try
      {

         ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
         Queue queue = (Queue)ic.lookup("/queue/expiredMessageTestQueue");

         Connection conn = cf.createConnection();

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = session.createConsumer(queue);

         conn.start();

         MessageProducer prod = session.createProducer(queue);
         prod.setTimeToLive(100);

         Message m = session.createTextMessage("This message will die");

         prod.send(m);

         assertNull(cons.receive(3000));

         // wait for the message to die

         Thread.sleep(2000);

         Queue queueExpiryQueue = (Queue)ic.lookup("/queue/expiredTarget");

         MessageConsumer consumerExpiredQueue = session.createConsumer(queue);

         TextMessage txt = (TextMessage) consumerExpiredQueue.receive(1000);

         assertEquals("This message will die", txt.getText());

         assertNull(consumerExpiredQueue.receive(1000));
      }
      finally
      {
         ServerManagement.destroyQueue("expiredSource");
         ServerManagement.destroyQueue("expiredTarget");
         ServerManagement.setAttribute(ServerManagement.getServerPeerObjectName(), "DefaultExpiryQueue", originalValue.toString());
      }

   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("all");

      ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      ServerManagement.deployQueue("expiredMessageTestQueue");

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("expiredMessageTestQueue");

      ServerManagement.stop();

      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}
