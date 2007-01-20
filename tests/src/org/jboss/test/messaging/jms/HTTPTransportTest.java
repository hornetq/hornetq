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
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;
import javax.management.ObjectName;

/**
 * This class contain tests that only make sense for a HTTP transport. They will be ignored for
 * any other kind of transport.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class HTTPTransportTest extends MessagingTestCase
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

   private InitialContext ic;
   private ConnectionFactory cf;
   private Queue queue;
   private ObjectName queueObjectName;

   // Constructors ---------------------------------------------------------------------------------

   public HTTPTransportTest(String name)
   {
      super(name);
   }

   // Public ---------------------------------------------------------------------------------------

   public void testCallbackList() throws Exception
   {
      if (!"http".equals(ServerManagement.getRemotingTransport(0)))
      {
         log.warn("The server we are connecting to did not start its remoting service " +
            "with HTTP transport enabled, skipping test ...");
         return;
      }

      // send a bunch of messages and let them accumulate in the queue
      Connection conn = cf.createConnection();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = session.createProducer(queue);

      int messageCount = 20;

      for(int i = 0; i < messageCount; i++)
      {
         Message m = session.createTextMessage("krakatau" + i);
         prod.send(m);
      }

      conn.close();

      // make sure messages made it to the queue
      Integer count = (Integer)ServerManagement.getAttribute(queueObjectName, "MessageCount");
      assertEquals(messageCount, count.intValue());


      conn = cf.createConnection();
      session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer cons = session.createConsumer(queue);

      conn.start();

      // messages will be sent in bulk from server side, on the next HTTP client listner poll

      for(int i = 0; i < messageCount; i++)
      {
         TextMessage t = (TextMessage)cons.receive(2000);
         assertNotNull(t);
         assertEquals("krakatau" + i, t.getText());
      }
      
      conn.close();

   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("all");

      ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      ServerManagement.deployQueue("HTTPTestQueue");

      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

      queue = (Queue)ic.lookup("/queue/HTTPTestQueue");

      queueObjectName =
         new ObjectName("jboss.messaging.destination:service=Queue,name=HTTPTestQueue");

      ServerManagement.invoke(queueObjectName, "removeAllMessages", new Object[0], new String[0]);

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("HTTPTestQueue");

      ic.close();

      super.tearDown();
   }

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}
