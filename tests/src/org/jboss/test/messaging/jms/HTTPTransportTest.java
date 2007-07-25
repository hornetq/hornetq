/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.jboss.test.messaging.tools.ServerManagement;

/**
 * This class contain tests that only make sense for a HTTP transport. They will be ignored for
 * any other kind of transport.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 * $Id$
 */
public class HTTPTransportTest extends JMSTestCase
{
   // Constants ------------------------------------------------------------------------------------

   // Static ---------------------------------------------------------------------------------------

   // Attributes -----------------------------------------------------------------------------------

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
      MessageProducer prod = session.createProducer(queue1);

      int messageCount = 20;

      for(int i = 0; i < messageCount; i++)
      {
         Message m = session.createTextMessage("krakatau" + i);
         prod.send(m);
      }

      conn.close();

      // make sure messages made it to the queue
      assertRemainingMessages(messageCount);

      conn = cf.createConnection();
      session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer cons = session.createConsumer(queue1);

      conn.start();

      // messages will be sent in bulk from server side, on the next HTTP client listener poll

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

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}
