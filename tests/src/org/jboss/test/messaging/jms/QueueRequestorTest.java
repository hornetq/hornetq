/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;

import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueReceiver;
import javax.jms.QueueRequestor;
import javax.jms.QueueSender;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class QueueRequestorTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;
   
   protected QueueConnectionFactory cf;
   protected Queue queue;

   // Constructors --------------------------------------------------

   public QueueRequestorTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.setRemote(false);
      ServerManagement.startInVMServer("all");
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (QueueConnectionFactory)initialContext.lookup("/ConnectionFactory");
      ServerManagement.deployQueue("Queue");
      queue = (Queue)initialContext.lookup("/queue/Queue");
   }

   public void tearDown() throws Exception
   {
      ServerManagement.stopInVMServer();
      super.tearDown();
   }


   // Public --------------------------------------------------------

   public void testQueueRequestor() throws Exception
   {
      // Set up the requestor
      QueueConnection conn1 = cf.createQueueConnection();
      QueueSession sess1 = conn1.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      QueueRequestor requestor = new QueueRequestor(sess1, queue);
      conn1.start();
      

      // And the responder
      QueueConnection conn2 = cf.createQueueConnection();
      QueueSession sess2 = conn2.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      TestMessageListener listener = new TestMessageListener(sess2);
      QueueReceiver receiver = sess2.createReceiver(queue);
      receiver.setMessageListener(listener);
      conn2.start();
      
      Message m1 = sess1.createMessage();
      log.trace("Sending request message");
      TextMessage m2 = (TextMessage)requestor.request(m1);
      
      
      assertNotNull(m2);
      
      assertEquals("This is the response", m2.getText());
      
      conn1.close();
      conn2.close();
      
      
   }
   
   class TestMessageListener implements MessageListener
   {
      private QueueSession sess;
      private QueueSender sender;
      
      public TestMessageListener(QueueSession sess)
         throws JMSException
      {
         this.sess = sess;
         this.sender = sess.createSender(null);
      }
      
      public void onMessage(Message m)
      {
         try
         {
            log.trace("Received message");
            Destination queue = m.getJMSReplyTo();
            log.trace("Sending response back to:" + queue);
            Message m2 = sess.createTextMessage("This is the response");
            sender.send(queue, m2);
         }
         catch (JMSException e)
         {
            log.error(e);
         }
      }
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

   
}
