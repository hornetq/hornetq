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
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
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
      ServerManagement.start("all");
      
      
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (QueueConnectionFactory)initialContext.lookup("/ConnectionFactory");
      ServerManagement.undeployQueue("Queue");
      ServerManagement.deployQueue("Queue");
      queue = (Queue)initialContext.lookup("/queue/Queue");          
   }

   public void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("Queue");
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
