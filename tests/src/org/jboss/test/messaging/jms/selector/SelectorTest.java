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
package org.jboss.test.messaging.jms.selector;

import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.MessagingTestCase;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.naming.InitialContext;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SelectorTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected ConnectionFactory cf;
   protected Connection conn;
   protected Session session;
   protected MessageProducer prod;
   protected Queue queue;

   // Constructors --------------------------------------------------

   public SelectorTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("all");
      
      
      ServerManagement.undeployQueue("Queue");
      ServerManagement.deployQueue("Queue");

      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      queue = (Queue)ic.lookup("/queue/Queue");
      conn = cf.createConnection();
      session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      prod = session.createProducer(queue);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      conn.close();
      ServerManagement.undeployQueue("Queue");
      
      super.tearDown();
   }


   // Public --------------------------------------------------------


   /**
    * Test case for http://jira.jboss.org/jira/browse/JBMESSAGING-105
    *
    * Two Messages are sent to a queue. There is one receiver on the queue. The receiver only
    * receives one of the messages due to a message selector only matching one of them. The receiver
    * is then closed. A new receiver is now attached to the queue. Redelivery of the remaining
    * message is now attempted. The message should be redelivered.
    */
   public void testSelectiveClosingConsumer() throws Exception
   {
      String selector = "color = 'red'";
      MessageConsumer redConsumer = session.createConsumer(queue, selector);
      conn.start();

      Message redMessage = session.createMessage();
      redMessage.setStringProperty("color", "red");

      Message blueMessage = session.createMessage();
      blueMessage.setStringProperty("color", "blue");

      prod.send(redMessage);
      prod.send(blueMessage);
      
      Message rec = redConsumer.receive();
      assertEquals(redMessage.getJMSMessageID(), rec.getJMSMessageID());
      assertEquals("red", rec.getStringProperty("color"));
      
      assertNull(redConsumer.receive(3000));
      
      redConsumer.close();
      
      MessageConsumer universalConsumer = session.createConsumer(queue);
      
      rec = universalConsumer.receive();
      
      assertEquals(rec.getJMSMessageID(), blueMessage.getJMSMessageID());
      assertEquals("blue", rec.getStringProperty("color"));
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
