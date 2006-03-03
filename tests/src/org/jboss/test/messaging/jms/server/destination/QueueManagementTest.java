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
package org.jboss.test.messaging.jms.server.destination;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import javax.management.ObjectName;

import org.jboss.test.messaging.jms.server.destination.base.DestinationManagementTestBase;
import org.jboss.test.messaging.tools.ServerManagement;


/**
 * Tests a queue's management interface.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class QueueManagementTest extends DestinationManagementTestBase
{
   // Constants -----------------------------------------------------
   private static final String MESSAGE_TWO = "message two";

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public QueueManagementTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      super.tearDown();
   }

   public void testMessageCount() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      
      ServerManagement.deployQueue("QueueMessageCount");
      Queue queue = (Queue)ic.lookup("/queue/QueueMessageCount");
      
      // Test MessageCount, should be 0 msg
      ObjectName destObjectName = 
         new ObjectName("jboss.messaging.destination:service=Queue,name=QueueMessageCount");
      Integer count = (Integer)ServerManagement.getAttribute(destObjectName, "MessageCount");
      assertEquals(0, count.intValue());
      
      // Send 1 message to queue
      Connection conn = cf.createConnection();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = session.createProducer(queue);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      TextMessage m = session.createTextMessage("message one");
      prod.send(m);
      conn.close();
      
      // Test MessageCount again, should be 1 msg
      count = (Integer)ServerManagement.getAttribute(destObjectName, "MessageCount");
      assertEquals(1, count.intValue());
      
      
      // Consume the message
      conn = cf.createConnection();
      session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer cons = session.createConsumer(queue);
      conn.start();
      
      cons.receive();
      conn.close();
      
      // Test MessageCount again, should be 0 msg
      count = (Integer)ServerManagement.getAttribute(destObjectName, "MessageCount");
      assertEquals(0, count.intValue());
      
      ServerManagement.undeployQueue("QueueMessageCount");
   }
   
   // TODO this test should be done in DestinationManagementTestBase, once implemented in Topic
   // TODO this only tests reliable non-tx messages
   public void testRemoveAllMessages() throws Exception
   {
      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      
      ServerManagement.deployQueue("QueueRemoveMessages");
      Queue queue = (Queue)ic.lookup("/queue/QueueRemoveMessages");
      
      // Send 1 message to queue
      Connection conn = cf.createConnection();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = session.createProducer(queue);
      prod.setDeliveryMode(DeliveryMode.PERSISTENT);
      
      TextMessage m = session.createTextMessage("message one");
      prod.send(m);
      
      // Remove all messages from the queue
      ObjectName destObjectName = 
         new ObjectName("jboss.messaging.destination:service=Queue,name=QueueRemoveMessages");
      ServerManagement.invoke(destObjectName, "removeAllMessages", null, null);

      // Test MessageCount again, should be 0 msg
      Integer count = (Integer)ServerManagement.getAttribute(destObjectName, "MessageCount");
      assertEquals(0, count.intValue());

      // Send another message
      m = session.createTextMessage(MESSAGE_TWO);
      prod.send(m);
      conn.close();

      // Consume the 2nd message
      conn = cf.createConnection();
      session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer cons = session.createConsumer(queue);
      conn.start();
      
      Object ms = cons.receive();
      assertTrue(ms instanceof TextMessage);
      assertEquals(((TextMessage)ms).getText(), MESSAGE_TWO);
      assertNull(cons.receiveNoWait());
      conn.close();
      
      ServerManagement.undeployQueue("QueueRemoveMessages");
   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

   protected boolean isQueue()
   {
      return true;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
