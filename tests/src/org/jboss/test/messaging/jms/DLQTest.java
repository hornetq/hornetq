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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import javax.naming.NameNotFoundException;

import org.jboss.jms.destination.JBossQueue;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * A DLQTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 1.1 $</tt>
 *
 * $Id$
 *
 */
public class DLQTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected InitialContext ic;
   protected ConnectionFactory cf;
   protected Queue queue;

   // Constructors --------------------------------------------------

   public DLQTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testDLQAlreadyDeployed() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         //This test can only run in local mode
         return;
      }

      ServerManagement.deployQueue("DLQ");

      org.jboss.messaging.core.Queue dlq = ServerManagement.getServer().getServerPeer().getDLQ();

      assertNotNull(dlq);

      InitialContext ic = null;

      try
      {
         ic = new InitialContext(ServerManagement.getJNDIEnvironment());

         JBossQueue q = (JBossQueue)ic.lookup("/queue/DLQ");

         assertNotNull(q);

         assertEquals("DLQ", q.getName());
      }
      finally
      {
         if (ic != null) ic.close();

         log.info("undeploying dlq");
         ServerManagement.undeployQueue("DLQ");
         log.info("undeployed dlq");
      }
   }

   public void testDLQNotAlreadyDeployed() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         //This test can only run in local mode
         return;
      }

      org.jboss.messaging.core.Queue dlq = ServerManagement.getServer().getServerPeer().getDLQ();

      assertNull(dlq);

      InitialContext ic = null;

      try
      {
         ic = new InitialContext(ServerManagement.getJNDIEnvironment());

         try
         {
            ic.lookup("/queue/DLQ");

            fail();
         }
         catch (NameNotFoundException e)
         {
            //Ok
         }
      }
      finally
      {
         if (ic != null) ic.close();
      }
   }
//
//   public void testSendToDLQWithMessageListenerPersistent() throws Exception
//   {
//      sendToDLQWithMessageListener(true);
//   }
//
//   public void testSendToDLQWithMessageListenerNonPersistent() throws Exception
//   {
//      sendToDLQWithMessageListener(false);
//   }
//
//   public void testSendToDLQWithReceivePersistent() throws Exception
//   {
//      sendToDLQWithReceive(true);
//   }
//
//   public void testSendToDLQWithReceiveNonPersistent() throws Exception
//   {
//      sendToDLQWithReceive(false);
//   }
//
//   public void testSendToDLQWithReceivePartialPersistent() throws Exception
//   {
//      sendToDLQWithReceivePartial(true);
//   }
//
//   public void testSendToDLQWithReceivePartialNonPersistent() throws Exception
//   {
//      sendToDLQWithReceivePartial(false);
//   }
//
//   public void sendToDLQWithMessageListener(boolean persistent) throws Exception
//   {
//      Connection conn = null;
//
//      ServerManagement.deployQueue("DLQ");
//
//      Queue dlq = (Queue)ic.lookup("/queue/DLQ");
//
//      try
//      {
//         conn = cf.createConnection();
//
//         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//
//         MessageProducer prod = sess.createProducer(queue);
//
//         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
//
//         for (int i = 0; i < 10; i++)
//         {
//            TextMessage tm = sess.createTextMessage("Message:" + i);
//
//            prod.send(tm);
//         }
//
//         MessageConsumer cons = sess.createConsumer(queue);
//
//         cons.setMessageListener(new FailingMessageListener());
//
//         conn.start();
//
//         Thread.sleep(4000);
//
//         cons.setMessageListener(null);
//
//         Message m = cons.receive(1000);
//
//         assertNull(m);
//
//         //Message should all be in the dlq - let's check
//
//         MessageConsumer cons2 = sess.createConsumer(dlq);
//
//         for (int i = 0; i < 10; i++)
//         {
//            TextMessage tm = (TextMessage)cons2.receive(1000);
//
//            assertNotNull(tm);
//
//            assertEquals("Message:" + i, tm.getText());
//         }
//
//      }
//      finally
//      {
//         ServerManagement.undeployQueue("DLQ");
//
//         if (conn != null) conn.close();
//      }
//   }
//
//   public void sendToDLQWithReceive(boolean persistent) throws Exception
//   {
//      Connection conn = null;
//
//      ServerManagement.deployQueue("DLQ");
//
//      Queue dlq = (Queue)ic.lookup("/queue/DLQ");
//
//      try
//      {
//         conn = cf.createConnection();
//
//         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//
//         MessageProducer prod = sess.createProducer(queue);
//
//         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
//
//         for (int i = 0; i < 10; i++)
//         {
//            TextMessage tm = sess.createTextMessage("Message:" + i);
//
//            prod.send(tm);
//         }
//
//         Session sess2 = conn.createSession(true, Session.SESSION_TRANSACTED);
//
//         MessageConsumer cons = sess2.createConsumer(queue);
//
//         conn.start();
//
//         for (int i = 0; i < 10; i++)  // retries - default is 10
//         {
//            for (int j = 0; j < 10; j++)
//            {
//               TextMessage tm = (TextMessage)cons.receive(1000);
//
//               assertNotNull(tm);
//
//               assertEquals("Message:" + j, tm.getText());
//            }
//
//            //rollback should cause redelivery
//            sess2.rollback();
//         }
//
//         cons.close();
//
//         MessageConsumer cons2 = sess2.createConsumer(queue);
//
//         Message m = cons2.receive(1000);
//
//         assertNull(m);
//
//         //Message should all be in the dlq - let's check
//
//         MessageConsumer cons3 = sess.createConsumer(dlq);
//
//         for (int i = 0; i < 10; i++)
//         {
//            TextMessage tm = (TextMessage)cons3.receive(1000);
//
//            assertNotNull(tm);
//
//            assertEquals("Message:" + i, tm.getText());
//         }
//
//      }
//      finally
//      {
//         ServerManagement.undeployQueue("DLQ");
//
//         if (conn != null) conn.close();
//      }
//   }
//
//   public void sendToDLQWithReceivePartial(boolean persistent) throws Exception
//   {
//      Connection conn = null;
//
//      ServerManagement.deployQueue("DLQ");
//
//      Queue dlq = (Queue)ic.lookup("/queue/DLQ");
//
//      try
//      {
//         conn = cf.createConnection();
//
//         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
//
//         MessageProducer prod = sess.createProducer(queue);
//
//         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);
//
//         for (int i = 0; i < 10; i++)
//         {
//            TextMessage tm = sess.createTextMessage("Message:" + i);
//
//            prod.send(tm);
//         }
//
//         Session sess2 = conn.createSession(true, Session.SESSION_TRANSACTED);
//
//         MessageConsumer cons = sess2.createConsumer(queue);
//
//         conn.start();
//
//         for (int i = 0; i < 5; i++)  // retries - default is 10
//         {
//            for (int j = 0; j < 10; j++)
//            {
//               TextMessage tm = (TextMessage)cons.receive(1000);
//
//               assertNotNull(tm);
//
//               assertEquals("Message:" + j, tm.getText());
//            }
//
//            //rollback should cause redelivery
//            sess2.rollback();
//         }
//
//         //They should now be cancelled back to the server
//         cons.close();
//
//         cons = sess2.createConsumer(queue);
//
//         for (int i = 0; i < 5; i++)  // retries - default is 10
//         {
//            for (int j = 0; j < 10; j++)
//            {
//               TextMessage tm = (TextMessage)cons.receive(1000);
//
//               assertNotNull(tm);
//
//               assertEquals("Message:" + j, tm.getText());
//            }
//
//            //rollback should cause redelivery
//            sess2.rollback();
//         }
//
//         cons.close();
//
//         //Now they should be in DLQ
//
//         MessageConsumer cons2 = sess2.createConsumer(queue);
//
//         Message m = cons2.receive(1000);
//
//         assertNull(m);
//
//         //Message should all be in the dlq - let's check
//
//         MessageConsumer cons3 = sess.createConsumer(dlq);
//
//         for (int i = 0; i < 10; i++)
//         {
//            TextMessage tm = (TextMessage)cons3.receive(1000);
//
//            assertNotNull(tm);
//
//            assertEquals("Message:" + i, tm.getText());
//         }
//
//      }
//      finally
//      {
//         ServerManagement.undeployQueue("DLQ");
//
//         if (conn != null) conn.close();
//      }
//   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("all");

      ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");

      ServerManagement.deployQueue("Queue");

      queue = (Queue)ic.lookup("/queue/Queue");

   }

   protected void tearDown() throws Exception
   {
      super.tearDown();

      ServerManagement.undeployQueue("Queue");

      if (ic != null) ic.close();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   
   class FailingMessageListener implements MessageListener
   {

      public void onMessage(Message msg)
      {
         throw new RuntimeException("Your mum!");
      }
      
   }

}
