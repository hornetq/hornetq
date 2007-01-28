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
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.management.ObjectName;
import javax.naming.InitialContext;
import javax.transaction.Transaction;
import javax.transaction.xa.XAResource;

import org.jboss.jms.client.JBossConnection;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.delegate.ClientConnectionDelegate;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.tx.ResourceManager;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.jms.XAJBossTxMgrTestBase.DummyXAResource;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * $Id$
 */
public class TransactedSessionTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected InitialContext initialContext;
   
   protected JBossConnectionFactory cf;
   protected Destination queue;
   protected Destination topic;
   
   // Constructors --------------------------------------------------
   
   public TransactedSessionTest(String name)
   {
      super(name);
   }
   
   // TestCase overrides -------------------------------------------
   
   public void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.start("all");
      
      
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");
      
      ServerManagement.undeployQueue("Queue");
      ServerManagement.undeployTopic("Topic");
      ServerManagement.deployQueue("Queue");
      ServerManagement.deployTopic("Topic");
      queue = (Destination)initialContext.lookup("/queue/Queue");
      topic = (Destination)initialContext.lookup("/topic/Topic");
      
      this.drainDestination(cf, queue);
      
      log.debug("setup done");
   }
   
   public void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("Queue");
      ServerManagement.undeployTopic("Topic");
    //  TransactionManagerImpl.getInstance().setState(TransactionManagerImpl.OPERATIONAL);
      super.tearDown();
   }
   
   
   // Public --------------------------------------------------------

   public void testResourceManagerMemoryLeakOnCommit() throws Exception
   {

      Connection conn = null;
      
      try
      {
         conn = cf.createConnection();
         
         JBossConnection jbConn = (JBossConnection)conn;
         
         ClientConnectionDelegate del = (ClientConnectionDelegate)jbConn.getDelegate();
         
         ConnectionState state = (ConnectionState)del.getState();
         
         ResourceManager rm = state.getResourceManager();
         
         Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
         
         
         for (int i = 0; i < 100; i++)
         {
            assertEquals(1, rm.size());
            
            session.commit();
            
            assertEquals(1, rm.size());
         }                  
         
         assertEquals(1, rm.size());
         
         conn.close();
         
         conn = null;
         
         assertEquals(0, rm.size());

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   public void testResourceManagerMemoryLeakOnRollback() throws Exception
   {

      Connection conn = null;
      
      try
      {
         conn = cf.createConnection();
         
         JBossConnection jbConn = (JBossConnection)conn;
         
         ClientConnectionDelegate del = (ClientConnectionDelegate)jbConn.getDelegate();
         
         ConnectionState state = (ConnectionState)del.getState();
         
         ResourceManager rm = state.getResourceManager();
         
         Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
         
         
         for (int i = 0; i < 100; i++)
         {
            assertEquals(1, rm.size());
            
            session.commit();
            
            assertEquals(1, rm.size());
         }                  
         
         assertEquals(1, rm.size());
         
         conn.close();
         
         conn = null;
         
         assertEquals(0, rm.size());

      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   

   public void testSimpleRollback() throws Exception
   {
      // send a message
      Connection conn = cf.createConnection();
      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      s.createProducer(queue).send(s.createTextMessage("one"));

      log.debug("message sent");

      s.close();

      s = conn.createSession(true, Session.SESSION_TRANSACTED);
      MessageConsumer c = s.createConsumer(queue);
      conn.start();
      Message m = c.receive();

      assertEquals("one", ((TextMessage)m).getText());
      assertFalse(m.getJMSRedelivered());
      assertEquals(1, m.getIntProperty("JMSXDeliveryCount"));

      s.rollback();

      // get the message again
      m = c.receive();
      assertTrue(m.getJMSRedelivered());
      assertEquals(2, m.getIntProperty("JMSXDeliveryCount"));

      conn.close();
      
      //Need to pause a little while - cancelling back to the queue is async
      
      Thread.sleep(500);

      ObjectName on = new ObjectName("jboss.messaging.destination:service=Queue,name=Queue");
      Integer i = (Integer)ServerManagement.getAttribute(on, "MessageCount");

      assertEquals(1, i.intValue());
   }
   
   public void testRedeliveredFlagTopic() throws Exception
   {
      Connection conn = null;
      
      try
      {
         conn = cf.createConnection();
         
         Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
         Session sess1 = conn.createSession(true, Session.SESSION_TRANSACTED);         
         MessageConsumer consumer1 = sess1.createConsumer(topic);
         
         MessageProducer producer = sessSend.createProducer(topic);
         Message mSent = sessSend.createTextMessage("igloo");
         producer.send(mSent);      
         sessSend.commit();
               
         conn.start();
              
         TextMessage mRec1 = (TextMessage)consumer1.receive(2000);
         assertEquals("igloo", mRec1.getText());
         assertFalse(mRec1.getJMSRedelivered());
         
         sess1.rollback(); //causes redelivery for session

         mRec1 = (TextMessage)consumer1.receive(2000);
         assertEquals("igloo", mRec1.getText());
         assertTrue(mRec1.getJMSRedelivered());
                           
         sess1.commit();
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }
      
   }
   
   
   /** Test redelivery works ok for Topic */
   public void testRedeliveredTopic() throws Exception
   {
      Connection conn = null;
      
      try
      {
         conn = cf.createConnection();
   
         Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = sess.createProducer(topic);
         
         MessageConsumer consumer = sess.createConsumer(topic);
         conn.start();
   
         
         Message mSent = sess.createTextMessage("igloo");
         producer.send(mSent);
         
         sess.commit();
         
         TextMessage mRec = (TextMessage)consumer.receive(2000);

         assertEquals("igloo", mRec.getText());
         assertFalse(mRec.getJMSRedelivered());
         
         sess.rollback();

         mRec = (TextMessage)consumer.receive(2000);

         assertNotNull(mRec);
         assertEquals("igloo", mRec.getText());
         assertTrue(mRec.getJMSRedelivered());
         
         sess.commit();
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   public void testReceivedRollbackTopic() throws Exception
   {
      Connection conn = null;
      
      try
      {
      
         conn = cf.createConnection();
   
         Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = sess.createProducer(topic);
         
         MessageConsumer consumer = sess.createConsumer(topic);
         conn.start();
   
         
         TextMessage mSent = sess.createTextMessage("igloo");
         producer.send(mSent);
         
         sess.commit();
         
         TextMessage mRec = (TextMessage)consumer.receive(2000);
         assertEquals("igloo", mRec.getText());
         
         sess.commit();
         
         mSent.setText("rollback");
         producer.send(mSent);
         
         sess.commit();
         
         mRec = (TextMessage)consumer.receive(2000);
         sess.rollback();
         
         TextMessage mRec2 = (TextMessage)consumer.receive(2000);
         
         sess.commit();
         
         assertNotNull(mRec2);
         
         assertEquals(mRec.getText(), mRec2.getText());
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   /**
    * Send some messages in transacted session. Don't commit.
    * Verify message are not received by consumer.
    */
   public void testSendNoCommitTopic() throws Exception
   {
      Connection conn = null;
      
      try
      {
         conn = cf.createConnection();
   
         Session producerSess = conn.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = producerSess.createProducer(topic);
   
         Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSess.createConsumer(topic);
         conn.start();
   
         final int NUM_MESSAGES = 10;
   
         //Send some messages
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message m = producerSess.createMessage();
            producer.send(m);
         }
   
         log.trace("Sent messages");
   
         Message m = consumer.receive(2000);
         assertNull(m);
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }
   
   }
   
   

   
   /**
    * Send some messages in transacted session. Commit.
    * Verify message are received by consumer.
    */
   public void testSendCommitTopic() throws Exception
   {
      Connection conn = null;
      
      try
      {
      
         conn = cf.createConnection();
         
         Session producerSess = conn.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = producerSess.createProducer(topic);
         
         Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSess.createConsumer(topic);
         conn.start();
         
         final int NUM_MESSAGES = 10;
         
         //Send some messages
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message m = producerSess.createMessage();
            producer.send(m);
         }
         
         producerSess.commit();
         
         log.trace("Sent messages");
         
         int count = 0;
         while (true)
         {
            Message m = consumer.receive(500);
            if (m == null) break;
            count++;
         }
         
         assertEquals(NUM_MESSAGES, count);
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   


  
   /**
    * Send some messages.
    * Receive them in a transacted session.
    * Commit the receiving session
    * Close the connection
    * Create a new connection, session and consumer - verify messages are not redelivered
    */
   public void testAckCommitTopic() throws Exception
   {
      Connection conn = null;
      
      try
      {
      
         conn = cf.createConnection();
   
         Session producerSess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = producerSess.createProducer(topic);
   
         Session consumerSess = conn.createSession(true, Session.SESSION_TRANSACTED);
         MessageConsumer consumer = consumerSess.createConsumer(topic);
         conn.start();
   
         final int NUM_MESSAGES = 10;
   
         //Send some messages
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message m = producerSess.createMessage();
            producer.send(m);
         }
   
         log.trace("Sent messages");
   
         int count = 0;
         while (true)
         {
            Message m = consumer.receive(500);
            if (m == null) break;
            count++;
         }
         
         log.trace("Received " + count + " messages");
   
         assertEquals(NUM_MESSAGES, count);
   
         consumerSess.commit();
         
         log.trace("Committed session");
   
         conn.stop();
         consumer.close();
   
         conn.close();
   
         conn = cf.createConnection();
   
         consumerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
         consumer = consumerSess.createConsumer(queue);
         conn.start();
   
         Message m = consumer.receive(2000);
         
         log.trace("Message is " + m);
   
         assertNull(m);
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }

   }



   /*
    * Send some messages in a transacted session.
    * Rollback the session.
    * Verify messages aren't received by consumer.
    */
   public void testSendRollbackTopic() throws Exception
   {
      Connection conn = null;
      
      try
      {
      
         conn = cf.createConnection();
   
         Session producerSess = conn.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = producerSess.createProducer(topic);
   
         Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSess.createConsumer(topic);
         conn.start();
   
         final int NUM_MESSAGES = 10;
   
         //Send some messages
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message m = producerSess.createMessage();
            producer.send(m);
         }
   
         log.trace("Sent messages");
   
         producerSess.rollback();
   
         Message m = consumer.receive(2000);
   
         assertNull(m);
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   /**
    * Make sure redelivered flag is set on redelivery via rollback
    */
   public void testRedeliveredQueue() throws Exception
   {
      Connection conn = null;
      
      try
      {
         conn = cf.createConnection();
      
   
         Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer producer = sess.createProducer(queue);
         
         MessageConsumer consumer = sess.createConsumer(queue);
         conn.start();
   
         
         Message mSent = sess.createTextMessage("igloo");
         producer.send(mSent);
         
         sess.commit();
         
         TextMessage mRec = (TextMessage)consumer.receive(2000);
         assertEquals("igloo", mRec.getText());
         assertFalse(mRec.getJMSRedelivered());
         
         sess.rollback();
         mRec = (TextMessage)consumer.receive(2000);
         assertEquals("igloo", mRec.getText());
         assertTrue(mRec.getJMSRedelivered());
         
         sess.commit();
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }

   }

   /**
    * Make sure redelivered flag is set on redelivery via rollback, different setup: we close the
    * rolled back session and we receive the message whose acknowledgment was cancelled on a new
    * session.
    */
   public void testRedeliveredQueue2() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();

         Session sendSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sendSession.createProducer(queue);
         prod.send(sendSession.createTextMessage("a message"));

         log.debug("Message was sent to the queue");

         conn.close();

         conn = cf.createConnection();
         Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);

         MessageConsumer cons = sess.createConsumer(queue);

         conn.start();

         TextMessage tm = (TextMessage)cons.receive();

         assertEquals("a message", tm.getText());
         
         assertFalse(tm.getJMSRedelivered());
         assertEquals(1, tm.getIntProperty("JMSXDeliveryCount"));

         sess.rollback();
         sess.close();

         Session sess2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         cons = sess2.createConsumer(queue);

         tm = (TextMessage)cons.receive();

         assertEquals("a message", tm.getText());
 
         assertEquals(2, tm.getIntProperty("JMSXDeliveryCount"));

         assertTrue(tm.getJMSRedelivered());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testReceivedRollbackQueue() throws Exception
   {
      Connection conn = cf.createConnection();

      Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
      MessageProducer producer = sess.createProducer(queue);
      
      MessageConsumer consumer = sess.createConsumer(queue);
      conn.start();

      
      TextMessage mSent = sess.createTextMessage("igloo");
      producer.send(mSent);
      log.trace("sent1");
      
      sess.commit();
      
      TextMessage mRec = (TextMessage)consumer.receive();
      log.trace("Got 1");
      assertNotNull(mRec);
      assertEquals("igloo", mRec.getText());
      
      sess.commit();
      
      mSent.setText("rollback");
      producer.send(mSent);
      
      sess.commit();
      
      log.trace("Receiving 2");
      mRec = (TextMessage)consumer.receive();
      log.trace("Received 2");
      assertNotNull(mRec);
      assertEquals("rollback", mRec.getText());
            
      sess.rollback();
      
      TextMessage mRec2 = (TextMessage)consumer.receive();
      assertNotNull(mRec2);
      assertEquals("rollback", mRec2.getText());
      
      sess.commit();
      
      assertEquals(mRec.getText(), mRec2.getText());
      
      conn.close();
   }
   
   /**
    * Send some messages in transacted session. Don't commit.
    * Verify message are not received by consumer.
    */
   public void testSendNoCommitQueue() throws Exception
   {
      Connection conn = null;
      
      try
      {
         conn = cf.createConnection();
   
         Session producerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
         MessageProducer producer = producerSess.createProducer(queue);
   
         Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSess.createConsumer(queue);
         conn.start();
   
         final int NUM_MESSAGES = 10;
   
         //Send some messages
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message m = producerSess.createMessage();
            producer.send(m);
         }
   
         log.trace("Sent messages");
   
         Message m = consumer.receive(2000);
         assertNull(m);
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }

   }
   
   

   
   /**
    * Send some messages in transacted session. Commit.
    * Verify message are received by consumer.
    */
   public void testSendCommitQueue() throws Exception
   {
      Connection conn = null;
      
      try
      {
         
         conn = cf.createConnection();
         
         Session producerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
         MessageProducer producer = producerSess.createProducer(queue);
         
         Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSess.createConsumer(queue);
         conn.start();
         
         final int NUM_MESSAGES = 10;
         
         //Send some messages
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message m = producerSess.createMessage();
            producer.send(m);
         }
         
         producerSess.commit();
         
         log.trace("Sent messages");
         
         int count = 0;
         while (true)
         {
            Message m = consumer.receive(500);
            if (m == null) break;
            count++;
         }
         
         assertEquals(NUM_MESSAGES, count);
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }
      
   }
   
   
   /**
    * Test IllegateStateException is thrown if commit is called on a non-transacted session
    */
   public void testCommitIllegalState() throws Exception
   {
      Connection conn = null;
      
      try
      {
      
         conn = cf.createConnection();
   
         Session producerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
   
         boolean thrown = false;
         try
         {
            producerSess.commit();
         }
         catch (javax.jms.IllegalStateException e)
         {
            thrown = true;
         }
   
         assertTrue(thrown);
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }

   }


   /**
    * Send some messages.
    * Receive them in a transacted session.
    * Do not commit the receiving session.
    * Close the connection
    * Create a new connection, session and consumer - verify messages are redelivered
    *
    */
   public void testAckNoCommitQueue() throws Exception
   {
      Connection conn = null;
      
      try
      {
      
         conn = cf.createConnection();
   
         Session producerSess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = producerSess.createProducer(queue);
   
         Session consumerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSess.createConsumer(queue);
         conn.start();
   
         final int NUM_MESSAGES = 10;
   
         //Send some messages
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message m = producerSess.createMessage();
            producer.send(m);
         }
   
         log.trace("Sent messages");
   
         int count = 0;
         while (true)
         {
            Message m = consumer.receive(500);
            if (m == null) break;
            count++;
         }
   
         assertEquals(NUM_MESSAGES, count);
   
         conn.stop();
         consumer.close();
   
         conn.close();
   
         conn = cf.createConnection();
   
         consumerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
         consumer = consumerSess.createConsumer(queue);
         conn.start();
   
         count = 0;
         while (true)
         {
            Message m = consumer.receive(500);
            if (m == null) break;
            count++;
         }
   
         assertEquals(NUM_MESSAGES, count);
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }
   }




   /**
    * Send some messages.
    * Receive them in a transacted session.
    * Commit the receiving session
    * Close the connection
    * Create a new connection, session and consumer - verify messages are not redelivered
    */
   public void testAckCommitQueue() throws Exception
   {
      Connection conn = null;
      
      try
      
      {
         
         conn = cf.createConnection();
      
   
         Session producerSess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = producerSess.createProducer(queue);
   
         Session consumerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSess.createConsumer(queue);
         conn.start();
   
         final int NUM_MESSAGES = 10;
   
         //Send some messages
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message m = producerSess.createMessage();
            producer.send(m);
         }
   
         log.trace("Sent messages");
   
         int count = 0;
         while (true)
         {
            Message m = consumer.receive(500);
            if (m == null) break;
            count++;
         }
         
         log.trace("Received " + count + " messages");
   
         assertEquals(NUM_MESSAGES, count);
   
         consumerSess.commit();
         
         log.trace("Committed session");
   
         conn.stop();
         consumer.close();
   
         conn.close();
   
         conn = cf.createConnection();
   
         consumerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
         consumer = consumerSess.createConsumer(queue);
         conn.start();
   
         Message m = consumer.receive(2000);
         
         log.trace("Message is " + m);
   
         assertNull(m);
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }

   }



   /*
    * Send some messages in a transacted session.
    * Rollback the session.
    * Verify messages aren't received by consumer.
    */
   public void testSendRollbackQueue() throws Exception
   {
      Connection conn = null;
      
      try
      {
      
         conn = cf.createConnection();
   
         Session producerSess = conn.createSession(true, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = producerSess.createProducer(queue);
   
         Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSess.createConsumer(queue);
         conn.start();
   
         final int NUM_MESSAGES = 10;
   
         //Send some messages
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message m = producerSess.createMessage();
            producer.send(m);
         }
   
         log.trace("Sent messages");
   
         producerSess.rollback();
   
         Message m = consumer.receive(2000);
   
         assertNull(m);
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }


   }


   /*
    * Test IllegateStateException is thrown if rollback is
    * called on a non-transacted session
    *
    */

   public void testRollbackIllegalState() throws Exception
   {
      Connection conn = null;
      
      try
      {
         
         conn = cf.createConnection();
   
         Session producerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
   
         boolean thrown = false;
         try
         {
            producerSess.rollback();
         }
         catch (javax.jms.IllegalStateException e)
         {
            thrown = true;
         }
   
         assertTrue(thrown);
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }

   }


   /*
    * Send some messages.
    * Receive them in a transacted session.
    * Rollback the receiving session
    * Close the connection
    * Create a new connection, session and consumer - verify messages are redelivered
    *
    */

   public void testAckRollbackQueue() throws Exception
   {
      Connection conn = null;
      
      try
      {
         
         conn = cf.createConnection();
   
         Session producerSess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = producerSess.createProducer(queue);
   
         Session consumerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSess.createConsumer(queue);
         conn.start();
   
         final int NUM_MESSAGES = 10;
   
         //Send some messages
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message m = producerSess.createMessage();
            producer.send(m);
         }
   
         log.trace("Sent messages");
   
         int count = 0;
         while (true)
         {
            Message m = consumer.receive(500);
            if (m == null) break;
            count++;
         }
   
         assertEquals(NUM_MESSAGES, count);
   
         consumerSess.rollback();
   
         conn.stop();
         consumer.close();
   
         conn.close();
   
         conn = cf.createConnection();
   
         consumerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
         consumer = consumerSess.createConsumer(queue);
         conn.start();
   
         count = 0;
         while (true)
         {
            Message m = consumer.receive(500);
            if (m == null) break;
            count++;
         }
   
         assertEquals(NUM_MESSAGES, count);
         
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }

   }


   /*
    * Send multiple messages in multiple contiguous sessions
    */
   public void testSendMultipleQueue() throws Exception
   {
      Connection conn = null;
      
      try
      {
         
         conn = cf.createConnection();
   
         Session producerSess = conn.createSession(true, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = producerSess.createProducer(queue);
   
         Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSess.createConsumer(queue);
         conn.start();
   
         final int NUM_MESSAGES = 10;
         final int NUM_TX = 10;
   
         //Send some messages
   
         for (int j = 0; j < NUM_TX; j++)
         {
   
            for (int i = 0; i < NUM_MESSAGES; i++)
            {
               Message m = producerSess.createMessage();
               producer.send(m);
            }
   
            producerSess.commit();
         }
   
         log.trace("Sent messages");
   
         int count = 0;
         while (true)
         {
            Message m = consumer.receive(500);
            if (m == null) break;
            count++;
         }
   
         assertEquals(NUM_MESSAGES * NUM_TX, count);
         
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }

   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}


