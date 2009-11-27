/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.jms.tests;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * $Id$
 */
public class TransactedSessionTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testSimpleRollback() throws Exception
   {
      // send a message
      Connection conn = null;

      try
      {
         conn = cf.createConnection();
         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         s.createProducer(queue1).send(s.createTextMessage("one"));

         s.close();

         s = conn.createSession(true, Session.SESSION_TRANSACTED);
         MessageConsumer c = s.createConsumer(queue1);
         conn.start();
         Message m = c.receive(1000);
         assertNotNull(m);

         assertEquals("one", ((TextMessage)m).getText());
         assertFalse(m.getJMSRedelivered());
         assertEquals(1, m.getIntProperty("JMSXDeliveryCount"));

         s.rollback();

         // get the message again
         m = c.receive(1000);
         assertNotNull(m);

         assertTrue(m.getJMSRedelivered());
         assertEquals(2, m.getIntProperty("JMSXDeliveryCount"));

         conn.close();

         Integer i = getMessageCountForQueue("Queue1");

         assertEquals(1, i.intValue());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         removeAllMessages(queue1.getQueueName(), true);
      }
   }

   public void testRedeliveredFlagTopic() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();

         Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
         Session sess1 = conn.createSession(true, Session.SESSION_TRANSACTED);
         MessageConsumer consumer1 = sess1.createConsumer(topic1);

         MessageProducer producer = sessSend.createProducer(topic1);
         Message mSent = sessSend.createTextMessage("igloo");
         producer.send(mSent);
         sessSend.commit();

         conn.start();

         TextMessage mRec1 = (TextMessage)consumer1.receive(2000);
         assertNotNull(mRec1);

         assertEquals("igloo", mRec1.getText());
         assertFalse(mRec1.getJMSRedelivered());

         sess1.rollback(); // causes redelivery for session

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
         MessageProducer producer = sess.createProducer(topic1);

         MessageConsumer consumer = sess.createConsumer(topic1);
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
         MessageProducer producer = sess.createProducer(topic1);

         MessageConsumer consumer = sess.createConsumer(topic1);
         conn.start();

         log.info("sending message first time");
         TextMessage mSent = sess.createTextMessage("igloo");
         producer.send(mSent);
         log.info("sent message first time");

         sess.commit();

         TextMessage mRec = (TextMessage)consumer.receive(2000);
         assertEquals("igloo", mRec.getText());

         sess.commit();

         log.info("sending message again");
         mSent.setText("rollback");
         producer.send(mSent);
         log.info("sent message again");

         sess.commit();

         mRec = (TextMessage)consumer.receive(2000);
         assertEquals("rollback", mRec.getText());
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
         MessageProducer producer = producerSess.createProducer(topic1);

         Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSess.createConsumer(topic1);
         conn.start();

         final int NUM_MESSAGES = 10;

         // Send some messages
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message m = producerSess.createMessage();
            producer.send(m);
         }

         Message m = consumer.receive(500);
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
         MessageProducer producer = producerSess.createProducer(topic1);

         Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSess.createConsumer(topic1);
         conn.start();

         final int NUM_MESSAGES = 10;

         // Send some messages
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message m = producerSess.createMessage();
            producer.send(m);
         }

         producerSess.commit();

         int count = 0;
         while (true)
         {
            Message m = consumer.receive(500);
            if (m == null)
               break;
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
         MessageProducer producer = producerSess.createProducer(topic1);

         Session consumerSess = conn.createSession(true, Session.SESSION_TRANSACTED);
         MessageConsumer consumer = consumerSess.createConsumer(topic1);
         conn.start();

         final int NUM_MESSAGES = 10;

         // Send some messages
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message m = producerSess.createMessage();
            producer.send(m);
         }
         int count = 0;
         while (true)
         {
            Message m = consumer.receive(500);
            if (m == null)
               break;
            count++;
         }

         assertEquals(NUM_MESSAGES, count);

         consumerSess.commit();

         conn.stop();
         consumer.close();

         conn.close();

         conn = cf.createConnection();

         consumerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
         consumer = consumerSess.createConsumer(queue1);
         conn.start();

         Message m = consumer.receive(500);

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
         MessageProducer producer = producerSess.createProducer(topic1);

         Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSess.createConsumer(topic1);
         conn.start();

         final int NUM_MESSAGES = 10;

         // Send some messages
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message m = producerSess.createMessage();
            producer.send(m);
         }

         producerSess.rollback();

         Message m = consumer.receive(500);

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
         MessageProducer producer = sess.createProducer(queue1);

         MessageConsumer consumer = sess.createConsumer(queue1);
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

         MessageProducer prod = sendSession.createProducer(queue1);
         prod.send(sendSession.createTextMessage("a message"));

         conn.close();

         conn = cf.createConnection();
         Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);

         MessageConsumer cons = sess.createConsumer(queue1);

         conn.start();

         TextMessage tm = (TextMessage)cons.receive(1000);
         assertNotNull(tm);

         assertEquals("a message", tm.getText());

         assertFalse(tm.getJMSRedelivered());
         assertEquals(1, tm.getIntProperty("JMSXDeliveryCount"));

         sess.rollback();

         sess.close();

         Session sess2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         cons = sess2.createConsumer(queue1);

         tm = (TextMessage)cons.receive(1000);

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
      MessageProducer producer = sess.createProducer(queue1);

      MessageConsumer consumer = sess.createConsumer(queue1);
      conn.start();

      TextMessage mSent = sess.createTextMessage("igloo");
      producer.send(mSent);
      log.trace("sent1");

      sess.commit();

      TextMessage mRec = (TextMessage)consumer.receive(1000);
      assertNotNull(mRec);
      log.trace("Got 1");
      assertNotNull(mRec);
      assertEquals("igloo", mRec.getText());

      sess.commit();

      mSent.setText("rollback");
      producer.send(mSent);

      sess.commit();

      log.trace("Receiving 2");
      mRec = (TextMessage)consumer.receive(1000);
      assertNotNull(mRec);

      log.trace("Received 2");
      assertNotNull(mRec);
      assertEquals("rollback", mRec.getText());

      sess.rollback();

      TextMessage mRec2 = (TextMessage)consumer.receive(1000);
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
         MessageProducer producer = producerSess.createProducer(queue1);

         Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSess.createConsumer(queue1);
         conn.start();

         final int NUM_MESSAGES = 10;

         // Send some messages
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message m = producerSess.createMessage();
            producer.send(m);
         }

         checkEmpty(queue1);
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
         MessageProducer producer = producerSess.createProducer(queue1);

         Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSess.createConsumer(queue1);
         conn.start();

         final int NUM_MESSAGES = 10;

         // Send some messages
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message m = producerSess.createMessage();
            producer.send(m);
         }

         producerSess.commit();

         int count = 0;
         while (true)
         {
            Message m = consumer.receive(500);
            if (m == null)
               break;
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
         removeAllMessages(queue1.getQueueName(), true);
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
         MessageProducer producer = producerSess.createProducer(queue1);

         Session consumerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSess.createConsumer(queue1);
         conn.start();

         final int NUM_MESSAGES = 10;

         // Send some messages
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message m = producerSess.createMessage();
            producer.send(m);
         }

         int count = 0;
         while (true)
         {
            Message m = consumer.receive(500);
            if (m == null)
               break;
            count++;
         }

         assertEquals(NUM_MESSAGES, count);

         conn.stop();
         consumer.close();

         conn.close();

         conn = cf.createConnection();

         consumerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
         consumer = consumerSess.createConsumer(queue1);
         conn.start();

         count = 0;

         while (true)
         {
            Message m = consumer.receive(500);
            if (m == null)
               break;
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
         removeAllMessages(queue1.getQueueName(), true);
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
         MessageProducer producer = producerSess.createProducer(queue1);

         Session consumerSess = conn.createSession(true, Session.SESSION_TRANSACTED);
         MessageConsumer consumer = consumerSess.createConsumer(queue1);
         conn.start();

         final int NUM_MESSAGES = 10;

         // Send some messages
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message m = producerSess.createMessage();
            producer.send(m);
         }

         int count = 0;
         while (true)
         {
            Message m = consumer.receive(500);
            if (m == null)
               break;
            count++;
         }

         assertEquals(NUM_MESSAGES, count);

         consumerSess.commit();

         conn.stop();
         consumer.close();

         conn.close();

         conn = cf.createConnection();

         consumerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
         consumer = consumerSess.createConsumer(queue1);
         conn.start();

         Message m = consumer.receive(500);

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
         MessageProducer producer = producerSess.createProducer(queue1);

         Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSess.createConsumer(queue1);
         conn.start();

         final int NUM_MESSAGES = 10;

         // Send some messages
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message m = producerSess.createMessage();
            producer.send(m);
         }

         producerSess.rollback();

         Message m = consumer.receive(500);

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
         MessageProducer producer = producerSess.createProducer(queue1);

         Session consumerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSess.createConsumer(queue1);
         conn.start();

         final int NUM_MESSAGES = 10;

         // Send some messages
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message m = producerSess.createMessage();
            producer.send(m);
         }

         int count = 0;
         while (true)
         {
            Message m = consumer.receive(500);
            if (m == null)
               break;
            count++;
         }

         assertEquals(NUM_MESSAGES, count);

         consumerSess.rollback();

         conn.stop();
         consumer.close();

         conn.close();

         conn = cf.createConnection();

         consumerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
         consumer = consumerSess.createConsumer(queue1);
         conn.start();

         count = 0;
         while (true)
         {
            Message m = consumer.receive(500);
            if (m == null)
               break;
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
         removeAllMessages(queue1.getQueueName(), true);
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
         MessageProducer producer = producerSess.createProducer(queue1);

         Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer consumer = consumerSess.createConsumer(queue1);
         conn.start();

         final int NUM_MESSAGES = 10;
         final int NUM_TX = 10;

         // Send some messages

         for (int j = 0; j < NUM_TX; j++)
         {
            for (int i = 0; i < NUM_MESSAGES; i++)
            {
               Message m = producerSess.createMessage();
               producer.send(m);
            }

            producerSess.commit();
         }

         int count = 0;
         while (true)
         {
            Message m = consumer.receive(500);
            if (m == null)
               break;
            count++;
            m.acknowledge();
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
