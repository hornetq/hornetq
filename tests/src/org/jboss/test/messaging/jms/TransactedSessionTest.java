/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;

import javax.jms.*;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.test.messaging.tools.tx.TransactionManagerImpl;

/**
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
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
      ServerManagement.init("all");
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");
      
      ServerManagement.undeployQueue("Queue");
      ServerManagement.undeployTopic("Topic");
      ServerManagement.deployQueue("Queue");
      ServerManagement.deployTopic("Topic");
      queue = (Destination)initialContext.lookup("/queue/Queue");
      topic = (Destination)initialContext.lookup("/topic/Topic");
   }
   
   public void tearDown() throws Exception
   {
      ServerManagement.deInit();
      TransactionManagerImpl.getInstance().setState(TransactionManagerImpl.OPERATIONAL);
      super.tearDown();
   }
   
   
   // Public --------------------------------------------------------
   
   public void testRedel0() throws Exception
   {
      Connection conn = null;
      
      try
      {
      
         conn = cf.createConnection();
         conn.start();
         
         Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer prod = sess.createProducer(queue);
         TextMessage tm1 = sess.createTextMessage("hello1");
         TextMessage tm2 = sess.createTextMessage("hello2");
         TextMessage tm3 = sess.createTextMessage("hello3");
         prod.send(tm1);
         prod.send(tm2);
         prod.send(tm3);
         sess.commit();
         
         MessageConsumer cons1 = sess.createConsumer(queue);
         
         TextMessage rm1 = (TextMessage)cons1.receive(2000);
         assertNotNull(rm1);
         assertEquals("hello1", rm1.getText());
         
         cons1.close();
         
         MessageConsumer cons2 = sess.createConsumer(queue);
         
         sess.commit();
         
         TextMessage rm2 = (TextMessage)cons2.receive(2000);
         assertNotNull(rm2);
         assertEquals("hello2", rm2.getText());
         
         TextMessage rm3 = (TextMessage)cons2.receive(2000);
         assertNotNull(rm3);
         assertEquals("hello3", rm3.getText());
         
         TextMessage rm4 = (TextMessage)cons2.receive(2000);
         assertNull(rm4);        
         
         
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }
      
   }
   
   
   public void testRedel1() throws Exception
   {
      Connection conn = null;
      
      try
      {
      
         conn = cf.createConnection();
         conn.start();
         
         Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer prod = sess.createProducer(queue);
         TextMessage tm1 = sess.createTextMessage("hello1");
         TextMessage tm2 = sess.createTextMessage("hello2");
         TextMessage tm3 = sess.createTextMessage("hello3");
         prod.send(tm1);
         prod.send(tm2);
         prod.send(tm3);
         sess.commit();
         
         MessageConsumer cons1 = sess.createConsumer(queue);
         
         TextMessage rm1 = (TextMessage)cons1.receive(2000);
         assertNotNull(rm1);
         assertEquals("hello1", rm1.getText());
         
         cons1.close();
         
         MessageConsumer cons2 = sess.createConsumer(queue);
         
         sess.commit();
         
         TextMessage rm2 = (TextMessage)cons2.receive(2000);
         assertNotNull(rm2);
         assertEquals("hello2", rm2.getText());
         
         TextMessage rm3 = (TextMessage)cons2.receive(2000);
         assertNotNull(rm3);
         assertEquals("hello3", rm3.getText());
         
         TextMessage rm4 = (TextMessage)cons2.receive(2000);
         assertNull(rm4);        
         
         
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }
      
   }
   
   public void testRedel2() throws Exception
   {
      Connection conn = null;
      
      try
      {
      
         conn = cf.createConnection();
         conn.start();
         
         Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer prod = sess.createProducer(queue);
         TextMessage tm1 = sess.createTextMessage("hello1");
         TextMessage tm2 = sess.createTextMessage("hello2");
         TextMessage tm3 = sess.createTextMessage("hello3");
         prod.send(tm1);
         prod.send(tm2);
         prod.send(tm3);
         sess.commit();
         
         MessageConsumer cons1 = sess.createConsumer(queue);
         
         TextMessage rm1 = (TextMessage)cons1.receive(2000);
         assertNotNull(rm1);
         assertEquals("hello1", rm1.getText());
         
         cons1.close();
         
         sess.commit();
         
         MessageConsumer cons2 = sess.createConsumer(queue);
         
         TextMessage rm2 = (TextMessage)cons2.receive(2000);
         assertNotNull(rm2);
         assertEquals("hello2", rm2.getText());
         
         TextMessage rm3 = (TextMessage)cons2.receive(2000);
         assertNotNull(rm3);
         assertEquals("hello3", rm3.getText());
         
         TextMessage rm4 = (TextMessage)cons2.receive(2000);
         assertNull(rm4);        
         
         
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }
      
   }
   
   public void testRedel3() throws Exception
   {
      Connection conn = null;
      
      try
      {
      
         conn = cf.createConnection();
         conn.start();
         
         Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer prod = sess.createProducer(queue);
         TextMessage tm1 = sess.createTextMessage("hello1");
         TextMessage tm2 = sess.createTextMessage("hello2");
         TextMessage tm3 = sess.createTextMessage("hello3");
         prod.send(tm1);
         prod.send(tm2);
         prod.send(tm3);
         sess.commit();
         
         MessageConsumer cons1 = sess.createConsumer(queue);
         
         TextMessage rm1 = (TextMessage)cons1.receive(2000);
         assertNotNull(rm1);
         assertEquals("hello1", rm1.getText());
         
         //rollback should cause redelivery of messages not acked
         sess.rollback();
                 
         TextMessage rm2 = (TextMessage)cons1.receive(2000);
         assertNotNull(rm2);
         assertEquals("hello1", rm2.getText());
         
         TextMessage rm3 = (TextMessage)cons1.receive(2000);
         assertNotNull(rm3);
         assertEquals("hello2", rm3.getText());
         
         TextMessage rm4 = (TextMessage)cons1.receive(2000);
         assertNotNull(rm4);
         assertEquals("hello3", rm4.getText());
         
         //This last step is important - there shouldn't be any more messages to receive
         TextMessage rm5 = (TextMessage)cons1.receive(2000);
         assertNull(rm5);        
         
         
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }
      
   }
   
   public void testRedel4() throws Exception
   {
      Connection conn = null;
      
      try
      {
      
         conn = cf.createConnection();
         conn.start();
         
         Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
         MessageProducer prod = sess.createProducer(queue);
         TextMessage tm1 = sess.createTextMessage("hello1");
         TextMessage tm2 = sess.createTextMessage("hello2");
         TextMessage tm3 = sess.createTextMessage("hello3");
         prod.send(tm1);
         prod.send(tm2);
         prod.send(tm3);
         sess.commit();
         
         MessageConsumer cons1 = sess.createConsumer(queue);
         
         TextMessage rm1 = (TextMessage)cons1.receive(2000);
         assertNotNull(rm1);
         assertEquals("hello1", rm1.getText());
         
         cons1.close();
         
         MessageConsumer cons2 = sess.createConsumer(queue);
         
         //rollback should cause redelivery of messages
         
         //in this case redelivery occurs to a different receiver
         
         sess.rollback();
                 
         TextMessage rm2 = (TextMessage)cons2.receive(2000);
         assertNotNull(rm2);
         assertEquals("hello1", rm2.getText());
         
         TextMessage rm3 = (TextMessage)cons2.receive(2000);
         assertNotNull(rm3);
         assertEquals("hello2", rm3.getText());
         
         TextMessage rm4 = (TextMessage)cons2.receive(2000);
         assertNotNull(rm4);
         assertEquals("hello3", rm4.getText());
         
         //This last step is important - there shouldn't be any more messages to receive
         TextMessage rm5 = (TextMessage)cons2.receive(2000);
         assertNull(rm5);            
         
         
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }
      
   }
   
   
   public void testRedel5() throws Exception
   {
      Connection conn = null;
      
      try
      {
      
         conn = cf.createConnection();
         conn.start();
         
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sess.createProducer(queue);
         TextMessage tm1 = sess.createTextMessage("hello1");
         TextMessage tm2 = sess.createTextMessage("hello2");
         TextMessage tm3 = sess.createTextMessage("hello3");
         prod.send(tm1);
         prod.send(tm2);
         prod.send(tm3);
         
         MessageConsumer cons1 = sess.createConsumer(queue);
         
         TextMessage rm1 = (TextMessage)cons1.receive(2000);
         assertNotNull(rm1);
         assertEquals("hello1", rm1.getText());
         
         //redeliver
         sess.recover();
                 
         TextMessage rm2 = (TextMessage)cons1.receive(2000);
         assertNotNull(rm2);
         assertEquals("hello1", rm2.getText());
         
         TextMessage rm3 = (TextMessage)cons1.receive(2000);
         assertNotNull(rm3);
         assertEquals("hello2", rm3.getText());
         
         TextMessage rm4 = (TextMessage)cons1.receive(2000);
         assertNotNull(rm4);
         assertEquals("hello3", rm4.getText());
         
         
         //This last step is important - there shouldn't be any more messages to receive
         TextMessage rm5 = (TextMessage)cons1.receive(2000);
         assertNull(rm5);        
         
         
      }
      finally
      {      
         if (conn != null)
         {
            conn.close();
         }
      }
      
   }
   
   
   public void testRedel6() throws Exception
   {
      Connection conn = null;
      
      try
      {
      
         conn = cf.createConnection();
         conn.start();
         
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sess.createProducer(queue);
         TextMessage tm1 = sess.createTextMessage("hello1");
         TextMessage tm2 = sess.createTextMessage("hello2");
         TextMessage tm3 = sess.createTextMessage("hello3");
         prod.send(tm1);
         prod.send(tm2);
         prod.send(tm3);
         
         MessageConsumer cons1 = sess.createConsumer(queue);
         
         TextMessage rm1 = (TextMessage)cons1.receive(2000);
         assertNotNull(rm1);
         assertEquals("hello1", rm1.getText());
         
         cons1.close();
         
         MessageConsumer cons2 = sess.createConsumer(queue);
         
         //redeliver
         sess.recover();
                 
         TextMessage rm2 = (TextMessage)cons2.receive(2000);
         assertNotNull(rm2);
         assertEquals("hello1", rm2.getText());
         
         TextMessage rm3 = (TextMessage)cons2.receive(2000);
         assertNotNull(rm3);
         assertEquals("hello2", rm3.getText());
         
         TextMessage rm4 = (TextMessage)cons2.receive(2000);
         assertNotNull(rm4);
         assertEquals("hello3", rm4.getText());
         
         
         //This last step is important - there shouldn't be any more messages to receive
         TextMessage rm5 = (TextMessage)cons2.receive(2000);
         assertNull(rm5);        
         
         
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
    * 
    * Test that when the redelivered flag is set for one consumer that it's not set globally
    * 
    */
   public void testRedeliveredFlagLocalTopic() throws Exception
   {
      Connection conn = null;
      
      try
      {
      
         conn = cf.createConnection();
         
         Session sessSend = conn.createSession(true, Session.SESSION_TRANSACTED);
         Session sess1 = conn.createSession(true, Session.SESSION_TRANSACTED);
         Session sess2 = conn.createSession(true, Session.SESSION_TRANSACTED);
         MessageConsumer consumer1 = sess1.createConsumer(topic);
         MessageConsumer consumer2 = sess2.createConsumer(topic);
         
         
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
         
         TextMessage mRec2 = (TextMessage)consumer2.receive(2000);
         assertEquals("igloo", mRec2.getText());
         assertFalse(mRec2.getJMSRedelivered());
         
         sess2.rollback();
         
         mRec2 = (TextMessage)consumer2.receive(2000);
         assertEquals("igloo", mRec2.getText());
         assertTrue(mRec2.getJMSRedelivered());
         
         sess1.commit();
         sess2.commit();
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



   

   
   
   /** Make sure redelivered flag is set on redelivery via rollback*/
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
   
  
   
   public void testReceivedRollbackQueue() throws Exception
   {
      Connection conn = cf.createConnection();

      Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
      MessageProducer producer = sess.createProducer(queue);
      
      MessageConsumer consumer = sess.createConsumer(queue);
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


