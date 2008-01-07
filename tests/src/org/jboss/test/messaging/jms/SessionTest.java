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

import org.jboss.jms.client.JBossSession;
import org.jboss.jms.client.delegate.ClientSessionDelegate;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.SessionState;
import org.jboss.jms.tx.ResourceManagerFactory;
import org.jboss.test.messaging.JBMServerTestCase;

import javax.jms.*;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SessionTest extends JBMServerTestCase
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   public SessionTest(String name)
   {
      super(name);
   }
   
   // Public --------------------------------------------------------
   
   public void testNoTransactionAfterClose() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection();
      conn.start();
      Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
      MessageProducer prod = sess.createProducer(queue1);
      prod.send(sess.createMessage());
      sess.commit();
      MessageConsumer cons = sess.createConsumer(queue1);
      cons.receive();
      sess.commit();
      
      ClientSessionDelegate del = getDelegate(sess);
      
      SessionState state = (SessionState)del.getState();
      ConnectionState cState = (ConnectionState)state.getParent();
      
      Object xid = state.getCurrentTxId();
      assertNotNull(xid);
      assertNotNull(cState.getResourceManager().getTx(xid));
      
      //Now close the session
      sess.close();
      
      //Session should be removed from resource manager
      xid = state.getCurrentTxId();
      assertNotNull(xid);
      assertNull(cState.getResourceManager().getTx(xid));
      
      conn.close();
      
      assertEquals(0, cState.getResourceManager().size());
   }

   public void testCreateProducer() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);          
      sess.createProducer(topic1);
      conn.close();
   }

   public void testCreateProducerOnNullQueue() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Message m = sess.createTextMessage("something");

      MessageProducer p = sess.createProducer(null);

      p.send(queue1, m);

      MessageConsumer c = sess.createConsumer(queue1);
      conn.start();
      
      //receiveNoWait is not guaranteed to return message immediately
      TextMessage rm = (TextMessage)c.receive(1000);

      assertEquals("something", rm.getText());
      
      conn.close();
   }
   
   public void testCreateConsumer() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      sess.createConsumer(topic1);
      conn.close();
   }

   public void testGetSession1() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      try
      {
         ((XASession)sess).getSession();
         fail("Should throw IllegalStateException");
      }
      catch (javax.jms.IllegalStateException e)
      {}
      conn.close();
   }
   
   public void testGetSession2() throws Exception
   {
      XAConnection conn = getConnectionFactory().createXAConnection();
      XASession sess = conn.createXASession();
      
      sess.getSession();
      conn.close();
   }
   
   //
   // createQueue()/createTopic()
   //
   
   
   public void testCreateNonExistentQueue() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      try
      {
         sess.createQueue("QueueThatDoesNotExist");
         fail();
      }
      catch (JMSException e)
      {}
      conn.close();
   }
   
   public void testCreateQueueOnATopicSession() throws Exception
   {
      TopicConnection c = (TopicConnection)getConnectionFactory().createConnection();
      TopicSession s = c.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
      
      try
      {
         s.createQueue("TestQueue");
         fail("should throw IllegalStateException");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }
      c.close();
   }
   
   public void testCreateQueueWhileTopicWithSameNameExists() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      try
      {
         sess.createQueue("TestTopic");
         fail("should throw JMSException");
      }
      catch (JMSException e)
      {
         // OK
      }
      conn.close();
   }
   
   public void testCreateQueue() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = sess.createQueue("Queue1");
      
      MessageProducer producer = sess.createProducer(queue);
      MessageConsumer consumer = sess.createConsumer(queue);
      conn.start();
      
      Message m = sess.createTextMessage("testing");
      producer.send(m);
      
      Message m2 = consumer.receive(3000);
      
      assertNotNull(m2);
      conn.close();
   }
   
   public void testCreateNonExistentTopic() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      try
      {
         sess.createTopic("TopicThatDoesNotExist");
         fail("should throw JMSException");
      }
      catch (JMSException e)
      {
         // OK
      }
      conn.close();
   }
   
   public void testCreateTopicOnAQueueSession() throws Exception
   {
      QueueConnection c = (QueueConnection)getConnectionFactory().createConnection();
      QueueSession s = c.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      
      try
      {
         s.createTopic("TestTopic");
         fail("should throw IllegalStateException");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }
      c.close();
   }
   
   public void testCreateTopicWhileQueueWithSameNameExists() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      try
      {
         sess.createTopic("TestQueue");
         fail("should throw JMSException");
      }
      catch (JMSException e)
      {
         // OK
      }
      conn.close();
   }
   
   public void testCreateTopic() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      Topic topic = sess.createTopic("Topic1");
      
      MessageProducer producer = sess.createProducer(topic);
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      MessageConsumer consumer = sess.createConsumer(topic);
      conn.start();
      
      
      class TestRunnable implements Runnable
      {
         boolean exceptionThrown;
         public Message m;
         MessageConsumer consumer;
         TestRunnable(MessageConsumer consumer)
         {
            this.consumer = consumer;
         }
         
         public void run()
         {
            try
            {
               m = consumer.receive(3000);               
            }
            catch (Exception e)
            {
               exceptionThrown = true;
            }
         }
      }
      
      TestRunnable tr1 = new TestRunnable(consumer);
      Thread t1 = new Thread(tr1);
      t1.start();
      
      Message m = sess.createTextMessage("testing");
      producer.send(m);
      
      t1.join();
      
      assertFalse(tr1.exceptionThrown);
      assertNotNull(tr1.m);
      
      conn.close();
   }

   public void testGetXAResource() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      ((XASession)sess).getXAResource();
      conn.close();
   }


   public void testGetXAResource2() throws Exception
   {
      XAConnection conn = getConnectionFactory().createXAConnection();
      XASession sess = conn.createXASession();

      sess.getXAResource();
      conn.close();
   }


   public void testIllegalState() throws Exception
   {
      //IllegalStateException should be thrown if commit or rollback
      //is invoked on a non transacted session
      Connection conn = getConnectionFactory().createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prod = sess.createProducer(queue1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      Message m = sess.createTextMessage("hello");
      prod.send(m);
      
      try
      {
         sess.rollback();
         fail();
      }
      catch (javax.jms.IllegalStateException e)
      {}
      
      try
      {
         sess.commit();
         fail();
      }
      catch (javax.jms.IllegalStateException e)
      {}
      
      conn.close();
      
      removeAllMessages(queue1.getQueueName(), true, 0);
   }


   //
   // Test session state
   //

   public void testCreateTwoSessions() throws Exception
   {
      Connection conn = getConnectionFactory().createConnection();
      Session sessionOne = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      assertFalse(sessionOne.getTransacted());
      Session sessionTwo = conn.createSession(true, -1);
      assertTrue(sessionTwo.getTransacted());

      // this test whether session's transacted state is correctly scoped per instance (by an
      // interceptor or othewise)
      assertFalse(sessionOne.getTransacted());
      
      conn.close();
   }

   public void testCloseAndCreateSession() throws Exception
   {
      Connection c = getConnectionFactory().createConnection();
      Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);

      s.close();

      // this test whether session's closed state is correctly scoped per instance (by an
      // interceptor or othewise)
      s = c.createSession(true, -1);
      
      c.close();
   }


   public void testCloseNoClientAcknowledgment() throws Exception
   {
      // send a message to the queue

      Connection conn = getConnectionFactory().createConnection();
      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      s.createProducer(queue1).send(s.createTextMessage("wont_ack"));
      conn.close();

      conn = getConnectionFactory().createConnection();
      s = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      conn.start();

      TextMessage m = (TextMessage)s.createConsumer(queue1).receive(1000);

      assertEquals("wont_ack", m.getText());

      // Do NOT ACK

      s.close(); // this should cancel the delivery

      // get the message again
      s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      m = (TextMessage)s.createConsumer(queue1).receive(1000);

      assertEquals("wont_ack", m.getText());

      conn.close();
   }

   public void testCloseInTransaction() throws Exception
   {
      // send a message to the queue

      Connection conn = getConnectionFactory().createConnection();
      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      s.createProducer(queue1).send(s.createTextMessage("bex"));
      conn.close();

      conn = getConnectionFactory().createConnection();
      Session session = conn.createSession(true, -1);
      conn.start();

      TextMessage m = (TextMessage)session.createConsumer(queue1).receive(1000);

      assertEquals("bex", m.getText());

      // make sure the acknowledment hasn't been sent to the channel
      assertRemainingMessages(1);
      
      // close the session
      session.close();

      // JMS 1.1 4.4.1: "Closing a transacted session must roll back its transaction in progress"
      
      assertRemainingMessages(1);

      conn.close();

      // make sure I can still get the right message

      conn = getConnectionFactory().createConnection();
      s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      conn.start();
      TextMessage rm = (TextMessage)s.createConsumer(queue1).receive(1000);

      assertEquals("bex", rm.getText());

      conn.close();
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   protected void setUp() throws Exception
	{
		super.setUp();
		
		ResourceManagerFactory.instance.clear();
	}
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}

