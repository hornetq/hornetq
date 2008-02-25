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
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;

import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.jms.client.JBossSession;

import EDU.oswego.cs.dl.util.concurrent.Latch;

import java.util.List;
import java.util.ArrayList;


/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * $Id$
 */
public class AcknowledgementTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public AcknowledgementTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   // Public --------------------------------------------------------

   /* Topics shouldn't hold on to messages if there are no subscribers */
   public void testPersistentMessagesForTopicDropped() throws Exception
   {
      TopicConnection conn = null;
      
      try
      {
         conn = cf.createTopicConnection();
         TopicSession sess = conn.createTopicSession(true, 0);
         TopicPublisher pub = sess.createPublisher(topic1);
         pub.setDeliveryMode(DeliveryMode.PERSISTENT);
         
         Message m = sess.createTextMessage("testing123");
         pub.publish(m);
         sess.commit();
         
         conn.close();
         
         checkEmpty(topic1);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   /* Topics shouldn't hold on to messages when the non-durable subscribers close */
   public void testPersistentMessagesForTopicDropped2() throws Exception
   {
      TopicConnection conn = null;
      
      try
      {
         conn = cf.createTopicConnection();
         conn.start();
         TopicSession sess = conn.createTopicSession(true, 0);
         TopicPublisher pub = sess.createPublisher(topic1);
         TopicSubscriber sub = sess.createSubscriber(topic1);
         pub.setDeliveryMode(DeliveryMode.PERSISTENT);
         
         Message m = sess.createTextMessage("testing123");
         pub.publish(m);
         sess.commit();
         
         //receive but rollback
         TextMessage m2 = (TextMessage)sub.receive(3000);
                
         assertNotNull(m2);
         assertEquals("testing123", m2.getText());
         
         sess.rollback();
         
         conn.close();
         
         checkEmpty(topic1);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   public void testRollbackRecover() throws Exception
   {
      TopicConnection conn = null;
      
      try
      {
         conn = cf.createTopicConnection();
         TopicSession sess = conn.createTopicSession(true, 0);
         TopicPublisher pub = sess.createPublisher(topic1);
         TopicSubscriber cons = sess.createSubscriber(topic1);
         conn.start();
         
         Message m = sess.createTextMessage("testing123");
         pub.publish(m);
         sess.commit();
         
         TextMessage m2 = (TextMessage)cons.receive(3000);
         assertNotNull(m2);
         assertEquals("testing123", m2.getText());
         
         sess.rollback();
         
         m2 = (TextMessage)cons.receive(3000);
         assertNotNull(m2);
         assertEquals("testing123", m2.getText());
           
         conn.close();
         
         conn = cf.createTopicConnection();
         conn.start();
         
         //test 2
         
         TopicSession newsess = conn.createTopicSession(true, 0);
         
         TopicPublisher newpub = newsess.createPublisher(topic1);
                  
         TopicSubscriber newcons = newsess.createSubscriber(topic1);
         
         Message m3 = newsess.createTextMessage("testing456");
         newpub.publish(m3);
         newsess.commit();
         
         TextMessage m4 = (TextMessage)newcons.receive(3000);
         assertNotNull(m4);
         assertEquals("testing456", m4.getText());
         
         newsess.commit();
         
         newpub.publish(m3);
         newsess.commit();
         
         TextMessage m5 = (TextMessage)newcons.receive(3000);
         assertNotNull(m5);
         assertEquals("testing456", m5.getText());
         
         newsess.rollback();
         
         TextMessage m6 = (TextMessage)newcons.receive(3000);
         assertNotNull(m6);
         assertEquals("testing456", m6.getText());
         
         newsess.commit();         
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }
   
   public void testTransactionalAcknowledgement() throws Exception
   {
      Connection conn = null;
      
      try
      {
	      
	      conn = cf.createConnection();
	
	      Session producerSess = conn.createSession(true, Session.SESSION_TRANSACTED);
	      MessageProducer producer = producerSess.createProducer(queue1);
	
	      Session consumerSess = conn.createSession(true, Session.SESSION_TRANSACTED);
	      MessageConsumer consumer = consumerSess.createConsumer(queue1);
	      conn.start();
	
	      final int NUM_MESSAGES = 20;
	
	      //Send some messages
	      for (int i = 0; i < NUM_MESSAGES; i++)
	      {
	         Message m = producerSess.createMessage();
	         producer.send(m);
	      }
	
	      assertRemainingMessages(0);
	      
	      producerSess.rollback();
	      
	      //Send some messages
	      for (int i = 0; i < NUM_MESSAGES; i++)
	      {
	         Message m = producerSess.createMessage();
	         producer.send(m);
	      }
	      assertRemainingMessages(0);
	      
	      producerSess.commit();
	
	      assertRemainingMessages(NUM_MESSAGES);
	
	      log.trace("Sent messages");
	
	      int count = 0;
	      while (true)
	      {
	         Message m = consumer.receive(1000);
	         if (m == null) break;
	         count++;
	      }
	      
	      assertRemainingMessages(NUM_MESSAGES);
	
	      log.trace("Received " + count +  " messages");
	
	      assertEquals(count, NUM_MESSAGES);
	
	      consumerSess.rollback();
	      
	      assertRemainingMessages(NUM_MESSAGES);
	
	      log.info("Session rollback called");
	
	      int i = 0;
	      for(; i < NUM_MESSAGES; i++)
	      {
	         consumer.receive();
	         log.info("Received message " + i);	  
	      }
	      
	      assertRemainingMessages(NUM_MESSAGES);
	      
	      log.info("Got here");
	
	      // if I don't receive enough messages, the test will timeout
	
	      log.trace("Received " + i +  " messages after recover");
	      
	      consumerSess.commit();
	      
	      assertRemainingMessages(0);
	
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
	 * Send some messages, don't acknowledge them and verify that they are re-sent on recovery.
	 */
	public void testClientAcknowledgeNoAcknowledgement() throws Exception
   {
		Connection conn = null;
		
		try
		{		
			conn = cf.createConnection();
	
			Session producerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
			MessageProducer producer = producerSess.createProducer(queue1);
	
			Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
			MessageConsumer consumer = consumerSess.createConsumer(queue1);
			conn.start();
	
			final int NUM_MESSAGES = 20;
	
			//Send some messages
			for (int i = 0; i < NUM_MESSAGES; i++)
			{
				Message m = producerSess.createMessage();
				producer.send(m);
			}
	      
	      assertRemainingMessages(NUM_MESSAGES);
	
			log.trace("Sent messages");
	
			int count = 0;
			while (true)
			{
				Message m = consumer.receive(200);
				if (m == null) break;
				count++;
			}
	      
	      assertRemainingMessages(NUM_MESSAGES);
	
			log.info("Received " + count +  " messages");
	
			assertEquals(count, NUM_MESSAGES);
	
			log.info("calling recover");
			consumerSess.recover();
	      
	      assertRemainingMessages(NUM_MESSAGES);
	
			log.info("Session recover called");
	
	      Message m = null;
	
	      int i = 0;
	      for(; i < NUM_MESSAGES; i++)
	      {
	         m = consumer.receive(1000);
	         log.trace("Received message " + i);	
	         
	         if (m == null)
	         {
	            break;
	         }
	      }
	      
	      assertRemainingMessages(NUM_MESSAGES);
	
	      // if I don't receive enough messages, the test will timeout
	
			log.info("Received " + i +  " messages after recover");
	      
	      m.acknowledge();
	      
	      assertRemainingMessages(0);
	
	      // make sure I don't receive anything else
	
	      checkEmpty(queue1);
	
			conn.close();
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
	 * Send some messages, acknowledge them individually and verify they are not resent after
    * recovery.
	 */
	public void testIndividualClientAcknowledge() throws Exception
   {		
		Connection conn = null;
		
		try
		{			
			conn = cf.createConnection();
	
	      Session producerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
			MessageProducer producer = producerSess.createProducer(queue1);
			
			Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
			MessageConsumer consumer = consumerSess.createConsumer(queue1);
			conn.start();
			
			final int NUM_MESSAGES = 20;
			
			for (int i = 0; i < NUM_MESSAGES; i++)
			{
				Message m = producerSess.createMessage();
				producer.send(m);
			}
	      
	      assertRemainingMessages(NUM_MESSAGES);
			
			for (int i = 0; i < NUM_MESSAGES; i++)
			{
				Message m = consumer.receive(200);
				
				assertNotNull(m);
      
	         assertRemainingMessages(NUM_MESSAGES - i);
	         
				m.acknowledge();
	         
	         assertRemainingMessages(NUM_MESSAGES - (i + 1));
			}
	
	      assertRemainingMessages(0);
	       
			consumerSess.recover();
			
			Message m = consumer.receive(200);
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
	 * Send some messages, acknowledge them once after all have been received verify they are not
    * resent after recovery
	 */
	public void testBulkClientAcknowledge() throws Exception
   {
		Connection conn = null;
		
		try
		{
			conn = cf.createConnection();
	
			Session producerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
			MessageProducer producer = producerSess.createProducer(queue1);
	
			Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
			MessageConsumer consumer = consumerSess.createConsumer(queue1);
			conn.start();
	
			final int NUM_MESSAGES = 20;
	
			//Send some messages
			for (int i = 0; i < NUM_MESSAGES; i++)
			{
				Message m = producerSess.createMessage();
				producer.send(m);
			}
	      
	      assertRemainingMessages(NUM_MESSAGES);
	
			log.trace("Sent messages");
	
			Message m = null;
			int count = 0;
			for (int i = 0; i < NUM_MESSAGES; i++)
			{
				m = consumer.receive(200);
				if (m == null) break;
				count++;
			}
	      
	      assertRemainingMessages(NUM_MESSAGES);
	
			assertNotNull(m);
	
			m.acknowledge();
	      
	      assertRemainingMessages(0);
	
			log.trace("Received " + count +  " messages");
	
			assertEquals(count, NUM_MESSAGES);
	
			consumerSess.recover();
	
			log.trace("Session recover called");
	
			m = consumer.receive(200);
	
			log.trace("Message is:" + m);
	
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
	 * Send some messages, acknowledge some of them, and verify that the others are resent after
    * delivery
	 */
	public void testPartialClientAcknowledge() throws Exception
   {
		Connection conn = null;
		
		try
		{
			
			conn = cf.createConnection();
	
			Session producerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
			MessageProducer producer = producerSess.createProducer(queue1);
	
			Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
			MessageConsumer consumer = consumerSess.createConsumer(queue1);
			conn.start();
	
			final int NUM_MESSAGES = 20;
			final int ACKED_MESSAGES = 11;
	
			//Send some messages
			for (int i = 0; i < NUM_MESSAGES; i++)
			{
				Message m = producerSess.createMessage();
				producer.send(m);
			}
	      
	      assertRemainingMessages(NUM_MESSAGES);
	
			log.trace("Sent messages");
	
			int count = 0;
	
			Message m = null;
			for (int i = 0; i < NUM_MESSAGES; i++)
			{
				m = consumer.receive(200);
				if (m == null)
	         {
	            break;
	         }
				if (count == ACKED_MESSAGES -1)
	         {
	            m.acknowledge();
	         }
				count++;
			}
	      
	      assertRemainingMessages(NUM_MESSAGES - ACKED_MESSAGES);
	      
			assertNotNull(m);
	
			log.trace("Received " + count +  " messages");
	
			assertEquals(count, NUM_MESSAGES);
	
			consumerSess.recover();
	
			log.trace("Session recover called");
	
			count = 0;
			while (true)
			{
				m = consumer.receive(200);
				if (m == null) break;
				count++;
			}
	
			assertEquals(NUM_MESSAGES - ACKED_MESSAGES, count);            
		}
		finally
		{
			if (conn != null)
			{
				conn.close();
			}
			
			log.info("Removing all messages from queue " + queue1.getQueueName());
			removeAllMessages(queue1.getQueueName(), true, 0);
			
			log.info("Done remove");
			
			assertRemainingMessages(0);
		}
   }



	/*
	 * Send some messages, consume them and verify the messages are not sent upon recovery
	 *
	 */
	public void testAutoAcknowledge() throws Exception
   {
		Connection conn = null;
		
		try
		{
		   assertRemainingMessages(0);
			
			conn = cf.createConnection();
	
			Session producerSess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			MessageProducer producer = producerSess.createProducer(queue1);
	
			Session consumerSess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			MessageConsumer consumer = consumerSess.createConsumer(queue1);
			conn.start();
	
			final int NUM_MESSAGES = 20;
	
			//Send some messages
			for (int i = 0; i < NUM_MESSAGES; i++)
			{
				Message m = producerSess.createMessage();
				producer.send(m);
			}
	      
	      assertRemainingMessages(NUM_MESSAGES);
	
			int count = 0;
	
			Message m = null;
			for (int i = 0; i < NUM_MESSAGES; i++)
			{
	         assertRemainingMessages(NUM_MESSAGES - i);
	         
				m = consumer.receive(200);
				
	         assertRemainingMessages(NUM_MESSAGES - (i + 1));
	         
				if (m == null) break;
				count++;
			}
	      
	      assertRemainingMessages(0);      		
	
			assertNotNull(m);
	
			log.trace("Received " + count +  " messages");
	
			assertEquals(count, NUM_MESSAGES);
	
			consumerSess.recover();
	
			log.trace("Session recover called");
	
			m = consumer.receive(200);
	
			log.trace("Message is:" + m);
	
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
   
      
   public void testDupsOKAcknowledgeQueue() throws Exception
   {       
      final int BATCH_SIZE = 10;

      ArrayList<String> bindings = new ArrayList<String>();
      bindings.add("mycf");
      deployConnectionFactory(null,"mycf", bindings, -1, -1, -1, -1, false, false, false, BATCH_SIZE);
      Connection conn = null;
      
      try
      {
	      
	      ConnectionFactory myCF = (ConnectionFactory)ic.lookup("/mycf");
	      
	      conn = myCF.createConnection();
	
	      Session producerSess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      MessageProducer producer = producerSess.createProducer(queue1);
	
	      Session consumerSess = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
	      MessageConsumer consumer = consumerSess.createConsumer(queue1);
	      conn.start();
	
	      //Send some messages
	      for (int i = 0; i < 19; i++)
	      {
	         Message m = producerSess.createMessage();
	         producer.send(m);
	      }
	      
	      assertRemainingMessages(19);
	
	      log.trace("Sent messages");
	
	      Message m = null;
	      for (int i = 0; i < 10; i++)
	      {
	         m = consumer.receive(200);
	         
	         assertNotNull(m);
	         
	         log.info("Got message " + i);
	          
	         if (i == 9)
	         {
	            assertRemainingMessages(9);
	         }
	         else
	         {
	            assertRemainingMessages(19);
	         }
	      }
	      
	      for (int i = 0; i < 9; i++)
	      {
	         m = consumer.receive(200);
	         
	         assertNotNull(m);
	         
	         assertRemainingMessages(9);
	      }
	      
	      //Make sure the last are acked on close
	      
	      consumerSess.close();
	      
	      assertRemainingMessages(0);
      }
      finally
      {
      
      	if (conn != null)
      	{
      		conn.close();
      	}
         undeployConnectionFactory("mycf");
      }
      

   }
   
   
   public void testDupsOKAcknowledgeTopic() throws Exception
   {       
      final int BATCH_SIZE = 10;
      ArrayList<String> bindings = new ArrayList<String>();
      bindings.add("mycf");
      deployConnectionFactory(null,"mycf", bindings, -1, -1, -1, -1, false, false, false, BATCH_SIZE);
      Connection conn = null;
      
      try
      {
	      
	      ConnectionFactory myCF = (ConnectionFactory)ic.lookup("/mycf");
	      
	      conn = myCF.createConnection();
	
	      Session producerSess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      MessageProducer producer = producerSess.createProducer(topic1);
	
	      Session consumerSess = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
	      MessageConsumer consumer = consumerSess.createConsumer(topic1);
	      conn.start();
	
	      //Send some messages
	      for (int i = 0; i < 19; i++)
	      {
	         Message m = producerSess.createMessage();
	         producer.send(m);
	      }
	      
	      log.trace("Sent messages");
	
	      Message m = null;
	      for (int i = 0; i < 19; i++)
	      {
	         m = consumer.receive(200);
	         
	         assertNotNull(m);
	      }
	      	   	      
	      consumerSess.close();
      }
      finally
      {
      
      	if (conn != null)
      	{
      		conn.close();
      	}
      	
      	undeployConnectionFactory("mycf");
      }     
   }
   
     
   public void testMessageListenerAutoAck() throws Exception
   {
      // FIXME the test hangs due to a race condition and never finish
      fail("temporarily fails the test so that it does not hang the test suite");
      Connection conn = null;
      
      try
      {	      
	      conn = cf.createConnection();
	      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      MessageProducer prod = sessSend.createProducer(queue1);
  
	      log.info("Sending messages");
	      
	      TextMessage tm1 = sessSend.createTextMessage("a");
	      TextMessage tm2 = sessSend.createTextMessage("b");
	      TextMessage tm3 = sessSend.createTextMessage("c");
	      prod.send(tm1);
	      prod.send(tm2);
	      prod.send(tm3);
	      
	      log.info("Sent messages");
	      
	      sessSend.close();
	      
	      assertRemainingMessages(3);
	   
	      conn.start();
	
	      Session sessReceive = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      
	      log.info("Creating consumer");
	      
	      MessageConsumer cons = sessReceive.createConsumer(queue1);
	      
	      log.info("Created consumer");
	      
	      MessageListenerAutoAck listener = new MessageListenerAutoAck(sessReceive);
	      
	      log.info("Setting message listener");
	      
	      cons.setMessageListener(listener);
	      
	      log.info("Set message listener");
	
	      listener.waitForMessages();
	                  
	      Thread.sleep(500);
	      
	      assertRemainingMessages(0);
	      
	      assertFalse(listener.failed);
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
    *   This test will:
    *     - Send two messages over a producer
    *     - Receive one message over a consumer
    *     - Call Recover
    *     - Receive the second message
    *     - The queue should be empty after that
    *   Note: testMessageListenerAutoAck will test a similar case using MessageListeners
    */
   public void testRecoverAutoACK() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = cf.createConnection();
         Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = s.createProducer(queue1);
         p.setDeliveryMode(DeliveryMode.PERSISTENT);
         Message m = s.createTextMessage("one");
         p.send(m);
         m = s.createTextMessage("two");
         p.send(m);
         conn.close();

         conn = null;

         assertRemainingMessages(2);

         conn = cf.createConnection();

         conn.start();

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer consumer = session.createConsumer(queue1);
         
         log.info("here1");

         TextMessage messageReceived = (TextMessage)consumer.receive(1000);
         
         log.info("here2");

         assertNotNull(messageReceived);

         assertEquals("one", messageReceived.getText());
         
         log.info("here3");

         session.recover();

         messageReceived = (TextMessage)consumer.receive(1000);
                          
         log.info("here4");
         
         assertNotNull(messageReceived);

         assertEquals("two", messageReceived.getText());

         log.info("closing consumer");
         
         consumer.close();
         
         log.info("consumer closed");

         assertRemainingMessages(0);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();                        
         }
      }
   }


   
   public void testMessageListenerDupsOK() throws Exception
   {
      Connection conn = null;
      
      try
      {
	      
         this.assertRemainingMessages(0);
         
	      conn = cf.createConnection();
	      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      MessageProducer prod = sessSend.createProducer(queue1);

	      log.trace("Sending messages");
	      
	      TextMessage tm1 = sessSend.createTextMessage("a");
	      TextMessage tm2 = sessSend.createTextMessage("b");
	      TextMessage tm3 = sessSend.createTextMessage("c");
	      prod.send(tm1);
	      prod.send(tm2);
	      prod.send(tm3);
	      
	      log.trace("Sent messages");
	      
	      sessSend.close();
	      
	      assertRemainingMessages(3);
	   
	      conn.start();
	
	      Session sessReceive = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
	      
	      log.trace("Creating consumer");
	      
	      MessageConsumer cons = sessReceive.createConsumer(queue1);
	      
	      log.trace("Created consumer");
	      
	      MessageListenerDupsOK listener = new MessageListenerDupsOK(sessReceive);
	      
	      log.trace("Setting message listener");
	      
	      cons.setMessageListener(listener);
	      
	      log.trace("Set message listener");
	
	      listener.waitForMessages();
	      
	      assertFalse(listener.failed);
	      
	      assertRemainingMessages(1);
	      
	      log.info("Closing session");
	      sessReceive.close();
	      log.info("Closed session");
	      
	      assertRemainingMessages(0);
	      
	      conn.close();
	           
	      assertRemainingMessages(0);
	      
      }
      finally
      {
      	if (conn != null)
      	{
      		conn.close();
      	}
      	
      }
   }
   
   public void testMessageListenerClientAck() throws Exception
   {
      Connection conn = null;
      
      try
      {      
	      conn = cf.createConnection();
	      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      MessageProducer prod = sessSend.createProducer(queue1);

	      TextMessage tm1 = sessSend.createTextMessage("a");
	      TextMessage tm2 = sessSend.createTextMessage("b");
	      TextMessage tm3 = sessSend.createTextMessage("c");
	      prod.send(tm1);
	      prod.send(tm2);
	      prod.send(tm3);
	      sessSend.close();
	      
	      assertRemainingMessages(3);
	      
	      conn.start();
	      Session sessReceive = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
	      MessageConsumer cons = sessReceive.createConsumer(queue1);
	      MessageListenerClientAck listener = new MessageListenerClientAck(sessReceive);
	      cons.setMessageListener(listener);
	      
	      listener.waitForMessages();
	      
	      Thread.sleep(500);
	      
	      assertRemainingMessages(0);
	      
	      conn.close();
	      
	      assertFalse(listener.failed);
      }
      finally
      {
      	if (conn != null)
      	{
      		conn.close();
      	}
      }
   }
   
   
   public void testMessageListenerTransactionalAck() throws Exception
   {
      Connection conn = null;
      
      try
      {	      
	      conn = cf.createConnection();
	      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      MessageProducer prod = sessSend.createProducer(queue1);

	      TextMessage tm1 = sessSend.createTextMessage("a");
	      TextMessage tm2 = sessSend.createTextMessage("b");
	      TextMessage tm3 = sessSend.createTextMessage("c");
	      prod.send(tm1);
	      prod.send(tm2);
	      prod.send(tm3);
	      sessSend.close();
	      
	      assertRemainingMessages(3);
	      
	      conn.start();
	      Session sessReceive = conn.createSession(true, Session.SESSION_TRANSACTED);
	      MessageConsumer cons = sessReceive.createConsumer(queue1);
	      MessageListenerTransactionalAck listener = new MessageListenerTransactionalAck(sessReceive);
	      cons.setMessageListener(listener);
	      listener.waitForMessages();
	      
	      Thread.sleep(500);
	      
	      assertRemainingMessages(0);
	      
	      conn.close();
	      
	      assertFalse(listener.failed);
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


   private abstract class LatchListener implements MessageListener
   {
      protected Latch latch = new Latch();

      protected Session sess;

      protected int count = 0;

      boolean failed;

      LatchListener(Session sess)
      {
         this.sess = sess;
      }

      public void waitForMessages() throws InterruptedException
      {
         latch.acquire();         
      }

      public abstract void onMessage(Message m);
      
   }
   
   private class MessageListenerAutoAck extends LatchListener
   {
      
      MessageListenerAutoAck(Session sess)
      {
         super(sess);
      }
      
      public void onMessage(Message m)
      {
         try
         {
            count++;
                  
            TextMessage tm = (TextMessage)m;
            
            log.info("Got message: " + tm.getText());
                      
            // Receive first three messages then recover() session
            // Only last message should be redelivered
            if (count == 1)
            {
               assertRemainingMessages(3);
               
               if (!"a".equals(tm.getText()))
               {
                  failed = true;
                  latch.release();
               }
            }
            if (count == 2)
            {
               assertRemainingMessages(2);
               
               if (!"b".equals(tm.getText()))
               {
                  failed = true;
                  latch.release();
               }
            }
            if (count == 3)
            {
               assertRemainingMessages(1);
               
               if (!"c".equals(tm.getText()))
               {
                  failed = true;
                  latch.release();
               }
               log.info("Recovering session");
               sess.recover();
               log.info("Session recovered");
            }
            if (count == 4)
            {
               assertRemainingMessages(1);
               
               if (!"c".equals(tm.getText()))
               {
                  failed = true;
                  latch.release();
               }               
               latch.release();
            }            
               
         }
         catch (Exception e)
         {
            e.printStackTrace();
            failed = true;
            latch.release();
         }
      }
            
   }
   
   private class MessageListenerDupsOK  extends LatchListener
   {
      
      MessageListenerDupsOK(Session sess)
      {
         super(sess);
      }

      public void onMessage(Message m)
      {
         try
         {
            count++;
                  
            TextMessage tm = (TextMessage)m;
            
            log.info("Got message " + tm.getText());

            // Receive first three messages then recover() session
            // Only last message should be redelivered
            if (count == 1)
            {
               assertRemainingMessages(3);
               
               if (!"a".equals(tm.getText()))
               {
                  failed = true;
                  log.info("Failed1");
                  latch.release();
               }
            }
            if (count == 2)
            {
               assertRemainingMessages(3);
               
               if (!"b".equals(tm.getText()))
               {
                  failed = true;
                  log.info("Failed2");
                  latch.release();
               }
            }
            if (count == 3)
            {
               assertRemainingMessages(3);
               
               if (!"c".equals(tm.getText()))
               {
                  failed = true;
                  log.info("Failed3");
                  latch.release();
               }
               sess.recover();
            }
            if (count == 4)
            {
               assertRemainingMessages(1);
               
               if (!"c".equals(tm.getText()))
               {
                  failed = true;
                  log.info("Failed4");
                  latch.release();
               }      
               latch.release();
            }   
            
         }
         catch (Exception e)
         {
            failed = true;
            latch.release();
            e.printStackTrace();
         }
      }
            
   }
   
   
   private class MessageListenerClientAck extends LatchListener
   {
      
      MessageListenerClientAck(Session sess)
      {
         super(sess);
      }


      public void onMessage(Message m)
      {
         try
         {
            count++;
            
            TextMessage tm = (TextMessage)m;

            if (count == 1)
            {
               assertRemainingMessages(3);
               if (!"a".equals(tm.getText()))
               {
                  log.trace("Expected a but got " + tm.getText());
                  failed = true;
                  latch.release();
               }               
            }
            if (count == 2)
            {
               assertRemainingMessages(3);
               if (!"b".equals(tm.getText()))
               {
                  log.trace("Expected b but got " + tm.getText());
                  failed = true;
                  latch.release();
               }               
            }
            if (count == 3)
            {
               assertRemainingMessages(3);
               if (!"c".equals(tm.getText()))
               {
                  log.trace("Expected c but got " + tm.getText());
                  failed = true;
                  latch.release();
               }
               sess.recover();
            }
            if (count == 4)
            {
               assertRemainingMessages(3);
               if (!"a".equals(tm.getText()))
               {
                  log.trace("Expected a but got " + tm.getText());
                  failed = true;
                  latch.release();
               }     
               log.info("acknowledging manually");
               tm.acknowledge();
               assertRemainingMessages(2);
               sess.recover();
            } 
            if (count == 5)
            {
               assertRemainingMessages(2);
               if (!"b".equals(tm.getText()))
               {
                  log.trace("Expected b but got " + tm.getText());
                  failed = true;
                  latch.release();
               }  
               sess.recover();
            }
            if (count == 6)
            {
               assertRemainingMessages(2);
               if (!"b".equals(tm.getText()))
               {
                  log.trace("Expected b but got " + tm.getText());
                  failed = true;
                  latch.release();
               }               
            }
            if (count == 7)
            {
               assertRemainingMessages(2);
               if (!"c".equals(tm.getText()))
               {
                  log.trace("Expected c but got " + tm.getText());
                  failed = true;
                  latch.release();
               }
               tm.acknowledge();
               assertRemainingMessages(0);
               latch.release();
            }
               
         }
         catch (Exception e)
         {
            log.error("Caught exception", e);
            failed = true;
            latch.release();
         }
      }
            
   }
   
   private class MessageListenerTransactionalAck extends LatchListener
   {
      
      MessageListenerTransactionalAck(Session sess)
      {
         super(sess);
      }

      
      public void onMessage(Message m)
      {
         try
         {
            count++;
            
            TextMessage tm = (TextMessage)m;
            
            log.info("Got message " + tm.getText());
                   
            if (count == 1)
            {
               assertRemainingMessages(3);
               if (!"a".equals(tm.getText()))
               {
                  failed = true;
                  latch.release();
               }               
            }
            if (count == 2)
            {
               assertRemainingMessages(3);
               if (!"b".equals(tm.getText()))
               {
                  failed = true;
                  latch.release();
               }               
            }
            if (count == 3)
            {
               assertRemainingMessages(3);
               if (!"c".equals(tm.getText()))
               {
                  failed = true;
                  latch.release();
               }
               log.info("Rollback");
               sess.rollback();
            }
            if (count == 4)
            {
               assertRemainingMessages(3);
               if (!"a".equals(tm.getText()))
               {
                  failed = true;
                  latch.release();
               }     
            } 
            if (count == 5)
            {
               assertRemainingMessages(3);
               if (!"b".equals(tm.getText()))
               {
                  failed = true;
                  latch.release();
               }  
               log.info("commit");
               sess.commit();
               assertRemainingMessages(1);
            }
            if (count == 6)
            {
               assertRemainingMessages(1);
               if (!"c".equals(tm.getText()))
               {
                  failed = true;
                  latch.release();
               }  
               log.info("rollback2");
               sess.rollback();
            }
            if (count == 7)
            {
               assertRemainingMessages(1);
               if (!"c".equals(tm.getText()))
               {
                  failed = true;
                  latch.release();
               }  
               log.info("Commit2");
               sess.commit();
               assertRemainingMessages(0);
               latch.release();
            }        
         }
         catch (Exception e)
         {
            //log.error(e);
            failed = true;
            latch.release();
         }
      }
            
   }
}


