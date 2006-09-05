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
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.management.ObjectName;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

import EDU.oswego.cs.dl.util.concurrent.Latch;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 * $Id$
 */
public class AcknowledgementTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;
   
   protected JBossConnectionFactory cf;
   protected Destination queue;
   protected Topic topic;

   // Constructors --------------------------------------------------

   public AcknowledgementTest(String name)
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
      ServerManagement.deployQueue("Queue");
      ServerManagement.undeployTopic("Topic");
      ServerManagement.deployTopic("Topic");
      queue = (Destination)initialContext.lookup("/queue/Queue"); 
      topic = (Topic)initialContext.lookup("/topic/Topic");     
      
      drainDestination(cf, queue);

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("Queue");
      ServerManagement.undeployTopic("Topic");
      
      super.tearDown();
      
   }


   // Public --------------------------------------------------------
   
   
   /* Topics shouldn't hold on to messages if there are no subscribers */
   public void testPersistentMessagesForTopicDropped() throws Exception
   {
      TopicConnection conn = null;
      
      try
      {
         conn = cf.createTopicConnection();
         TopicSession sess = conn.createTopicSession(true, 0);
         TopicPublisher pub = sess.createPublisher(topic);
         pub.setDeliveryMode(DeliveryMode.PERSISTENT);
         
         Message m = sess.createTextMessage("testing123");
         pub.publish(m);
         sess.commit();
         
         conn.close();
         conn = cf.createTopicConnection();
         conn.start();
         
         TopicSession newsess = conn.createTopicSession(true, 0);
         TopicSubscriber newcons = newsess.createSubscriber(topic);
         
         Message m2 = (Message)newcons.receive(200);
         assertNull(m2);
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
         TopicPublisher pub = sess.createPublisher(topic);
         TopicSubscriber sub = sess.createSubscriber(topic);
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
         conn = cf.createTopicConnection();
         conn.start();
         
         TopicSession newsess = conn.createTopicSession(true, 0);
         TopicSubscriber newcons = newsess.createSubscriber(topic);
         
         Message m3 = (Message)newcons.receive(200);
         assertNull(m3);
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
         TopicPublisher pub = sess.createPublisher(topic);
         TopicSubscriber cons = sess.createSubscriber(topic);
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
         TopicPublisher newpub = newsess.createPublisher(topic);
         TopicSubscriber newcons = newsess.createSubscriber(topic);
         
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
   
   public void testTransactionalAcknowlegment() throws Exception
   {

      Connection conn = cf.createConnection();

      Session producerSess = conn.createSession(true, Session.SESSION_TRANSACTED);
      MessageProducer producer = producerSess.createProducer(queue);

      Session consumerSess = conn.createSession(true, Session.SESSION_TRANSACTED);
      MessageConsumer consumer = consumerSess.createConsumer(queue);
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
         Message m = consumer.receive(200);
         if (m == null) break;
         count++;
      }
      
      assertRemainingMessages(NUM_MESSAGES);

      log.trace("Received " + count +  " messages");

      assertEquals(count, NUM_MESSAGES);

      consumerSess.rollback();
      
      assertRemainingMessages(NUM_MESSAGES);

      log.trace("Session rollback called");

      Message m = null;

      int i = 0;
      for(; i < NUM_MESSAGES; i++)
      {
         m = consumer.receive();
         log.trace("Received message " + i);

      }
      
      assertRemainingMessages(NUM_MESSAGES);

      // if I don't receive enough messages, the test will timeout

      log.trace("Received " + i +  " messages after recover");
      
      consumerSess.commit();
      
      assertRemainingMessages(0);

      // make sure I don't receive anything else

      m = consumer.receive(200);
      assertNull(m);

      conn.close();

   }

	/**
	 * Send some messages, don't acknowledge them and verify that they are re-sent on recovery.
	 */
	public void testClientAcknowledgeNoAcknowlegment() throws Exception
   {

		Connection conn = cf.createConnection();

		Session producerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
		MessageProducer producer = producerSess.createProducer(queue);

		Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
		MessageConsumer consumer = consumerSess.createConsumer(queue);
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

		log.trace("Received " + count +  " messages");

		assertEquals(count, NUM_MESSAGES);

		consumerSess.recover();
      
      assertRemainingMessages(NUM_MESSAGES);

		log.trace("Session recover called");

      Message m = null;

      int i = 0;
      for(; i < NUM_MESSAGES; i++)
      {
         m = consumer.receive();
         log.trace("Received message " + i);

      }
      
      assertRemainingMessages(NUM_MESSAGES);

      // if I don't receive enough messages, the test will timeout

		log.trace("Received " + i +  " messages after recover");
      
      m.acknowledge();
      
      assertRemainingMessages(0);

      // make sure I don't receive anything else

      m = consumer.receive(200);
      assertNull(m);

		conn.close();

   }

	
	
	/**
	 * Send some messages, acknowledge them individually and verify they are not resent after
    * recovery.
	 */
	public void testIndividualClientAcknowledge() throws Exception
   {
		
		Connection conn = cf.createConnection();     
		
		Session producerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
		MessageProducer producer = producerSess.createProducer(queue);
		
		Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
		MessageConsumer consumer = consumerSess.createConsumer(queue);
		conn.start();
		
		final int NUM_MESSAGES = 20;
		
		for (int i = 0; i < NUM_MESSAGES; i++)
		{
			Message m = producerSess.createMessage();
			producer.send(m);
		}
      
      assertRemainingMessages(NUM_MESSAGES);
		
		log.trace("Sent " + NUM_MESSAGES + " messages");
		
		int count = 0;
		while (true)		
		{
			Message m = consumer.receive(200);
         log.trace("Received message " + m);
			if (m == null)
         {
            break;
         }
         log.trace("Acking session");
         
         assertRemainingMessages(NUM_MESSAGES - count);
         
			m.acknowledge();
         
         assertRemainingMessages(NUM_MESSAGES - (count + 1));
			count++;
		}

      assertRemainingMessages(0);
      
      assertEquals(NUM_MESSAGES, count);
      log.trace("received and acknowledged " + count +  " messages");

      Message m = consumer.receive(200);
		assertNull(m);
      
      log.trace("mesage is " + m);

      log.trace("calling session recover()");
		consumerSess.recover();
		log.trace("recover called");
		
		m = consumer.receive(200);
		assertNull(m);
		
		conn.close();
		
   }


	/**
	 * Send some messages, acknowledge them once after all have been received verify they are not
    * resent after recovery
	 */
	public void testBulkClientAcknowledge() throws Exception
   {

		Connection conn = cf.createConnection();

		Session producerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
		MessageProducer producer = producerSess.createProducer(queue);

		Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
		MessageConsumer consumer = consumerSess.createConsumer(queue);
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

		conn.close();

   }


	/**
	 * Send some messages, acknowledge some of them, and verify that the others are resent after
    * delivery
	 */
	public void testPartialClientAcknowledge() throws Exception
   {

		Connection conn = cf.createConnection();

		Session producerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
		MessageProducer producer = producerSess.createProducer(queue);

		Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
		MessageConsumer consumer = consumerSess.createConsumer(queue);
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

		conn.close();

   }



	/*
	 * Send some messages, consume them and verify the messages are not sent upon recovery
	 *
	 */
	public void testAutoAcknowledge() throws Exception
   {

		Connection conn = cf.createConnection();

		Session producerSess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
		MessageProducer producer = producerSess.createProducer(queue);

		Session consumerSess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
		MessageConsumer consumer = consumerSess.createConsumer(queue);
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

		conn.close();

   }


	/*
	 * Send some messages, consume them and verify the messages are not sent upon recovery
	 *
	 */
	public void testLazyAcknowledge() throws Exception
   {

		Connection conn = cf.createConnection();

		Session producerSess = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
		MessageProducer producer = producerSess.createProducer(queue);

		Session consumerSess = conn.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
		MessageConsumer consumer = consumerSess.createConsumer(queue);
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

		Message m = null;
		for (int i = 0; i < NUM_MESSAGES; i++)
		{
			m = consumer.receive(200);
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

		conn.close();

   }
   
   public void testMessageListenerAutoAck() throws Exception
   {
      Connection conn = cf.createConnection();
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = sessSend.createProducer(queue);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      TextMessage tm1 = sessSend.createTextMessage("a");
      TextMessage tm2 = sessSend.createTextMessage("b");
      TextMessage tm3 = sessSend.createTextMessage("c");
      prod.send(tm1);
      prod.send(tm2);
      prod.send(tm3);
      sessSend.close();
      
      assertRemainingMessages(3);
   
      conn.start();

      Session sessReceive = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer cons = sessReceive.createConsumer(queue);
      
      MessageListenerAutoAck listener = new MessageListenerAutoAck(sessReceive);
      cons.setMessageListener(listener);

      listener.waitForMessages();
      
      assertRemainingMessages(0);
      
      conn.close();
      assertFalse(listener.failed);
   }
   
   public void testMessageListenerClientAck() throws Exception
   {
      Connection conn = cf.createConnection();
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = sessSend.createProducer(queue);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
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
      MessageConsumer cons = sessReceive.createConsumer(queue);
      MessageListenerClientAck listener = new MessageListenerClientAck(sessReceive);
      cons.setMessageListener(listener);
      
      listener.waitForMessages();
      
      assertRemainingMessages(0);
      
      conn.close();
      
      assertFalse(listener.failed);
   }
   
   
   public void testMessageListenerTransactionalAck() throws Exception
   {
      Connection conn = cf.createConnection();
      Session sessSend = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = sessSend.createProducer(queue);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
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
      MessageConsumer cons = sessReceive.createConsumer(queue);
      MessageListenerTransactionalAck listener = new MessageListenerTransactionalAck(sessReceive);
      cons.setMessageListener(listener);
      listener.waitForMessages();
      
      assertRemainingMessages(0);
      
      conn.close();
      
      assertFalse(listener.failed);
   }
   
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
   
   private class MessageListenerAutoAck implements MessageListener
   {
      
      private Latch latch = new Latch();
      
      private Session sess;
      
      private int count = 0;
      
      boolean failed;
      
      MessageListenerAutoAck(Session sess)
      {
         this.sess = sess;
      }
      
      public void waitForMessages() throws InterruptedException
      {
         latch.acquire();
         Thread.sleep(500);
      }

      public void onMessage(Message m)
      {
         try
         {
            count++;
                  
            TextMessage tm = (TextMessage)m;
                      
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
               sess.recover();
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
            failed = true;
            latch.release();
         }
      }
            
   }
   
   
   private class MessageListenerClientAck implements MessageListener
   {
      
      private Latch latch = new Latch();
      
      private Session sess;
      
      private int count = 0;
      
      boolean failed;
      
      MessageListenerClientAck(Session sess)
      {
         this.sess = sess;
      }

      
      public void waitForMessages() throws InterruptedException
      {
         latch.acquire();
         Thread.sleep(500);
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
   
   private class MessageListenerTransactionalAck implements MessageListener
   {
      
      private Latch latch = new Latch();
      
      private Session sess;
      
      private int count = 0;
      
      boolean failed;
      
      MessageListenerTransactionalAck(Session sess)
      {
         this.sess = sess;
      }

      
      public void waitForMessages() throws InterruptedException
      {
         latch.acquire();
         
         //Wait for postdeliver to be called
         Thread.sleep(500);
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
               log.trace("Rollback");
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
               log.trace("commit");
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
               log.trace("recover");
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
               log.trace("Commit");
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
   
   private boolean assertRemainingMessages(int expected) throws Exception
   {
      //Need to pause since delivery may still be in progress
      Thread.sleep(500);
      ObjectName destObjectName = 
         new ObjectName("jboss.messaging.destination:service=Queue,name=Queue");
      Integer messageCount = (Integer)ServerManagement.getAttribute(destObjectName, "MessageCount");      
      assertEquals(expected, messageCount.intValue());      
      return expected == messageCount.intValue();
   }
   

}


