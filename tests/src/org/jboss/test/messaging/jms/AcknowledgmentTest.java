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
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicPublisher;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 *
 * $Id$
 */
public class AcknowledgmentTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;
   
   protected JBossConnectionFactory cf;
   protected Destination queue;
   protected Topic topic;

   // Constructors --------------------------------------------------

   public AcknowledgmentTest(String name)
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
      ServerManagement.deployQueue("Queue");
      ServerManagement.undeployTopic("Topic");
      ServerManagement.deployTopic("Topic");
      queue = (Destination)initialContext.lookup("/queue/Queue"); 
      topic = (Topic)initialContext.lookup("/topic/Topic");
      drainDestination(cf, queue);
   }

   public void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("Queue");
      ServerManagement.deInit();
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
         
         Message m2 = (Message)newcons.receive(3000);
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
         
         Message m3 = (Message)newcons.receive(3000);
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

		log.trace("Sent messages");

		int count = 0;
		while (true)
		{
			Message m = consumer.receive(500);
			if (m == null) break;
			count++;
		}

		log.trace("Received " + count +  " messages");

		assertEquals(count, NUM_MESSAGES);

		consumerSess.recover();

		log.trace("Session recover called");


      int i = 0;
      for(; i < NUM_MESSAGES; i++)
      {
         consumer.receive();
         log.trace("Received message " + i);

      }

      // if I don't receive enough messages, the test will timeout

		log.trace("Received " + i +  " messages after recover");

      // make sure I don't receive anything else

      Message m = consumer.receive(2000);
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
		
		log.trace("Sent " + NUM_MESSAGES + " messages");
		
		int count = 0;
		while (true)		
		{
			Message m = consumer.receive(500);
         log.trace("Received message " + m);
			if (m == null)
         {
            break;
         }
         log.trace("Acking session");
			m.acknowledge();
			count++;
		}

      assertEquals(NUM_MESSAGES, count);
      log.trace("received and acknowledged " + count +  " messages");

      Message m = consumer.receive(2000);
		assertNull(m);
      
      log.trace("mesage is " + m);

      log.trace("calling session recover()");
		consumerSess.recover();
		log.trace("recover called");
		
		m = consumer.receive(2000);
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

		log.trace("Sent messages");

		Message m = null;
		int count = 0;
		for (int i = 0; i < NUM_MESSAGES; i++)
		{
			m = consumer.receive(500);
			if (m == null) break;
			count++;
		}

		assertNotNull(m);

		m.acknowledge();

		log.trace("Received " + count +  " messages");

		assertEquals(count, NUM_MESSAGES);

		consumerSess.recover();

		log.trace("Session recover called");

		m = consumer.receive(2000);

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

		log.trace("Sent messages");

		int count = 0;

		Message m = null;
		for (int i = 0; i < NUM_MESSAGES; i++)
		{
			m = consumer.receive(500);
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

		assertNotNull(m);

		log.trace("Received " + count +  " messages");

		assertEquals(count, NUM_MESSAGES);

		consumerSess.recover();

		log.trace("Session recover called");

		count = 0;
		while (true)
		{
			m = consumer.receive(500);
			if (m == null) break;
			count++;
		}

		assertEquals(count, NUM_MESSAGES - ACKED_MESSAGES);

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

		log.trace("Sent messages");

		int count = 0;

		Message m = null;
		for (int i = 0; i < NUM_MESSAGES; i++)
		{
			m = consumer.receive(500);
			if (m == null) break;
			count++;
		}

		assertNotNull(m);

		log.trace("Received " + count +  " messages");

		assertEquals(count, NUM_MESSAGES);

		consumerSess.recover();

		log.trace("Session recover called");

		m = consumer.receive(2000);

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

		log.trace("Sent messages");

		int count = 0;

		Message m = null;
		for (int i = 0; i < NUM_MESSAGES; i++)
		{
			m = consumer.receive(500);
			if (m == null) break;
			count++;
		}

		assertNotNull(m);

		log.trace("Received " + count +  " messages");

		assertEquals(count, NUM_MESSAGES);

		consumerSess.recover();

		log.trace("Session recover called");

		m = consumer.receive(2000);

		log.trace("Message is:" + m);

		assertNull(m);

		conn.close();

   }
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}


