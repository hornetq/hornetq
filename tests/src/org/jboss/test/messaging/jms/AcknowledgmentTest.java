/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.util.InVMInitialContextFactory;

import javax.naming.InitialContext;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

/**
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 */
public class AcknowledgmentTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;
   
   protected JBossConnectionFactory cf;
   protected Destination queue;

   // Constructors --------------------------------------------------

   public AcknowledgmentTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.startInVMServer();
      initialContext = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());
      cf =
            (JBossConnectionFactory)initialContext.lookup("/messaging/ConnectionFactory");
      
      ServerManagement.deployQueue("Queue");
      queue = (Destination)initialContext.lookup("/messaging/queues/Queue");

      
   }

   public void tearDown() throws Exception
   {
      ServerManagement.stopInVMServer();
      //connection.stop();
      //connection = null;
      super.tearDown();
   }


   // Public --------------------------------------------------------

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
		
		log.trace("Received " + count +  " messages");
		
		assertEquals(count, NUM_MESSAGES);
		
		consumerSess.recover();
		
		log.trace("Session recover called");
		
		count = 0;
		while (true)		
		{
			Message m = consumer.receive(500);
			if (m == null) break;
			count++;
		}
		
		log.trace("Received " + count +  " messages after recover");
		
		assertEquals(count, NUM_MESSAGES);
		
		conn.close();
		
   }
   
	
	
	/**
	 * Send some messages, acknowledge them individually and  verify they are not resent after
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
			m.acknowledge();
			count++;
		}
		
		
		log.trace("Received " + count +  " messages");
		
		assertEquals(count, NUM_MESSAGES);
		
		consumerSess.recover();
		
		log.trace("Session recover called");
		
		Message m = consumer.receive(2000);
		
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
		
		final int NUM_MESSAGES = 10;
		
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
		
		final int NUM_MESSAGES = 10;
		final int ACKED_MESSAGES = 7;
		
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
		
		final int NUM_MESSAGES = 10;	
		
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
		
		final int NUM_MESSAGES = 10;	
		
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


