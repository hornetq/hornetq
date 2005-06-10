/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;


import java.util.Enumeration;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.util.InVMInitialContextFactory;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;



/**
 * 
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 */
public class QueueBrowserTest extends MessagingTestCase
{
	
	//	 Constants -----------------------------------------------------
	
	// Static --------------------------------------------------------
	
	// Attributes ----------------------------------------------------
	
	protected InitialContext initialContext;
	
	
	protected JBossConnectionFactory cf;
	protected Queue queue;
	protected Topic topic;
	
	// Constructors --------------------------------------------------
	
	public QueueBrowserTest(String name)
	{
		super(name);
	}
	
	// TestCase overrides -------------------------------------------
	
	public void setUp() throws Exception
	{
		super.setUp();
		ServerManagement.startInVMServer();
		initialContext = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());
		cf = (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");
		
		ServerManagement.deployQueue("Queue");
		queue = (Queue)initialContext.lookup("/queue/Queue");
		
		ServerManagement.deployTopic("Topic");
		topic = (Topic)initialContext.lookup("/topic/Topic");
	}
	
	public void tearDown() throws Exception
	{
		ServerManagement.stopInVMServer();
		//connection.stop();
		//connection = null;
		super.tearDown();
	}
	
	// Public --------------------------------------------------------
	
	
	public void testBrowse() throws Exception
	{
		
		log.trace("Starting testBrowse()");						
		
		Connection conn = cf.createConnection();      
		Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
		
		MessageProducer mp = sess.createProducer(queue);
		
		
		
		final int numMessages = 100;
		
		for (int i = 0; i < numMessages; i++)
		{
			Message m = sess.createMessage();
			mp.send(m);
		}
		
		log.trace("Sent messages");
		
		
		
		QueueBrowser browser = sess.createBrowser(queue);
		
		assertEquals(browser.getQueue(), queue);
		
		assertNull(browser.getMessageSelector());
		
		Enumeration en = browser.getEnumeration();
		
		int count = 0;
		while (en.hasMoreElements())
		{
			Message m = (Message)en.nextElement();			 
			count++;
		}
		
		assertEquals(count, numMessages);
		
		MessageConsumer mc = sess.createConsumer(queue);
		
		conn.start();
		
		for (int i = 0; i < numMessages; i++)
		{
			Message m = mc.receive();
		}
		
		browser = sess.createBrowser(queue);
		en = browser.getEnumeration();
		
		
		count = 0;
		while (en.hasMoreElements())
		{
			Message mess = (Message)en.nextElement();
			log.trace("message:" + mess);
			count++;
		}
		
		log.trace("Received " + count + " messages");
		
		conn.close();
	}
	
	
	
	public void testBrowseWithSelector() throws Exception
	{

		Connection conn = cf.createConnection();      
		Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
		
		MessageProducer mp = sess.createProducer(queue);
						
		final int numMessages = 100;
		
		for (int i = 0; i < numMessages; i++)
		{
			Message m = sess.createMessage();
			m.setIntProperty("test_counter", i+1);
			mp.send(m);
		}
		
		log.trace("Sent messages");
		
		QueueBrowser browser = sess.createBrowser(queue, "test_counter > 30");
		
		Enumeration en = browser.getEnumeration();
		int count = 0;
		while (en.hasMoreElements())
		{
			Message m = (Message)en.nextElement();
			int testCounter = m.getIntProperty("test_counter");
			assertTrue(testCounter > 30);
			count++;
		}
		assertEquals(70, count);
		
	}
	
	
	// Package protected ---------------------------------------------
	
	// Protected -----------------------------------------------------
	
	// Private -------------------------------------------------------
	
	// Inner classes -------------------------------------------------
	
}

