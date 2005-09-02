/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;


import java.util.Enumeration;

import javax.jms.Connection;
import javax.jms.InvalidDestinationException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;



/**
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class BrowserTest extends MessagingTestCase
{
	
	//	 Constants -----------------------------------------------------
	
	// Static --------------------------------------------------------
	
	// Attributes ----------------------------------------------------
	
	protected InitialContext initialContext;
	
	
	protected JBossConnectionFactory cf;
	protected Queue queue;
	protected Topic topic;
   protected Connection connection;
   protected Session session;
   protected MessageProducer producer;

	// Constructors --------------------------------------------------
	
	public BrowserTest(String name)
	{
		super(name);
	}
	
	// TestCase overrides -------------------------------------------
	
	public void setUp() throws Exception
	{

		super.setUp();
      ServerManagement.setRemote(false);
      ServerManagement.startInVMServer("all");
		initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
		cf = (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");
		
      ServerManagement.undeployTopic("Topic");
      ServerManagement.undeployQueue("Queue");
      
		ServerManagement.deployQueue("Queue");
		queue = (Queue)initialContext.lookup("/queue/Queue");
		
		ServerManagement.deployTopic("Topic");
		topic = (Topic)initialContext.lookup("/topic/Topic");

      connection = cf.createConnection();
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      producer = session.createProducer(queue);
    
	}
	
	public void tearDown() throws Exception
	{
      connection.stop();
      connection = null;
		ServerManagement.stopInVMServer();
		super.tearDown();
	}
	
	// Public --------------------------------------------------------


//   public void testCreateBrowserOnNullDestination() throws Exception
//   {
//      try
//      {
//         session.createBrowser(null);
//         fail("should throw exception");
//      }
//      catch(InvalidDestinationException e)
//      {
//         // OK
//      }
//   }

	public void testBrowse() throws Exception
	{
		
		log.trace("Starting testBrowse()");						
		

		final int numMessages = 100;
		
		for (int i = 0; i < numMessages; i++)
		{
			Message m = session.createMessage();
			producer.send(m);
		}
		
		log.trace("Sent messages");

		QueueBrowser browser = session.createBrowser(queue);
		
		assertEquals(browser.getQueue(), queue);
		
		assertNull(browser.getMessageSelector());
		
		Enumeration en = browser.getEnumeration();
		
		int count = 0;
		while (en.hasMoreElements())
		{
			en.nextElement();
			count++;
		}
		
		assertEquals(count, numMessages);
		
		MessageConsumer mc = session.createConsumer(queue);
		
		connection.start();
		
		for (int i = 0; i < numMessages; i++)
		{
			mc.receive();
		}
		
		browser = session.createBrowser(queue);
		en = browser.getEnumeration();
		
		
		count = 0;
		while (en.hasMoreElements())
		{
			Message mess = (Message)en.nextElement();
			log.trace("message:" + mess);
			count++;
		}
		
		log.trace("Received " + count + " messages");
	}
	
	
//	
//	public void testBrowseWithSelector() throws Exception
//	{
//
//		final int numMessages = 100;
//		
//		for (int i = 0; i < numMessages; i++)
//		{
//			Message m = session.createMessage();
//			m.setIntProperty("test_counter", i+1);
//			producer.send(m);
//		}
//		
//		log.trace("Sent messages");
//		
//		QueueBrowser browser = session.createBrowser(queue, "test_counter > 30");
//		
//		Enumeration en = browser.getEnumeration();
//		int count = 0;
//		while (en.hasMoreElements())
//		{
//			Message m = (Message)en.nextElement();
//			int testCounter = m.getIntProperty("test_counter");
//			assertTrue(testCounter > 30);
//			count++;
//		}
//		assertEquals(70, count);
//	}
	
	
	// Package protected ---------------------------------------------
	
	// Protected -----------------------------------------------------
	
	// Private -------------------------------------------------------
	
	// Inner classes -------------------------------------------------
}

