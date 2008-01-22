/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.stress;

import java.util.HashSet;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.jboss.messaging.util.Logger;
import org.jboss.test.messaging.JBMServerTestCase;


/**
 * 
 * Create 500 connections each with a consumer, consuming from a topic
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: $</tt>4 Jul 2007
 *
 * $Id: $
 *
 */
public class ManyConnectionsStressTest extends JBMServerTestCase
{
   // Constants -----------------------------------------------------

   private static Logger log = Logger.getLogger(RelayStressTest.class);
   
	private static final int NUM_CONNECTIONS = 500;
	
	private static final int NUM_MESSAGES = 100;
	

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private InitialContext ic;
   
   private volatile boolean failed;
   
   private Set listeners = new HashSet();
   

   // Constructors --------------------------------------------------

   public ManyConnectionsStressTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      //ServerManagement.start("all");
      
      ic = getInitialContext();
      
      createTopic("StressTestTopic");

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      destroyTopic("StressTestTopic");
      
      ic.close();
      
      super.tearDown();
   }
   
   public void testManyConnections() throws Exception
   {
   	ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
   	
   	Topic topic = (Topic)ic.lookup("/topic/StressTestTopic");
   	
   	Connection[] conns = new Connection[NUM_CONNECTIONS];
   	
   	Connection connSend = null;
   	
   	try
   	{
   		for (int i = 0; i < NUM_CONNECTIONS; i++)
   		{
   			conns[i] = cf.createConnection();
   			
   			Session sess = conns[i].createSession(false, Session.AUTO_ACKNOWLEDGE);
   			
   			MessageConsumer cons = sess.createConsumer(topic);
   			
   			MyListener listener = new MyListener();
   			
   			synchronized (listeners)
   			{
   				listeners.add(listener);
   			}
   			
   			cons.setMessageListener(listener);
   			
   			conns[i].start();
   			
   			log.info("Created " + i);
   		}
   		
   		//Thread.sleep(100 * 60 * 1000);
   		
   		connSend = cf.createConnection();
   		
   		Session sessSend = connSend.createSession(false, Session.AUTO_ACKNOWLEDGE);
   		
   		MessageProducer prod = sessSend.createProducer(topic);
   		
   		for (int i = 0; i < NUM_MESSAGES; i++)
   		{
   			TextMessage tm = sessSend.createTextMessage("message" + i);
   			
   			tm.setIntProperty("count", i);
   			
   			prod.send(tm);
   		}
   		
   		long wait = 30000;
   		
   		synchronized (listeners)
   		{
   			while (!listeners.isEmpty() && wait > 0)
   			{
   				long start = System.currentTimeMillis();               
   				try
   				{
   					listeners.wait(wait);
   				}
   				catch (InterruptedException e)
   				{  
   					//Ignore
   				}
   				wait -= (System.currentTimeMillis() - start);
   			} 
   		}
   		
   		if (wait <= 0)
   		{
   			fail("Timed out");
   		}
   		
   		assertFalse(failed);

   		log.info("Done");
   	}
   	finally
   	{
   		for (int i = 0; i < NUM_CONNECTIONS; i++)
   		{
   			try
   			{
   				if (conns[i] != null)
   				{
   					conns[i].close();
   				}
   			}
   			catch (Throwable t)
   			{
   				log.error("Failed to close connection", t);
   			}
   		}
   		
   		if (connSend != null)
   		{
   			connSend.close();
   		}
   	}
   }
   
   private void finished(MyListener listener)
   {
   	synchronized (listeners)
   	{
   		log.info("consumer " + listener + " has finished");
   		
   		listeners.remove(listener);
   		
   		listeners.notify();
   	}   	   	
   }
   
   private void failed(MyListener listener)
   {
   	synchronized (listeners)
   	{
   		log.error("consumer " + listener + " has failed");
   		
   		listeners.remove(listener);
   		
   		failed = true;
   		
   		listeners.notify();
   	}
   }
   
   private class MyListener implements MessageListener
   {
		public void onMessage(Message msg)
		{
			try
			{
				int count = msg.getIntProperty("count");
				
				//log.info(this + " got message " + msg);
				
				if (count == NUM_MESSAGES - 1)
				{
					finished(this);
				}
			}
			catch (JMSException e)
			{
				log.error("Failed to get int property", e);
				
				failed(this);
			}
		}
   	
   }
}


