/**
 * JBoss, Home of Professional Open Source
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms.stress.clustering;

import java.util.HashSet;
import java.util.Set;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.jboss.logging.Logger;
import org.jboss.test.messaging.jms.clustering.ClusteringTestBase;


public class ClusteredTopicStressTest extends ClusteringTestBase
{
   // Constants -----------------------------------------------------

   private static Logger log = Logger.getLogger(ClusteredTopicStressTest.class);
   

   // Static --------------------------------------------------------
   
   private static final int NODE_COUNT = 10;
   
   private static final int NUM_MESSAGES = 100000;
   
   // Attributes ----------------------------------------------------
   
   private Set listeners = new HashSet();
   
   private volatile boolean failed;
   
   // Constructors --------------------------------------------------

   public ClusteredTopicStressTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   protected void setUp() throws Exception
   {
   	this.nodeCount = NODE_COUNT;
   	
      super.setUp();      
   }

   protected void tearDown() throws Exception
   {
      super.tearDown();
   }
   
   public void testTopic() throws Throwable
   {
   	Connection[] conns = new Connection[nodeCount];
   	
   	Connection connSend = null;
   	
   	try
   	{
   		for (int i = 0; i < nodeCount; i++)
   		{
   			conns[i] = cf.createConnection();
   		}
   		
   		this.checkConnectionsDifferentServers(conns);
   			
   		for (int i = 0; i < nodeCount; i++)
   		{
   			Session sess = conns[i].createSession(false, Session.AUTO_ACKNOWLEDGE);
   			
   			MessageConsumer cons = sess.createConsumer(topic[i]);
   			
   			MyListener listener = new MyListener();
   			
   			synchronized (listeners)
   			{
   				listeners.add(listener);
   			}
   			
   			cons.setMessageListener(listener);
   			
   			conns[i].start();
   			
   			log.info("Created " + i);
   		}
   		
   		connSend = cf.createConnection();
   		
   		Session sessSend = connSend.createSession(false, Session.AUTO_ACKNOWLEDGE);
   		
   		MessageProducer prod = sessSend.createProducer(topic[0]);
   		
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
   	}
   	finally
   	{
   		for (int i = 0; i < nodeCount; i++)
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
				
				if (count % 100 == 0)
				{
					log.info(this + " got message " + msg);
				}
				
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


