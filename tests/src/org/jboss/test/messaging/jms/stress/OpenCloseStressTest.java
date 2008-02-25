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
package org.jboss.test.messaging.jms.stress;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.jboss.messaging.jms.client.JBossConnectionFactory;
import org.jboss.test.messaging.JBMServerTestCase;
import org.jboss.util.id.GUID;

/**
 * 
 * A OpenCloseStressTest.
 * 
 * This stress test starts several publisher connections and several subscriber connections, then sends and consumes
 * messages while concurrently closing the sessions.
 * 
 * This test will help catch race conditions that occurred with rapid open/closing of sessions when messages are being 
 * sent/received
 * 
 * E.g. http://jira.jboss.com/jira/browse/JBMESSAGING-982
 * 
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision: 2349 $</tt>
 *
 * $Id: StressTest.java 2349 2007-02-19 14:15:53Z timfox $
 */
public class OpenCloseStressTest extends JBMServerTestCase
{
   public OpenCloseStressTest(String name)
   {
      super(name);
   }
   
   InitialContext ic;
   JBossConnectionFactory cf;
   Topic topic;

   public void setUp() throws Exception
   {
      super.setUp();

      //ServerManagement.start("all");

      ic = getInitialContext();
      cf = (JBossConnectionFactory)ic.lookup("/ConnectionFactory");

      destroyTopic("TestTopic");
      createTopic("TestTopic");

      topic = (Topic) ic.lookup("topic/TestTopic");

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      destroyQueue("TestQueue");

      super.tearDown();

      log.debug("tear down done");
   }
   
   public void testOpenClose() throws Exception
   {
   	Connection conn1 = null;
   	Connection conn2 = null;
   	Connection conn3 = null;
   	
   	Connection conn4 = null;
   	Connection conn5 = null;
   	Connection conn6 = null;
   	Connection conn7 = null;
   	Connection conn8 = null;
   	
   	try
   	{   	
	      Publisher[] publishers = new Publisher[3];
	      
	      final int MSGS_PER_PUBLISHER = 10000;
	      
	      conn1 = cf.createConnection();
	      Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      MessageProducer prod1 = sess1.createProducer(topic);
	      prod1.setDeliveryMode(DeliveryMode.PERSISTENT);
	      publishers[0] = new Publisher(sess1, prod1, MSGS_PER_PUBLISHER, 2);
	      
	      conn2 = cf.createConnection();
	      Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      MessageProducer prod2 = sess2.createProducer(topic);
	      prod2.setDeliveryMode(DeliveryMode.PERSISTENT);
	      publishers[1] = new Publisher(sess2, prod2, MSGS_PER_PUBLISHER, 5);
	      
	      conn3 = cf.createConnection();
	      Session sess3 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      MessageProducer prod3 = sess3.createProducer(topic);
	      prod3.setDeliveryMode(DeliveryMode.PERSISTENT);
	      publishers[2] = new Publisher(sess3, prod3, MSGS_PER_PUBLISHER, 1);
	      
	      Subscriber[] subscribers = new Subscriber[5];
   			      
	      conn4 = cf.createConnection();
	      subscribers[0] = new Subscriber(conn4, 3 * MSGS_PER_PUBLISHER, 500, 1000 * 60 * 15, topic, false);
	      
	      conn5 = cf.createConnection();
	      subscribers[1] = new Subscriber(conn5, 3 * MSGS_PER_PUBLISHER, 2000, 1000 * 60 * 15, topic, false);
	      
	      conn6 = cf.createConnection();
	      subscribers[2] = new Subscriber(conn6, 3 * MSGS_PER_PUBLISHER, 700, 1000 * 60 * 15, topic, false);
	      
	      conn7 = cf.createConnection();
	      subscribers[3] = new Subscriber(conn7, 3 * MSGS_PER_PUBLISHER, 1500, 1000 * 60 * 15, topic, true);
	      
	      conn8 = cf.createConnection();
	      subscribers[4] = new Subscriber(conn8, 3 * MSGS_PER_PUBLISHER, 1200, 1000 * 60 * 15, topic, true);
	      
	      Thread[] threads = new Thread[8];
	      
	      //subscribers
	      threads[0] = new Thread(subscribers[0]);
	      
	      threads[1] = new Thread(subscribers[1]);
	      
	      threads[2] = new Thread(subscribers[2]);
	      
	      threads[3] = new Thread(subscribers[3]);
	      
	      threads[4] = new Thread(subscribers[4]);
	      
	      //publishers
	      
	      threads[5] = new Thread(publishers[0]);
	      
	      threads[6] = new Thread(publishers[1]);
	      
	      threads[7] = new Thread(publishers[2]);
	      
	      for (int i = 0; i < subscribers.length; i++)
	      {
	      	threads[i].start();
	      }
	      
	      // Pause before creating producers otherwise subscribers to make sure they're all created
	      
	      Thread.sleep(5000);
	      
	      for (int i = subscribers.length; i < threads.length; i++)
	      {
	      	threads[i].start();
	      }
	      
	      for (int i = 0; i < threads.length; i++)
	      {
	      	threads[i].join();
	      }
	      
	      for (int i = 0; i < subscribers.length; i++)
	      {	      	
	      	if (subscribers[i].isDurable())
	      	{
	      		assertEquals(3 * MSGS_PER_PUBLISHER, subscribers[i].getMessagesReceived());
	      	}
	      	else
	      	{
	      		//Note that for a non durable subscriber the number of messages received in total
		      	//will be somewhat less than the total number received since when recycling the session
		      	//there is a period of time after closing the previous session and starting the next one
		      	//when messages are being sent and won't be received (since there is no consumer)
	      	}
	      			      	
	      	assertFalse(subscribers[i].isFailed());
	      }
	      
	      for (int i = 0; i < publishers.length; i++)
	      {
	      	assertFalse(publishers[i].isFailed());
	      }	   	
   	}
   	finally
   	{
   		if (conn1 != null)
   		{
   			conn1.close();
   		}
   		if (conn2 != null)
   		{
   			conn2.close();
   		}
   		if (conn3 != null)
   		{
   			conn3.close();
   		}
   		if (conn4 != null)
   		{
   			conn4.close();
   		}
   		if (conn5 != null)
   		{
   			conn5.close();
   		}
   		if (conn6 != null)
   		{
   			conn6.close();
   		}
   		if (conn7 != null)
   		{
   			conn7.close();
   		}
   		if (conn8 != null)
   		{
   			conn8.close();
   		}
   	}
      
   }
   
   class Publisher implements Runnable
   {
   	private Session sess;
   	
   	private int numMessages;
   	
   	private int delay;
   	
   	private MessageProducer prod;
   	
   	private boolean failed;
   	
   	boolean isFailed()
   	{
   		return failed;
   	}
   	   	   	
   	Publisher(Session sess, MessageProducer prod, int numMessages, int delay)
   	{
   		this.sess = sess;
   		
   		this.prod = prod;
   		
   		this.numMessages = numMessages;
   		
   		this.delay = delay;
   	}

		public void run()
		{
			try
			{
				for (int i = 0; i < numMessages; i++)
				{
					TextMessage tm = sess.createTextMessage("message" + i);
					
					prod.send(tm);
														
					try
					{
						Thread.sleep(delay);
					}
					catch (Exception ignore)
					{						
					}
				}
			}
			catch (JMSException e)
			{
				log.error("Failed to send message", e);
				failed = true;
			}
		}
   	
   }
   
   
   class Subscriber implements Runnable
   {
   	private Session sess;
   	
   	private MessageConsumer cons;
   	
   	private int msgsReceived;
   	
   	private int numMessages;
   	
   	private int delay;
   	
   	private Connection conn;
   	   	
   	private boolean failed;
   	
   	private long timeout;
   	
   	private Destination dest;
   	
   	private boolean durable;
   	
   	private String subname;
   	
   	boolean isFailed()
   	{
   		return failed;
   	}
   	
   	boolean isDurable()
   	{
   		return durable;
   	}
   	
   	   	
   	synchronized void msgReceived()
   	{   		
   		msgsReceived++;   		
   	}
   	
   	synchronized int getMessagesReceived()
   	{
   		return msgsReceived;
   	}
   	
   	class Listener implements MessageListener
   	{

			public void onMessage(Message msg)
			{
				msgReceived();
			}
   		
   	}
   	
   	
   	   	   	
   	Subscriber(Connection conn, int numMessages, int delay, long timeout, Destination dest, boolean durable) throws Exception
   	{
   		this.conn = conn;
   		
   		this.numMessages = numMessages;
   		
   		this.delay = delay;
   		
   		this.timeout = timeout;
   		
   		this.dest = dest;
   		
   		this.durable = durable;
   		
   		if (durable)
   		{
   			conn.setClientID(new GUID().toString());
   			
   			this.subname = new GUID().toString();
   		}
   	}

		public void run()
		{
			try
			{
				long start = System.currentTimeMillis();
				
				while (((System.currentTimeMillis() - start) < timeout) && msgsReceived < numMessages)
				{
					//recycle the session
					
					recycleSession();
					
					Thread.sleep(delay);
				}
				
				//Delete the durable sub
				
				if (durable)
				{
					recycleSession();
					
					cons.close();
					
					sess.unsubscribe(subname);
				}
			}
			catch (Exception e)
			{
				log.error("Failed in subscriber", e);
				failed = true;
			}
						
		}
		
		void recycleSession() throws Exception
		{
			conn.stop();
			
			if (sess != null)
			{
				sess.close();
			}
			
			sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
			
			if (durable)
			{
				cons = sess.createDurableSubscriber((Topic)dest, subname);
			}
			else
			{
				cons = sess.createConsumer(dest);
			}
						
			cons.setMessageListener(new Listener());
			
			conn.start();
		}
   	
   }

}

