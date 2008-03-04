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
package org.jboss.messaging.core.integration.test;

import junit.framework.TestCase;

import org.jboss.messaging.core.client.ClientConnection;
import org.jboss.messaging.core.client.ClientConnectionFactory;
import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.MessageHandler;
import org.jboss.messaging.core.client.impl.ClientConnectionFactoryImpl;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.remoting.TransportType;
import org.jboss.messaging.core.remoting.impl.RemotingConfiguration;
import org.jboss.messaging.core.server.MessagingServer;
import org.jboss.messaging.core.server.impl.MessagingServerImpl;
import org.jboss.messaging.core.settings.impl.QueueSettings;

/**
 * 
 * A ProducerFlowControlTest
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class ProducerFlowControlTest extends TestCase
{
	private static final Logger log = Logger.getLogger(ProducerFlowControlTest.class);

	
//	public void testFlowControl() throws Exception
//	{		
//		ClientConnection conn = null;
//		
//		MessagingServer server = null;
//		
//		try
//		{
//   		RemotingConfiguration remotingConf = new RemotingConfiguration(TransportType.TCP, "localhost", 7654);
//   		
//   		remotingConf.setInvmDisabled(true);
//   		
//   		server = new MessagingServerImpl(remotingConf);
//   		
//   		QueueSettings settings = new QueueSettings();
//   		
//   		settings.setMaxSize(10);
//   		
//   		server.getQueueSettingsRepository().addMatch("queue1", settings);
//   		
//   		server.start();
//   		
//   		ClientConnectionFactory cf = new ClientConnectionFactoryImpl(0, remotingConf, server.getVersion());
//   
//   		conn = cf.createConnection(null, null);
//   		
//   		final ClientSession session1 = conn.createClientSession(false, true, true, 1, false);
//   		
//   		session1.createQueue("queue1", "queue1", null, false, false);
//   		
//   		final ClientSession session2 = conn.createClientSession(false, true, true, 1, false);
//   		
//   		final ClientSession session3 = conn.createClientSession(false, true, true, 1, false);
//   		 
//   		ClientProducer producer1 = session1.createProducer("queue1");
//   		
//   		ClientProducer producer2 = session2.createProducer("queue1");
//   		
//   		ClientProducer producer3 = session3.createProducer("queue1");
//   
//   		ClientConsumer consumer =
//   			session1.createConsumer("queue1", null, false, false, false);
//   		
//   		MessageHandler handler = new MessageHandler() {
//   			public void onMessage(Message msg)   			
//   			{
//   				try
//   				{
//   					log.info("Got message " + msg.getHeader("count"));
//   					
//   					Thread.sleep(1000);
//   					
//   					session1.acknowledge();
//   				}
//   				catch(Exception e)
//   				{
//   					e.printStackTrace();
//   				}
//   			}
//   		};
//   		
//   		consumer.setMessageHandler(handler);
//   		
//   		conn.start();
//   
//   		Thread thread1 = new ProducerThread(producer1, "producer1");
//   		
//   		Thread thread2 = new ProducerThread(producer2, "producer2");
//   		
//   	   Thread thread3 = new ProducerThread(producer3, "producer3");
//   		
//   	   thread1.start();
//   	   
//   	   thread2.start();
//   	   
//   	   thread3.start();
//   	   
//   	   thread1.join();
//   	   
//   	   thread2.join();
//   	   
//   	   thread3.join();
//   	   
//		}
//		finally
//		{
//			if (conn != null)
//			{
//				conn.close();
//			}
//			
//			if (server != null)
//			{
//				server.stop();
//			}			
//		}
//		
//		
//	}
//	
	
//	public void testFlowControlRate() throws Exception
//	{		
//		ClientConnection conn = null;
//		
//		MessagingServer server = null;
//		
//		try
//		{
//   		RemotingConfiguration remotingConf = new RemotingConfiguration(TransportType.TCP, "localhost", 7654);
//   		
//   		remotingConf.setInvmDisabled(true);
//   		
//   		server = new MessagingServerImpl(remotingConf);
//   		
//   		server.start();
//   		
//   		ClientConnectionFactory cf = new ClientConnectionFactoryImpl(0, remotingConf, server.getVersion());
//   
//   		conn = cf.createConnection(null, null);
//   		
//   		final ClientSession session1 = conn.createClientSession(false, true, true, 1, false);
//   		
//   		session1.createQueue("queue1", "queue1", null, false, false);
//   		
//   		final ClientSession session2 = conn.createClientSession(false, true, true, 1, false);
//   		
//   		final ClientSession session3 = conn.createClientSession(false, true, true, 1, false);
//   		 
//   		ClientProducer producer1 = session1.createRateLimitedProducer("queue1", 10);
//   		
//   		ClientProducer producer2 = session2.createRateLimitedProducer("queue1", 1);
//   		
//   		ClientProducer producer3 = session3.createRateLimitedProducer("queue1", 5);
//   
//   		ClientConsumer consumer =
//   			session1.createConsumer("queue1", null, false, false, false);
//   		
//   		MessageHandler handler = new MessageHandler() {
//   			public void onMessage(Message msg)   			
//   			{
//   				try
//   				{
//   					log.info("Got message " + msg.getHeader("count"));
//   					
//   					Thread.sleep(1000);
//   					
//   					session1.acknowledge();
//   				}
//   				catch(Exception e)
//   				{
//   					e.printStackTrace();
//   				}
//   			}
//   		};
//   		
//   		consumer.setMessageHandler(handler);
//   		
//   		conn.start();
//   
//   		Thread thread1 = new ProducerThread(producer1, "producer1");
//   		
//   		Thread thread2 = new ProducerThread(producer2, "producer2");
//   		
//   	   Thread thread3 = new ProducerThread(producer3, "producer3");
//   		
//   	   thread1.start();
//   	   
//   	   thread2.start();
//   	   
//   	   thread3.start();
//   	   
//   	   thread1.join();
//   	   
//   	   thread2.join();
//   	   
//   	   thread3.join();
//   	   
//		}
//		finally
//		{
//			if (conn != null)
//			{
//				conn.close();
//			}
//			
//			if (server != null)
//			{
//				server.stop();
//			}			
//		}
//		
//		
//	}
	
	
	public void testNull()
	{}
	
	class ProducerThread extends Thread
	{
		final ClientProducer producer;
		
		final String producerName;
		
		int count;
		
		ProducerThread(ClientProducer producer, String producerName)
		{
			this.producer = producer;
			
			this.producerName = producerName;
		}
		
		public void run()
		{
			try
			{
				while (true)
				{
					Message message = new MessageImpl(7, false, 0, System.currentTimeMillis(), (byte) 1);
	   			
	   			message.putHeader("count", count++);
	   			
	      		producer.send(message);
	      		
	      		log.info("Producer " + producerName + " sent message " + count);
				}
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	
}
