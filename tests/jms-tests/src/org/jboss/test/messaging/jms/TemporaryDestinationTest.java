/*
  * JBoss, Home of Professional Open Source
  * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.naming.NamingException;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class TemporaryDestinationTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testTemp() throws Exception
   {
   	Connection conn = null;
   	
   	try
   	{
   		conn = cf.createConnection();
   		
   		Session producerSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
   		
   		Session consumerSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
   		
	      TemporaryTopic tempTopic = producerSession.createTemporaryTopic();

	      MessageProducer producer = producerSession.createProducer(tempTopic);
	
	      MessageConsumer consumer = consumerSession.createConsumer(tempTopic);
	
	      conn.start();
	
	      final String messageText = "This is a message";
	
	      Message m = producerSession.createTextMessage(messageText);
	
	      producer.send(m);
	
	      TextMessage m2 = (TextMessage)consumer.receive(2000);
	
	      assertNotNull(m2);
	
	      assertEquals(messageText, m2.getText());
	
	      try
	      {
	         tempTopic.delete();
	         fail();
	      }
	      catch (javax.jms.IllegalStateException e)
	      {
	         //Can't delete temp dest if there are open consumers
	      }
	
	      consumer.close();
	      
	      tempTopic.delete();      
   	}
   	finally
   	{
   		if (conn != null)
   		{
   			conn.close();
   		}
   	}
   }


   public void testTemporaryQueueBasic() throws Exception
   {
   	Connection conn = null;
   	
   	try
   	{
   		conn = cf.createConnection();
   		
   		Session producerSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
   		
   		Session consumerSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
   		
	      TemporaryQueue tempQueue = producerSession.createTemporaryQueue();
	
	      MessageProducer producer = producerSession.createProducer(tempQueue);
	
	      MessageConsumer consumer = consumerSession.createConsumer(tempQueue);
	      
	      conn.start();
	
	      final String messageText = "This is a message";
	
	      Message m = producerSession.createTextMessage(messageText);
	
	      producer.send(m);
	
	      TextMessage m2 = (TextMessage)consumer.receive(2000);
	
	      assertNotNull(m2);
	
	      assertEquals(messageText, m2.getText());
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
    * http://jira.jboss.com/jira/browse/JBMESSAGING-93
    */
   public void testTemporaryQueueOnClosedSession() throws Exception
   {
   	Connection producerConnection = null;
   	
   	try
   	{
   		producerConnection = cf.createConnection();
   		
   		Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
   		   	   	
	      producerSession.close();
	
	      try
	      {
	         producerSession.createTemporaryQueue();
	         fail("should throw exception");
	      }
	      catch(javax.jms.IllegalStateException e)
	      {
	         // OK
	      }
   	}
   	finally
   	{
   		if (producerConnection != null)
   		{
   			producerConnection.close();
   		}
   	}
   }
   
   public void testTemporaryQueueDeleteWithConsumer() throws Exception
   {
   	Connection conn = null;
   	
   	try
   	{
   		conn = cf.createConnection();
   		
   		Session producerSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
   		
   		Session consumerSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
   		
	   	TemporaryQueue tempQueue = producerSession.createTemporaryQueue();
	   	
	   	MessageConsumer consumer = consumerSession.createConsumer(tempQueue);
	   	
	   	try
	   	{
	   		tempQueue.delete();
	   		
	   		fail("Should throw JMSException");
	   	}
	   	catch (JMSException e)
	   	{
	   		//Should fail - you can't delete a temp queue if it has active consumers
	   	}
	   	
	   	consumer.close();   	
   	}
   	finally
   	{
   		if (conn != null)
   		{
   			conn.close();
   		}
   	}
   }
   
   public void testTemporaryTopicDeleteWithConsumer() throws Exception
   {
   	Connection conn = null;
   	
   	try
   	{
   		conn = cf.createConnection();
   		
   		Session producerSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
   		
   		Session consumerSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
   		
	   	TemporaryTopic tempTopic = producerSession.createTemporaryTopic();
	   	
	   	MessageConsumer consumer = consumerSession.createConsumer(tempTopic);
	   	
	   	try
	   	{
	   		tempTopic.delete();
	   		
	   		fail("Should throw JMSException");
	   	}
	   	catch (JMSException e)
	   	{
	   		//Should fail - you can't delete a temp topic if it has active consumers
	   	}
	   	
	   	consumer.close();   	
	   }
		finally
		{
			if (conn != null)
			{
				conn.close();
			}
		}
   }

   public void testTemporaryQueueDeleted() throws Exception
   {
   	Connection conn = null;
   	
   	try
   	{
   		conn = cf.createConnection();
   		
   		Session producerSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
   		
   		Session consumerSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
   		
	      //Make sure temporary queue cannot be used after it has been deleted
	
	      TemporaryQueue tempQueue = producerSession.createTemporaryQueue();
	
	      MessageProducer producer = producerSession.createProducer(tempQueue);
	
	      MessageConsumer consumer = consumerSession.createConsumer(tempQueue);
	
	      conn.start();
	
	      final String messageText = "This is a message";
	
	      Message m = producerSession.createTextMessage(messageText);
	
	      producer.send(m);
	
	      TextMessage m2 = (TextMessage)consumer.receive(2000);
	
	      assertNotNull(m2);
	
	      assertEquals(messageText, m2.getText());
	
	      consumer.close();
	
	      tempQueue.delete();
	      conn.close();
         conn = cf.createConnection("nobody", "nobody");
         try
	      {
	         producer.send(m);
	         fail();
	      }
	      catch (JMSException e) {}
	   }
		finally
		{
			if (conn != null)
			{
				conn.close();
			}
		}
   }



   public void testTemporaryTopicBasic() throws Exception
   {
   	Connection conn = null;
   	
   	try
   	{
   		conn = cf.createConnection();
   		
   		Session producerSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
   		
   		Session consumerSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
   		   	
	      TemporaryTopic tempTopic = producerSession.createTemporaryTopic();
	
	      final MessageProducer producer = producerSession.createProducer(tempTopic);
	
	      MessageConsumer consumer = consumerSession.createConsumer(tempTopic);
	
	      conn.start();
	
	      final String messageText = "This is a message";
	
	      final Message m = producerSession.createTextMessage(messageText);

	      Thread t = new Thread(new Runnable()
	      {
	         public void run()
	         {
	            try
	            {
	               // this is needed to make sure the main thread has enough time to block
	               Thread.sleep(500);
	               producer.send(m);
	            }
	            catch(Exception e)
	            {
	               log.error(e);
	            }
	         }
	      }, "Producer");
	      t.start();
	
	      TextMessage m2 = (TextMessage)consumer.receive(3000);
	
	      assertNotNull(m2);
	
	      assertEquals(messageText, m2.getText());
	
	      t.join();
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
    * http://jira.jboss.com/jira/browse/JBMESSAGING-93
    */
   public void testTemporaryTopicOnClosedSession() throws Exception
   {
   	Connection producerConnection = null;
   	
   	try
   	{
   		producerConnection = cf.createConnection();
   		
   		Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
   		
	      producerSession.close();
	
	      try
	      {
	         producerSession.createTemporaryTopic();
	         fail("should throw exception");
	      }
	      catch(javax.jms.IllegalStateException e)
	      {
	         // OK
	      }
   	}
   	finally
   	{
   		if (producerConnection != null)
   		{
   			producerConnection.close();
   		}
   	}
   }

   public void testTemporaryTopicShouldNotBeInJNDI() throws Exception
   {
   	Connection producerConnection = null;
   	
   	try
   	{
   		producerConnection = cf.createConnection();
   		
   		Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
   	
	      TemporaryTopic tempTopic = producerSession.createTemporaryTopic();
	      String topicName = tempTopic.getTopicName();
	      
	      try
	      {
	         ic.lookup("/topic/" + topicName);
	         fail("The temporary queue should not be bound to JNDI");
	      }
	      catch (NamingException e)
	      {
	         // Expected
	      }
   	}
   	finally
   	{
   		if (producerConnection != null)
   		{
   			producerConnection.close();
   		}
   	}
   }

   public void testTemporaryQueueShouldNotBeInJNDI() throws Exception
   {
   	Connection producerConnection = null;
   	
   	try
   	{
   		producerConnection = cf.createConnection();
   		
   		Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
   	
	      TemporaryQueue tempQueue = producerSession.createTemporaryQueue();
	      String queueName = tempQueue.getQueueName();
	      
	      try
	      {
	         ic.lookup("/queue/" + queueName);
	         fail("The temporary queue should not be bound to JNDI");
	      }
	      catch (NamingException e)
	      {
	         // Expected
	      }
   	}
   	finally
   	{
   		if (producerConnection != null)
   		{
   			producerConnection.close();
   		}
   	}
   }


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}

