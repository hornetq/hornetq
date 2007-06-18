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
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.naming.InitialContext;
import javax.naming.NamingException;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.jms.message.MessageProxy;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class TemporaryDestinationTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;

   protected ConnectionFactory cf;

   protected Connection connection;

   protected Session producerSession, consumerSession;

   // Constructors --------------------------------------------------

   public TemporaryDestinationTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.start("all");
      
      
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
      ServerManagement.undeployTopic("Topic");
      ServerManagement.deployTopic("Topic");
      //topic = (Destination)initialContext.lookup("/topic/Topic");

      connection = cf.createConnection();
      producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

   }

   public void tearDown() throws Exception
   {
      connection.close();

      ServerManagement.undeployTopic("Topic");
      
      super.tearDown();
   }


   // Public --------------------------------------------------------

   public void testTemp() throws Exception
   {
      TemporaryTopic tempTopic = producerSession.createTemporaryTopic();

      MessageProducer producer = producerSession.createProducer(tempTopic);

      MessageConsumer consumer = consumerSession.createConsumer(tempTopic);

      connection.start();

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
      catch (JMSException e)
      {
         //Can't delete temp dest if there are open consumers
      }

      consumer.close();
      tempTopic.delete();


   }


   public void testTemporaryQueueBasic() throws Exception
   {

      TemporaryQueue tempQueue = producerSession.createTemporaryQueue();

      MessageProducer producer = producerSession.createProducer(tempQueue);

      MessageConsumer consumer = consumerSession.createConsumer(tempQueue);
      
      connection.start();

      final String messageText = "This is a message";

      Message m = producerSession.createTextMessage(messageText);

      producer.send(m);

      TextMessage m2 = (TextMessage)consumer.receive(2000);

      assertNotNull(m2);

      assertEquals(messageText, m2.getText());
   }

   /**
    * http://jira.jboss.com/jira/browse/JBMESSAGING-93
    */
   public void testTemporaryQueueOnClosedSession() throws Exception
   {
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
   
   public void testTemporaryQueueDeleteWithConsumer() throws Exception
   {
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
   
   public void testTemporaryTopicDeleteWithConsumer() throws Exception
   {
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

   public void testTemporaryQueueDeleted() throws Exception
   {
      //Make sure temporary queue cannot be used after it has been deleted

      TemporaryQueue tempQueue = producerSession.createTemporaryQueue();

      MessageProducer producer = producerSession.createProducer(tempQueue);

      MessageConsumer consumer = consumerSession.createConsumer(tempQueue);

      connection.start();

      final String messageText = "This is a message";

      Message m = producerSession.createTextMessage(messageText);

      producer.send(m);

      TextMessage m2 = (TextMessage)consumer.receive(2000);

      assertNotNull(m2);

      assertEquals(messageText, m2.getText());

      consumer.close();

      tempQueue.delete();

      try
      {
         producer.send(m);
         fail();
      }
      catch (JMSException e) {}
   }



   public void testTemporaryTopicBasic() throws Exception
   {
      TemporaryTopic tempTopic = producerSession.createTemporaryTopic();

      final MessageProducer producer = producerSession.createProducer(tempTopic);

      MessageConsumer consumer = consumerSession.createConsumer(tempTopic);

      connection.start();

      final String messageText = "This is a message";

      final Message m = producerSession.createTextMessage(messageText);
      log.trace("Message reliable:" + ((MessageProxy)m).getMessage().isReliable());

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


   /**
    * http://jira.jboss.com/jira/browse/JBMESSAGING-93
    */
   public void testTemporaryTopicOnClosedSession() throws Exception
   {
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

   public void testTemporaryTopicShouldNotBeInJNDI() throws Exception
   {
      TemporaryTopic tempTopic = producerSession.createTemporaryTopic();
      String topicName = tempTopic.getTopicName();
      
      try
      {
         initialContext.lookup("/topic/" + topicName);
         fail("The temporary queue should not be bound to JNDI");
      }
      catch (NamingException e)
      {
         // Expected
      }
   }

   public void testTemporaryQueueShouldNotBeInJNDI() throws Exception
   {
      TemporaryQueue tempQueue = producerSession.createTemporaryQueue();
      String queueName = tempQueue.getQueueName();
      
      try
      {
         initialContext.lookup("/queue/" + queueName);
         fail("The temporary queue should not be bound to JNDI");
      }
      catch (NamingException e)
      {
         // Expected
      }
   }



   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------



}

