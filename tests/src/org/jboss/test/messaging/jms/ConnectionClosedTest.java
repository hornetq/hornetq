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
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TopicConnection;
import javax.jms.TopicConnectionFactory;
import javax.jms.TopicSession;
import javax.jms.TopicSubscriber;


/**
 * All tests related to closing a Connection.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class ConnectionClosedTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public ConnectionClosedTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   // Public --------------------------------------------------------

   public void testCloseOnce() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.close();
   }

   public void testCloseTwice() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.close();
      conn.close();
   }
   
   /** See TCK test: topicconntests.connNotStartedTopicTest */
   public void testCannotReceiveMessageOnStoppedConnection() throws Exception
   {
      TopicConnection conn1 = ((TopicConnectionFactory)cf).createTopicConnection();
      TopicConnection conn2 = ((TopicConnectionFactory)cf).createTopicConnection();
      
      TopicSession sess1 = conn1.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
      TopicSession sess2 = conn2.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
      
      TopicSubscriber sub1 = sess1.createSubscriber(topic1);
      TopicSubscriber sub2 = sess2.createSubscriber(topic1);
      
      conn1.start();
      
      Connection conn3 = cf.createConnection();
      
      Session sess3 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = sess3.createProducer(topic1);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      final int NUM_MESSAGES = 10;
      
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         TextMessage tm = sess3.createTextMessage("hello");
         prod.send(tm);
      }

      int count = 0;
      while (true)
      {
         TextMessage tm = (TextMessage)sub1.receive(10000);
         if (tm == null)
         {
            break;
         }
         assertEquals("hello", tm.getText());
         count++;
      }
      assertEquals(NUM_MESSAGES, count);

      Message m = sub2.receive(200);
      
      assertNull(m);
      
      conn2.start();
      
      count = 0;
      while (true)
      {
         TextMessage tm = (TextMessage)sub2.receive(10000);
         if (tm == null)
         {
            break;
         }
         assertEquals("hello", tm.getText());
         count++;
      }
      assertEquals(NUM_MESSAGES, count);

      log.debug("all messages received by sub2");
      
      conn1.close();
      
      conn2.close();
      
      conn3.close();
      
   }

  
   /**
    * A close terminates all pending message receives on the connection�s session�s  consumers. The
    * receives may return with a message or null depending on whether or not there was a message
    * available at the time of the close.
    */
   public void testCloseWhileReceiving() throws Exception
   {
      Connection conn = cf.createConnection();

      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      conn.start();

      final MessageConsumer consumer = session.createConsumer(topic1);

      class TestRunnable implements Runnable
      {
         public String failed;

         public void run()
         {
            try
            {
               long start = System.currentTimeMillis();
               Message m = consumer.receive(2100);
               if (System.currentTimeMillis() - start >= 2000)
               {
                  //It timed out
                  failed = "Timed out";
               }
               else
               {
                  if (m != null)
                  {
                     failed = "Message Not null";
                  }
               }
            }
            catch(Exception e)
            {
               log.error(e);
               failed = e.getMessage();
            }
         }
      }

      TestRunnable runnable = new TestRunnable();
      Thread t = new Thread(runnable);
      t.start();

      Thread.sleep(1000);

      conn.close();

      t.join();

      if (runnable.failed != null)
      {
         fail(runnable.failed);
      }

   }

   public void testGetMetadataOnClosedConnection() throws Exception
   {
      Connection connection = cf.createConnection();
      
      connection.close();
      
      try
      {
         connection.getMetaData();
         fail("should throw exception");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }
   }

   public void testCreateSessionOnClosedConnection() throws Exception
   {
      Connection conn = cf.createConnection();
      conn.close();

      try
      {
         conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         fail("Did not throw javax.jms.IllegalStateException");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }
   }

   /**
    * Test that close() hierarchically closes all child objects
    */
   public void testCloseHierarchy() throws Exception
   {
      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer consumer = sess.createConsumer(topic1);
      MessageProducer producer = sess.createProducer(topic1);
      sess.createBrowser(queue1);
      Message m = sess.createMessage();

      conn.close();

      // Session

      /* If the session is closed then any method invocation apart from close()
       * will throw an IllegalStateException
       */
      try
      {
         sess.createMessage();
         fail("Session is not closed");
      }
      catch (javax.jms.IllegalStateException e)
      {
      }

      try
      {
         sess.getAcknowledgeMode();
         fail("should throw IllegalStateException");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }

      try
      {
         sess.getTransacted();
         fail("should throw IllegalStateException");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }

      try
      {
         sess.getMessageListener();
         fail("should throw IllegalStateException");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }

      try
      {
         sess.createProducer(queue1);
         fail("should throw IllegalStateException");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }

      try
      {
         sess.createConsumer(queue1);
         fail("should throw IllegalStateException");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }


      // Producer

      /* If the producer is closed then any method invocation apart from close()
       * will throw an IllegalStateException
       */
      try
      {
         producer.send(m);
         fail("Producer is not closed");
      }
      catch (javax.jms.IllegalStateException e)
      {
      }

      try
      {
         producer.getDisableMessageID();
         fail("should throw IllegalStateException");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }

      try
      {
         producer.getPriority();
         fail("should throw IllegalStateException");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }

      try
      {
         producer.getDestination();
         fail("should throw IllegalStateException");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }

      try
      {
         producer.getTimeToLive();
         fail("should throw IllegalStateException");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }

      // ClientConsumer

      try
      {
         consumer.getMessageSelector();
         fail("should throw exception");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }

      try
      {
         consumer.getMessageListener();
         fail("should throw exception");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }

      try
      {
         consumer.receive();
         fail("should throw exception");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }

      // Browser

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
