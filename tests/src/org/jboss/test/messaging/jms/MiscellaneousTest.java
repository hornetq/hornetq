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

import java.util.Enumeration;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * Various use cases, added here while trying things or fixing forum issues.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MiscellaneousTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public MiscellaneousTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testBrowser() throws Exception
   {
      Connection conn = null;
      
      try
      {	      
	      conn = cf.createConnection();
	      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      MessageProducer prod = session.createProducer(queue1);
	
	      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
	
	      TextMessage m = session.createTextMessage("message one");
	
	      prod.send(m);
	
	      QueueBrowser browser = session.createBrowser(queue1);
	
	      Enumeration e = browser.getEnumeration();
	
	      TextMessage bm = (TextMessage)e.nextElement();
	
	      assertEquals("message one", bm.getText());
      }
      finally
      {
      	if (conn != null)
      	{
      		conn.close();
      	}
      	
      	removeAllMessages(queue1.getQueueName(), true, 0);
      }
   }

   /**
    * Test case for http://jira.jboss.org/jira/browse/JBMESSAGING-542
    */
   public void testClosingConsumerFromMessageListener() throws Exception
   {
      Connection c = null;
      
      try
      {	      
	      c = cf.createConnection();
	      Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      MessageProducer prod = s.createProducer(queue1);
	      Message m = s.createMessage();
	      prod.send(m);
      }
      finally
      {
      	if (c != null)
      	{
      		c.close();
      	}
      }
	
      final Result result = new Result();
      Connection conn = cf.createConnection();
      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer cons = s.createConsumer(queue1);
      cons.setMessageListener(new MessageListener()
      {
         public void onMessage(Message m)
         {
            // close the connection on the same thread that processed the message
            try
            {
               cons.close();
               result.setSuccess();
            }
            catch(Exception e)
            {
               result.setFailure(e);
            }
         }
      });

      conn.start();

      result.waitForResult();

      assertTrue(result.isSuccess());
      assertNull(result.getFailure());

      // make sure the acknowledgment made it back to the queue

      Thread.sleep(1000);
      assertRemainingMessages(0);
      
      conn.close();

   }

   /**
    * Test case for http://jira.jboss.org/jira/browse/JBMESSAGING-542
    */
   public void testClosingSessionFromMessageListener() throws Exception
   {
      Connection c = null;
      
      try
      {	      
	      c = cf.createConnection();
	      Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      MessageProducer prod = s.createProducer(queue1);
	      Message m = s.createMessage();
	      prod.send(m);
      }
      finally
      {
      	if (c != null)
      	{
      		c.close();
      	}
      }
	
      final Result result = new Result();
      Connection conn = cf.createConnection();
      final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer cons = session.createConsumer(queue1);
      cons.setMessageListener(new MessageListener()
      {
         public void onMessage(Message m)
         {
            // close the connection on the same thread that processed the message
            try
            {
               session.close();
               result.setSuccess();
            }
            catch(Exception e)
            {
               result.setFailure(e);
            }
         }
      });

      conn.start();

      result.waitForResult();

      assertTrue(result.isSuccess());
      assertNull(result.getFailure());

      // make sure the acknowledgment made it back to the queue

      Thread.sleep(1000);
      assertRemainingMessages(0);
      conn.close();
   }

   /**
    * Test case for http://jira.jboss.org/jira/browse/JBMESSAGING-542
    */
   public void testClosingConnectionFromMessageListener() throws Exception
   {
      Connection c = null;
      
      try
      {      
	      c = cf.createConnection();
	      Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
	      MessageProducer prod = s.createProducer(queue1);
	      Message m = s.createMessage();
	      prod.send(m);
      }
      finally
      {
      	if (c != null)
      	{
      		c.close();
      	}
      }
	
      final Result result = new Result();
      final Connection conn = cf.createConnection();
      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer cons = s.createConsumer(queue1);
      cons.setMessageListener(new MessageListener()
      {
         public void onMessage(Message m)
         {
            // close the connection on the same thread that processed the message
            try
            {
               conn.close();
               result.setSuccess();
            }
            catch(Exception e)
            {
               e.printStackTrace();
               result.setFailure(e);
            }
         }
      });

      conn.start();

      result.waitForResult();

      assertTrue(result.isSuccess());
      assertNull(result.getFailure());

      // make sure the acknowledgment made it back to the queue

      Thread.sleep(1000);
      assertRemainingMessages(0);
      
      conn.close();
      
   }
   
   // Test case for http://jira.jboss.com/jira/browse/JBMESSAGING-788
   public void testGetDeliveriesForSession() throws Exception
   {
      Connection conn = null;
      
      try
      {
         conn = cf.createConnection();
         
         Session session1 = conn.createSession(true, Session.SESSION_TRANSACTED);
         
         Session session2 = conn.createSession(true, Session.SESSION_TRANSACTED);
         
         MessageProducer prod = session2.createProducer(queue1);
         
         Message msg = session2.createMessage();
         
         prod.send(msg);
         
         session1.close();
         
         session2.commit();
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
         
         removeAllMessages(queue1.getQueueName(), true, 0);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private class Result
   {
      private boolean success;
      private Exception e;
      
      private boolean resultSet;

      public Result()
      {
         success = false;
         e = null;
      }

      public synchronized void setSuccess()
      {
         success = true;
         
         this.resultSet = true;
         
         this.notify();
      }

      public synchronized boolean isSuccess()
      {
         return success;
      }

      public synchronized void setFailure(Exception e)
      {
         this.e = e;
         
         this.resultSet = true;
         
         this.notify();
      }

      public synchronized Exception getFailure()
      {
         return e;
      }
      
      public synchronized void waitForResult() throws Exception
      {
      	while (!resultSet)
      	{
      		this.wait();
      	}
      }
   }

}
