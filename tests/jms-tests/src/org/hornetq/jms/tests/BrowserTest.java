/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.jms.tests;

import java.util.Enumeration;

import javax.jms.Connection;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class BrowserTest extends JMSTestCase
{
	//	 Constants -----------------------------------------------------------------------------------

	// Static ---------------------------------------------------------------------------------------

	// Attributes -----------------------------------------------------------------------------------

	// Constructors ---------------------------------------------------------------------------------

	// Public ---------------------------------------------------------------------------------------

   public void testCreateBrowserOnNullDestination() throws Exception
   {
   	Connection conn = null;

   	try
   	{
	   	conn = getConnectionFactory().createConnection();

	   	Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

	      try
	      {
	         session.createBrowser(null);
	         fail("should throw exception");
	      }
	      catch(InvalidDestinationException e)
	      {
	         // OK
	      }
   	}
   	finally
   	{
   		if (conn != null)
   		{
   			conn.close();
   		}
   	}
   }

   public void testCreateBrowserOnNonExistentQueue() throws Exception
   {
      Connection pconn = getConnectionFactory().createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         try
         {
            ps.createBrowser(new Queue()
            {
               public String getQueueName() throws JMSException
               {
                  return "NoSuchQueue";
               }
            });
            fail("should throw exception");
         }
         catch(InvalidDestinationException e)
         {
            // OK
         }
      }
      finally
      {
         if (pconn != null)
         {
         	pconn.close();
         }
      }
   }

	public void testBrowse() throws Exception
	{
		Connection conn = null;

   	try
   	{
	   	conn = getConnectionFactory().createConnection();

	   	Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

	   	MessageProducer producer = session.createProducer(queue1);

			final int numMessages = 10;

			for (int i = 0; i < numMessages; i++)
			{
				Message m = session.createMessage();
	         m.setIntProperty("cnt", i);
				producer.send(m);
			}

			QueueBrowser browser = session.createBrowser(queue1);

			assertEquals(browser.getQueue(), queue1);

			assertNull(browser.getMessageSelector());

			Enumeration en = browser.getEnumeration();

			int count = 0;
			while (en.hasMoreElements())
			{
				en.nextElement();
				count++;
			}
			
			assertEquals(numMessages, count);

			MessageConsumer mc = session.createConsumer(queue1);

			conn.start();

			for (int i = 0; i < numMessages; i++)
			{
				Message m = mc.receive();
	         assertNotNull(m);
			}

			browser = session.createBrowser(queue1);
			en = browser.getEnumeration();

			log.info("browsing");
			
			count = 0;
			while (en.hasMoreElements())
			{
				Message mess = (Message)en.nextElement();
				log.info("message:" + mess);
				count++;
			}

			log.trace("Received " + count + " messages");

	      assertEquals(0, count);
   	}
   	finally
   	{
   		if (conn != null)
   		{
   			conn.close();
   		}
   	}
	}

	public void testBrowseWithSelector() throws Exception
	{
		Connection conn = null;

   	try
   	{
	   	conn = getConnectionFactory().createConnection();

	   	Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

	   	MessageProducer producer = session.createProducer(queue1);

			final int numMessages = 100;

			for (int i = 0; i < numMessages; i++)
			{
				Message m = session.createMessage();
				m.setIntProperty("test_counter", i+1);
				producer.send(m);
			}

			QueueBrowser browser = session.createBrowser(queue1, "test_counter > 30");

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
		finally
		{
			if (conn != null)
			{
				conn.close();
			}

			removeAllMessages(queue1.getQueueName(), true);
		}
	}

   public void testGetEnumeration() throws Exception
   {
   	Connection conn = null;

   	try
   	{
	   	conn = getConnectionFactory().createConnection();

	   	Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

	   	MessageProducer producer = session.createProducer(queue1);

	      // send a message to the queue

	      Message m = session.createTextMessage("A");
	      producer.send(m);

	      // make sure we can browse it

	      QueueBrowser browser = session.createBrowser(queue1);

	      Enumeration en = browser.getEnumeration();

	      assertTrue(en.hasMoreElements());

	      TextMessage rm = (TextMessage)en.nextElement();

	      assertNotNull(rm);
	      assertEquals("A", rm.getText());

	      assertFalse(en.hasMoreElements());

	      // create a *new* enumeration, that should reset it

	      en = browser.getEnumeration();

	      assertTrue(en.hasMoreElements());

	      rm = (TextMessage)en.nextElement();

	      assertNotNull(rm);
	      assertEquals("A", rm.getText());

	      assertFalse(en.hasMoreElements());
   	}
		finally
		{
			if (conn != null)
			{
				conn.close();
			}

			removeAllMessages(queue1.getQueueName(), true);
		}
   }

   // Package protected ----------------------------------------------------------------------------
	
	// Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
	
	// Inner classes --------------------------------------------------------------------------------
}

