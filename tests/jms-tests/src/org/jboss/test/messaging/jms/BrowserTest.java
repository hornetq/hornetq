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

	public BrowserTest(String name)
	{
		super(name);
	}

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

			count = 0;
			while (en.hasMoreElements())
			{
				Message mess = (Message)en.nextElement();
				log.trace("message:" + mess);
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

			log.info("****** sending messages");
			
			for (int i = 0; i < numMessages; i++)
			{
				Message m = session.createMessage();
				m.setIntProperty("test_counter", i+1);
				producer.send(m);
			}

			log.info(" ****** Sent messages");

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

			removeAllMessages(queue1.getQueueName(), true, 0);
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

			removeAllMessages(queue1.getQueueName(), true, 0);
		}
   }

   // Package protected ----------------------------------------------------------------------------
	
	// Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------
	
	// Inner classes --------------------------------------------------------------------------------
}

