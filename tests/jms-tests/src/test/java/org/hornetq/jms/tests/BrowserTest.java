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

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.MessageHandler;
import org.hornetq.jms.client.HornetQConnectionFactory;
import org.hornetq.jms.tests.util.ProxyAssertSupport;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class BrowserTest extends JMSTestCase
{
   // Constants -----------------------------------------------------------------------------------

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
            ProxyAssertSupport.fail("should throw exception");
         }
         catch (InvalidDestinationException e)
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
            ProxyAssertSupport.fail("should throw exception");
         }
         catch (InvalidDestinationException e)
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
   
   public void testBrowse2() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = getConnectionFactory().createConnection();

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer producer = session.createProducer(HornetQServerTestCase.queue1);
         
         HornetQConnectionFactory cf = (HornetQConnectionFactory) getConnectionFactory();

         ClientSession coreSession = cf.getCoreFactory().createSession(true, true);

         coreSession.start();
         
         ClientConsumer browser = coreSession.createConsumer("jms.queue.Queue1", true);
       
         conn.start();

         Message m = session.createMessage();
         m.setIntProperty("cnt", 0);
         producer.send(m);
         
          
         assertNotNull(browser.receive(5000));
         
         Thread.sleep(5000);
         
         coreSession.close();
         
         
         System.out.println("Draining destination...");
         drainDestination(getConnectionFactory(), queue1);
         
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
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

         MessageProducer producer = session.createProducer(HornetQServerTestCase.queue1);

         QueueBrowser browser = session.createBrowser(HornetQServerTestCase.queue1);

         ProxyAssertSupport.assertEquals(browser.getQueue(), HornetQServerTestCase.queue1);

         ProxyAssertSupport.assertNull(browser.getMessageSelector());

         Enumeration<Message> en = (Enumeration<Message>)browser.getEnumeration();

         conn.start();

         Message m = session.createMessage();
         m.setIntProperty("cnt", 0);
         producer.send(m);
         Message m2 = en.nextElement();
         
         assertNotNull(m2);
         
         
         System.out.println("Draining destination...");
         drainDestination(getConnectionFactory(), queue1);
         
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

         MessageProducer producer = session.createProducer(HornetQServerTestCase.queue1);

         final int numMessages = 100;

         for (int i = 0; i < numMessages; i++)
         {
            Message m = session.createMessage();
            m.setIntProperty("test_counter", i + 1);
            producer.send(m);
         }
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }

         removeAllMessages(HornetQServerTestCase.queue1.getQueueName(), true);
      }
   }

   public void testGetEnumeration() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = getConnectionFactory().createConnection();

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer producer = session.createProducer(HornetQServerTestCase.queue1);

         // send a message to the queue

         Message m = session.createTextMessage("A");
         producer.send(m);

         // make sure we can browse it

         QueueBrowser browser = session.createBrowser(HornetQServerTestCase.queue1);

         Enumeration en = browser.getEnumeration();

         ProxyAssertSupport.assertTrue(en.hasMoreElements());

         TextMessage rm = (TextMessage)en.nextElement();

         ProxyAssertSupport.assertNotNull(rm);
         ProxyAssertSupport.assertEquals("A", rm.getText());

         ProxyAssertSupport.assertFalse(en.hasMoreElements());

         // create a *new* enumeration, that should reset it

         en = browser.getEnumeration();

         ProxyAssertSupport.assertTrue(en.hasMoreElements());

         rm = (TextMessage)en.nextElement();

         ProxyAssertSupport.assertNotNull(rm);
         ProxyAssertSupport.assertEquals("A", rm.getText());

         ProxyAssertSupport.assertFalse(en.hasMoreElements());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }

         removeAllMessages(HornetQServerTestCase.queue1.getQueueName(), true);
      }
   }

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------
}
