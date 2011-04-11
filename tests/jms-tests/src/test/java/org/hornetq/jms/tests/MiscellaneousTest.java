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
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.jms.tests.util.ProxyAssertSupport;

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

   @Override
   protected void tearDown() throws Exception
   {
      removeAllMessages(HornetQServerTestCase.queue1.getQueueName(), true);

      super.tearDown();
   }

   // Public --------------------------------------------------------

  
   public void testBrowser() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = JMSTestCase.cf.createConnection();
         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = session.createProducer(HornetQServerTestCase.queue1);

         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         TextMessage m = session.createTextMessage("message one");

         prod.send(m);

         // Give the message time to reach the queue
         Thread.sleep(2000);

         QueueBrowser browser = session.createBrowser(HornetQServerTestCase.queue1);

         Enumeration e = browser.getEnumeration();

         TextMessage bm = (TextMessage)e.nextElement();

         ProxyAssertSupport.assertEquals("message one", bm.getText());
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

   /**
    * Test case for http://jira.jboss.org/jira/browse/JBMESSAGING-542
    */
   public void testClosingConsumerFromMessageListenerAutoAck() throws Exception
   {
      Connection c = null;

      try
      {
         c = JMSTestCase.cf.createConnection();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = s.createProducer(HornetQServerTestCase.queue1);
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
      Connection conn = JMSTestCase.cf.createConnection();
      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer cons = s.createConsumer(HornetQServerTestCase.queue1);
      cons.setMessageListener(new MessageListener()
      {
         public void onMessage(final Message m)
         {
            // close the connection on the same thread that processed the message
            try
            {
               cons.close();
               result.setSuccess();
            }
            catch (Exception e)
            {
               result.setFailure(e);
            }
         }
      });

      conn.start();

      result.waitForResult();

      ProxyAssertSupport.assertTrue(result.isSuccess());
      ProxyAssertSupport.assertNull(result.getFailure());

      // it's auto _ack so message *should not* be acked (auto ack acks after successfully completion of onMessage

      Thread.sleep(1000);
      assertRemainingMessages(1);

      conn.close();

   }

   /**
    * Test case for http://jira.jboss.org/jira/browse/JBMESSAGING-542
    */
   public void testClosingSessionFromMessageListenerAutoAck() throws Exception
   {
      Connection c = null;

      try
      {
         c = JMSTestCase.cf.createConnection();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = s.createProducer(HornetQServerTestCase.queue1);
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
      Connection conn = JMSTestCase.cf.createConnection();
      final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer cons = session.createConsumer(HornetQServerTestCase.queue1);
      cons.setMessageListener(new MessageListener()
      {
         public void onMessage(final Message m)
         {
            // close the connection on the same thread that processed the message
            try
            {
               session.close();
               result.setSuccess();
            }
            catch (Exception e)
            {
               result.setFailure(e);
            }
         }
      });

      conn.start();

      result.waitForResult();

      ProxyAssertSupport.assertTrue(result.isSuccess());
      ProxyAssertSupport.assertNull(result.getFailure());

      Thread.sleep(1000);
      assertRemainingMessages(1);
      conn.close();
   }

   /**
    * Test case for http://jira.jboss.org/jira/browse/JBMESSAGING-542
    */
   public void testClosingConnectionFromMessageListenerAutoAck() throws Exception
   {
      Connection c = null;

      try
      {
         c = JMSTestCase.cf.createConnection();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = s.createProducer(HornetQServerTestCase.queue1);
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
      final Connection conn = JMSTestCase.cf.createConnection();
      Session s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer cons = s.createConsumer(HornetQServerTestCase.queue1);
      cons.setMessageListener(new MessageListener()
      {
         public void onMessage(final Message m)
         {
            // close the connection on the same thread that processed the message
            try
            {
               conn.close();
               result.setSuccess();
            }
            catch (Exception e)
            {
               e.printStackTrace();
               result.setFailure(e);
            }
         }
      });

      conn.start();

      result.waitForResult();

      ProxyAssertSupport.assertTrue(result.isSuccess());
      ProxyAssertSupport.assertNull(result.getFailure());

      Thread.sleep(1000);
      assertRemainingMessages(1);

      conn.close();

   }

   /**
    * Test case for http://jira.jboss.org/jira/browse/JBMESSAGING-542
    */
   public void testClosingConsumerFromMessageListenerTransacted() throws Exception
   {
      Connection c = null;

      try
      {
         c = JMSTestCase.cf.createConnection();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = s.createProducer(HornetQServerTestCase.queue1);
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
      Connection conn = JMSTestCase.cf.createConnection();
      Session s = conn.createSession(true, Session.SESSION_TRANSACTED);
      final MessageConsumer cons = s.createConsumer(HornetQServerTestCase.queue1);
      cons.setMessageListener(new MessageListener()
      {
         public void onMessage(final Message m)
         {
            // close the connection on the same thread that processed the message
            try
            {
               cons.close();
               result.setSuccess();
            }
            catch (Exception e)
            {
               result.setFailure(e);
            }
         }
      });

      conn.start();

      result.waitForResult();

      ProxyAssertSupport.assertTrue(result.isSuccess());
      ProxyAssertSupport.assertNull(result.getFailure());

      Thread.sleep(1000);
      assertRemainingMessages(1);

      conn.close();

   }

   /**
    * Test case for http://jira.jboss.org/jira/browse/JBMESSAGING-542
    */
   public void testClosingSessionFromMessageListenerTransacted() throws Exception
   {
      Connection c = null;

      try
      {
         c = JMSTestCase.cf.createConnection();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = s.createProducer(HornetQServerTestCase.queue1);
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
      Connection conn = JMSTestCase.cf.createConnection();
      final Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
      MessageConsumer cons = session.createConsumer(HornetQServerTestCase.queue1);
      cons.setMessageListener(new MessageListener()
      {
         public void onMessage(final Message m)
         {
            // close the connection on the same thread that processed the message
            try
            {
               session.close();
               result.setSuccess();
            }
            catch (Exception e)
            {
               result.setFailure(e);
            }
         }
      });

      conn.start();

      result.waitForResult();

      ProxyAssertSupport.assertTrue(result.isSuccess());
      ProxyAssertSupport.assertNull(result.getFailure());

      Thread.sleep(1000);
      assertRemainingMessages(1);
      conn.close();
   }

   /**
    * Test case for http://jira.jboss.org/jira/browse/JBMESSAGING-542
    */
   public void testClosingConnectionFromMessageListenerTransacted() throws Exception
   {
      Connection c = null;

      try
      {
         c = JMSTestCase.cf.createConnection();
         Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = s.createProducer(HornetQServerTestCase.queue1);
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
      final Connection conn = JMSTestCase.cf.createConnection();
      Session s = conn.createSession(true, Session.SESSION_TRANSACTED);
      MessageConsumer cons = s.createConsumer(HornetQServerTestCase.queue1);
      cons.setMessageListener(new MessageListener()
      {
         public void onMessage(final Message m)
         {
            // close the connection on the same thread that processed the message
            try
            {
               conn.close();
               result.setSuccess();
            }
            catch (Exception e)
            {
               e.printStackTrace();
               result.setFailure(e);
            }
         }
      });

      conn.start();

      result.waitForResult();

      ProxyAssertSupport.assertTrue(result.isSuccess());
      ProxyAssertSupport.assertNull(result.getFailure());

      Thread.sleep(1000);
      assertRemainingMessages(1);

      conn.close();

   }

   // Test case for http://jira.jboss.com/jira/browse/JBMESSAGING-788
   public void testGetDeliveriesForSession() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = JMSTestCase.cf.createConnection();

         Session session1 = conn.createSession(true, Session.SESSION_TRANSACTED);

         Session session2 = conn.createSession(true, Session.SESSION_TRANSACTED);

         MessageProducer prod = session2.createProducer(HornetQServerTestCase.queue1);

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

         removeAllMessages(HornetQServerTestCase.queue1.getQueueName(), true);
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

         resultSet = true;

         notify();
      }

      public synchronized boolean isSuccess()
      {
         return success;
      }

      public synchronized void setFailure(final Exception e)
      {
         this.e = e;

         resultSet = true;

         notify();
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
