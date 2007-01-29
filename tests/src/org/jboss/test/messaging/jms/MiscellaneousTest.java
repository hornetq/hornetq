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
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.QueueBrowser;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.naming.InitialContext;
import javax.management.ObjectName;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

import java.util.Enumeration;

/**
 * Various use cases, added here while trying things or fixing forum issues.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MiscellaneousTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   InitialContext ic;

   // Constructors --------------------------------------------------

   public MiscellaneousTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testBrowser() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      Queue queue = (Queue)ic.lookup("/queue/MiscellaneousQueue");

      Connection conn = cf.createConnection();
      Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = session.createProducer(queue);

      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

      TextMessage m = session.createTextMessage("message one");

      prod.send(m);

      QueueBrowser browser = session.createBrowser(queue);


      Enumeration e = browser.getEnumeration();

      TextMessage bm = (TextMessage)e.nextElement();

      assertEquals("message one", bm.getText());

      conn.close();
   }

   /**
    * Test case for http://jira.jboss.org/jira/browse/JBMESSAGING-542
    */
   public void testClosingConsumerFromMessageListener() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      Queue queue = (Queue)ic.lookup("/queue/MiscellaneousQueue");

      // load the queue

      Connection c = cf.createConnection();
      Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = s.createProducer(queue);
      Message m = s.createMessage();
      prod.send(m);
      c.close();

      final Result result = new Result();
      Connection conn = cf.createConnection();
      s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      final MessageConsumer cons = s.createConsumer(queue);
      cons.setMessageListener(new MessageListener()
      {
         public void onMessage(Message m)
         {
            // close the connection on the same thread that processed the message
            try
            {
               log.debug("attempting close");
               cons.close();
               log.debug("consumer closed");
               result.setSuccess();
            }
            catch(Exception e)
            {
               result.setFailure(e);
            }
         }
      });

      conn.start();

      // wait for the message to propagate
      Thread.sleep(3000);

      assertTrue(result.isSuccess());
      assertNull(result.getFailure());

      // make sure the acknowledgment made it back to the queue

      Integer count = (Integer)ServerManagement.
         getAttribute(new ObjectName("jboss.messaging.destination:service=Queue,name=MiscellaneousQueue"),
                      "MessageCount");
      assertEquals(0, count.intValue());
   }

   /**
    * Test case for http://jira.jboss.org/jira/browse/JBMESSAGING-542
    */
   public void testClosingSessionFromMessageListener() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      Queue queue = (Queue)ic.lookup("/queue/MiscellaneousQueue");

      // load the queue

      Connection c = cf.createConnection();
      Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = s.createProducer(queue);
      Message m = s.createMessage();
      prod.send(m);
      c.close();

      final Result result = new Result();
      Connection conn = cf.createConnection();
      final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer cons = session.createConsumer(queue);
      cons.setMessageListener(new MessageListener()
      {
         public void onMessage(Message m)
         {
            // close the connection on the same thread that processed the message
            try
            {
               log.debug("attempting close");
               session.close();
               log.debug("session closed");
               result.setSuccess();
            }
            catch(Exception e)
            {
               result.setFailure(e);
            }
         }
      });

      conn.start();

      // wait for the message to propagate
      Thread.sleep(3000);

      assertTrue(result.isSuccess());
      assertNull(result.getFailure());

      // make sure the acknowledgment made it back to the queue

      Integer count = (Integer)ServerManagement.
         getAttribute(new ObjectName("jboss.messaging.destination:service=Queue,name=MiscellaneousQueue"),
                      "MessageCount");
      assertEquals(0, count.intValue());

   }

   /**
    * Test case for http://jira.jboss.org/jira/browse/JBMESSAGING-542
    */
   public void testClosingConnectionFromMessageListener() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      Queue queue = (Queue)ic.lookup("/queue/MiscellaneousQueue");

      // load the queue

      Connection c = cf.createConnection();
      Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = s.createProducer(queue);
      Message m = s.createMessage();
      prod.send(m);
      c.close();

      final Result result = new Result();
      final Connection conn = cf.createConnection();
      s = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer cons = s.createConsumer(queue);
      cons.setMessageListener(new MessageListener()
      {
         public void onMessage(Message m)
         {
            // close the connection on the same thread that processed the message
            try
            {
               log.debug("attempting close");
               conn.close();
               log.debug("conn closed");
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

      // wait for the message to propagate
      Thread.sleep(3000);

      assertTrue(result.isSuccess());
      assertNull(result.getFailure());

      // make sure the acknowledgment made it back to the queue

      Integer count = (Integer)ServerManagement.
         getAttribute(new ObjectName("jboss.messaging.destination:service=Queue,name=MiscellaneousQueue"),
                      "MessageCount");
      assertEquals(0, count.intValue());
   }
   
   // Test case for http://jira.jboss.com/jira/browse/JBMESSAGING-788
   public void testGetDeliveriesForSession() throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      Queue queue = (Queue)ic.lookup("/queue/MiscellaneousQueue");

      Connection conn = null;
      
      try
      {
         conn = cf.createConnection();
         
         Session session1 = conn.createSession(true, Session.SESSION_TRANSACTED);
         
         Session session2 = conn.createSession(true, Session.SESSION_TRANSACTED);
         
         MessageProducer prod = session2.createProducer(queue);
         
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
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.start("all");

      ic = new InitialContext(ServerManagement.getJNDIEnvironment());

      ServerManagement.deployQueue("MiscellaneousQueue");

      log.debug("setup done");
   }

   protected void tearDown() throws Exception
   {
      ServerManagement.undeployQueue("MiscellaneousQueue");

      ic.close();

      super.tearDown();
   }



   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private class Result
   {
      private boolean success;
      private Exception e;

      public Result()
      {
         success = false;
         e = null;
      }

      public synchronized void setSuccess()
      {
         success = true;
      }

      public synchronized boolean isSuccess()
      {
         return success;
      }

      public synchronized void setFailure(Exception e)
      {
         this.e = e;
      }

      public synchronized Exception getFailure()
      {
         return e;
      }
   }

}
