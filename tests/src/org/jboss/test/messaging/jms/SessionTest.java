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
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.naming.InitialContext;

import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.client.JBossSession;
import org.jboss.jms.client.delegate.ClientSessionDelegate;
import org.jboss.jms.client.state.ConnectionState;
import org.jboss.jms.client.state.SessionState;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SessionTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected InitialContext initialContext;
   
   protected JBossConnectionFactory cf;
   protected Topic topic;
   protected Queue queue;
   
   // Constructors --------------------------------------------------
   
   public SessionTest(String name)
   {
      super(name);
   }
   

   // TestCase overrides -------------------------------------------
   
   public void setUp() throws Exception
   {
      super.setUp();                  
      
      ServerManagement.start("all");
      
      
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");
      
      ServerManagement.undeployTopic("TestTopic");
      ServerManagement.deployTopic("TestTopic");
      topic = (Topic)initialContext.lookup("/topic/TestTopic");
      
      ServerManagement.undeployQueue("TestQueue");
      ServerManagement.deployQueue("TestQueue");
      queue = (Queue)initialContext.lookup("/queue/TestQueue");
      
      log.debug("Done setup()");
          
   }
   
   public void tearDown() throws Exception
   {
      ServerManagement.undeployTopic("TestTopic"); 
      ServerManagement.undeployQueue("TestQueue");
      super.tearDown();
   }
   
   

   // Public --------------------------------------------------------
   
   public void testNoTransactionAfterClose() throws Exception
   {
      Connection conn = cf.createConnection();      
      conn.start();
      Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
      MessageProducer prod = sess.createProducer(queue);
      prod.send(sess.createMessage());
      sess.commit();
      MessageConsumer cons = sess.createConsumer(queue);
      cons.receive();
      sess.commit();
      
      ClientSessionDelegate del = (ClientSessionDelegate)((JBossSession)sess).getDelegate();
      
      SessionState state = (SessionState)del.getState();
      ConnectionState cState = (ConnectionState)state.getParent();
      
      Object xid = state.getCurrentTxId();
      assertNotNull(xid);
      assertNotNull(cState.getResourceManager().getTx(xid));
      
      //Now close the session
      sess.close();
      
      //Session should be removed from resource manager
      xid = state.getCurrentTxId();
      assertNotNull(xid);
      assertNull(cState.getResourceManager().getTx(xid));
      
   }

   public void testCreateProducer() throws Exception
   {
      Connection conn = cf.createConnection();      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);          
      sess.createProducer(topic);
      conn.close();
   }

   public void testCreateProducerOnNullQueue() throws Exception
   {
      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Message m = sess.createTextMessage("something");

      MessageProducer p = sess.createProducer(null);

      p.send(queue, m);

      MessageConsumer c = sess.createConsumer(queue);
      conn.start();
      TextMessage rm = (TextMessage)c.receiveNoWait();

      assertEquals("something", rm.getText());
   }
   
   public void testCreateConsumer() throws Exception
   {
      Connection conn = cf.createConnection();      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      sess.createConsumer(topic);
      conn.close();
   }

   public void testGetSession1() throws Exception
   {
      Connection conn = cf.createConnection();      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      try
      {
         ((XASession)sess).getSession();
         fail("Should throw IllegalStateException");
      }
      catch (javax.jms.IllegalStateException e)
      {}
      conn.close();
   }
   
   public void testGetSession2() throws Exception
   {
      XAConnection conn = cf.createXAConnection();      
      XASession sess = conn.createXASession();
      
      sess.getSession();
      conn.close();
   }
   
   //
   // createQueue()/createTopic()
   //
   
   
   public void testCreateNonExistentQueue() throws Exception
   {
      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      try
      {
         sess.createQueue("QueueThatDoesNotExist");
         fail();
      }
      catch (JMSException e)
      {}
   }
   
   public void testCreateQueueOnATopicSession() throws Exception
   {
      TopicConnection c = (TopicConnection)cf.createConnection();
      TopicSession s = c.createTopicSession(false, Session.AUTO_ACKNOWLEDGE);
      
      try
      {
         s.createQueue("TestQueue");
         fail("should throw IllegalStateException");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }
   }
   
   public void testCreateQueueWhileTopicWithSameNameExists() throws Exception
   {
      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      try
      {
         sess.createQueue("TestTopic");
         fail("should throw JMSException");
      }
      catch (JMSException e)
      {
         // OK
      }
   }
   
   public void testCreateQueue() throws Exception
   {
      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Queue queue = sess.createQueue("TestQueue");
      
      MessageProducer producer = sess.createProducer(queue);
      MessageConsumer consumer = sess.createConsumer(queue);
      conn.start();
      
      Message m = sess.createTextMessage("testing");
      producer.send(m);
      
      Message m2 = consumer.receive(3000);
      
      assertNotNull(m2);
   }
   
   public void testCreateNonExistentTopic() throws Exception
   {
      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      try
      {
         sess.createTopic("TopicThatDoesNotExist");
         fail("should throw JMSException");
      }
      catch (JMSException e)
      {
         // OK
      }
   }
   
   public void testCreateTopicOnAQueueSession() throws Exception
   {
      QueueConnection c = (QueueConnection)cf.createConnection();
      QueueSession s = c.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
      
      try
      {
         s.createTopic("TestTopic");
         fail("should throw IllegalStateException");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }
   }
   
   public void testCreateTopicWhileQueueWithSameNameExists() throws Exception
   {
      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      try
      {
         sess.createTopic("TestQueue");
         fail("should throw JMSException");
      }
      catch (JMSException e)
      {
         // OK
      }
   }
   
   public void testCreateTopic() throws Exception
   {
      Connection conn = cf.createConnection();      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      Topic topic = sess.createTopic("TestTopic");
      
      MessageProducer producer = sess.createProducer(topic);
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      
      MessageConsumer consumer = sess.createConsumer(topic);
      conn.start();
      
      
      class TestRunnable implements Runnable
      {
         boolean exceptionThrown;
         public Message m;
         MessageConsumer consumer;
         TestRunnable(MessageConsumer consumer)
         {
            this.consumer = consumer;
         }
         
         public void run()
         {
            try
            {
               m = consumer.receive(3000);               
            }
            catch (Exception e)
            {
               exceptionThrown = true;
            }
         }
      }
      
      TestRunnable tr1 = new TestRunnable(consumer);
      Thread t1 = new Thread(tr1);
      t1.start();
      
      Message m = sess.createTextMessage("testing");
      producer.send(m);
      
      t1.join();
      
      assertFalse(tr1.exceptionThrown);
      assertNotNull(tr1.m);
   }

   public void testGetXAResource() throws Exception
   {
      Connection conn = cf.createConnection();
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

      ((XASession)sess).getXAResource();
      conn.close();
   }


   public void testGetXAResource2() throws Exception
   {
      XAConnection conn = cf.createXAConnection();
      XASession sess = conn.createXASession();

      sess.getXAResource();
      conn.close();
   }


   public void testIllegalState() throws Exception
   {
      //IllegalStateException should be thrown if commit or rollback
      //is invoked on a non transacted session
      Connection conn = cf.createConnection();      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prod = sess.createProducer(queue);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      Message m = sess.createTextMessage("hello");
      prod.send(m);
      
      try
      {
         sess.rollback();
         fail();
      }
      catch (javax.jms.IllegalStateException e)
      {}
      
      try
      {
         sess.commit();
         fail();
      }
      catch (javax.jms.IllegalStateException e)
      {}
      
      conn.close();
   }


   //
   // Test session state
   //

   public void testCreateTwoSessions() throws Exception
   {
      Connection conn = cf.createConnection();
      Session sessionOne = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      assertFalse(sessionOne.getTransacted());
      Session sessionTwo = conn.createSession(true, -1);
      assertTrue(sessionTwo.getTransacted());

      // this test whether session's transacted state is correctly scoped per instance (by an
      // interceptor or othewise)
      assertFalse(sessionOne.getTransacted());
   }

   public void testCloseAndCreateSession() throws Exception
   {
      Connection c = cf.createConnection();
      Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);

      s.close();

      // this test whether session's closed state is correctly scoped per instance (by an
      // interceptor or othewise)
      s = c.createSession(true, -1);
   }



   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------

  

   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}

