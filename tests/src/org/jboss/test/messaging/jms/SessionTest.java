/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.jms.client.JBossConnectionFactory;
import org.jboss.jms.util.InVMInitialContextFactory;

import javax.naming.InitialContext;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.XAConnection;
import javax.jms.XASession;
import javax.jms.TopicConnection;
import javax.jms.TopicSession;
import javax.jms.QueueConnection;
import javax.jms.QueueSession;

/**
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
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
      ServerManagement.startInVMServer();
      initialContext = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());
      cf = (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");
      
      ServerManagement.deployTopic("TestTopic");
      topic = (Topic)initialContext.lookup("/topic/TestTopic");
      
      ServerManagement.deployQueue("TestQueue");
      queue = (Queue)initialContext.lookup("/queue/TestQueue");

      
   }

   public void tearDown() throws Exception
   {
      ServerManagement.stopInVMServer();
      super.tearDown();
   }


   // Public --------------------------------------------------------

   public void testCreateProducer() throws Exception
   {
      Connection conn = cf.createConnection();      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      sess.createProducer(topic);
      conn.close();
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
   
	 // TODO: enable it after implementing JBossSession.getXAResource()
	/*
   public void testGetXAResource() throws Exception
   {
      Connection conn = cf.createConnection();      
      Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      try
      {
         XAResource xaResource = ((XASession)sess).getXAResource();
         fail("Should throw IllegalStateException");
      }
      catch (javax.jms.IllegalStateException e)
      {}
      conn.close();
   }
   */


   // TODO: enable it after implementing JBossSession.getXAResource()
//   public void testGetXAResource2() throws Exception
//   {
//      XAConnection conn = cf.createXAConnection();
//      XASession sess = conn.createXASession();
//
//      XAResource xaRessource = sess.getXAResource();
//      conn.close();
//   }
   
   
   
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}

