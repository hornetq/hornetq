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
import org.jboss.messaging.tools.jndi.InVMInitialContextFactory;
import org.jboss.messaging.core.util.transaction.TransactionManagerImpl;

import javax.naming.InitialContext;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.JMSException;

/**
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 */
public class TransactedSessionTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------
   
   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------
   
   protected InitialContext initialContext;
   
   protected JBossConnectionFactory cf;
   protected Destination queue;
   
   // Constructors --------------------------------------------------
   
   public TransactedSessionTest(String name)
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
      
      ServerManagement.deployQueue("Queue");
      queue = (Destination)initialContext.lookup("/queue/Queue");
   }
   
   public void tearDown() throws Exception
   {
      ServerManagement.stopInVMServer();
      TransactionManagerImpl.getInstance().setState(TransactionManagerImpl.OPERATIONAL);
      super.tearDown();
   }
   
   
   // Public --------------------------------------------------------
   
   
   public void testSendNoTransactionManager() throws Exception
   {
      ServerManagement.stopInVMServer();

      // start the server without a transaction manager
      ServerManagement.startInVMServer("remoting");
      initialContext = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());
      cf = (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");
      ServerManagement.deployQueue("Queue");
      queue = (Destination)initialContext.lookup("/queue/Queue");

      Connection c = cf.createConnection();

      Session s = c.createSession(true, -1);
      MessageProducer p = s.createProducer(queue);

      // send a message

      Message m = s.createMessage();
      p.send(m);

      try
      {
         s.commit();
         fail("should have thrown exception");
      }
      catch(JMSException e)
      {
         // OK
      }
   }


   public void testSendBrokenTransactionManager() throws Exception
   {
      ServerManagement.stopInVMServer();

      // start the server with a broken manager
      TransactionManagerImpl.getInstance().setState(TransactionManagerImpl.BROKEN);
      ServerManagement.startInVMServer(TransactionManagerImpl.getInstance());
      initialContext = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());
      cf = (JBossConnectionFactory)initialContext.lookup("/ConnectionFactory");
      ServerManagement.deployQueue("Queue");
      queue = (Destination)initialContext.lookup("/queue/Queue");

      Connection c = cf.createConnection();

      Session s = c.createSession(true, -1);
      MessageProducer p = s.createProducer(queue);

      // send a message

      Message m = s.createMessage();
      p.send(m);

      try
      {
         s.commit();
         fail("should have thrown exception");
      }
      catch(JMSException e)
      {
         // OK
      }
   }

   /**
    * Send some messages in transacted session. Don't commit.
    * Verify message are not received by consumer.
    */
   public void testSendNoCommit() throws Exception
   {
      Connection conn = cf.createConnection();

      Session producerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
      MessageProducer producer = producerSess.createProducer(queue);

      Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      MessageConsumer consumer = consumerSess.createConsumer(queue);
      conn.start();

      final int NUM_MESSAGES = 10;

      //Send some messages
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         Message m = producerSess.createMessage();
         producer.send(m);
      }

      log.trace("Sent messages");

      Message m = consumer.receive(2000);
      assertNull(m);

      conn.close();
   }

   
   /**
    * Send some messages in transacted session. Commit.
    * Verify message are received by consumer.
    */
   public void testSendCommit() throws Exception
   {
      Connection conn = cf.createConnection();
      
      Session producerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
      MessageProducer producer = producerSess.createProducer(queue);
      
      Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      MessageConsumer consumer = consumerSess.createConsumer(queue);
      conn.start();
      
      final int NUM_MESSAGES = 10;
      
      //Send some messages
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         Message m = producerSess.createMessage();
         producer.send(m);
      }
      
      producerSess.commit();
      
      log.trace("Sent messages");
      
      int count = 0;
      while (true)
      {
         Message m = consumer.receive(500);
         if (m == null) break;
         count++;
      }
      
      assertEquals(NUM_MESSAGES, count);
      
      conn.close();
   }
   
   
   /**
    * Test IllegateStateException is thrown if commit is called on a non-transacted session
    */
   public void testCommitIllegalState() throws Exception
   {
      Connection conn = cf.createConnection();

      Session producerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

      boolean thrown = false;
      try
      {
         producerSess.commit();
      }
      catch (javax.jms.IllegalStateException e)
      {
         thrown = true;
      }

      assertTrue(thrown);

      conn.close();
   }


   /**
    * Send some messages.
    * Receive them in a transacted session.
    * Do not commit the receiving session.
    * Close the connection
    * Create a new connection, session and consumer - verify messages are redelivered
    *
    */
   public void testAckNoCommit() throws Exception
   {
      Connection conn = cf.createConnection();

      Session producerSess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = producerSess.createProducer(queue);

      Session consumerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
      MessageConsumer consumer = consumerSess.createConsumer(queue);
      conn.start();

      final int NUM_MESSAGES = 10;

      //Send some messages
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         Message m = producerSess.createMessage();
         producer.send(m);
      }

      log.trace("Sent messages");

      int count = 0;
      while (true)
      {
         Message m = consumer.receive(500);
         if (m == null) break;
         count++;
      }

      assertEquals(NUM_MESSAGES, count);

      conn.stop();
      consumer.close();

      conn.close();

      conn = cf.createConnection();

      consumerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
      consumer = consumerSess.createConsumer(queue);
      conn.start();

      count = 0;
      while (true)
      {
         Message m = consumer.receive(500);
         if (m == null) break;
         count++;
      }

      assertEquals(NUM_MESSAGES, count);
      conn.close();

   }




   /**
    * Send some messages.
    * Receive them in a transacted session.
    * Commit the receiving session
    * Close the connection
    * Create a new connection, session and consumer - verify messages are not redelivered
    */
   public void testAckCommit() throws Exception
   {
      Connection conn = cf.createConnection();

      Session producerSess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = producerSess.createProducer(queue);

      Session consumerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
      MessageConsumer consumer = consumerSess.createConsumer(queue);
      conn.start();

      final int NUM_MESSAGES = 10;

      //Send some messages
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         Message m = producerSess.createMessage();
         producer.send(m);
      }

      log.trace("Sent messages");

      int count = 0;
      while (true)
      {
         Message m = consumer.receive(500);
         if (m == null) break;
         count++;
      }

      assertEquals(NUM_MESSAGES, count);

      consumerSess.commit();

      conn.stop();
      consumer.close();

      conn.close();

      conn = cf.createConnection();

      consumerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
      consumer = consumerSess.createConsumer(queue);
      conn.start();

      Message m = consumer.receive(2000);

      assertNull(m);

      conn.close();

   }



   /*
    * Send some messages in a transacted session.
    * Rollback the session.
    * Verify messages aren't received by consumer.
    */
   public void testSendRollback() throws Exception
   {
      Connection conn = cf.createConnection();

      Session producerSess = conn.createSession(true, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = producerSess.createProducer(queue);

      Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      MessageConsumer consumer = consumerSess.createConsumer(queue);
      conn.start();

      final int NUM_MESSAGES = 10;

      //Send some messages
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         Message m = producerSess.createMessage();
         producer.send(m);
      }

      log.trace("Sent messages");

      producerSess.rollback();

      Message m = consumer.receive(2000);

      assertNull(m);

      conn.close();


   }


   /*
    * Test IllegateStateException is thrown if rollback is
    * called on a non-transacted session
    *
    */

   public void testRollbackIllegalState() throws Exception
   {
      Connection conn = cf.createConnection();

      Session producerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

      boolean thrown = false;
      try
      {
         producerSess.rollback();
      }
      catch (javax.jms.IllegalStateException e)
      {
         thrown = true;
      }

      assertTrue(thrown);

      conn.close();
   }


   /*
    * Send some messages.
    * Receive them in a transacted session.
    * Rollback the receiving session
    * Close the connection
    * Create a new connection, session and consumer - verify messages are redelivered
    *
    */

   public void testAckRollback() throws Exception
   {
      Connection conn = cf.createConnection();

      Session producerSess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = producerSess.createProducer(queue);

      Session consumerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
      MessageConsumer consumer = consumerSess.createConsumer(queue);
      conn.start();

      final int NUM_MESSAGES = 10;

      //Send some messages
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         Message m = producerSess.createMessage();
         producer.send(m);
      }

      log.trace("Sent messages");

      int count = 0;
      while (true)
      {
         Message m = consumer.receive(500);
         if (m == null) break;
         count++;
      }

      assertEquals(NUM_MESSAGES, count);

      consumerSess.rollback();

      conn.stop();
      consumer.close();

      conn.close();

      conn = cf.createConnection();

      consumerSess = conn.createSession(true, Session.CLIENT_ACKNOWLEDGE);
      consumer = consumerSess.createConsumer(queue);
      conn.start();

      count = 0;
      while (true)
      {
         Message m = consumer.receive(500);
         if (m == null) break;
         count++;
      }

      assertEquals(NUM_MESSAGES, count);

      conn.close();

   }


   /*
    * Send multiple messages in multiple contiguous sessions
    */
   public void testSendMultiple() throws Exception
   {
      Connection conn = cf.createConnection();

      Session producerSess = conn.createSession(true, Session.AUTO_ACKNOWLEDGE);
      MessageProducer producer = producerSess.createProducer(queue);

      Session consumerSess = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      MessageConsumer consumer = consumerSess.createConsumer(queue);
      conn.start();

      final int NUM_MESSAGES = 10;
      final int NUM_TX = 10;

      //Send some messages

      for (int j = 0; j < NUM_TX; j++)
      {

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            Message m = producerSess.createMessage();
            producer.send(m);
         }

         producerSess.commit();
      }

      log.trace("Sent messages");

      int count = 0;
      while (true)
      {
         Message m = consumer.receive(500);
         if (m == null) break;
         count++;
      }

      assertEquals(NUM_MESSAGES * NUM_TX, count);

      conn.close();

   }
   
   
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------
   
}


