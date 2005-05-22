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
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

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
   protected Destination topic;

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
      cf =
            (JBossConnectionFactory)initialContext.lookup("/messaging/ConnectionFactory");
      
      ServerManagement.deployTopic("Topic");
      topic = (Destination)initialContext.lookup("/messaging/topics/Topic");

      
   }

   public void tearDown() throws Exception
   {
      ServerManagement.stopInVMServer();
      //connection.stop();
      //connection = null;
      super.tearDown();
   }


   // Public --------------------------------------------------------

   public void test1() throws Exception
   {
      Connection conn = cf.createConnection();     
      
      final Session sess = conn.createSession(true, Session.AUTO_ACKNOWLEDGE);
      final MessageProducer producer = sess.createProducer(topic);
      MessageConsumer consumer = sess.createConsumer(topic);  
      
      conn.start();      
      
      final Message m = sess.createMessage();
      
      new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               // this is needed to make sure the main thread has enough time to block
               Thread.sleep(1000);
               producer.send(m);
               sess.commit();
            }
            catch(Exception e)
            {
               log.error(e);
            }
         }
      }, "Producer").start();

      assertNotNull(consumer.receive(2000));
      
      conn.close();
   }
   
   /* Don't commit - message should not be sent */
   public void test2() throws Exception
   {
      Connection conn = cf.createConnection();     
      
      final Session sess = conn.createSession(true, Session.AUTO_ACKNOWLEDGE);
      final MessageProducer producer = sess.createProducer(topic);
      MessageConsumer consumer = sess.createConsumer(topic);  
      
      conn.start();      
      
      final Message m = sess.createMessage();
      
      new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               // this is needed to make sure the main thread has enough time to block
               Thread.sleep(1000);
               producer.send(m);
               
            }
            catch(Exception e)
            {
               log.error(e);
            }
         }
      }, "Producer").start();

      assertNull(consumer.receive(2000));
      
      conn.close();
   }
   
   
   /* Rollback - message should not be sent */
   public void test3() throws Exception
   {
      Connection conn = cf.createConnection();     
      
      final Session sess = conn.createSession(true, Session.AUTO_ACKNOWLEDGE);
      final MessageProducer producer = sess.createProducer(topic);
      MessageConsumer consumer = sess.createConsumer(topic);  
      
      conn.start();      
      
      final Message m = sess.createMessage();
      
      new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               // this is needed to make sure the main thread has enough time to block
               Thread.sleep(1000);
               producer.send(m);
               sess.rollback();
               
            }
            catch(Exception e)
            {
               log.error(e);
            }
         }
      }, "Producer").start();

      assertNull(consumer.receive(2000));
      
      conn.close();
   }
   
   
   
   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}


