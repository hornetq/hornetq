/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TemporaryTopic;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.jms.message.JBossMessage;
import org.jboss.jms.util.InVMInitialContextFactory;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 */
public class TemporaryDestinationTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;
   
   protected ConnectionFactory cf;
   //protected Destination topic;

   // Constructors --------------------------------------------------

   public TemporaryDestinationTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.startInVMServer();
      initialContext = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());
      cf = (ConnectionFactory)initialContext.lookup("/messaging/ConnectionFactory");
      ServerManagement.deployTopic("Topic");
      //topic = (Destination)initialContext.lookup("/messaging/topics/Topic");
   }

   public void tearDown() throws Exception
   {
      ServerManagement.stopInVMServer();
      super.tearDown();
   }


   // Public --------------------------------------------------------
   
   
   
   public void testTemporaryQueueBasic() throws Exception
   {
      Connection conn = cf.createConnection();
      
      Session sessProducer1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessConsumer1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      TemporaryQueue tempQueue = sessProducer1.createTemporaryQueue();
      
      MessageProducer producer = sessProducer1.createProducer(tempQueue);
      
      MessageConsumer consumer = sessConsumer1.createConsumer(tempQueue);
      
      conn.start();
      
      final String messageText = "This is a message";
      
      Message m = sessProducer1.createTextMessage(messageText);
      
      producer.send(m);
      
      TextMessage m2 = (TextMessage)consumer.receive(2000);
      
      assertNotNull(m2);
      
      assertEquals(messageText, m2.getText());
      
      conn.close();
      
   }
   
   
   
   
   public void testTemporaryQueueDeleted() throws Exception
   {
      Connection conn = cf.createConnection();
      
      //Make sure temporary queue cannot be used after it has been deleted
      
      Session sessProducer1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessConsumer1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      TemporaryQueue tempQueue = sessProducer1.createTemporaryQueue();
      
      MessageProducer producer = sessProducer1.createProducer(tempQueue);
      
      MessageConsumer consumer = sessConsumer1.createConsumer(tempQueue);
      
      conn.start();
      
      final String messageText = "This is a message";
      
      Message m = sessProducer1.createTextMessage(messageText);
      
      producer.send(m);
      
      TextMessage m2 = (TextMessage)consumer.receive(2000);
      
      assertNotNull(m2);
      
      assertEquals(messageText, m2.getText());
      
      tempQueue.delete();
      
      try
      {
         producer.send(m);
         fail();
      }
      catch (JMSException e) {}
        
      conn.close();    
   }
   
  
   
   public void testTemporaryTopicBasic() throws Exception
   {
      Connection conn = cf.createConnection();
      
      Session sessProducer1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sessConsumer1 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      TemporaryTopic tempTopic = sessProducer1.createTemporaryTopic();
      
      final MessageProducer producer = sessProducer1.createProducer(tempTopic);
      
      MessageConsumer consumer = sessConsumer1.createConsumer(tempTopic);
      
      conn.start();
      
      final String messageText = "This is a message";
      
      final Message m = sessProducer1.createTextMessage(messageText);
      log.trace("Message reliable:" + ((JBossMessage)m).isReliable());
      
      Thread t = new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               // this is needed to make sure the main thread has enough time to block
               Thread.sleep(500);
               producer.send(m);
            }
            catch(Exception e)
            {
               log.error(e);
            }
         }
      }, "Producer");
      t.start();
        
      TextMessage m2 = (TextMessage)consumer.receive(3000);
      
      assertNotNull(m2);
      
      assertEquals(messageText, m2.getText());
      
      t.join();
      
      conn.close();
      
   }
   
   
   
   
   

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

  
   
}

