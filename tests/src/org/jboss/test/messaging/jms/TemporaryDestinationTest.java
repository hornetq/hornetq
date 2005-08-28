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
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:tim.l.fox@gmail.com">Tim Fox</a>
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class TemporaryDestinationTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected InitialContext initialContext;
   
   protected ConnectionFactory cf;
   //protected Destination topic;

   protected Connection connection;

   protected Session producerSession, consumerSession;

   // Constructors --------------------------------------------------

   public TemporaryDestinationTest(String name)
   {
      super(name);
   }

   // TestCase overrides -------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();
      ServerManagement.setRemote(false);
      ServerManagement.startInVMServer("all");
      initialContext = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
      ServerManagement.deployTopic("Topic");
      //topic = (Destination)initialContext.lookup("/topic/Topic");

      connection = cf.createConnection();
      producerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

   }

   public void tearDown() throws Exception
   {
      connection.close();
      ServerManagement.stopInVMServer();
      super.tearDown();
   }


   // Public --------------------------------------------------------
   
   
   
   public void testTemporaryQueueBasic() throws Exception
   {

      TemporaryQueue tempQueue = producerSession.createTemporaryQueue();
      
      MessageProducer producer = producerSession.createProducer(tempQueue);
      
      MessageConsumer consumer = consumerSession.createConsumer(tempQueue);
      
      connection.start();
      
      final String messageText = "This is a message";
      
      Message m = producerSession.createTextMessage(messageText);
      
      producer.send(m);
      
      TextMessage m2 = (TextMessage)consumer.receive(2000);
      
      assertNotNull(m2);
      
      assertEquals(messageText, m2.getText());
   }

   /**
    * http://jira.jboss.com/jira/browse/JBMESSAGING-93
    */
   public void testTemporaryQueueOnClosedSession() throws Exception
   {
      producerSession.close();

      try
      {
         producerSession.createTemporaryQueue();
         fail("should throw exception");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }
   }

   public void testTemporaryQueueDeleted() throws Exception
   {
      //Make sure temporary queue cannot be used after it has been deleted
      
      TemporaryQueue tempQueue = producerSession.createTemporaryQueue();
      
      MessageProducer producer = producerSession.createProducer(tempQueue);
      
      MessageConsumer consumer = consumerSession.createConsumer(tempQueue);
      
      connection.start();
      
      final String messageText = "This is a message";
      
      Message m = producerSession.createTextMessage(messageText);
      
      producer.send(m);
      
      TextMessage m2 = (TextMessage)consumer.receive(2000);
      
      assertNotNull(m2);
      
      assertEquals(messageText, m2.getText());
      
      consumer.close();
      
      tempQueue.delete();
      
      try
      {
         producer.send(m);
         fail();
      }
      catch (JMSException e) {}
   }
   
  
   
   public void testTemporaryTopicBasic() throws Exception
   {
      TemporaryTopic tempTopic = producerSession.createTemporaryTopic();
      
      final MessageProducer producer = producerSession.createProducer(tempTopic);
      
      MessageConsumer consumer = consumerSession.createConsumer(tempTopic);
      
      connection.start();
      
      final String messageText = "This is a message";
      
      final Message m = producerSession.createTextMessage(messageText);
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
   }


   /**
    * http://jira.jboss.com/jira/browse/JBMESSAGING-93
    */
   public void testTemporaryTopicOnClosedSession() throws Exception
   {
      producerSession.close();

      try
      {
         producerSession.createTemporaryTopic();
         fail("should throw exception");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }
   }


   
   

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

  
   
}

