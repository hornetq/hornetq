/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import javax.naming.InitialContext;

import org.jboss.jms.server.remoting.JMSServerInvocationHandler;
import org.jboss.remoting.ServerInvoker;
import org.jboss.remoting.transport.Connector;
import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

import EDU.oswego.cs.dl.util.concurrent.Latch;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MessageConsumerTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Connection producerConnection, consumerConnection;
   protected Session producerSession, consumerSession;
   protected MessageProducer topicProducer, queueProducer;
   protected MessageConsumer topicConsumer, queueConsumer;
   protected Topic topic;
   protected Queue queue;
   protected Queue queue2;
   protected ConnectionFactory cf;

   protected Thread worker1;

   // Constructors --------------------------------------------------

   public MessageConsumerTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.init("all");
      
      ServerManagement.undeployTopic("Topic");
      ServerManagement.undeployQueue("Queue");
      ServerManagement.undeployQueue("Queue2");
      
      ServerManagement.deployTopic("Topic");
      ServerManagement.deployQueue("Queue");
      ServerManagement.deployQueue("Queue2");

      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (ConnectionFactory)ic.lookup("jms/QueueConnectionFactory");
      topic = (Topic)ic.lookup("/topic/Topic");
      queue = (Queue)ic.lookup("/queue/Queue");
      queue2 = (Queue)ic.lookup("/queue/Queue2");

      
      
      producerConnection = cf.createConnection();
      consumerConnection = cf.createConnection();

      producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      topicProducer = producerSession.createProducer(topic);
      topicProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      topicConsumer = consumerSession.createConsumer(topic);

      queueProducer = producerSession.createProducer(queue);
      queueProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      queueConsumer = consumerSession.createConsumer(queue);

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
    
      producerConnection.close();
      consumerConnection.close();

      if (worker1 != null)
      {
         worker1.interrupt();
      }

      ServerManagement.undeployTopic("Topic");
      ServerManagement.undeployQueue("Queue");
      ServerManagement.undeployQueue("Queue2");
      
      
      ServerManagement.deInit();

      super.tearDown();
   }

   /**
    * The simplest possible test.
    */
   public void testConsumer1() throws Exception
   {
      // start consumer connection before the message is submitted
      consumerConnection.start();

      TextMessage tm = producerSession.createTextMessage("someText");
      queueProducer.send(tm);

      TextMessage m = (TextMessage)queueConsumer.receive();
      assertEquals(tm.getText(), m.getText());
   }

   /**
    * Another simples test.
    */
   public void testConsumer2() throws Exception
   {
      TextMessage tm = producerSession.createTextMessage("someText");
      queueProducer.send(tm);

      // start consumer connection after the message is submitted
      consumerConnection.start();
      
      TextMessage m = (TextMessage)queueConsumer.receive(3000);
      assertEquals(tm.getText(), m.getText());
   }

   /* Test that an ack can be sent after the consumer that received the message has been closed.
    * Acks are scoped per session.
    */
   public void testAckAfterConsumerClosed() throws Exception
   {
      Connection connSend = null;
      Connection connReceive = null;
      
      try
      {
         
         connSend = cf.createConnection();
         
         connSend.start();
         
         Session sessSend = connSend.createSession(true, Session.SESSION_TRANSACTED);
         
         MessageProducer prod = sessSend.createProducer(queue2);
         
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
         
         Message m = sessSend.createTextMessage("hello");
         
         prod.send(m);
         
         sessSend.commit();
         
         connReceive = cf.createConnection();
         
         connReceive.start();
         
         Session sessReceive = connReceive.createSession(true, Session.SESSION_TRANSACTED);
         
         MessageConsumer cons = sessReceive.createConsumer(queue2);
         
         TextMessage m2 = (TextMessage)cons.receive(1500);
         
         assertNotNull(m2);
         
         assertEquals("hello", m2.getText());
         
         //It is legal to close the consumer before committing the tx which is when
         //the acks are sent
         cons.close();
         
         sessReceive.commit();
         
         connReceive.close();
         
         log.trace("Done test");
  
      }
      finally
      {
         if (connSend != null) connSend.close();
         if (connReceive != null) connReceive.close();
      }
   }


    public void testClientAcknowledgmentOnClosedConsumer() throws Exception
    {
       // create my consumer from scratch
       consumerConnection.close();

       TextMessage tm = producerSession.createTextMessage();

       tm.setText("One");
       queueProducer.send(tm);


       consumerConnection = cf.createConnection();
       consumerSession = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
       queueConsumer = consumerSession.createConsumer(queue);

       consumerConnection.start();

       TextMessage m =  (TextMessage)queueConsumer.receive(1500);
       assertEquals(m.getText(), "One");

       queueConsumer.close();

       m.acknowledge();


       try
       {
          queueConsumer.receive();
          fail("should throw exception");
       }
       catch(javax.jms.IllegalStateException e)
       {
          // OK
       }
    }

   
   /*
    * FIXME
    * The following tests will fail until JIRA task JBMESSAGING-148 is complete
    * testClosedConsumer
    * testRedel0
    * testRedel1
    * testRedel2
    * testRedel3
    * testRedel4
    * testRedel5
    * testRedel6
    * 
    */

    public void testClosedConsumer() throws Exception
    {
       // create my consumer from scratch
       consumerConnection.close();

       TextMessage tm = producerSession.createTextMessage();

       tm.setText("One");
       queueProducer.send(tm);

       tm.setText("Two");
       queueProducer.send(tm);

       consumerConnection = cf.createConnection();
       consumerSession = consumerConnection.createSession(true, 0);
       queueConsumer = consumerSession.createConsumer(queue);

       consumerConnection.start();

       TextMessage m =  (TextMessage)queueConsumer.receive(1500);
       assertEquals(m.getText(), "One");

       queueConsumer.close();
       consumerSession.commit();

       // I expect that "Two" is still in the queue

       MessageConsumer queueConsumer2 = consumerSession.createConsumer(queue);
       m =  (TextMessage)queueConsumer2.receive(1500);
       assertNotNull(m);
       assertEquals(m.getText(), "Two");

       consumerConnection.close();
    }
    
    public void testRedel0() throws Exception
    {
       Connection conn = null;
       
       try
       {
       
          conn = cf.createConnection();
          conn.start();
          
          Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
          MessageProducer prod = sess.createProducer(queue);
          TextMessage tm1 = sess.createTextMessage("hello1");
          TextMessage tm2 = sess.createTextMessage("hello2");
          TextMessage tm3 = sess.createTextMessage("hello3");
          prod.send(tm1);
          prod.send(tm2);
          prod.send(tm3);
          sess.commit();
          
          MessageConsumer cons1 = sess.createConsumer(queue);
          
          TextMessage rm1 = (TextMessage)cons1.receive(1500);
          assertNotNull(rm1);
          assertEquals("hello1", rm1.getText());
          
          cons1.close();
          
          MessageConsumer cons2 = sess.createConsumer(queue);
          
          sess.commit();
          
          TextMessage rm2 = (TextMessage)cons2.receive(1500);
          assertNotNull(rm2);
          assertEquals("hello2", rm2.getText());
          
          TextMessage rm3 = (TextMessage)cons2.receive(1500);
          assertNotNull(rm3);
          assertEquals("hello3", rm3.getText());
          
          TextMessage rm4 = (TextMessage)cons2.receive(1500);
          assertNull(rm4);        
          
          
       }
       finally
       {      
          if (conn != null)
          {
             conn.close();
          }
       }
       
    }
    
    
    public void testRedel1() throws Exception
    {
       Connection conn = null;
       
       try
       {
       
          conn = cf.createConnection();
          conn.start();
          
          Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
          MessageProducer prod = sess.createProducer(queue);
          TextMessage tm1 = sess.createTextMessage("hello1");
          TextMessage tm2 = sess.createTextMessage("hello2");
          TextMessage tm3 = sess.createTextMessage("hello3");
          prod.send(tm1);
          prod.send(tm2);
          prod.send(tm3);
          sess.commit();
          
          MessageConsumer cons1 = sess.createConsumer(queue);
          
          TextMessage rm1 = (TextMessage)cons1.receive(1500);
          assertNotNull(rm1);
          assertEquals("hello1", rm1.getText());
          
          cons1.close();
          
          MessageConsumer cons2 = sess.createConsumer(queue);
          
          sess.commit();
          
          TextMessage rm2 = (TextMessage)cons2.receive(1500);
          assertNotNull(rm2);
          assertEquals("hello2", rm2.getText());
          
          TextMessage rm3 = (TextMessage)cons2.receive(1500);
          assertNotNull(rm3);
          assertEquals("hello3", rm3.getText());
          
          TextMessage rm4 = (TextMessage)cons2.receive(1500);
          assertNull(rm4);        
          
          
       }
       finally
       {      
          if (conn != null)
          {
             conn.close();
          }
       }
       
    }
    
    public void testRedel2() throws Exception
    {
       Connection conn = null;
       
       try
       {
       
          conn = cf.createConnection();
          conn.start();
          
          Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
          MessageProducer prod = sess.createProducer(queue);
          TextMessage tm1 = sess.createTextMessage("hello1");
          TextMessage tm2 = sess.createTextMessage("hello2");
          TextMessage tm3 = sess.createTextMessage("hello3");
          prod.send(tm1);
          prod.send(tm2);
          prod.send(tm3);
          sess.commit();
          
          MessageConsumer cons1 = sess.createConsumer(queue);
          
          TextMessage rm1 = (TextMessage)cons1.receive(1500);
          assertNotNull(rm1);
          assertEquals("hello1", rm1.getText());
          
          cons1.close();
          
          sess.commit();
          
          MessageConsumer cons2 = sess.createConsumer(queue);
          
          TextMessage rm2 = (TextMessage)cons2.receive(1500);
          assertNotNull(rm2);
          assertEquals("hello2", rm2.getText());
          
          TextMessage rm3 = (TextMessage)cons2.receive(1500);
          assertNotNull(rm3);
          assertEquals("hello3", rm3.getText());
          
          TextMessage rm4 = (TextMessage)cons2.receive(1500);
          assertNull(rm4);        
          
          
       }
       finally
       {      
          if (conn != null)
          {
             conn.close();
          }
       }
       
    }
    
    public void testRedel3() throws Exception
    {
       Connection conn = null;
       
       try
       {
       
          conn = cf.createConnection();
          conn.start();
          
          Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
          MessageProducer prod = sess.createProducer(queue);
          TextMessage tm1 = sess.createTextMessage("hello1");
          TextMessage tm2 = sess.createTextMessage("hello2");
          TextMessage tm3 = sess.createTextMessage("hello3");
          prod.send(tm1);
          prod.send(tm2);
          prod.send(tm3);
          sess.commit();
          
          MessageConsumer cons1 = sess.createConsumer(queue);
          
          TextMessage rm1 = (TextMessage)cons1.receive(1500);
          assertNotNull(rm1);
          assertEquals("hello1", rm1.getText());
          
          //rollback should cause redelivery of messages not acked
          sess.rollback();
                  
          TextMessage rm2 = (TextMessage)cons1.receive(1500);
          assertNotNull(rm2);
          assertEquals("hello1", rm2.getText());
          
          TextMessage rm3 = (TextMessage)cons1.receive(1500);
          assertNotNull(rm3);
          assertEquals("hello2", rm3.getText());
          
          TextMessage rm4 = (TextMessage)cons1.receive(1500);
          assertNotNull(rm4);
          assertEquals("hello3", rm4.getText());
          
          //This last step is important - there shouldn't be any more messages to receive
          TextMessage rm5 = (TextMessage)cons1.receive(1500);
          assertNull(rm5);        
          
          
       }
       finally
       {      
          if (conn != null)
          {
             conn.close();
          }
       }
       
    }
    
    public void testRedel4() throws Exception
    {
       Connection conn = null;
       
       try
       {
       
          conn = cf.createConnection();
          conn.start();
          
          Session sess = conn.createSession(true, Session.SESSION_TRANSACTED);
          MessageProducer prod = sess.createProducer(queue);
          TextMessage tm1 = sess.createTextMessage("hello1");
          TextMessage tm2 = sess.createTextMessage("hello2");
          TextMessage tm3 = sess.createTextMessage("hello3");
          prod.send(tm1);
          prod.send(tm2);
          prod.send(tm3);
          sess.commit();
          
          MessageConsumer cons1 = sess.createConsumer(queue);
          
          TextMessage rm1 = (TextMessage)cons1.receive(1500);
          assertNotNull(rm1);
          assertEquals("hello1", rm1.getText());
          
          cons1.close();
          
          MessageConsumer cons2 = sess.createConsumer(queue);
          
          //rollback should cause redelivery of messages
          
          //in this case redelivery occurs to a different receiver
          
          sess.rollback();
                  
          TextMessage rm2 = (TextMessage)cons2.receive(1500);
          assertNotNull(rm2);
          assertEquals("hello1", rm2.getText());
          
          TextMessage rm3 = (TextMessage)cons2.receive(1500);
          assertNotNull(rm3);
          assertEquals("hello2", rm3.getText());
          
          TextMessage rm4 = (TextMessage)cons2.receive(1500);
          assertNotNull(rm4);
          assertEquals("hello3", rm4.getText());
          
          //This last step is important - there shouldn't be any more messages to receive
          TextMessage rm5 = (TextMessage)cons2.receive(1500);
          assertNull(rm5);            
          
          
       }
       finally
       {      
          if (conn != null)
          {
             conn.close();
          }
       }
       
    }
    
    
    public void testRedel5() throws Exception
    {
       Connection conn = null;
       
       try
       {
       
          conn = cf.createConnection();
          conn.start();
          
          Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
          MessageProducer prod = sess.createProducer(queue);
          TextMessage tm1 = sess.createTextMessage("hello1");
          TextMessage tm2 = sess.createTextMessage("hello2");
          TextMessage tm3 = sess.createTextMessage("hello3");
          prod.send(tm1);
          prod.send(tm2);
          prod.send(tm3);
          
          MessageConsumer cons1 = sess.createConsumer(queue);
          
          TextMessage rm1 = (TextMessage)cons1.receive(1500);
          assertNotNull(rm1);
          assertEquals("hello1", rm1.getText());
          
          //redeliver
          sess.recover();
                  
          TextMessage rm2 = (TextMessage)cons1.receive(1500);
          assertNotNull(rm2);
          assertEquals("hello1", rm2.getText());
          
          TextMessage rm3 = (TextMessage)cons1.receive(1500);
          assertNotNull(rm3);
          assertEquals("hello2", rm3.getText());
          
          TextMessage rm4 = (TextMessage)cons1.receive(1500);
          assertNotNull(rm4);
          assertEquals("hello3", rm4.getText());
          
          
          //This last step is important - there shouldn't be any more messages to receive
          TextMessage rm5 = (TextMessage)cons1.receive(1500);
          assertNull(rm5);        
          
          
       }
       finally
       {      
          if (conn != null)
          {
             conn.close();
          }
       }
       
    }
    
    
    public void testRedel6() throws Exception
    {
       Connection conn = null;
       
       try
       {
       
          conn = cf.createConnection();
          conn.start();
          
          Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
          MessageProducer prod = sess.createProducer(queue);
          TextMessage tm1 = sess.createTextMessage("hello1");
          TextMessage tm2 = sess.createTextMessage("hello2");
          TextMessage tm3 = sess.createTextMessage("hello3");
          prod.send(tm1);
          prod.send(tm2);
          prod.send(tm3);
          
          MessageConsumer cons1 = sess.createConsumer(queue);
          
          TextMessage rm1 = (TextMessage)cons1.receive(1500);
          assertNotNull(rm1);
          assertEquals("hello1", rm1.getText());
          
          cons1.close();
          
          MessageConsumer cons2 = sess.createConsumer(queue);
          
          //redeliver
          sess.recover();
                  
          TextMessage rm2 = (TextMessage)cons2.receive(1500);
          assertNotNull(rm2);
          assertEquals("hello1", rm2.getText());
          
          TextMessage rm3 = (TextMessage)cons2.receive(1500);
          assertNotNull(rm3);
          assertEquals("hello2", rm3.getText());
          
          TextMessage rm4 = (TextMessage)cons2.receive(1500);
          assertNotNull(rm4);
          assertEquals("hello3", rm4.getText());
          
          
          //This last step is important - there shouldn't be any more messages to receive
          TextMessage rm5 = (TextMessage)cons2.receive(1500);
          assertNull(rm5);        
          
          
       }
       finally
       {      
          if (conn != null)
          {
             conn.close();
          }
       }
       
    }


   public void testSendAndReceivePersistentDifferentConnections() throws Exception
   {
      Connection connSend = null;
      Connection connReceive = null;
      
      try
      {
         
         log.trace("Running testSendAndReceivePersistentDifferentConnections");
         
         connSend = cf.createConnection();
         
         connSend.start();
         
         Session sessSend = connSend.createSession(true, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer prod = sessSend.createProducer(null);
         
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
         
         Message m = sessSend.createTextMessage("hello");
         
         prod.send(queue2, m);
         
         sessSend.commit();
         
         connReceive = cf.createConnection();
         
         connReceive.start();
         
         Session sessReceive = connReceive.createSession(true, Session.SESSION_TRANSACTED);
         
         MessageConsumer cons = sessReceive.createConsumer(queue2);
         
         TextMessage m2 = (TextMessage)cons.receive(1500);
         
         assertNotNull(m2);
         
         assertEquals("hello", m2.getText());
         
         sessReceive.commit();
         
         cons.close();
         
                  
         connReceive = cf.createConnection();
         
         connReceive.start();
         
         sessReceive = connReceive.createSession(true, Session.SESSION_TRANSACTED);
         
         cons = sessReceive.createConsumer(queue2);
         
         TextMessage m3 = (TextMessage)cons.receive(1500);
         
         assertNull(m3);
         
         
         log.trace("Done test");
  
      }
      finally
      {
         if (connSend != null) connSend.close();
         if (connReceive != null) connReceive.close();
      }
   }


   /**
    * TODO Get rid of this (http://jira.jboss.org/jira/browse/JBMESSAGING-92)
    */
   public void testRemotingInternals() throws Exception
   {
      if (ServerManagement.isRemote()) return;
      
      Connector serverConnector = ServerManagement.getConnector();
      ServerInvoker serverInvoker = serverConnector.getServerInvoker();
      JMSServerInvocationHandler invocationHandler =
            (JMSServerInvocationHandler)serverInvoker.getInvocationHandler("JMS");
      Collection listeners = invocationHandler.getListeners();

      assertEquals(2, listeners.size());  // topicConsumer's and queueConsumer's

      MessageConsumer c = consumerSession.createConsumer(queue);

      listeners = invocationHandler.getListeners();
      assertEquals(3, listeners.size());

      c.close();

      listeners = invocationHandler.getListeners();
      assertEquals(2, listeners.size());

   }
   
   public void testMultipleConcurrentConsumers() throws Exception
   {
      consumerConnection.start();
      Session sess1 = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess2 = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      Session sess3 = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageConsumer cons1 = sess1.createConsumer(topic);
      MessageConsumer cons2 = sess2.createConsumer(topic);
      MessageConsumer cons3 = sess3.createConsumer(topic);
        
      final int NUM_MESSAGES = 100;
      
      class Receiver implements Runnable
      {
         Receiver(MessageConsumer c)
         {
            cons = c;
         }
         MessageConsumer cons;
         boolean failed;
         public void run()
         {
            try
            {
               for (int i = 0; i < NUM_MESSAGES; i++)
               {
                  TextMessage m = (TextMessage)cons.receive(5000);                  
                  if (m == null)
                  {
                     log.error("Didn't receive all the messages");
                     failed = true;
                     break;
                  }
                  log.trace("received message");
                  if (!m.getText().equals("testing"))
                  {                     
                     failed = true;
                  }
               }
            }
            catch (Exception e)
            {
               log.error("Failed in receiving messages", e);
               failed = true;
            }
         }
      }
      
      
      Receiver rec1 = new Receiver(cons1);
      Receiver rec2 = new Receiver(cons2);
      Receiver rec3 = new Receiver(cons3);
      
      Thread t1 = new Thread(rec1);
      Thread t2 = new Thread(rec2);
      Thread t3 = new Thread(rec3);
      
      log.trace("Starting threads");
      
      t1.start();
      t2.start();
      t3.start();
      
      log.trace("Sending messages to topic");
      
      producerConnection.start();
      Session prodSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = prodSession.createProducer(topic);
      
      for (int i = 0; i < NUM_MESSAGES; i++)
      {
         Message m = prodSession.createTextMessage("testing");
         prod.send(m);
         log.trace("Sent message to topic");
      }
      
      t1.join();
      t2.join();
      t3.join();
      
      sess1.close();
      prodSession.close();
      
      assertTrue(!rec1.failed);
      assertTrue(!rec2.failed);
      assertTrue(!rec3.failed);
   }
   
   

   public void testGetSelector() throws Exception
   {
      String selector = "JMSType = 'something'";
      topicConsumer = consumerSession.createConsumer(topic, selector);
      assertEquals(selector, topicConsumer.getMessageSelector());
   }

   public void testGetSelectorOnClosedConsumer() throws Exception
   {
      topicConsumer.close();

      try
      {
         topicConsumer.getMessageSelector();
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }
   }


   public void testGetTopic() throws Exception
   {
      Topic t = ((TopicSubscriber)topicConsumer).getTopic();
      assertEquals(topic, t);
   }

   public void testGetTopicOnClosedConsumer() throws Exception
   {
      topicConsumer.close();

      try
      {
         ((TopicSubscriber)topicConsumer).getTopic();
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }
   }


   public void testGetQueue() throws Exception
   {
      Queue q = ((QueueReceiver)queueConsumer).getQueue();
      assertEquals(queue, q);
   }

   public void testGetQueueOnClosedConsumer() throws Exception
   {
      queueConsumer.close();

      try
      {
         ((QueueReceiver)queueConsumer).getQueue();
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }
   }


   public void testReceiveOnTopicTimeoutNoMessage() throws Exception
   {
      if (log.isTraceEnabled()) log.trace("testReceiveOnTopicTimeoutNoMessage");
      Message m = topicConsumer.receive(1000);
      assertNull(m);
   }

   public void testReceiveOnTopicConnectionStopped() throws Exception
   {
      if (log.isTraceEnabled()) log.trace("testReceiveOnTopicConnectionStopped");
      consumerConnection.stop();

      final Message m = producerSession.createMessage();
      new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               // this is needed to make sure the main thread has enough time to block
               Thread.sleep(1000);
               topicProducer.send(m);
            }
            catch(Exception e)
            {
               log.error(e);
            }
         }
      }, "Producer").start();

      assertNull(topicConsumer.receive(1500));
   }


   public void testReceiveOnTopicTimeout() throws Exception
   {
      if (log.isTraceEnabled()) log.trace("testReceiveOnTopicTimeout");
      consumerConnection.start();

      final Message m1 = producerSession.createMessage();
      new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               // this is needed to make sure the main thread has enough time to block
               Thread.sleep(1000);
               topicProducer.send(m1);
            }
            catch(Exception e)
            {
               log.error(e);
            }
         }
      }, "Producer").start();

      Message m2 = topicConsumer.receive(1500);
      assertEquals(m1.getJMSMessageID(), m2.getJMSMessageID());
   }


   public void testReceiveOnTopic() throws Exception
   {
      if (log.isTraceEnabled()) log.trace("testReceiveOnTopic");
      consumerConnection.start();

      final Message m1 = producerSession.createMessage();
      new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               // this is needed to make sure the main thread has enough time to block
               Thread.sleep(1000);
               topicProducer.send(m1);
            }
            catch(Exception e)
            {
               log.error(e);
            }
         }
      }, "Producer").start();

      Message m2 = topicConsumer.receive(3000);

      if (log.isTraceEnabled()) log.trace("m1:" + m1 + ", m2:" + m2) ;

      assertEquals(m1.getJMSMessageID(), m2.getJMSMessageID());
   }



   public void testReceiveNoWaitOnTopic() throws Exception
   {
      consumerConnection.start();

      Message m = topicConsumer.receiveNoWait();

      assertNull(m);

      Message m1 = producerSession.createMessage();
      topicProducer.send(m1);

      // block this thread for a while to allow ServerConsumerDelegate's delivery thread to kick in
      Thread.sleep(5);

      m = topicConsumer.receiveNoWait();

      assertEquals(m1.getJMSMessageID(), m.getJMSMessageID());
   }




   /**
    * The test sends a burst of messages and verifies if the consumer receives all of them.
    */


   public void testStressReceiveOnQueue() throws Exception
   {
      if (log.isTraceEnabled()) log.trace("testStressReceiveOnQueue");
      final int count = 100;

      consumerConnection.start();

      new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               // this is needed to make sure the main thread has enough time to block
               Thread.sleep(1000);


               for (int i = 0; i < count; i++)
               {
                  Message m = producerSession.createMessage();
                  queueProducer.send(m);
               }
            }
            catch(Exception e)
            {
               log.error(e);
            }
         }
      }, "ProducerTestThread").start();

      int received = 0;
      while(true)
      {
         Message m = queueConsumer.receive(1500);
         if (m == null)
         {
            break;
         }
         //Thread.sleep(1000);
         received++;
      }

      assertEquals(count, received);

   }


   /**
    * The test sends a burst of messages and verifies if the consumer receives all of them.
    */




   public void testStressReceiveOnTopic() throws Exception
   {
      if (log.isTraceEnabled()) log.trace("testStressReceiveOnTopic");
      final int count = 100;

      consumerConnection.start();

      new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               // this is needed to make sure the main thread has enough time to block
               Thread.sleep(1000);


               for (int i = 0; i < count; i++)
               {
                  Message m = producerSession.createMessage();
                  topicProducer.send(m);
               }
            }
            catch(Exception e)
            {
               log.error(e);
            }
         }
      }, "ProducerTestThread").start();

      int received = 0;
      while(true)
      {
         Message m = topicConsumer.receive(1500);
         if (m == null)
         {
            break;
         }
         //Thread.sleep(1000);
         received++;
      }

      assertEquals(count, received);

   }



   public void testReceiveOnClose() throws Exception
   {
      if (log.isTraceEnabled()) log.trace("testReceiveOnClose");
      consumerConnection.start();
      final Latch latch = new Latch();
      Thread closerThread = new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               // this is needed to make sure the main thread has enough time to block
               Thread.sleep(1000);
               topicConsumer.close();
            }
            catch(Exception e)
            {
               log.error(e);
            }
            finally
            {
               latch.release();
            }
         }
      }, "closing thread");
      closerThread.start();

      assertNull(topicConsumer.receive(3000));

      // wait for the closing thread to finish
      latch.acquire();
   }

   public void testTimeoutReceiveOnClose() throws Exception
   {
      if (log.isTraceEnabled()) log.trace("testTimeoutReceiveOnClose");
      consumerConnection.start();
      final Latch latch = new Latch();
      final long timeToSleep = 1000;
      Thread closerThread = new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               // this is needed to make sure the main thread has enough time to block
               Thread.sleep(timeToSleep);
               topicConsumer.close();
            }
            catch(Exception e)
            {
               log.error(e);
            }
            finally
            {
               latch.release();
            }
         }
      }, "closing thread");
      closerThread.start();

      long t1 = System.currentTimeMillis();
      assertNull(topicConsumer.receive(1500));
      long elapsed = System.currentTimeMillis() - t1;
      log.info("timeToSleep = " + timeToSleep + " ms, elapsed = " + elapsed + " ms");

      // make sure it didn't wait 5 seconds to return null; allow 10 ms for overhead
      assertTrue(elapsed <= timeToSleep + 100);

      // wait for the closing thread to finish
      latch.acquire();
   }


   //
   // MessageListener tests
   //

   public void testMessageListenerOnTopic() throws Exception
   {
      if (log.isTraceEnabled()) log.trace("testMessageListenerOnTopic");
      MessageListenerImpl l = new MessageListenerImpl();
      topicConsumer.setMessageListener(l);

      consumerConnection.start();

      Message m1 = producerSession.createMessage();
      topicProducer.send(m1);

      // block the current thread until the listener gets something; this is to avoid closing
      // the connection too early
      l.waitForMessages();

      assertEquals(m1.getJMSMessageID(), l.getNextMessage().getJMSMessageID());
   }

   public void testSetMessageListenerTwice() throws Exception
   {
      if (log.isTraceEnabled()) log.trace("testSetMessageListenerTwice");
      MessageListenerImpl listener1 = new MessageListenerImpl();
      topicConsumer.setMessageListener(listener1);

      MessageListenerImpl listener2 = new MessageListenerImpl();
      topicConsumer.setMessageListener(listener2);

      consumerConnection.start();

      Message m1 = producerSession.createMessage();
      topicProducer.send(m1);

      // block the current thread until the listener gets something; this is to avoid closing
      // connection too early
      listener2.waitForMessages();

      assertEquals(m1.getJMSMessageID(), listener2.getNextMessage().getJMSMessageID());
      assertEquals(0, listener1.size());
   }

   public void testSetMessageListenerWhileReceiving() throws Exception
   {
      if (log.isTraceEnabled()) log.trace("testSetMessageListenerWhileReceiving");
      consumerConnection.start();
      worker1= new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               topicConsumer.receive(3000);
            }
            catch(Exception e)
            {
               e.printStackTrace();
            }
         }}, "Receiver");

      worker1.start();

      Thread.sleep(100);

      try
      {
         topicConsumer.setMessageListener(new MessageListenerImpl());
         fail("should have thrown JMSException");
      }
      catch(JMSException e)
      {
          // ok
         log.info(e.getMessage());
      }
   }

   //
   // Multiple consumers
   //

   public void testTwoConsumersNonTransacted() throws Exception
   {

      consumerSession.close();

      TextMessage tm = producerSession.createTextMessage();
      tm.setText("One");
      queueProducer.send(tm);
      tm.setText("Two");
      queueProducer.send(tm);

      // recreate the connection and receive the first message
      consumerConnection = cf.createConnection();
      consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      queueConsumer = consumerSession.createConsumer(queue);
      consumerConnection.start();

      TextMessage m = (TextMessage)queueConsumer.receive(1500);
      assertEquals("One", m.getText());

      consumerConnection.close();

      // recreate the connection and receive the second message
      consumerConnection = cf.createConnection();
      consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      queueConsumer = consumerSession.createConsumer(queue);
      consumerConnection.start();

      m = (TextMessage)queueConsumer.receive(1500);
      assertEquals("Two", m.getText());

      consumerConnection.close();
   }

   public void testTwoConsumersTransacted() throws Exception
   {

      consumerSession.close();

      TextMessage tm = producerSession.createTextMessage();
      tm.setText("One");
      queueProducer.send(tm);
      tm.setText("Two");
      queueProducer.send(tm);

      // recreate the connection and receive the first message
      consumerConnection = cf.createConnection();
      consumerSession = consumerConnection.createSession(true, -1);
      queueConsumer = consumerSession.createConsumer(queue);
      consumerConnection.start();

      TextMessage m = (TextMessage)queueConsumer.receive(1500);
      assertEquals("One", m.getText());

      consumerSession.commit();
      consumerConnection.close();

      // recreate the connection and receive the second message
      consumerConnection = cf.createConnection();
      consumerSession = consumerConnection.createSession(true, -1);
      queueConsumer = consumerSession.createConsumer(queue);
      consumerConnection.start();

      m = (TextMessage)queueConsumer.receive(1500);
      assertEquals("Two", m.getText());

      consumerConnection.close();
   }




   //
   // NoLocal
   //



   public void testNoLocal() throws Exception
   {
      if (log.isTraceEnabled()) log.trace("testNoLocal");

      Connection conn1 = null;
      Connection conn2 = null;
      
      try
      {
      
         conn1 = cf.createConnection();
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
   
         MessageProducer producer1 = sess1.createProducer(topic);
         producer1.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         MessageConsumer consumer1 = sess1.createConsumer(topic, null, true);
   
         conn2 = cf.createConnection();
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
   
         assertEquals(Session.AUTO_ACKNOWLEDGE, sess2.getAcknowledgeMode());
   
         MessageConsumer consumer2 = sess2.createConsumer(topic, null, true);
   
         MessageConsumer consumer3 = sess2.createConsumer(topic, null, false);
   
         //Consumer 1 should not get the message but consumers 2 and 3 should
   
         conn1.start();
         conn2.start();
   
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
                  m = consumer.receive(1500);
               }
               catch (Exception e)
               {
                  exceptionThrown = true;
               }
            }
         }
   
         TestRunnable tr1 = new TestRunnable(consumer1);
         TestRunnable tr2 = new TestRunnable(consumer2);
         TestRunnable tr3 = new TestRunnable(consumer3);
   
         Thread t1 = new Thread(tr1);
         Thread t2 = new Thread(tr2);
         Thread t3 = new Thread(tr3);
   
         t1.start();
         t2.start();
         t3.start();
   
         Message m2 = sess1.createTextMessage("Hello");
         producer1.send(m2);
   
         t1.join();
         t2.join();
         t3.join();
   
         assertTrue(!tr1.exceptionThrown);
         assertTrue(!tr2.exceptionThrown);
         assertTrue(!tr3.exceptionThrown);
   
         assertNull(tr1.m);
   
         assertNotNull(tr2.m);
         assertNotNull(tr3.m);
         
      }
      finally
      {      
         if (conn1 != null)
         {
            conn1.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }


   
   
   
   
   /* 
    * 
    * Also see JMS 1.1 Spec. 6.12
    */
   public void testTopicRedelivery() throws Exception
   {
      Connection conn1 = null;
      
      try
      {
         
         conn1 = cf.createConnection();
         conn1.start();
   
         //Create 2 non durable subscribers on topic
         
         Session sess1 = conn1.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         Session sess2 = conn1.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         
         MessageConsumer cons1 = sess1.createConsumer(topic);
         MessageConsumer cons2 = sess2.createConsumer(topic);
         conn1.start();
         
         Session sess3 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sess3.createProducer(topic);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         
         TextMessage tm = sess3.createTextMessage("nurse!");
         prod.send(tm);
         
         TextMessage tm1 = (TextMessage)cons1.receive(1500);
         TextMessage tm2 = (TextMessage)cons2.receive(1500);
         
         assertNotNull(tm1);
         assertNotNull(tm2);
         assertEquals("nurse!", tm1.getText());
         assertEquals("nurse!", tm2.getText());
         
         //acknowledge tm1
         tm1.acknowledge();
         
         //tm2 has not been acknowledged
         //so should be redelivered on session.recover
         
         sess2.recover();
         
         tm2 = (TextMessage)cons2.receive(1500);
         assertNotNull(tm2);
         assertEquals("nurse!", tm2.getText());
         
         
         //but tm1 should not be redelivered
         tm1 = (TextMessage)cons1.receive(1500);
         assertNull(tm1);
      }
      finally
      {
         if (conn1 != null)
         {
            conn1.close();
         }
      }

   }
   

   /**
      Topics shouldn't persist messages for non durable subscribers and redeliver them on reconnection
      even if delivery mode of persistent is specified
      See JMS spec. sec. 6.12
   
   */
   public void testNoRedeliveryOnNonDurableSubscriber() throws Exception
   {
      Connection conn1 = null;
      Connection conn2 = null;
      
      try
      {
         
         conn1 = cf.createConnection();
         conn1.start();
   
         Session sess1 = conn1.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageProducer prod = sess1.createProducer(topic);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
   
         final int NUM_MESSAGES = 1;
         
         MessageConsumer cons = sess1.createConsumer(topic);
   
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("helloxyz");
            prod.send(topic, tm);
         }
         
         //receive but don't ack

         int count = 0;
         while (true)
         {
            TextMessage tm = (TextMessage)cons.receive(1000);
            if (tm == null) break;
            assertEquals(tm.getText(), "helloxyz");
            count++;
         }
         assertEquals(NUM_MESSAGES, count);
         
         conn1.close();
         
         conn2 = cf.createConnection();
         conn2.start();
         
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer cons2 = sess2.createConsumer(topic);
         Message m = cons2.receive(1500);
         assertNull(m);
         
         conn2.close();
      }
      finally
      {
         if (conn1 != null)
         {
            conn1.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

      
   }
   
   //Check messages have correct type after being resurrected from persistent storage
   public void testPersistedMessageType() throws Exception
   {

      Connection theConn = null;
      Connection theOtherConn = null;
      
      try
      {
      
         theConn = cf.createConnection();
         theConn.start();
         
         //Send some persistent messages to a queue with no receivers
         Session sessSend = theConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer theProducer = sessSend.createProducer(queue2);
         theProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
         
   
         Message m = sessSend.createMessage();
         m.setStringProperty("p1", "aardvark");
         
         BytesMessage bm = sessSend.createBytesMessage();
         bm.writeObject("aardvark");
         
         MapMessage mm = sessSend.createMapMessage();
         mm.setString("s1", "aardvark");
         
         ObjectMessage om = sessSend.createObjectMessage();
         om.setObject("aardvark");
         
         StreamMessage sm = sessSend.createStreamMessage();
         sm.writeString("aardvark");
         
         TextMessage tm = sessSend.createTextMessage("aardvark");
        
         
         theProducer.send(m);
         theProducer.send(bm);
         theProducer.send(mm);
         theProducer.send(om);
         theProducer.send(sm);
         theProducer.send(tm);
         
         theConn.close();
         
         
         theOtherConn = cf.createConnection();
         theOtherConn.start();
         
         Session sessReceive = theOtherConn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageConsumer theConsumer = sessReceive.createConsumer(queue2);
         
         Message m2 = theConsumer.receive(1500);
         
         log.trace("m2 is " + m2);
         
         assertNotNull(m2);
         
         
         assertEquals("aardvark", m2.getStringProperty("p1"));
         
         BytesMessage bm2 = (BytesMessage)theConsumer.receive(1500);
         assertEquals("aardvark", bm2.readUTF());
         
         MapMessage mm2 = (MapMessage)theConsumer.receive(1500);
         assertEquals("aardvark", mm2.getString("s1"));
         
         ObjectMessage om2 = (ObjectMessage)theConsumer.receive(1500);
         assertEquals("aardvark", (String)om2.getObject());
         
         StreamMessage sm2 = (StreamMessage)theConsumer.receive(1500);
         assertEquals("aardvark", sm2.readString());
      }
      finally
      {
         if (theConn != null)
         {
            theConn.close();
         }
         if (theOtherConn != null)
         {
            theOtherConn.close();
         }
      }
      

   }
   

   

   public void testDurableSubscriptionSimple() throws Exception
   {
      final String CLIENT_ID1 = "test-client-id1";

      Connection conn1 = null;
      
      try
      {
      
         conn1 = cf.createConnection();
   
   
         conn1.setClientID(CLIENT_ID1);
   
   
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sess1.createProducer(topic);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
   
         MessageConsumer durable = sess1.createDurableSubscriber(topic, "mySubscription");
   
         conn1.start();
   
         final int NUM_MESSAGES = 50;
   
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("hello");
            prod.send(topic, tm);
         }
   
         int count = 0;
         while (true)
         {
            TextMessage tm = (TextMessage)durable.receive(1500);
            if (tm == null)
            {
               break;
            }
            count++;
         }
   
         assertEquals(NUM_MESSAGES, count);
   
         sess1.unsubscribe("mySubscription");
      }
      finally
      {
         if (conn1 != null)
         {
            conn1.close();
         }
      }

   }

   
   public void testDurableSubscriptionMultipleSubscriptions() throws Exception
   {
      final String CLIENT_ID1 = "test-client-id1";

      Connection conn1 = null;
      Connection conn2 = null;
      
      try
      {
         
         conn1 = cf.createConnection();
   
         conn1.setClientID(CLIENT_ID1);
   
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
   
         sess1.createDurableSubscriber(topic, "mySubscription1");
         sess1.createDurableSubscriber(topic, "mySubscription2");
               
         conn1.close();
         
         
         
         conn2 = cf.createConnection();
         conn2.setClientID(CLIENT_ID1);
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = sess2.createProducer(topic);
         producer.setDeliveryMode(DeliveryMode.PERSISTENT);
   
         final int NUM_MESSAGES = 50;
    
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess2.createTextMessage("hello");
            producer.send(tm);
         }
         
         sess2.unsubscribe("mySubscription1");
         
         conn2.close();
         
         
         
         
         Connection conn3 = cf.createConnection();
         conn3.setClientID(CLIENT_ID1);
         conn3.start();
         Session sess3 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer durable3 = sess3.createDurableSubscriber(topic, "mySubscription2");
         
         int count = 0;
         while (true)
         {
            TextMessage tm = (TextMessage)durable3.receive(1000);
            if (tm == null)
            {
               break;
            }
            assertEquals("hello", tm.getText());
            count++;
         }
         
         assertEquals(NUM_MESSAGES, count);
         
         MessageConsumer durable4 = sess3.createDurableSubscriber(topic, "mySubscription1");
         
         Message m = durable4.receive(1000);
         assertNull(m);
      }
      finally
      {
         if (conn1 != null)
         {
            conn1.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }
      
   }
   
   

   public void testDurableSubscriptionDataRemaining() throws Exception
   {
      final String CLIENT_ID1 = "test-client-id1";
      
      Connection conn1 = null;
      Connection conn2 = null;
      Connection conn3 = null;
      Connection conn4 = null;
      Connection conn5 = null;
      Connection conn6 = null;

      try
      {
         
         //Create a durable subscriber on one connection and close it
         conn1 = cf.createConnection();
         conn1.setClientID(CLIENT_ID1);
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);      
         MessageConsumer durable = sess1.createDurableSubscriber(topic, "mySubscription");      
         conn1.close();
         
         
         //Send some messages on another connection and close it
         conn2 = cf.createConnection();
         conn2.setClientID(CLIENT_ID1);
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod2 = sess2.createProducer(topic); 
         prod2.setDeliveryMode(DeliveryMode.PERSISTENT);
         final int NUM_MESSAGES = 10;
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess2.createTextMessage("hello");
            prod2.send(topic, tm);
         }
         conn2.close();
         
         //Receive the messages on another connection
         conn3 = cf.createConnection();      
         conn3.setClientID(CLIENT_ID1);
         conn3.start();
         Session sess3 = conn3.createSession(false, Session.AUTO_ACKNOWLEDGE);
         durable = sess3.createDurableSubscriber(topic, "mySubscription");
         int count = 0;
         while (true)
         {
            TextMessage tm = (TextMessage)durable.receive(1000);
            if (tm == null)
            {
               break;
            }
            assertEquals("hello", tm.getText());
            count++;
         }
         assertEquals(NUM_MESSAGES, count);
         conn3.close();
         
         //Try and receive them again
         conn4 = cf.createConnection();
         conn4.setClientID(CLIENT_ID1);
         conn4.start();
         Session sess4 = conn4.createSession(false, Session.AUTO_ACKNOWLEDGE);
         durable = sess4.createDurableSubscriber(topic, "mySubscription");
   
         TextMessage tm = (TextMessage)durable.receive(1000);
         assertNull(tm);
         conn4.close();
         
         //Send some more messages and unsubscribe
         conn5 = cf.createConnection();
         conn5.setClientID(CLIENT_ID1);
         conn5.start();
         Session sess5 = conn5.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod5 = sess5.createProducer(topic); 
         prod5.setDeliveryMode(DeliveryMode.PERSISTENT);      
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm2 = sess5.createTextMessage("hello");
            prod5.send(topic, tm2);
         }
         sess5.unsubscribe("mySubscription");
         conn5.close();
         
         //Resubscribe with the same name
         conn6 = cf.createConnection();
         conn6.setClientID(CLIENT_ID1);
         conn6.start();
         Session sess6 = conn6.createSession(false, Session.AUTO_ACKNOWLEDGE);
         durable = sess6.createDurableSubscriber(topic, "mySubscription");
   
         TextMessage tm3 = (TextMessage)durable.receive(1000);
         assertNull(tm3);
      }
      finally
      {
         if (conn1 != null)
         {
            conn1.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
         if (conn3 != null)
         {
            conn3.close();
         }
         if (conn4 != null)
         {
            conn4.close();
         }
         if (conn5 != null)
         {
            conn5.close();
         }
         if (conn6 != null)
         {
            conn6.close();
         }
      }
      
   }
   
   
   public void testDurableSubscriptionReconnect() throws Exception
   {
      final String CLIENT_ID1 = "test-client-id1";

      Connection conn1 = null;
      Connection conn2 = null;
      
      try
      {
      
         conn1 = cf.createConnection();
         conn1.setClientID(CLIENT_ID1);
   
   
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sess1.createProducer(topic);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
   
   
         MessageConsumer durable = sess1.createDurableSubscriber(topic, "mySubscription");
   
         conn1.start();
   
         final int NUM_MESSAGES = 2;
   
   
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("hello");
            prod.send(topic, tm);
         }
   
         final int NUM_TO_RECEIVE = 1;
   
         for (int i = 0; i < NUM_TO_RECEIVE; i++)
         {
            TextMessage tm = (TextMessage)durable.receive(3000);
            assertNotNull(tm);
         }
   
         // Close the connection
         conn1.close();
         conn1 = null;
   
         conn2 = cf.createConnection();
   
         conn2.setClientID(CLIENT_ID1);
   
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
   
         // Re-subscribe to the subscription
   
         log.trace("Resubscribing");
   
         MessageConsumer durable2 = sess2.createDurableSubscriber(topic, "mySubscription");
   
         conn2.start();
   
         int count = 0;
         while (true)
         {
            TextMessage tm = (TextMessage)durable2.receive(1500);
            if (tm == null)
            {
               break;
            }
            count++;
         }
   
         log.trace("Received " + count  + " messages");
   
         assertEquals(NUM_MESSAGES - NUM_TO_RECEIVE, count);
   
         sess2.unsubscribe("mySubscription");
      }
      finally
      {
         if (conn1 != null)
         {
            conn1.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }


   }

   public void testDurableSubscriptionReconnectDifferentClientID() throws Exception
   {
      final String CLIENT_ID1 = "test-client-id1";
      final String CLIENT_ID2 = "test-client-id2";

      Connection conn1 = null;
      Connection conn2 = null;
      
      try
      {
         
         conn1 = cf.createConnection();
   
   
         conn1.setClientID(CLIENT_ID1);
   
   
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = sess1.createProducer(topic);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);
   
         MessageConsumer durable = sess1.createDurableSubscriber(topic, "mySubscription");
   
         conn1.start();
   
         final int NUM_MESSAGES = 50;
   
   
         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess1.createTextMessage("hello");
            prod.send(topic, tm);
         }
   
         final int NUM_TO_RECEIVE1 = 22;
   
         for (int i = 0; i < NUM_TO_RECEIVE1; i++)
         {
            TextMessage tm = (TextMessage)durable.receive(1500);
            if (tm == null)
            {
               fail();
            }
         }
   
         //Close the connection
         conn1.close();
         conn1 = null;
   
         conn2 = cf.createConnection();
   
         conn2.setClientID(CLIENT_ID2);
   
         Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);
   
         //Re-subscribe to the subscription
         MessageConsumer durable2 = sess2.createDurableSubscriber(topic, "mySubscription");
   
         conn2.start();
   
         int count = 0;
         while (true)
         {
            TextMessage tm = (TextMessage)durable2.receive(1500);
            if (tm == null)
            {
               break;
            }
            count++;
         }
   
         assertEquals(0, count);
   
         sess2.unsubscribe("mySubscription");
      }
      finally
      {
         if (conn1 != null)
         {
            conn1.close();
         }
         if (conn2 != null)
         {
            conn2.close();
         }
      }

   }



   public void testDurableSubscriptionInvalidUnsubscribe() throws Exception
   {
      final String CLIENT_ID1 = "test-client-id1";

      Connection conn1 = null;
      
      try
      {
         conn1 = cf.createConnection();
      
   
         conn1.setClientID(CLIENT_ID1);
   
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
   
         try
         {
            sess1.unsubscribe("non-existent subscription");
            fail();
         }
         catch (JMSException e)
         {
         }
      }
      finally
      {
         if (conn1 != null)
         {
            conn1.close();
         }
      }
   }




   public void testDurableSubscriptionClientIDNotSet() throws Exception
   {
      //Client id must be set before creating a durable subscription
      //This assumes we are not setting it in the connection factory which
      //is currently true but may change in the future

      Connection conn1 = null;
      
      try
      {
      
         conn1 = cf.createConnection();
   
         assertNull(conn1.getClientID());
   
         Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
   
         try
         {
            sess1.createDurableSubscriber(topic, "mySubscription");
            fail();
         }
         catch (JMSException e)
         {}
      
      }
      finally
      {
         if (conn1 != null)
         {
            conn1.close();
         }
      }
   }
   

   
   public void testRedeliveredDifferentSessions() throws Exception
   {
      Session sessProducer = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      
      MessageProducer prod = sessProducer.createProducer(queue);
      TextMessage tm = sessProducer.createTextMessage("testRedeliveredDifferentSessions");
      prod.send(tm);
      
      
      consumerConnection.start();
      Session sess1 = consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      MessageConsumer cons1 = sess1.createConsumer(topic);
      TextMessage tm2 = (TextMessage)cons1.receive(3000);
      assertNotNull(tm2);
      assertEquals("testRedeliveredDifferentSessions", tm2.getText());
      //don't acknowledge it
      sess1.close();
      
      consumerConnection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
      sess1.createConsumer(topic);
      TextMessage tm3 = (TextMessage)cons1.receive(3000);
      assertNotNull(tm3);
      assertEquals("testRedeliveredDifferentSessions", tm3.getText());
      
      assertTrue(tm3.getJMSRedelivered());
      
   }



   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   private class MessageListenerImpl implements MessageListener
   {
      private List messages = Collections.synchronizedList(new ArrayList());
      private Latch latch = new Latch();

      /** Blocks the calling thread until at least a message is received */
      public void waitForMessages() throws InterruptedException
      {
         latch.acquire();
      }

      public void onMessage(Message m)
      {
         messages.add(m);
         log.info("Added message " + m + " to my list");

         latch.release();

      };

      public Message getNextMessage()
      {
         Iterator i = messages.iterator();
         if (!i.hasNext())
         {
            return null;
         }
         Message m = (Message)i.next();
         i.remove();
         return m;
      }

      public int size()
      {
         return messages.size();
      }

      public void clear()
      {
         messages.clear();
      }
   }
}
