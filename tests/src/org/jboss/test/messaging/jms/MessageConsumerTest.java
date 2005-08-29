/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;

import java.util.*;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueReceiver;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import javax.naming.InitialContext;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.jms.server.remoting.JMSServerInvocationHandler;
import org.jboss.remoting.transport.Connector;
import org.jboss.remoting.ServerInvoker;
import org.jboss.messaging.core.Channel;

import EDU.oswego.cs.dl.util.concurrent.Latch;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
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

      ServerManagement.setRemote(false);
      ServerManagement.startInVMServer("all");
      ServerManagement.deployTopic("Topic");
      ServerManagement.deployQueue("Queue");

      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      topic = (Topic)ic.lookup("/topic/Topic");
      queue = (Queue)ic.lookup("/queue/Queue");

      
      
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
      ServerManagement.stopInVMServer();

      super.tearDown();
   }


   /**
    * TODO Get rid of this (http://jira.jboss.org/jira/browse/JBMESSAGING-92)
    */
   public void testRemotingInternals() throws Exception
   {
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

      assertNull(topicConsumer.receive(2000));
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

      Message m2 = topicConsumer.receive(2000);
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

      Message m2 = topicConsumer.receive();

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
         Message m = queueConsumer.receive(3000);
         if (m == null)
         {
            break;
         }
         Thread.sleep(1000);
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
         Message m = topicConsumer.receive(3000);
         if (m == null)
         {
            break;
         }
         Thread.sleep(1000);
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

      assertNull(topicConsumer.receive());

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
      assertNull(topicConsumer.receive(5000));
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
               topicConsumer.receive();
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



   public void testNoLocal() throws Exception
   {
      if (log.isTraceEnabled()) log.trace("testNoLocal");

      Connection conn1 = cf.createConnection();
      Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);

      MessageProducer producer1 = sess1.createProducer(topic);
      producer1.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      MessageConsumer consumer1 = sess1.createConsumer(topic, null, true);

      Connection conn2 = cf.createConnection();
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
               m = consumer.receive(3000);
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

      conn1.close();
      conn2.close();

   }




   public void testDurableSubscriptionSimple() throws Exception
   {
      final String CLIENT_ID1 = "test-client-id1";

      Connection conn1 = cf.createConnection();


      conn1.setClientID(CLIENT_ID1);


      Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = sess1.createProducer(topic);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

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
         TextMessage tm = (TextMessage)durable.receive(3000);
         if (tm == null)
         {
            break;
         }
         count++;
      }

      assertEquals(NUM_MESSAGES, count);

      sess1.unsubscribe("mySubscription");

      conn1.close();
   }



   /* This test will not work until JBMESSAGING-105 is fixed */
   public void testDurableSubscriptionReconnect() throws Exception
   {
      final String CLIENT_ID1 = "test-client-id1";

      Connection conn1 = cf.createConnection();
      conn1.setClientID(CLIENT_ID1);


      Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = sess1.createProducer(topic);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);


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
         TextMessage tm = (TextMessage)durable.receive();
         assertNotNull(tm);
      }

      // Close the connection
      conn1.close();
      conn1 = null;

      Connection conn2 = cf.createConnection();

      conn2.setClientID(CLIENT_ID1);

      Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

      // Re-subscribe to the subscription

      log.trace("Resubscribing");

      MessageConsumer durable2 = sess2.createDurableSubscriber(topic, "mySubscription");

      conn2.start();

      int count = 0;
      while (true)
      {
         TextMessage tm = (TextMessage)durable2.receive(3000);
         if (tm == null)
         {
            break;
         }
         count++;
      }

      log.trace("Received " + count  + " messages");

      assertEquals(NUM_MESSAGES - NUM_TO_RECEIVE, count);

      sess2.unsubscribe("mySubscription");

      conn2.close();

   }

   public void testDurableSubscriptionReconnectDifferentClientID() throws Exception
   {
      final String CLIENT_ID1 = "test-client-id1";
      final String CLIENT_ID2 = "test-client-id2";

      Connection conn1 = cf.createConnection();


      conn1.setClientID(CLIENT_ID1);


      Session sess1 = conn1.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer prod = sess1.createProducer(topic);
      prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

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
         TextMessage tm = (TextMessage)durable.receive(3000);
         if (tm == null)
         {
            fail();
         }
      }

      //Close the connection
      conn1.close();
      conn1 = null;

      Connection conn2 = cf.createConnection();

      conn2.setClientID(CLIENT_ID2);

      Session sess2 = conn2.createSession(false, Session.AUTO_ACKNOWLEDGE);

      //Re-subscribe to the subscription
      MessageConsumer durable2 = sess2.createDurableSubscriber(topic, "mySubscription");

      conn2.start();

      int count = 0;
      while (true)
      {
         TextMessage tm = (TextMessage)durable2.receive(3000);
         if (tm == null)
         {
            break;
         }
         count++;
      }

      assertEquals(0, count);

      sess2.unsubscribe("mySubscription");

      conn2.close();

   }



   public void testDurableSubscriptionInvalidUnsubscribe() throws Exception
   {
      final String CLIENT_ID1 = "test-client-id1";

      Connection conn1 = cf.createConnection();

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




   public void testDurableSubscriptionClientIDNotSet() throws Exception
   {
      //Client id must be set before creating a durable subscription
      //This assumes we are not setting it in the connection factory which
      //is currently true but may change in the future

      Connection conn1 = cf.createConnection();

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
