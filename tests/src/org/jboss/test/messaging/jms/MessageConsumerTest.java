/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;
import org.jboss.jms.util.InVMInitialContextFactory;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.JMSException;
import javax.jms.DeliveryMode;
import javax.naming.InitialContext;
import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Iterator;

import EDU.oswego.cs.dl.util.concurrent.Latch;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
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

      ServerManagement.startInVMServer();
      ServerManagement.deployTopic("Topic");
      ServerManagement.deployQueue("Queue");

      InitialContext ic = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/messaging/ConnectionFactory");
      Destination topic = (Destination)ic.lookup("/messaging/topics/Topic");
      Destination queue = (Destination)ic.lookup("/messaging/queues/Queue");

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

   public void testReceiveOnTopicTimeoutNoMessage() throws Exception
   {
      Message m = topicConsumer.receive(1000);
      assertNull(m);
   }

   public void testReceiveOnTopicConnectionStopped() throws Exception
   {
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
      assertEquals(m1.getJMSMessageID(), m2.getJMSMessageID());
   }

   /**
    * The test sends a burst of messages and verifies if the consumer receives all of them.
    */
   public void testStressReceiveOnQueue() throws Exception
   {

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









   // TODO uncomment this when I fix consumer delivery
//   /**
//    * The test sends a burst of messages and verifies if the consumer receives all of them.
//    */
//   public void testStressReceiveOnTopic() throws Exception
//   {
//
//      final int count = 100;
//
//      consumerConnection.start();
//
//      new Thread(new Runnable()
//      {
//         public void run()
//         {
//            try
//            {
//               // this is needed to make sure the main thread has enough time to block
//               Thread.sleep(1000);
//
//
//               for (int i = 0; i < count; i++)
//               {
//                  Message m = producerSession.createMessage();
//                  topicProducer.send(m);
//               }
//            }
//            catch(Exception e)
//            {
//               log.error(e);
//            }
//         }
//      }, "ProducerTestThread").start();
//
//      int received = 0;
//      while(true)
//      {
//         Message m = topicConsumer.receive(3000);
//         if (m == null)
//         {
//            break;
//         }
//         Thread.sleep(1000);
//         received++;
//      }
//
//      assertEquals(count, received);
//
//   }

   //
   // MessageListener tests
   //

   public void testMessageListenerOnTopic() throws Exception
   {
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
      MessageListenerImpl listener1 = new MessageListenerImpl();
      topicConsumer.setMessageListener(listener1);

      MessageListenerImpl listener2 = new MessageListenerImpl();
      topicConsumer.setMessageListener(listener2);

      consumerConnection.start();

      Message m1 = producerSession.createMessage();
      topicProducer.send(m1);

      // block the current thread until the listener gets something; this is to avoid closing
      // the connection too early
      listener2.waitForMessages();

      assertEquals(m1.getJMSMessageID(), listener2.getNextMessage().getJMSMessageID());
      assertEquals(0, listener1.size());
   }

   public void testSetMessageListenerWhileReceiving() throws Exception
   {
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
