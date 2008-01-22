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

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.management.ObjectName;

import org.jboss.jms.destination.JBossQueue;
import org.jboss.jms.message.JBossMessage;
import org.jboss.messaging.core.impl.server.MessagingServerImpl;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * A DLQTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *          <p/>
 *          $Id$
 */
public class DLQTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   public DLQTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void testDLQAlreadyDeployed() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         return;
      }


      assertNotNull(((MessagingServerImpl)getJmsServer()).getDefaultDLQInstance());

      String name = getJmsServer().getConfiguration().getDefaultDLQ();

      assertNotNull(name);

      assertEquals("DLQ", name);

      JBossQueue q = (JBossQueue) ic.lookup("/queue/DLQ");

      assertNotNull(q);

      assertEquals("DLQ", q.getName());
   }


   public void testDefaultAndOverrideDLQ() throws Exception
   {
      if (ServerManagement.isRemote())
      {
         return;
      }

      final int NUM_MESSAGES = 5;

      final int MAX_DELIVERIES = 8;

      ObjectName serverPeerObjectName = ServerManagement.getServerPeerObjectName();

      String testQueueObjectName = "jboss.messaging.destination:service=Queue,name=Queue1";

      Connection conn = null;

      try
      {
         String defaultDLQObjectName = "jboss.messaging.destination:service=Queue,name=Queue2";

         String overrideDLQObjectName = "jboss.messaging.destination:service=Queue,name=Queue3";

         ServerManagement.setAttribute(serverPeerObjectName, "DefaultMaxDeliveryAttempts", String.valueOf(MAX_DELIVERIES));

         ServerManagement.setAttribute(serverPeerObjectName, "DefaultDLQ", defaultDLQObjectName);

         ServerManagement.setAttribute(new ObjectName(testQueueObjectName), "DLQ", "");

         conn = cf.createConnection();

         {
            Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer prod = sess.createProducer(queue1);

            for (int i = 0; i < NUM_MESSAGES; i++)
            {
               TextMessage tm = sess.createTextMessage("Message:" + i);

               prod.send(tm);
            }

            Session sess2 = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

            MessageConsumer cons = sess2.createConsumer(queue1);

            conn.start();

            for (int i = 0; i < MAX_DELIVERIES; i++)
            {
               for (int j = 0; j < NUM_MESSAGES; j++)
               {
                  TextMessage tm = (TextMessage) cons.receive(1000);

                  assertNotNull(tm);

                  assertEquals("Message:" + j, tm.getText());
               }

               sess2.recover();
            }

            //Prompt them to go to DLQ
            cons.receive(100);

            //At this point all the messages have been delivered exactly MAX_DELIVERIES times 

            checkEmpty(queue1);

            //Now should be in default dlq

            MessageConsumer cons3 = sess.createConsumer(queue2);

            for (int i = 0; i < NUM_MESSAGES; i++)
            {
               TextMessage tm = (TextMessage) cons3.receive(1000);

               assertNotNull(tm);

               assertEquals("Message:" + i, tm.getText());
            }

            conn.close();
         }


         {
            //Now try with overriding the default dlq

            conn = cf.createConnection();

            ServerManagement.setAttribute(new ObjectName(testQueueObjectName), "DLQ", overrideDLQObjectName);

            Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageProducer prod = sess.createProducer(queue1);

            for (int i = 0; i < NUM_MESSAGES; i++)
            {
               TextMessage tm = sess.createTextMessage("Message:" + i);

               prod.send(tm);
            }

            Session sess2 = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

            MessageConsumer cons = sess2.createConsumer(queue1);

            conn.start();

            for (int i = 0; i < MAX_DELIVERIES; i++)
            {
               for (int j = 0; j < NUM_MESSAGES; j++)
               {
                  TextMessage tm = (TextMessage) cons.receive(1000);

                  assertNotNull(tm);

                  assertEquals("Message:" + j, tm.getText());
               }

               sess2.recover();
            }

            cons.receive(100);

            //At this point all the messages have been delivered exactly MAX_DELIVERIES times 

            checkEmpty(queue1);

            //Now should be in override dlq

            MessageConsumer cons3 = sess.createConsumer(queue3);

            for (int i = 0; i < NUM_MESSAGES; i++)
            {
               TextMessage tm = (TextMessage) cons3.receive(1000);

               assertNotNull(tm);

               assertEquals("Message:" + i, tm.getText());
            }
         }
      }
      finally
      {
         ServerManagement.setAttribute(serverPeerObjectName, "DefaultDLQ", "jboss.messaging.destination:service=Queue,name=DLQ");

         ServerManagement.setAttribute(new ObjectName(testQueueObjectName), "DLQ", "");

         if (conn != null)
         {
            conn.close();
         }
      }
   }


   public void testWithMessageListenerPersistent() throws Exception
   {
      testWithMessageListener(true);
   }

   public void testWithMessageListenerNonPersistent() throws Exception
   {
      testWithMessageListener(false);
   }

   public void testWithReceiveClientAckPersistent() throws Exception
   {
      this.testWithReceiveClientAck(true);
   }

   public void testWithReceiveClientAckNonPersistent() throws Exception
   {
      testWithReceiveClientAck(false);
   }

   public void testWithReceiveTransactionalPersistent() throws Exception
   {
      this.testWithReceiveTransactional(true);
   }

   public void testWithReceiveTransactionalNonPersistent() throws Exception
   {
      testWithReceiveTransactional(false);
   }

   public void testHeadersSet() throws Exception
   {
      Connection conn = null;

      try
      {
         ObjectName serverPeerObjectName = ServerManagement.getServerPeerObjectName();

         ServerManagement.setAttribute(serverPeerObjectName, "DefaultDLQ", "jboss.messaging.destination:service=Queue,name=Queue2");

         final int MAX_DELIVERIES = 16;

         final int NUM_MESSAGES = 5;

         ServerManagement.setAttribute(serverPeerObjectName, "DefaultMaxDeliveryAttempts", String.valueOf(MAX_DELIVERIES));

         int maxRedeliveryAttempts =
                 ((Integer) ServerManagement.getAttribute(serverPeerObjectName, "DefaultMaxDeliveryAttempts")).intValue();

         assertEquals(MAX_DELIVERIES, maxRedeliveryAttempts);

         conn = cf.createConnection();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sess.createProducer(queue1);

         Map origIds = new HashMap();

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess.createTextMessage("Message:" + i);

            prod.send(tm);

            origIds.put(tm.getText(), tm.getJMSMessageID());
         }

         Session sess2 = conn.createSession(true, Session.SESSION_TRANSACTED);

         MessageConsumer cons = sess2.createConsumer(queue1);

         conn.start();

         for (int i = 0; i < MAX_DELIVERIES; i++)
         {
            for (int j = 0; j < NUM_MESSAGES; j++)
            {
               TextMessage tm = (TextMessage) cons.receive(1000);

               assertNotNull(tm);

               assertEquals("Message:" + j, tm.getText());
            }

            sess2.rollback();
         }

         //At this point all the messages have been delivered exactly MAX_DELIVERIES times - this is ok
         //they haven't exceeded max delivery attempts so shouldn't be in the DLQ - let's check

         checkEmpty(queue2);

         // So let's try and consume them - this should cause them to go to the DLQ - since they
         // will then exceed max delivery attempts
         Message m = cons.receive(100);

         assertNull(m);

         //All the messages should now be in the DLQ

         MessageConsumer cons3 = sess.createConsumer(queue2);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage) cons3.receive(1000);

            assertNotNull(tm);

            assertEquals("Message:" + i, tm.getText());

            // Check the headers
            String origDest =
                    tm.getStringProperty(JBossMessage.JBOSS_MESSAGING_ORIG_DESTINATION);

            String origMessageId =
                    tm.getStringProperty(JBossMessage.JBOSS_MESSAGING_ORIG_MESSAGE_ID);

            assertEquals(queue1.toString(), origDest);

            String origId = (String) origIds.get(tm.getText());

            assertEquals(origId, origMessageId);
         }
      }
      finally
      {
         if (conn != null) conn.close();
      }
   }

   public void testOverrideDefaultMaxDeliveryAttemptsForQueue() throws Exception
   {
      int md = getDefaultMaxDeliveryAttempts();
      try
      {
         int maxDeliveryAttempts = md - 5;
         setMaxDeliveryAttempts(
                 new ObjectName("jboss.messaging.destination:service=Queue,name=Queue1"),
                 maxDeliveryAttempts);
         testMaxDeliveryAttempts(queue1, maxDeliveryAttempts, true);
      }
      finally
      {
         setMaxDeliveryAttempts(
                 new ObjectName("jboss.messaging.destination:service=Queue,name=Queue1"),
                 md);
      }
   }

   public void testOverrideDefaultMaxDeliveryAttemptsForTopic() throws Exception
   {
      int md = getDefaultMaxDeliveryAttempts();
      try
      {
         int maxDeliveryAttempts = md - 5;
         setMaxDeliveryAttempts(
                 new ObjectName("jboss.messaging.destination:service=Topic,name=Topic1"),
                 maxDeliveryAttempts);

         testMaxDeliveryAttempts(topic1, maxDeliveryAttempts, false);
      }
      finally
      {
         setMaxDeliveryAttempts(
                 new ObjectName("jboss.messaging.destination:service=Queue,name=Queue1"),
                 md);
      }
   }

   public void testUseDefaultMaxDeliveryAttemptsForQueue() throws Exception
   {
      int md = getDefaultMaxDeliveryAttempts();
      try
      {
         setMaxDeliveryAttempts(
                 new ObjectName("jboss.messaging.destination:service=Queue,name=Queue1"),
                 -1);

         // Check that defaultMaxDeliveryAttempts takes effect
         testMaxDeliveryAttempts(queue1, getDefaultMaxDeliveryAttempts(), true);
      }
      finally
      {
         setMaxDeliveryAttempts(
                 new ObjectName("jboss.messaging.destination:service=Queue,name=Queue1"),
                 md);
      }
   }

   public void testUseDefaultMaxDeliveryAttemptsForTopic() throws Exception
   {
      int md = getDefaultMaxDeliveryAttempts();
      try
      {
         setMaxDeliveryAttempts(
                 new ObjectName("jboss.messaging.destination:service=Topic,name=Topic1"),
                 -1);

         // Check that defaultMaxDeliveryAttempts takes effect
         testMaxDeliveryAttempts(topic1, getDefaultMaxDeliveryAttempts(), false);
      }
      finally
      {
         setMaxDeliveryAttempts(
                 new ObjectName("jboss.messaging.destination:service=Queue,name=Queue1"),
                 md);
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   protected void testWithMessageListener(boolean persistent) throws Exception
   {
      Connection conn = null;

      try
      {
         ObjectName serverPeerObjectName = ServerManagement.getServerPeerObjectName();

         final int MAX_DELIVERIES = 16;

         final int NUM_MESSAGES = 5;

         ServerManagement.setAttribute(serverPeerObjectName, "DefaultMaxDeliveryAttempts", String.valueOf(MAX_DELIVERIES));

         String defaultDLQObjectName = "jboss.messaging.destination:service=Queue,name=Queue2";

         ServerManagement.setAttribute(serverPeerObjectName, "DefaultDLQ", defaultDLQObjectName);

         int maxRedeliveryAttempts =
                 ((Integer) ServerManagement.getAttribute(serverPeerObjectName, "DefaultMaxDeliveryAttempts")).intValue();

         assertEquals(MAX_DELIVERIES, maxRedeliveryAttempts);

         conn = cf.createConnection();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sess.createProducer(queue1);

         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess.createTextMessage("Message:" + i);

            prod.send(tm);
         }

         MessageConsumer cons = sess.createConsumer(queue1);

         FailingMessageListener listener = new FailingMessageListener(MAX_DELIVERIES * NUM_MESSAGES);

         cons.setMessageListener(listener);

         conn.start();

         listener.waitForMessages();

         assertEquals(MAX_DELIVERIES * NUM_MESSAGES, listener.deliveryCount);

         //Message should all be in the dlq - let's check

         MessageConsumer cons2 = sess.createConsumer(queue2);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage) cons2.receive(1000);

            assertNotNull(tm);

            log.info("Got mnessage" + tm);

            assertEquals("Message:" + i, tm.getText());
         }

         checkEmpty(queue1);
      }
      finally
      {
         if (conn != null) conn.close();
      }
   }


   protected void testWithReceiveClientAck(boolean persistent) throws Exception
   {
      Connection conn = null;

      try
      {
         ObjectName serverPeerObjectName = ServerManagement.getServerPeerObjectName();

         final int MAX_DELIVERIES = 16;

         final int NUM_MESSAGES = 5;

         String defaultDLQObjectName = "jboss.messaging.destination:service=Queue,name=Queue2";

         ServerManagement.setAttribute(serverPeerObjectName, "DefaultDLQ", defaultDLQObjectName);

         ServerManagement.setAttribute(serverPeerObjectName, "DefaultMaxDeliveryAttempts", String.valueOf(MAX_DELIVERIES));

         int maxRedeliveryAttempts =
                 ((Integer) ServerManagement.getAttribute(serverPeerObjectName, "DefaultMaxDeliveryAttempts")).intValue();

         assertEquals(MAX_DELIVERIES, maxRedeliveryAttempts);

         conn = cf.createConnection();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sess.createProducer(queue1);

         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess.createTextMessage("Message:" + i);

            prod.send(tm);
         }

         Session sess2 = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);

         MessageConsumer cons = sess2.createConsumer(queue1);

         conn.start();

         for (int i = 0; i < MAX_DELIVERIES; i++)
         {
            for (int j = 0; j < NUM_MESSAGES; j++)
            {
               TextMessage tm = (TextMessage) cons.receive(1000);

               assertNotNull(tm);

               assertEquals("Message:" + j, tm.getText());
            }

            sess2.recover();
         }

         //At this point all the messages have been delivered exactly MAX_DELIVERIES times - this is ok
         //they haven't exceeded max delivery attempts so shouldn't be in the DLQ - let's check

         checkEmpty(queue2);

         //So let's try and consume them - this should cause them to go to the DLQ - since they will then exceed max
         //delivery attempts

         Message m = cons.receive(100);

         assertNull(m);

         //Now, all the messages should now be in the DLQ

         MessageConsumer cons3 = sess.createConsumer(queue2);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage) cons3.receive(1000);

            assertNotNull(tm);

            assertEquals("Message:" + i, tm.getText());
         }

         //No more should be available

         cons.close();

         checkEmpty(queue1);
      }
      finally
      {
         destroyQueue("DLQ");

         if (conn != null) conn.close();
      }
   }

   protected void testWithReceiveTransactional(boolean persistent) throws Exception
   {
      Connection conn = null;

      try
      {
         ObjectName serverPeerObjectName = ServerManagement.getServerPeerObjectName();

         final int MAX_DELIVERIES = 16;

         final int NUM_MESSAGES = 5;

         ServerManagement.setAttribute(serverPeerObjectName, "DefaultMaxDeliveryAttempts", String.valueOf(MAX_DELIVERIES));

         String defaultDLQObjectName = "jboss.messaging.destination:service=Queue,name=Queue2";

         ServerManagement.setAttribute(serverPeerObjectName, "DefaultDLQ", defaultDLQObjectName);

         int maxRedeliveryAttempts =
                 ((Integer) ServerManagement.getAttribute(serverPeerObjectName, "DefaultMaxDeliveryAttempts")).intValue();

         assertEquals(MAX_DELIVERIES, maxRedeliveryAttempts);

         conn = cf.createConnection();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = sess.createProducer(queue1);

         prod.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = sess.createTextMessage("Message:" + i);

            prod.send(tm);
         }

         Session sess2 = conn.createSession(true, Session.SESSION_TRANSACTED);

         MessageConsumer cons = sess2.createConsumer(queue1);

         conn.start();

         for (int i = 0; i < MAX_DELIVERIES; i++)
         {
            for (int j = 0; j < NUM_MESSAGES; j++)
            {
               TextMessage tm = (TextMessage) cons.receive(1000);

               assertNotNull(tm);

               assertEquals("Message:" + j, tm.getText());
            }

            sess2.rollback();
         }

         //At this point all the messages have been delivered exactly MAX_DELIVERIES times - this is ok
         //they haven't exceeded max delivery attempts so shouldn't be in the DLQ - let's check

         checkEmpty(queue2);

         //So let's try and consume them - this should cause them to go to the DLQ - since they will then exceed max
         //delivery attempts
         Message m = cons.receive(100);

         assertNull(m);

         //All the messages should now be in the DLQ

         MessageConsumer cons3 = sess.createConsumer(queue2);

         for (int i = 0; i < NUM_MESSAGES; i++)
         {
            TextMessage tm = (TextMessage) cons3.receive(1000);

            assertNotNull(tm);

            assertEquals("Message:" + i, tm.getText());
         }

         //No more should be available

         checkEmpty(queue1);
      }
      finally
      {
         destroyQueue("DLQ");

         if (conn != null) conn.close();
      }
   }

   protected int getDefaultMaxDeliveryAttempts() throws Exception
   {
      return ((Integer) ServerManagement.getAttribute(
              ServerManagement.getServerPeerObjectName(),
              "DefaultMaxDeliveryAttempts"))
              .intValue();
   }

   protected void setMaxDeliveryAttempts(ObjectName dest, int maxDeliveryAttempts) throws Exception
   {
      ServerManagement.setAttribute(dest, "MaxDeliveryAttempts",
              Integer.toString(maxDeliveryAttempts));
   }

   protected void testMaxDeliveryAttempts(Destination destination, int destMaxDeliveryAttempts, boolean queue) throws Exception
   {
      Connection conn = cf.createConnection();

      if (!queue)
      {
         conn.setClientID("wib123");
      }

      try
      {
         ServerManagement.setAttribute(ServerManagement.getServerPeerObjectName(),
                 "DefaultDLQ", "jboss.messaging.destination:service=Queue,name=Queue2");

         // Create the consumer before the producer so that the message we send doesn't
         // get lost if the destination is a Topic.
         Session consumingSession = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageConsumer destinationConsumer;

         if (queue)
         {
            destinationConsumer = consumingSession.createConsumer(destination);
         }
         else
         {
            //For topics we only keep a delivery record on the server side for durable subs
            destinationConsumer = consumingSession.createDurableSubscriber((Topic) destination, "testsub1");
         }

         {
            Session producingSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer prod = producingSession.createProducer(destination);
            TextMessage tm = producingSession.createTextMessage("Message");
            prod.send(tm);
         }

         conn.start();

         // Make delivery attempts up to the maximum. The message should not end up in the DLQ.
         for (int i = 0; i < destMaxDeliveryAttempts; i++)
         {
            TextMessage tm = (TextMessage) destinationConsumer.receive(1000);
            assertNotNull("No message received on delivery attempt number " + (i + 1), tm);
            assertEquals("Message", tm.getText());
            consumingSession.recover();
         }

         // At this point the message should not yet be in the DLQ
         checkEmpty(queue2);

         // Now we try to consume the message again from the destination, which causes it
         // to go to the DLQ instead.
         Message m = destinationConsumer.receive(100);
         assertNull(m);

         // The message should be in the DLQ now
         MessageConsumer dlqConsumer = consumingSession.createConsumer(queue2);
         m = dlqConsumer.receive(1000);
         assertNotNull(m);
         assertTrue(m instanceof TextMessage);
         assertEquals("Message", ((TextMessage) m).getText());

         m.acknowledge();

         if (!queue)
         {
            destinationConsumer.close();

            consumingSession.unsubscribe("testsub1");
         }
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

   class FailingMessageListener implements MessageListener
   {
      volatile int deliveryCount;

      int numMessages;

      FailingMessageListener(int numMessages)
      {
         this.numMessages = numMessages;
      }

      synchronized void waitForMessages() throws Exception
      {
         while (deliveryCount != numMessages)
         {
            this.wait();
         }
      }

      public synchronized void onMessage(Message msg)
      {
         deliveryCount++;

         this.notify();

         throw new RuntimeException("Your mum!");
      }

   }

}
