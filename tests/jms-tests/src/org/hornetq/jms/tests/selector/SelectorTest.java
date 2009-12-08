/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.jms.tests.selector;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.jms.tests.HornetQServerTestCase;
import org.hornetq.jms.tests.util.ProxyAssertSupport;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class SelectorTest extends HornetQServerTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * Test case for http://jira.jboss.org/jira/browse/JBMESSAGING-105
    *
    * Two Messages are sent to a queue. There is one receiver on the queue. The receiver only
    * receives one of the messages due to a message selector only matching one of them. The receiver
    * is then closed. A new receiver is now attached to the queue. Redelivery of the remaining
    * message is now attempted. The message should be redelivered.
    */
   public void testSelectiveClosingConsumer() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = getConnectionFactory().createConnection();
         conn.start();

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prod = session.createProducer(HornetQServerTestCase.queue1);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         String selector = "color = 'red'";
         MessageConsumer redConsumer = session.createConsumer(HornetQServerTestCase.queue1, selector);
         conn.start();

         Message redMessage = session.createMessage();
         redMessage.setStringProperty("color", "red");

         Message blueMessage = session.createMessage();
         blueMessage.setStringProperty("color", "blue");

         prod.send(redMessage);
         prod.send(blueMessage);

         Message rec = redConsumer.receive();
         ProxyAssertSupport.assertEquals(redMessage.getJMSMessageID(), rec.getJMSMessageID());
         ProxyAssertSupport.assertEquals("red", rec.getStringProperty("color"));

         ProxyAssertSupport.assertNull(redConsumer.receive(3000));

         redConsumer.close();

         MessageConsumer universalConsumer = session.createConsumer(HornetQServerTestCase.queue1);

         rec = universalConsumer.receive();

         ProxyAssertSupport.assertEquals(rec.getJMSMessageID(), blueMessage.getJMSMessageID());
         ProxyAssertSupport.assertEquals("blue", rec.getStringProperty("color"));
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testManyTopic() throws Exception
   {
      String selector1 = "beatle = 'john'";

      Connection conn = null;

      try
      {
         conn = getConnectionFactory().createConnection();
         conn.start();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons1 = sess.createConsumer(HornetQServerTestCase.topic1, selector1);

         MessageProducer prod = sess.createProducer(HornetQServerTestCase.topic1);

         for (int j = 0; j < 100; j++)
         {
            Message m = sess.createMessage();

            m.setStringProperty("beatle", "john");

            prod.send(m);

            m = sess.createMessage();

            m.setStringProperty("beatle", "kermit the frog");

            prod.send(m);
         }

         for (int j = 0; j < 100; j++)
         {
            Message m = cons1.receive(1000);

            ProxyAssertSupport.assertNotNull(m);
         }

         Thread.sleep(500);

         Message m = cons1.receiveNoWait();

         ProxyAssertSupport.assertNull(m);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testManyQueue() throws Exception
   {
      String selector1 = "beatle = 'john'";

      Connection conn = null;

      try
      {
         conn = getConnectionFactory().createConnection();
         conn.start();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons1 = sess.createConsumer(HornetQServerTestCase.queue1, selector1);

         MessageProducer prod = sess.createProducer(HornetQServerTestCase.queue1);

         for (int j = 0; j < 100; j++)
         {
            Message m = sess.createMessage();

            m.setStringProperty("beatle", "john");

            prod.send(m);

            m = sess.createMessage();

            m.setStringProperty("beatle", "kermit the frog");

            prod.send(m);
         }

         for (int j = 0; j < 100; j++)
         {
            Message m = cons1.receive(1000);

            ProxyAssertSupport.assertNotNull(m);

            ProxyAssertSupport.assertEquals("john", m.getStringProperty("beatle"));
         }

         Message m = cons1.receiveNoWait();

         ProxyAssertSupport.assertNull(m);

         String selector2 = "beatle = 'kermit the frog'";

         MessageConsumer cons2 = sess.createConsumer(HornetQServerTestCase.queue1, selector2);

         for (int j = 0; j < 100; j++)
         {
            m = cons2.receive(1000);

            ProxyAssertSupport.assertNotNull(m);

            ProxyAssertSupport.assertEquals("kermit the frog", m.getStringProperty("beatle"));
         }

         m = cons2.receiveNoWait();

         ProxyAssertSupport.assertNull(m);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }

   }

   // http://jira.jboss.org/jira/browse/JBMESSAGING-775

   public void testManyQueueWithExpired() throws Exception
   {
      String selector1 = "beatle = 'john'";

      Connection conn = null;

      try
      {
         conn = getConnectionFactory().createConnection();
         conn.start();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         int NUM_MESSAGES = 2;

         MessageProducer prod = sess.createProducer(HornetQServerTestCase.queue1);

         for (int j = 0; j < NUM_MESSAGES; j++)
         {
            Message m = sess.createMessage();

            m.setStringProperty("beatle", "john");

            prod.setTimeToLive(0);

            prod.send(m);

            m = sess.createMessage();

            m.setStringProperty("beatle", "john");

            prod.setTimeToLive(1);

            prod.send(m);

            m = sess.createMessage();

            m.setStringProperty("beatle", "kermit the frog");

            prod.setTimeToLive(0);

            prod.send(m);

            m = sess.createMessage();

            m.setStringProperty("beatle", "kermit the frog");

            m.setJMSExpiration(System.currentTimeMillis());

            prod.setTimeToLive(1);

            prod.send(m);
         }

         MessageConsumer cons1 = sess.createConsumer(HornetQServerTestCase.queue1, selector1);

         for (int j = 0; j < NUM_MESSAGES; j++)
         {
            Message m = cons1.receive(1000);

            ProxyAssertSupport.assertNotNull(m);

            ProxyAssertSupport.assertEquals("john", m.getStringProperty("beatle"));
         }

         Message m = cons1.receiveNoWait();

         ProxyAssertSupport.assertNull(m);

         String selector2 = "beatle = 'kermit the frog'";

         MessageConsumer cons2 = sess.createConsumer(HornetQServerTestCase.queue1, selector2);

         for (int j = 0; j < NUM_MESSAGES; j++)
         {
            m = cons2.receive(1000);

            ProxyAssertSupport.assertNotNull(m);

            ProxyAssertSupport.assertEquals("kermit the frog", m.getStringProperty("beatle"));
         }

         m = cons1.receiveNoWait();

         ProxyAssertSupport.assertNull(m);
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testManyRedeliveriesTopic() throws Exception
   {
      String selector1 = "beatle = 'john'";

      Connection conn = null;

      try
      {
         conn = getConnectionFactory().createConnection();
         conn.start();

         for (int i = 0; i < 5; i++)
         {
            Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer cons1 = sess.createConsumer(HornetQServerTestCase.topic1, selector1);

            MessageProducer prod = sess.createProducer(HornetQServerTestCase.topic1);

            for (int j = 0; j < 10; j++)
            {
               Message m = sess.createMessage();

               m.setStringProperty("beatle", "john");

               prod.send(m);

               m = sess.createMessage();

               m.setStringProperty("beatle", "kermit the frog");

               prod.send(m);
            }

            for (int j = 0; j < 10; j++)
            {
               Message m = cons1.receive(1000);

               ProxyAssertSupport.assertNotNull(m);
            }

            Message m = cons1.receiveNoWait();

            ProxyAssertSupport.assertNull(m);

            sess.close();
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

   public void testManyRedeliveriesQueue() throws Exception
   {
      String selector1 = "beatle = 'john'";

      Connection conn = null;

      try
      {
         conn = getConnectionFactory().createConnection();

         conn.start();

         for (int i = 0; i < 5; i++)
         {
            Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

            MessageConsumer cons1 = sess.createConsumer(HornetQServerTestCase.queue1, selector1);

            MessageProducer prod = sess.createProducer(HornetQServerTestCase.queue1);

            for (int j = 0; j < 10; j++)
            {
               TextMessage m = sess.createTextMessage("message-a-" + j);

               m.setStringProperty("beatle", "john");

               prod.send(m);

               m = sess.createTextMessage("messag-b-" + j);

               m.setStringProperty("beatle", "kermit the frog");

               prod.send(m);
            }

            for (int j = 0; j < 10; j++)
            {
               Message m = cons1.receive(1000);

               ProxyAssertSupport.assertNotNull(m);
            }

            Message m = cons1.receiveNoWait();

            ProxyAssertSupport.assertNull(m);

            sess.close();
         }
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }

         removeAllMessages(HornetQServerTestCase.queue1.getQueueName(), true);
      }
   }

   public void testWithSelector() throws Exception
   {
      String selector1 = "beatle = 'john'";
      String selector2 = "beatle = 'paul'";
      String selector3 = "beatle = 'george'";
      String selector4 = "beatle = 'ringo'";
      String selector5 = "beatle = 'jesus'";

      Connection conn = null;

      try
      {
         conn = getConnectionFactory().createConnection();
         conn.start();
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer cons1 = sess.createConsumer(HornetQServerTestCase.topic1, selector1);
         MessageConsumer cons2 = sess.createConsumer(HornetQServerTestCase.topic1, selector2);
         MessageConsumer cons3 = sess.createConsumer(HornetQServerTestCase.topic1, selector3);
         MessageConsumer cons4 = sess.createConsumer(HornetQServerTestCase.topic1, selector4);
         MessageConsumer cons5 = sess.createConsumer(HornetQServerTestCase.topic1, selector5);

         Message m1 = sess.createMessage();
         m1.setStringProperty("beatle", "john");

         Message m2 = sess.createMessage();
         m2.setStringProperty("beatle", "paul");

         Message m3 = sess.createMessage();
         m3.setStringProperty("beatle", "george");

         Message m4 = sess.createMessage();
         m4.setStringProperty("beatle", "ringo");

         Message m5 = sess.createMessage();
         m5.setStringProperty("beatle", "jesus");

         MessageProducer prod = sess.createProducer(HornetQServerTestCase.topic1);

         prod.send(m1);
         prod.send(m2);
         prod.send(m3);
         prod.send(m4);
         prod.send(m5);

         Message r1 = cons1.receive(500);
         ProxyAssertSupport.assertNotNull(r1);
         Message n = cons1.receive(500);
         ProxyAssertSupport.assertNull(n);

         Message r2 = cons2.receive(500);
         ProxyAssertSupport.assertNotNull(r2);
         n = cons2.receive(500);
         ProxyAssertSupport.assertNull(n);

         Message r3 = cons3.receive(500);
         ProxyAssertSupport.assertNotNull(r3);
         n = cons3.receive(500);
         ProxyAssertSupport.assertNull(n);

         Message r4 = cons4.receive(500);
         ProxyAssertSupport.assertNotNull(r4);
         n = cons4.receive(500);
         ProxyAssertSupport.assertNull(n);

         Message r5 = cons5.receive(500);
         ProxyAssertSupport.assertNotNull(r5);
         n = cons5.receive(500);
         ProxyAssertSupport.assertNull(n);

         ProxyAssertSupport.assertEquals("john", r1.getStringProperty("beatle"));
         ProxyAssertSupport.assertEquals("paul", r2.getStringProperty("beatle"));
         ProxyAssertSupport.assertEquals("george", r3.getStringProperty("beatle"));
         ProxyAssertSupport.assertEquals("ringo", r4.getStringProperty("beatle"));
         ProxyAssertSupport.assertEquals("jesus", r5.getStringProperty("beatle"));
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void testManyConsumersWithDifferentSelectors() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = getConnectionFactory().createConnection();
         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = sess.createProducer(HornetQServerTestCase.queue1);

         Session cs = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final MessageConsumer c = cs.createConsumer(HornetQServerTestCase.queue1, "weight = 1");

         Session cs2 = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final MessageConsumer c2 = cs2.createConsumer(HornetQServerTestCase.queue1, "weight = 2");

         for (int i = 0; i < 10; i++)
         {
            Message m = sess.createTextMessage("message" + i);
            m.setIntProperty("weight", i % 2 + 1);
            p.send(m);
         }

         conn.start();

         final List received = new ArrayList();
         final List received2 = new ArrayList();
         final CountDownLatch latch = new CountDownLatch(1);
         final CountDownLatch latch2 = new CountDownLatch(1);

         new Thread(new Runnable()
         {
            public void run()
            {
               try
               {
                  while (true)
                  {
                     Message m = c.receive(1000);
                     if (m != null)
                     {
                        received.add(m);
                     }
                     else
                     {
                        latch.countDown();
                        return;
                     }
                  }
               }
               catch (Exception e)
               {
                  log.error("receive failed", e);
               }
            }
         }, "consumer thread 1").start();

         new Thread(new Runnable()
         {
            public void run()
            {
               try
               {
                  while (true)
                  {
                     Message m = c2.receive(1000);
                     if (m != null)
                     {
                        received2.add(m);
                     }
                     else
                     {
                        latch2.countDown();
                        return;
                     }
                  }
               }
               catch (Exception e)
               {
                  log.error("receive failed", e);
               }
            }
         }, "consumer thread 2").start();

         latch.await();
         latch2.await();

         ProxyAssertSupport.assertEquals(5, received.size());
         for (Iterator i = received.iterator(); i.hasNext();)
         {
            Message m = (Message)i.next();
            int value = m.getIntProperty("weight");
            ProxyAssertSupport.assertEquals(value, 1);
         }

         ProxyAssertSupport.assertEquals(5, received2.size());
         for (Iterator i = received2.iterator(); i.hasNext();)
         {
            Message m = (Message)i.next();
            int value = m.getIntProperty("weight");
            ProxyAssertSupport.assertEquals(value, 2);
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

   public void testDeliveryModeOnSelector() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = getConnectionFactory().createConnection();
         conn.start();

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer prodNonPersistent = session.createProducer(HornetQServerTestCase.queue1);
         prodNonPersistent.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         MessageProducer prodPersistent = session.createProducer(HornetQServerTestCase.queue1);
         prodPersistent.setDeliveryMode(DeliveryMode.PERSISTENT);

         String selector = "JMSDeliveryMode = 'PERSISTENT'";
         MessageConsumer persistentConsumer = session.createConsumer(HornetQServerTestCase.queue1, selector);
         conn.start();

         TextMessage msg = session.createTextMessage("NonPersistent");
         prodNonPersistent.send(msg);

         msg = session.createTextMessage("Persistent");
         prodPersistent.send(msg);

         msg = (TextMessage)persistentConsumer.receive(2000);
         ProxyAssertSupport.assertNotNull(msg);
         ProxyAssertSupport.assertEquals(DeliveryMode.PERSISTENT, msg.getJMSDeliveryMode());
         ProxyAssertSupport.assertEquals("Persistent", msg.getText());

         ProxyAssertSupport.assertNull(persistentConsumer.receive(1000));

         persistentConsumer.close();

         MessageConsumer genericConsumer = session.createConsumer(HornetQServerTestCase.queue1);
         msg = (TextMessage)genericConsumer.receive(1000);

         ProxyAssertSupport.assertNotNull(msg);

         ProxyAssertSupport.assertEquals("NonPersistent", msg.getText());
         ProxyAssertSupport.assertEquals(DeliveryMode.NON_PERSISTENT, msg.getJMSDeliveryMode());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
