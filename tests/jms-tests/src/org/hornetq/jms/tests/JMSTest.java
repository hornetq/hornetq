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

package org.hornetq.jms.tests;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.jms.tests.util.ProxyAssertSupport;

/**
 * The most comprehensive, yet simple, unit test.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Foxv</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class JMSTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void test_NonPersistent_NonTransactional() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = JMSTestCase.cf.createConnection();

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = session.createProducer(HornetQServerTestCase.queue1);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         TextMessage m = session.createTextMessage("message one");

         prod.send(m);

         conn.close();

         conn = JMSTestCase.cf.createConnection();

         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = session.createConsumer(HornetQServerTestCase.queue1);

         conn.start();

         TextMessage rm = (TextMessage)cons.receive();

         ProxyAssertSupport.assertNotNull(rm);

         ProxyAssertSupport.assertEquals("message one", rm.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void test_CreateTextMessageNull() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = JMSTestCase.cf.createConnection();

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = session.createProducer(HornetQServerTestCase.queue1);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         TextMessage m = session.createTextMessage();

         m.setText("message one");

         prod.send(m);

         conn.close();

         conn = JMSTestCase.cf.createConnection();

         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = session.createConsumer(HornetQServerTestCase.queue1);

         conn.start();

         TextMessage rm = (TextMessage)cons.receive();

         ProxyAssertSupport.assertEquals("message one", rm.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void test_Persistent_NonTransactional() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = JMSTestCase.cf.createConnection();

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = session.createProducer(HornetQServerTestCase.queue1);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);

         TextMessage m = session.createTextMessage("message one");

         prod.send(m);

         conn.close();

         conn = JMSTestCase.cf.createConnection();

         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = session.createConsumer(HornetQServerTestCase.queue1);

         conn.start();

         TextMessage rm = (TextMessage)cons.receive();

         ProxyAssertSupport.assertEquals("message one", rm.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void test_NonPersistent_Transactional_Send() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = JMSTestCase.cf.createConnection();

         Session session = conn.createSession(true, Session.SESSION_TRANSACTED);

         MessageProducer prod = session.createProducer(HornetQServerTestCase.queue1);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         TextMessage m = session.createTextMessage("message one");
         prod.send(m);
         m = session.createTextMessage("message two");
         prod.send(m);

         session.commit();

         conn.close();

         conn = JMSTestCase.cf.createConnection();

         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = session.createConsumer(HornetQServerTestCase.queue1);

         conn.start();

         TextMessage rm = (TextMessage)cons.receive();
         ProxyAssertSupport.assertEquals("message one", rm.getText());
         rm = (TextMessage)cons.receive();
         ProxyAssertSupport.assertEquals("message two", rm.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void test_Persistent_Transactional_Send() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = JMSTestCase.cf.createConnection();

         Session session = conn.createSession(true, Session.SESSION_TRANSACTED);

         MessageProducer prod = session.createProducer(HornetQServerTestCase.queue1);
         prod.setDeliveryMode(DeliveryMode.PERSISTENT);

         TextMessage m = session.createTextMessage("message one");
         prod.send(m);
         m = session.createTextMessage("message two");
         prod.send(m);

         session.commit();

         conn.close();

         conn = JMSTestCase.cf.createConnection();

         session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = session.createConsumer(HornetQServerTestCase.queue1);

         conn.start();

         TextMessage rm = (TextMessage)cons.receive();
         ProxyAssertSupport.assertEquals("message one", rm.getText());
         rm = (TextMessage)cons.receive();
         ProxyAssertSupport.assertEquals("message two", rm.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void test_NonPersistent_Transactional_Acknowledgment() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = JMSTestCase.cf.createConnection();

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer prod = session.createProducer(HornetQServerTestCase.queue1);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         TextMessage m = session.createTextMessage("one");
         prod.send(m);

         conn.close();

         conn = JMSTestCase.cf.createConnection();

         session = conn.createSession(true, Session.SESSION_TRANSACTED);

         MessageConsumer cons = session.createConsumer(HornetQServerTestCase.queue1);

         conn.start();

         TextMessage rm = (TextMessage)cons.receive();
         ProxyAssertSupport.assertEquals("one", rm.getText());

         session.commit();
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void test_Asynchronous_to_Client() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = JMSTestCase.cf.createConnection();

         final Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         final MessageConsumer cons = session.createConsumer(HornetQServerTestCase.queue1);

         conn.start();

         final AtomicReference<Message> message = new AtomicReference<Message>();
         final CountDownLatch latch = new CountDownLatch(1);

         new Thread(new Runnable()
         {
            public void run()
            {
               try
               {
                  // sleep a little bit to ensure that
                  // prod.send will be called before cons.reveive
                  Thread.sleep(500);

                  synchronized (session)
                  {
                     Message m = cons.receive(5000);
                     if (m != null)
                     {
                        message.set(m);
                        latch.countDown();
                     }
                  }
               }
               catch (Exception e)
               {
                  log.error("receive failed", e);
               }

            }
         }, "Receiving Thread").start();

         synchronized (session)
         {
            MessageProducer prod = session.createProducer(HornetQServerTestCase.queue1);
            prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            TextMessage m = session.createTextMessage("message one");

            prod.send(m);
         }

         boolean gotMessage = latch.await(5000, TimeUnit.MILLISECONDS);
         ProxyAssertSupport.assertTrue(gotMessage);
         TextMessage rm = (TextMessage)message.get();

         ProxyAssertSupport.assertEquals("message one", rm.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void test_MessageListener() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = JMSTestCase.cf.createConnection();

         Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer cons = session.createConsumer(HornetQServerTestCase.queue1);

         final AtomicReference<Message> message = new AtomicReference<Message>();
         final CountDownLatch latch = new CountDownLatch(1);

         cons.setMessageListener(new MessageListener()
         {
            public void onMessage(final Message m)
            {
               message.set(m);
               latch.countDown();
            }
         });

         conn.start();

         MessageProducer prod = session.createProducer(HornetQServerTestCase.queue1);
         prod.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         TextMessage m = session.createTextMessage("one");
         prod.send(m);

         boolean gotMessage = latch.await(5000, MILLISECONDS);
         ProxyAssertSupport.assertTrue(gotMessage);
         TextMessage rm = (TextMessage)message.get();

         ProxyAssertSupport.assertEquals("one", rm.getText());
      }
      finally
      {
         if (conn != null)
         {
            conn.close();
         }
      }
   }

   public void test_ClientAcknowledge() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = JMSTestCase.cf.createConnection();

         Session session = conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
         MessageProducer p = session.createProducer(HornetQServerTestCase.queue1);
         p.send(session.createTextMessage("CLACK"));

         MessageConsumer cons = session.createConsumer(HornetQServerTestCase.queue1);

         conn.start();

         TextMessage m = (TextMessage)cons.receive(1000);

         ProxyAssertSupport.assertEquals("CLACK", m.getText());

         // make sure the message is still in "delivering" state
         assertRemainingMessages(1);

         m.acknowledge();

         assertRemainingMessages(0);
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
