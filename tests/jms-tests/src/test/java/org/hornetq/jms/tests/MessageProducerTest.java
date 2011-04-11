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

import java.io.Serializable;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.InvalidDestinationException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.api.jms.HornetQJMSClient;
import org.hornetq.jms.tests.message.SimpleJMSMessage;
import org.hornetq.jms.tests.message.SimpleJMSTextMessage;
import org.hornetq.jms.tests.util.ProxyAssertSupport;

/**
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MessageProducerTest extends JMSTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testSendForeignWithForeignDestinationSet() throws Exception
   {
      Connection conn = null;

      try
      {
         conn = JMSTestCase.cf.createConnection();

         Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer p = sess.createProducer(HornetQServerTestCase.queue1);

         MessageConsumer c = sess.createConsumer(HornetQServerTestCase.queue1);

         conn.start();

         Message foreign = new SimpleJMSMessage(new SimpleDestination());

         foreign.setJMSDestination(new SimpleDestination());

         // the producer destination should override the foreign destination and the send should succeed

         p.send(foreign);

         Message m = c.receive(1000);

         ProxyAssertSupport.assertNotNull(m);

      }
      finally
      {
         conn.close();
      }
   }

   private static class SimpleDestination implements Destination, Serializable
   {

      /**
       * 
       */
      private static final long serialVersionUID = -2553676986492799801L;
   }

   public void testSendToQueuePersistent() throws Exception
   {
      sendToQueue(true);
   }

   public void testSendToQueueNonPersistent() throws Exception
   {
      sendToQueue(false);
   }

   private void sendToQueue(final boolean persistent) throws Exception
   {
      Connection pconn = null;
      Connection cconn = null;

      try
      {
         pconn = JMSTestCase.cf.createConnection();
         cconn = JMSTestCase.cf.createConnection();

         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session cs = cconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(HornetQServerTestCase.queue1);
         MessageConsumer c = cs.createConsumer(HornetQServerTestCase.queue1);

         cconn.start();

         TextMessage m = ps.createTextMessage("test");
         p.send(m);

         TextMessage r = (TextMessage)c.receive(3000);

         ProxyAssertSupport.assertEquals(m.getJMSMessageID(), r.getJMSMessageID());
         ProxyAssertSupport.assertEquals("test", r.getText());
      }
      finally
      {
         if (pconn != null)
         {
            pconn.close();
         }
         if (cconn != null)
         {
            cconn.close();
         }
      }
   }

   public void testTransactedSendPersistent() throws Exception
   {
      transactedSend(true);
   }

   public void testTransactedSendNonPersistent() throws Exception
   {
      transactedSend(false);
   }

   private void transactedSend(final boolean persistent) throws Exception
   {
      Connection pconn = null;
      Connection cconn = null;

      try
      {
         pconn = JMSTestCase.cf.createConnection();
         cconn = JMSTestCase.cf.createConnection();

         cconn.start();

         Session ts = pconn.createSession(true, -1);
         Session cs = cconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ts.createProducer(HornetQServerTestCase.queue1);
         MessageConsumer c = cs.createConsumer(HornetQServerTestCase.queue1);

         TextMessage m = ts.createTextMessage("test");
         p.send(m);

         ts.commit();

         TextMessage r = (TextMessage)c.receive();

         ProxyAssertSupport.assertEquals(m.getJMSMessageID(), r.getJMSMessageID());
         ProxyAssertSupport.assertEquals("test", r.getText());
      }
      finally
      {
         pconn.close();
         cconn.close();
      }
   }

   // I moved this into it's own class so we can catch any exception that occurs
   // Since this test intermittently fails.
   // (As an aside, technically this test is invalid anyway since the sessions is used for sending
   // and consuming concurrently - and sessions are supposed to be single threaded)
   private class Sender implements Runnable
   {
      volatile Exception ex;

      MessageProducer prod;

      Message m;

      Sender(final MessageProducer prod, final Message m)
      {
         this.prod = prod;

         this.m = m;
      }

      public synchronized void run()
      {
         try
         {
            prod.send(m);
         }
         catch (Exception e)
         {
            log.error(e);

            ex = e;
         }
      }
   }

   public void testPersistentSendToTopic() throws Exception
   {
      sendToTopic(true);
   }

   public void testNonPersistentSendToTopic() throws Exception
   {
      sendToTopic(false);
   }

   private void sendToTopic(final boolean persistent) throws Exception
   {

      Connection pconn = JMSTestCase.cf.createConnection();
      Connection cconn = JMSTestCase.cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session cs = cconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final MessageProducer p = ps.createProducer(HornetQServerTestCase.topic1);

         p.setDeliveryMode(persistent ? DeliveryMode.PERSISTENT : DeliveryMode.NON_PERSISTENT);

         MessageConsumer c = cs.createConsumer(HornetQServerTestCase.topic1);

         cconn.start();

         TextMessage m1 = ps.createTextMessage("test");

         Sender sender = new Sender(p, m1);

         Thread t = new Thread(sender, "Producer Thread");

         t.start();

         TextMessage m2 = (TextMessage)c.receive(5000);

         if (sender.ex != null)
         {
            // If an exception was caught in sending we rethrow here so as not to lose it
            throw sender.ex;
         }

         ProxyAssertSupport.assertEquals(m2.getJMSMessageID(), m1.getJMSMessageID());
         ProxyAssertSupport.assertEquals("test", m2.getText());

         t.join();
      }
      finally
      {
         pconn.close();
         cconn.close();
      }
   }

   /**
    *  Test sending via anonymous producer
    * */
   public void testSendDestination() throws Exception
   {
      Connection pconn = JMSTestCase.cf.createConnection();
      Connection cconn = JMSTestCase.cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session cs = cconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer c2 = cs.createConsumer(HornetQServerTestCase.topic2);
         final Message m1 = ps.createMessage();

         cconn.start();

         final MessageProducer anonProducer = ps.createProducer(null);

         new Thread(new Runnable()
         {
            public void run()
            {
               try
               {
                  anonProducer.send(HornetQServerTestCase.topic2, m1);
               }
               catch (Exception e)
               {
                  log.error(e);
               }
            }
         }, "Producer Thread").start();

         Message m2 = c2.receive(3000);
         ProxyAssertSupport.assertEquals(m1.getJMSMessageID(), m2.getJMSMessageID());

         log.debug("ending test");
      }
      finally
      {
         pconn.close();
         cconn.close();
      }
   }

   public void testSendForeignMessage() throws Exception
   {
      Connection pconn = JMSTestCase.cf.createConnection();
      Connection cconn = JMSTestCase.cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session cs = cconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(HornetQServerTestCase.queue1);
         MessageConsumer c = cs.createConsumer(HornetQServerTestCase.queue1);

         // send a message that is not created by the session

         cconn.start();

         Message m = new SimpleJMSTextMessage("something");
         p.send(m);

         TextMessage rec = (TextMessage)c.receive(3000);

         ProxyAssertSupport.assertEquals("something", rec.getText());

      }
      finally
      {
         pconn.close();
         cconn.close();
      }
   }

   public void testGetDestination() throws Exception
   {
      Connection pconn = JMSTestCase.cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(HornetQServerTestCase.topic1);
         Destination dest = p.getDestination();
         ProxyAssertSupport.assertEquals(dest, HornetQServerTestCase.topic1);
      }
      finally
      {
         pconn.close();
      }
   }

   public void testGetDestinationOnClosedProducer() throws Exception
   {
      Connection pconn = JMSTestCase.cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(HornetQServerTestCase.topic1);
         p.close();

         try
         {
            p.getDestination();
            ProxyAssertSupport.fail("should throw exception");
         }
         catch (javax.jms.IllegalStateException e)
         {
            // OK
         }
      }
      finally
      {
         pconn.close();
      }
   }

   public void testCreateProducerOnInexistentDestination() throws Exception
   {
      Connection pconn = JMSTestCase.cf.createConnection();
      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         try
         {
            ps.createProducer(HornetQJMSClient.createTopic("NoSuchTopic"));
            ProxyAssertSupport.fail("should throw exception");
         }
         catch (InvalidDestinationException e)
         {
            // OK
         }
      }
      finally
      {
         pconn.close();
      }
   }

   //
   // disabled MessageID tests
   //

   public void testGetDisableMessageID() throws Exception
   {
      Connection pconn = JMSTestCase.cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(HornetQServerTestCase.topic1);

         ProxyAssertSupport.assertFalse(p.getDisableMessageID());
      }
      finally
      {
         pconn.close();
      }
   }

   public void testGetDisableMessageIDOnClosedProducer() throws Exception
   {
      Connection pconn = JMSTestCase.cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(HornetQServerTestCase.topic1);

         p.close();

         try
         {
            p.getDisableMessageID();
            ProxyAssertSupport.fail("should throw exception");
         }
         catch (javax.jms.IllegalStateException e)
         {
            // OK
         }
      }
      finally
      {
         pconn.close();
      }
   }

   //
   // disabled timestamp tests
   //

   public void testDefaultTimestampDisabled() throws Exception
   {
      Connection pconn = JMSTestCase.cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer tp = ps.createProducer(HornetQServerTestCase.topic1);
         MessageProducer qp = ps.createProducer(HornetQServerTestCase.queue1);
         ProxyAssertSupport.assertFalse(tp.getDisableMessageTimestamp());
         ProxyAssertSupport.assertFalse(qp.getDisableMessageTimestamp());
      }
      finally
      {
         pconn.close();
      }
   }

   public void testSetTimestampDisabled() throws Exception
   {
      Connection pconn = JMSTestCase.cf.createConnection();
      Connection cconn = JMSTestCase.cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session cs = cconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(HornetQServerTestCase.queue1);
         MessageConsumer c = cs.createConsumer(HornetQServerTestCase.queue1);

         cconn.start();

         p.setDisableMessageTimestamp(true);
         ProxyAssertSupport.assertTrue(p.getDisableMessageTimestamp());

         p.send(ps.createMessage());

         Message m = c.receive(3000);

         ProxyAssertSupport.assertEquals(0l, m.getJMSTimestamp());

         p.setDisableMessageTimestamp(false);
         ProxyAssertSupport.assertFalse(p.getDisableMessageTimestamp());

         long t1 = System.currentTimeMillis();

         p.send(ps.createMessage());

         m = c.receive(3000);

         long t2 = System.currentTimeMillis();
         long timestamp = m.getJMSTimestamp();

         ProxyAssertSupport.assertTrue(timestamp >= t1);
         ProxyAssertSupport.assertTrue(timestamp <= t2);
      }
      finally
      {
         pconn.close();
         cconn.close();
      }
   }

   public void testGetTimestampDisabledOnClosedProducer() throws Exception
   {
      Connection pconn = JMSTestCase.cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(HornetQServerTestCase.topic1);

         p.close();

         try
         {
            p.getDisableMessageTimestamp();
            ProxyAssertSupport.fail("should throw exception");
         }
         catch (javax.jms.IllegalStateException e)
         {
            // OK
         }
      }
      finally
      {
         pconn.close();
      }
   }

   //
   // DeliverMode tests
   //

   public void testDefaultDeliveryMode() throws Exception
   {
      Connection pconn = JMSTestCase.cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer tp = ps.createProducer(HornetQServerTestCase.topic1);
         MessageProducer qp = ps.createProducer(HornetQServerTestCase.queue1);

         ProxyAssertSupport.assertEquals(DeliveryMode.PERSISTENT, tp.getDeliveryMode());
         ProxyAssertSupport.assertEquals(DeliveryMode.PERSISTENT, qp.getDeliveryMode());
      }
      finally
      {
         pconn.close();
      }
   }

   public void testSetDeliveryMode() throws Exception
   {
      Connection pconn = JMSTestCase.cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(HornetQServerTestCase.topic1);

         p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         ProxyAssertSupport.assertEquals(DeliveryMode.NON_PERSISTENT, p.getDeliveryMode());

         p.setDeliveryMode(DeliveryMode.PERSISTENT);
         ProxyAssertSupport.assertEquals(DeliveryMode.PERSISTENT, p.getDeliveryMode());
      }
      finally
      {
         pconn.close();
      }
   }

   public void testGetDeliveryModeOnClosedProducer() throws Exception
   {
      Connection pconn = JMSTestCase.cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(HornetQServerTestCase.topic1);

         p.close();

         try
         {
            p.getDeliveryMode();
            ProxyAssertSupport.fail("should throw exception");
         }
         catch (javax.jms.IllegalStateException e)
         {
            // OK
         }
      }
      finally
      {
         pconn.close();
      }
   }

   //
   // Priority tests
   //

   public void testDefaultPriority() throws Exception
   {
      Connection pconn = JMSTestCase.cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer tp = ps.createProducer(HornetQServerTestCase.topic1);
         MessageProducer qp = ps.createProducer(HornetQServerTestCase.queue1);

         ProxyAssertSupport.assertEquals(4, tp.getPriority());
         ProxyAssertSupport.assertEquals(4, qp.getPriority());
      }
      finally
      {
         pconn.close();
      }
   }

   public void testSetPriority() throws Exception
   {
      Connection pconn = JMSTestCase.cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(HornetQServerTestCase.topic1);

         p.setPriority(9);
         ProxyAssertSupport.assertEquals(9, p.getPriority());

         p.setPriority(0);
         ProxyAssertSupport.assertEquals(0, p.getPriority());
      }
      finally
      {
         pconn.close();
      }
   }

   public void testGetPriorityOnClosedProducer() throws Exception
   {
      Connection pconn = JMSTestCase.cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(HornetQServerTestCase.topic1);

         p.close();

         try
         {
            p.getPriority();
            ProxyAssertSupport.fail("should throw exception");
         }
         catch (javax.jms.IllegalStateException e)
         {
            // OK
         }
      }
      finally
      {
         pconn.close();
      }
   }

   //
   // TimeToLive test
   //

   public void testDefaultTimeToLive() throws Exception
   {
      Connection pconn = JMSTestCase.cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer tp = ps.createProducer(HornetQServerTestCase.topic1);
         MessageProducer qp = ps.createProducer(HornetQServerTestCase.queue1);

         ProxyAssertSupport.assertEquals(0l, tp.getTimeToLive());
         ProxyAssertSupport.assertEquals(0l, qp.getTimeToLive());
      }
      finally
      {
         pconn.close();
      }
   }

   public void testSetTimeToLive() throws Exception
   {
      Connection pconn = JMSTestCase.cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(HornetQServerTestCase.topic1);

         p.setTimeToLive(100l);
         ProxyAssertSupport.assertEquals(100l, p.getTimeToLive());

         p.setTimeToLive(0l);
         ProxyAssertSupport.assertEquals(0l, p.getTimeToLive());
      }
      finally
      {
         pconn.close();
      }
   }

   public void testGetTimeToLiveOnClosedProducer() throws Exception
   {
      Connection pconn = JMSTestCase.cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(HornetQServerTestCase.topic1);

         p.close();

         try
         {
            p.setTimeToLive(100l);
            ProxyAssertSupport.fail("should throw exception");
         }
         catch (javax.jms.IllegalStateException e)
         {
            // OK
         }
      }
      finally
      {
         pconn.close();
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
