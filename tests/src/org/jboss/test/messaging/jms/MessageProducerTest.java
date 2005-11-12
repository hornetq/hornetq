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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.DeliveryMode;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.jms.message.SimpleJMSMessage;
import org.jboss.test.messaging.tools.ServerManagement;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class MessageProducerTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected ConnectionFactory cf;

   protected Destination topic;
   protected Destination topic2;
   protected Destination queue;

   // Constructors --------------------------------------------------

   public MessageProducerTest(String name)
   {
      super(name);
   }

   // Public --------------------------------------------------------

   public void setUp() throws Exception
   {
      super.setUp();

      ServerManagement.init("all");
      ServerManagement.undeployTopic("Topic");
      ServerManagement.undeployTopic("Topic2");
      ServerManagement.undeployQueue("Queue");
      ServerManagement.deployTopic("Topic");
      ServerManagement.deployTopic("Topic2");
      ServerManagement.deployQueue("Queue");

      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      topic = (Destination)ic.lookup("/topic/Topic");
      topic2 = (Destination)ic.lookup("/topic/Topic2");
      queue = (Destination)ic.lookup("/queue/Queue");

      log.debug("setup done");
   }

   public void tearDown() throws Exception
   {
      ServerManagement.undeployTopic("Topic");
      ServerManagement.undeployTopic("Topic2");
      ServerManagement.undeployQueue("Queue");
      ServerManagement.deInit();

      super.tearDown();
   }

   /**
    * The simplest possible non-transacted test.
    */
   public void testSimpleSend() throws Exception
   {
      Connection pconn = cf.createConnection();
      Connection cconn = cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session cs = cconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(queue);
         MessageConsumer c = cs.createConsumer(queue);

         cconn.start();

         TextMessage m = ps.createTextMessage("test");
         p.send(m);

         TextMessage r = (TextMessage)c.receive(3000);

         assertEquals(m.getJMSMessageID(), r.getJMSMessageID());
         assertEquals("test", r.getText());
      }
      finally
      {
         pconn.close();
         cconn.close();
      }
   }

   /**
    * The simplest possible transacted test.
    */
   public void testTransactedSend() throws Exception
   {
      Connection pconn = cf.createConnection();
      Connection cconn = cf.createConnection();

      try
      {
         cconn.start();

         Session ts = pconn.createSession(true, -1);
         Session cs = cconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ts.createProducer(queue);
         MessageConsumer c = cs.createConsumer(queue);

         TextMessage m = ts.createTextMessage("test");
         p.send(m);

         ts.commit();

         TextMessage r = (TextMessage)c.receive();

         assertEquals(m.getJMSMessageID(), r.getJMSMessageID());
         assertEquals("test", r.getText());
      }
      finally
      {
         pconn.close();
         cconn.close();
      }
   }

   public void testPersistentSendToTopic() throws Exception
   {

      Connection pconn = cf.createConnection();
      Connection cconn = cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session cs = cconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         final MessageProducer p = ps.createProducer(topic);
         MessageConsumer c = cs.createConsumer(topic);

         cconn.start();

         final TextMessage m1 = ps.createTextMessage("test");

         Thread t = new Thread(new Runnable()
         {
            public void run()
            {
               try
               {
                  // this is needed to make sure the main thread has enough time to block
                  Thread.sleep(1000);
                  p.send(m1);
               }
               catch(Exception e)
               {
                  log.error(e);
               }
            }
         }, "Producer Thread");

         t.start();

         TextMessage m2 = (TextMessage)c.receive(5000);

         assertEquals(m2.getJMSMessageID(), m1.getJMSMessageID());
         assertEquals("test", m2.getText());

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
      Connection pconn = cf.createConnection();
      Connection cconn = cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session cs = cconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer c2 = cs.createConsumer(topic2);
         final Message m1 = ps.createMessage();

         cconn.start();

         final MessageProducer anonProducer = ps.createProducer(null);

         new Thread(new Runnable()
         {
            public void run()
            {
               try
               {
                  // this is needed to make sure the main thread has enough time to block
                  Thread.sleep(1000);
                  anonProducer.send(topic2, m1);
               }
               catch(Exception e)
               {
                  log.error(e);
               }
            }
         }, "Producer Thread").start();

         Message m2 = c2.receive(3000);
         assertEquals(m1.getJMSMessageID(), m2.getJMSMessageID());

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
      Connection pconn = cf.createConnection();
      Connection cconn = cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session cs = cconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(queue);
         MessageConsumer c = cs.createConsumer(queue);

         // send a message that is not created by the session

         cconn.start();

         Message m = new SimpleJMSMessage();
         p.send(m);

         Message rec = c.receive(3000);
         assertEquals(m.getJMSMessageID(), rec.getJMSMessageID());

      }
      finally
      {
         pconn.close();
         cconn.close();
      }
   }

   public void testGetDestination() throws Exception
   {
      Connection pconn = cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(topic);
         Destination dest = p.getDestination();
         assertEquals(dest, topic);
      }
      finally
      {
         pconn.close();
      }
   }

   public void testGetDestinationOnClosedProducer() throws Exception
   {
      Connection pconn = cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(topic);
         p.close();

         try
         {
            p.getDestination();
            fail("should throw exception");
         }
         catch(javax.jms.IllegalStateException e)
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
      Connection pconn = cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(topic);

         assertFalse(p.getDisableMessageID());
      }
      finally
      {
         pconn.close();
      }
   }

   public void testGetDisableMessageIDOnClosedProducer() throws Exception
   {
      Connection pconn = cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(topic);

         p.close();

         try
         {
            p.getDisableMessageID();
            fail("should throw exception");
         }
         catch(javax.jms.IllegalStateException e)
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
      Connection pconn = cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer tp = ps.createProducer(topic);
         MessageProducer qp = ps.createProducer(queue);
         assertFalse(tp.getDisableMessageTimestamp());
         assertFalse(qp.getDisableMessageTimestamp());
      }
      finally
      {
         pconn.close();
      }
   }

   public void testSetTimestampDisabled() throws Exception
   {
      Connection pconn = cf.createConnection();
      Connection cconn = cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session cs = cconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(queue);
         MessageConsumer c = cs.createConsumer(queue);

         cconn.start();

         p.setDisableMessageTimestamp(true);
         assertTrue(p.getDisableMessageTimestamp());

         p.send(ps.createMessage());

         Message m = c.receive(3000);

         assertEquals(0l, m.getJMSTimestamp());

         p.setDisableMessageTimestamp(false);
         assertFalse(p.getDisableMessageTimestamp());

         long t1 = System.currentTimeMillis();

         p.send(ps.createMessage());

         m = c.receive(3000);

         long t2 = System.currentTimeMillis();
         long timestamp = m.getJMSTimestamp();

         assertTrue(timestamp >= t1);
         assertTrue(timestamp <= t2);
      }
      finally
      {
         pconn.close();
         cconn.close();
      }
   }

   public void testGetTimestampDisabledOnClosedProducer() throws Exception
   {
      Connection pconn = cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(topic);

         p.close();

         try
         {
            p.getDisableMessageTimestamp();
            fail("should throw exception");
         }
         catch(javax.jms.IllegalStateException e)
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
      Connection pconn = cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer tp = ps.createProducer(topic);
         MessageProducer qp = ps.createProducer(queue);

         assertEquals(DeliveryMode.PERSISTENT, tp.getDeliveryMode());
         assertEquals(DeliveryMode.PERSISTENT, qp.getDeliveryMode());
      }
      finally
      {
         pconn.close();
      }
   }

   public void testSetDeliveryMode() throws Exception
   {
      Connection pconn = cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(topic);

         p.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         assertEquals(DeliveryMode.NON_PERSISTENT, p.getDeliveryMode());

         p.setDeliveryMode(DeliveryMode.PERSISTENT);
         assertEquals(DeliveryMode.PERSISTENT, p.getDeliveryMode());
      }
      finally
      {
         pconn.close();
      }
   }

   public void testGetDeliveryModeOnClosedProducer() throws Exception
   {
      Connection pconn = cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(topic);

         p.close();

         try
         {
            p.getDeliveryMode();
            fail("should throw exception");
         }
         catch(javax.jms.IllegalStateException e)
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
      Connection pconn = cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer tp = ps.createProducer(topic);
         MessageProducer qp = ps.createProducer(queue);

         assertEquals(4, tp.getPriority());
         assertEquals(4, qp.getPriority());
      }
      finally
      {
         pconn.close();
      }
   }

   public void testSetPriority() throws Exception
   {
      Connection pconn = cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(topic);

         p.setPriority(9);
         assertEquals(9, p.getPriority());

         p.setPriority(0);
         assertEquals(0, p.getPriority());
      }
      finally
      {
         pconn.close();
      }
   }

   public void testGetPriorityOnClosedProducer() throws Exception
   {
      Connection pconn = cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(topic);

         p.close();

         try
         {
            p.getPriority();
            fail("should throw exception");
         }
         catch(javax.jms.IllegalStateException e)
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
      Connection pconn = cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer tp = ps.createProducer(topic);
         MessageProducer qp = ps.createProducer(queue);

         assertEquals(0l, tp.getTimeToLive());
         assertEquals(0l, qp.getTimeToLive());
      }
      finally
      {
         pconn.close();
      }
   }

   public void testSetTimeToLive() throws Exception
   {
      Connection pconn = cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(topic);

         p.setTimeToLive(100l);
         assertEquals(100l, p.getTimeToLive());

         p.setTimeToLive(0l);
         assertEquals(0l, p.getTimeToLive());
      }
      finally
      {
         pconn.close();
      }
   }

   public void testGetTimeToLiveOnClosedProducer() throws Exception
   {
      Connection pconn = cf.createConnection();

      try
      {
         Session ps = pconn.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer p = ps.createProducer(topic);

         p.close();

         try
         {
            p.setTimeToLive(100l);
            fail("should throw exception");
         }
         catch(javax.jms.IllegalStateException e)
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
