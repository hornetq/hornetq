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

   protected Connection producerConnection, consumerConnection;
   protected Session producerSession, consumerSession;
   protected MessageProducer topicProducer;
   protected MessageConsumer topicConsumer;
   protected MessageConsumer topicConsumer2;
   protected MessageProducer queueProducer;
   protected MessageConsumer queueConsumer;
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
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      topic = (Destination)ic.lookup("/topic/Topic");
      topic2 = (Destination)ic.lookup("/topic/Topic2");
      queue = (Destination)ic.lookup("/queue/Queue");

      producerConnection = cf.createConnection();
      consumerConnection = cf.createConnection();

      producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      topicProducer = producerSession.createProducer(topic);
      topicConsumer = consumerSession.createConsumer(topic);
      topicConsumer2 = consumerSession.createConsumer(topic2);

      queueProducer = producerSession.createProducer(queue);
      queueConsumer = consumerSession.createConsumer(queue);
   }

   public void tearDown() throws Exception
   {
      producerConnection.close();
      consumerConnection.close();

      ServerManagement.undeployTopic("Topic");
      ServerManagement.deInit();

      super.tearDown();
   }

   /**
    * The simplest possible non-transacted test.
    */
   public void testSimpleSend() throws Exception
   {
      consumerConnection.start();
      TextMessage m = producerSession.createTextMessage("test");
      queueProducer.send(m);
      TextMessage r = (TextMessage)queueConsumer.receive(3000);
      assertEquals(m.getJMSMessageID(), r.getJMSMessageID());
      assertEquals("test", r.getText());
   }

   /**
    * The simplest possible transacted test.
    */
   public void testTransactedSend() throws Exception
   {
      consumerConnection.start();

      Session transactedSession = producerConnection.createSession(true, -1);
      MessageProducer prod = transactedSession.createProducer(queue);

      TextMessage m = transactedSession.createTextMessage("test");
      prod.send(m);

      transactedSession.commit();

      TextMessage r = (TextMessage)queueConsumer.receive();
      assertEquals(m.getJMSMessageID(), r.getJMSMessageID());
      assertEquals("test", r.getText());
   }

   public void testPersistentSendToTopic() throws Exception
   {
      consumerConnection.start();
      
      final TextMessage m1 = producerSession.createTextMessage("test");
      
      Thread t = 
      
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
      }, "Producer");
      
      t.start();
      
      
      TextMessage m2 = (TextMessage)topicConsumer.receive(5000);
      assertEquals(m2.getJMSMessageID(), m1.getJMSMessageID());
      assertEquals("test", m2.getText());
      
      
      t.join();
      
   }

   /* Test sending via anonymous producer */
   public void testSendDestination() throws Exception
   {
      final Message m1 = producerSession.createMessage();

      consumerConnection.start();
      
      final MessageProducer anonProducer = producerSession.createProducer(null);

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
      }, "Producer").start();

      Message m2 = topicConsumer2.receive(3000);
      assertEquals(m1.getJMSMessageID(), m2.getJMSMessageID());
      
      log.debug("ending test");
      
   }


   public void testSendForeignMessage() throws Exception
   {
      // send a message that is not created by the session

      consumerConnection.start();

      Message m = new SimpleJMSMessage();
      queueProducer.send(m);

      Message rec = queueConsumer.receive(3000);
      assertEquals(m.getJMSMessageID(), rec.getJMSMessageID());
   }


   public void testGetDestination() throws Exception
   {
      Destination dest = topicProducer.getDestination();
      assertEquals(dest, topic);
   }

   public void testGetDestinationOnClosedProducer() throws Exception
   {
      topicProducer.close();

      try
      {
         topicProducer.getDestination();
         fail("should throw exception");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }
   }





   //
   // disabled MessageID tests
   //

   public void testGetDisableMessageID() throws Exception
   {
      assertFalse(topicProducer.getDisableMessageID());
   }

   public void testGetDisableMessageIDOnClosedProducer() throws Exception
   {

      topicProducer.close();

      try
      {
         topicProducer.getDisableMessageID();
         fail("should throw exception");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }
   }

   //
   // disabled timestamp tests
   //

   public void testDefaultTimestampDisabled() throws Exception
   {
      assertFalse(topicProducer.getDisableMessageTimestamp());
      assertFalse(queueProducer.getDisableMessageTimestamp());
   }

   public void testSetTimestampDisabled() throws Exception
   {
      consumerConnection.start();

      queueProducer.setDisableMessageTimestamp(true);
      assertTrue(queueProducer.getDisableMessageTimestamp());

      queueProducer.send(producerSession.createMessage());
      Message m = queueConsumer.receive(3000);

      assertEquals(0l, m.getJMSTimestamp());

      queueProducer.setDisableMessageTimestamp(false);
      assertFalse(queueProducer.getDisableMessageTimestamp());

      long t1 = System.currentTimeMillis();
      queueProducer.send(producerSession.createMessage());
      m = queueConsumer.receive(3000);
      long t2 = System.currentTimeMillis();

      long timestamp = m.getJMSTimestamp();

      assertTrue(timestamp >= t1);
      assertTrue(timestamp <= t2);

   }

   public void testGetTimestampDisabledOnClosedProducer() throws Exception
   {

      topicProducer.close();

      try
      {
         topicProducer.getDisableMessageTimestamp();
         fail("should throw exception");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }
   }



   //
   // DeliverMode tests
   //

   public void testDefaultDeliveryMode() throws Exception
   {
      assertEquals(DeliveryMode.PERSISTENT, topicProducer.getDeliveryMode());
      assertEquals(DeliveryMode.PERSISTENT, queueProducer.getDeliveryMode());
   }

   public void testSetDeliveryMode() throws Exception
   {
      topicProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      assertEquals(DeliveryMode.NON_PERSISTENT, topicProducer.getDeliveryMode());

      topicProducer.setDeliveryMode(DeliveryMode.PERSISTENT);
      assertEquals(DeliveryMode.PERSISTENT, topicProducer.getDeliveryMode());
   }

   public void testGetDeliveryModeOnClosedProducer() throws Exception
   {

      topicProducer.close();

      try
      {
         topicProducer.getDeliveryMode();
         fail("should throw exception");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }
   }

   //
   // Priority tests
   //

   public void testDefaultPriority() throws Exception
   {
      assertEquals(4, topicProducer.getPriority());
      assertEquals(4, queueProducer.getPriority());
   }

   public void testSetPriority() throws Exception
   {
      topicProducer.setPriority(9);
      assertEquals(9, topicProducer.getPriority());

      topicProducer.setPriority(0);
      assertEquals(0, topicProducer.getPriority());
   }

   public void testGetPriorityOnClosedProducer() throws Exception
   {

      topicProducer.close();

      try
      {
         topicProducer.getPriority();
         fail("should throw exception");
      }
      catch(javax.jms.IllegalStateException e)
      {
         // OK
      }
   }

   //
   // TimeToLive test
   //

   public void testDefaultTimeToLive() throws Exception
   {
      assertEquals(0l, topicProducer.getTimeToLive());
      assertEquals(0l, queueProducer.getTimeToLive());
   }

   public void testSetTimeToLive() throws Exception
   {
      topicProducer.setTimeToLive(100l);
      assertEquals(100l, topicProducer.getTimeToLive());

      topicProducer.setTimeToLive(0l);
      assertEquals(0l, topicProducer.getTimeToLive());
   }

   public void testGetTimeToLiveOnClosedProducer() throws Exception
   {

      topicProducer.close();

      try
      {
         topicProducer.setTimeToLive(100l);
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
