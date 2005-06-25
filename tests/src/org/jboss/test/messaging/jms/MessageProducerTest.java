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
import javax.jms.DeliveryMode;
import javax.naming.InitialContext;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
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

      ServerManagement.startInVMServer();
      ServerManagement.deployTopic("Topic");
      ServerManagement.deployTopic("Topic2");
      ServerManagement.deployQueue("Queue");

      InitialContext ic = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());
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
      ServerManagement.stopInVMServer();

      super.tearDown();
   }

   
   /* Test sending to destination where the Destination is specified in the send */
   public void testSendDestination() throws Exception
   {      
     
      final Message m1 = producerSession.createMessage();
      
      consumerConnection.start();
      
      new Thread(new Runnable()
      {
         public void run()
         {
            try
            {
               // this is needed to make sure the main thread has enough time to block
               Thread.sleep(1000);
               topicProducer.send(topic2, m1);
            }
            catch(Exception e)
            {
               log.error(e);
            }
         }
      }, "Producer").start();

      Message m2 = topicConsumer2.receive(3000);
      assertNotNull(m2);
      assertEquals(m1.getJMSMessageID(), m2.getJMSMessageID());
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
      Message m = queueConsumer.receive();

      assertEquals(0l, m.getJMSTimestamp());

      queueProducer.setDisableMessageTimestamp(false);
      assertFalse(queueProducer.getDisableMessageTimestamp());

      long t1 = System.currentTimeMillis();
      queueProducer.send(producerSession.createMessage());
      m = queueConsumer.receive();
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
