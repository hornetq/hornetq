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
   protected MessageProducer producer;
   protected MessageConsumer consumer;
   protected MessageConsumer consumer2;
   protected Destination topic;
   protected Destination topic2;

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

      InitialContext ic = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/messaging/ConnectionFactory");
      topic = (Destination)ic.lookup("/messaging/topics/Topic");
      topic2 = (Destination)ic.lookup("/messaging/topics/Topic2");

      producerConnection = cf.createConnection();
      consumerConnection = cf.createConnection();

      producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      producer = producerSession.createProducer(topic);
      consumer = consumerSession.createConsumer(topic);
      consumer2 = consumerSession.createConsumer(topic2);

   }

   public void tearDown() throws Exception
   {
      producerConnection.close();
      consumerConnection.close();

      ServerManagement.undeployTopic("Topic");
      ServerManagement.stopInVMServer();

      super.tearDown();
   }

   
   public void testDefaultDeliveryMode() throws Exception
   {
      assertEquals(DeliveryMode.PERSISTENT, producer.getDeliveryMode());
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
               producer.send(topic2, m1);
            }
            catch(Exception e)
            {
               log.error(e);
            }
         }
      }, "Producer").start();

      Message m2 = consumer2.receive(3000);
      assertNotNull(m2);
      assertEquals(m1.getJMSMessageID(), m2.getJMSMessageID());
   }
   
   
   public void testGetDestination() throws Exception
   {      
      Destination dest = producer.getDestination();     
      assertEquals(dest, topic);
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

}
