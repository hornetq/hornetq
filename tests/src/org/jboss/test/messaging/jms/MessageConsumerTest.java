/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.test.messaging.jms;

import org.jboss.test.messaging.MessagingTestCase;
import org.jboss.test.messaging.tools.ServerManagement;

import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.Destination;
import javax.jms.Message;
import javax.naming.InitialContext;

/**
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class MessageConsumerTest extends MessagingTestCase
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------
   
   // Attributes ----------------------------------------------------

   protected Connection connectionOne, connectionTwo;
   protected Session sessionOne, sessionTwo;
   protected MessageProducer producer;
   protected MessageConsumer consumer;

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

      InitialContext ic = new InitialContext(ServerManagement.getJNDIEnvironment());
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/messaging/ConnectionFactory");
      Destination topic = (Destination)ic.lookup("/messaging/topics/Topic");

      connectionOne = cf.createConnection();
      connectionTwo = cf.createConnection();

      sessionOne = connectionOne.createSession(false, Session.AUTO_ACKNOWLEDGE);
      sessionTwo = connectionTwo.createSession(false, Session.AUTO_ACKNOWLEDGE);

      producer = sessionOne.createProducer(topic);
      consumer = sessionTwo.createConsumer(topic);

   }

   public void tearDown() throws Exception
   {
      // TODO uncomment all these
//      producer.close();
//      consumer.close();
//      sessionOne.close();
//      sessionTwo.close();
//      connectionOne.close();
//      connectionTwo.close();
//
//      ServerManagement.undeployTopic("Topic");
//      ServerManagement.stopInVMServer();
//
//      super.tearDown();
   }


   public void testReceive() throws Exception
   {
      // TODO uncomment all these
//      connectionTwo.start();
      Message m1 = sessionOne.createMessage();
      producer.send(m1);

      Message m2 = consumer.receive(10000);
      assertEquals(m1.getJMSMessageID(), m2.getJMSMessageID());

   }

//   public void testMessageListener() throws Exception
//   {
//
//      consumer.setMessageListener(new MessageListener()
//      {
//         public void onMessage(Message m)
//         {
//            System.out.println("Got message " + m);
//
//         };
//      });
//      connectionTwo.start();
//      Message m1 = sessionOne.createMessage();
//      producer.send(m1);
//
//   }
//

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------   
}
