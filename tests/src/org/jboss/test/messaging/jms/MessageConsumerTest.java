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
import javax.naming.InitialContext;
import java.util.List;
import java.util.Collections;
import java.util.ArrayList;
import java.util.Iterator;

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

      InitialContext ic = new InitialContext(InVMInitialContextFactory.getJNDIEnvironment());
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/messaging/ConnectionFactory");
      Destination topic = (Destination)ic.lookup("/messaging/topics/Topic");

      producerConnection = cf.createConnection();
      consumerConnection = cf.createConnection();

      producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      producer = producerSession.createProducer(topic);
      consumer = consumerSession.createConsumer(topic);

   }

   public void tearDown() throws Exception
   {
      // TODO uncomment these
//      producerConnection.close();
//      consumerConnection.close();

      ServerManagement.undeployTopic("Topic");
      ServerManagement.stopInVMServer();

      super.tearDown();
   }

   public void testReceiveTimeoutNoMessage() throws Exception
   {
      Message m = consumer.receive(1000);
      assertNull(m);
   }

   public void testReceiveConnectionStopped() throws Exception
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
               producer.send(m);
            }
            catch(Exception e)
            {
               log.error(e);
            }
         }
      }, "Producer").start();

      assertNull(consumer.receive(2000));
   }


   public void testReceiveTimeout() throws Exception
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
               producer.send(m1);
            }
            catch(Exception e)
            {
               log.error(e);
            }
         }
      }, "Producer").start();

      Message m2 = consumer.receive(2000);
      assertEquals(m1.getJMSMessageID(), m2.getJMSMessageID());
   }

   public void testReceive() throws Exception
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
               producer.send(m1);
            }
            catch(Exception e)
            {
               log.error(e);
            }
         }
      }, "Producer").start();

      Message m2 = consumer.receive();
      assertEquals(m1.getJMSMessageID(), m2.getJMSMessageID());
   }

   public void testMessageListener() throws Exception
   {
      MessageListenerImpl l = new MessageListenerImpl();
      consumer.setMessageListener(l);

      consumerConnection.start();

      Message m1 = producerSession.createMessage();
      producer.send(m1);
      assertEquals(m1.getJMSMessageID(), l.getNextMessage().getJMSMessageID());
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------
   
   // Inner classes -------------------------------------------------

   private class MessageListenerImpl implements MessageListener
   {
      private List messages = Collections.synchronizedList(new ArrayList());

      public void onMessage(Message m)
      {
         messages.add(m);
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

      public void clear()
      {
         messages.clear();
      }
   }
}
