/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.example.jms.queue;

import javax.naming.InitialContext;
import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.Queue;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;
import javax.jms.ConnectionMetaData;
import javax.jms.JMSException;

/**
 * The example creates a connection to the default provider and uses the connection to send a
 * message to the queue "queue/testQueue". Then, the example creates a second connection to the
 * provider and uses it to receive the message.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class QueueExample
{
   // Constants -----------------------------------------------------

   private static final String QUEUE_JNDI_NAME = "/messaging/queue/testQueue";

   // Static --------------------------------------------------------

   public static void main(String[] args)
   {
      boolean ok = false;
      try
      {
         Util.deployQueue(QUEUE_JNDI_NAME);

         ok = new QueueExample().run();

         Util.undeployQueue(QUEUE_JNDI_NAME);
      }
      catch(Exception e)
      {
         e.printStackTrace();
         ok = false;
      }

      if (ok)
      {
         System.out.println("The example executed correctly.");
         System.exit(0);
      }
      else
      {
         System.out.println("The example failed!");
         System.exit(1);
      }
   }

   // Attributes ----------------------------------------------------
   
   // Constructors --------------------------------------------------
   
   // Public --------------------------------------------------------

   public boolean run() throws Exception
   {
      InitialContext ic = new InitialContext();

      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      Queue queue = (Queue)ic.lookup(QUEUE_JNDI_NAME);

      Connection connection = cf.createConnection();

      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer sender = session.createProducer(queue);
      TextMessage message = session.createTextMessage("Hello!");


      sender.send(message);

      System.out.println("The message was sent successfully to the queue");

      connection.close();


      Connection connection2 = cf.createConnection();

      Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageConsumer consumer =  session2.createConsumer(queue);

      connection2.start();

      message = (TextMessage)consumer.receive(2000);

      System.out.println("Received message: " + message.getText());


      ConnectionMetaData metaData = connection2.getMetaData();

      connection2.close();

      printProviderInfo(metaData);


      if (message == null)
      {
         return false;
      }
      else
      {
         return true;
      }
   }

   // Package protected ---------------------------------------------
   
   // Protected -----------------------------------------------------
   
   // Private -------------------------------------------------------

   private void printProviderInfo(ConnectionMetaData metaData) throws JMSException
   {
      String info =
            "The example connected to " + metaData.getJMSProviderName() +
            " version " + metaData.getProviderVersion() + " (" +
            metaData.getProviderMajorVersion() + "." + metaData.getProviderMinorVersion() +
            ")";

     System.out.println(info);
   }

   // Inner classes -------------------------------------------------
}
