/**
 * JBoss, the OpenSource J2EE WebOS
 *
 * Distributable under LGPL license.
 * See terms of license at gnu.org.
 */
package org.jboss.jms.tools;

import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.naming.InitialContext;
import java.util.Hashtable;

/**
 * An interactive command-line JMS client. Allows you to look-up a ConnectionFactory and
 * Destinations and connect to the JMS server.
 * 
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 */
public class Client
{
   // Constants -----------------------------------------------------

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private ConnectionFactory connectionFactory;
   private Connection connection;
   private Session session;
   private MessageProducer producer;
   private MessageConsumer consumer;

   private InitialContext initialContext;

   // Constructors --------------------------------------------------

   /**
    * Uses default jndi properties (jndi.properties file)
    */
   public Client() throws Exception
   {
      this(null);
   }

   /**
    * @param jndiEnvironment - Contains jndi properties. Null means using default properties
    *        (jndi.properties file)
    */
   public Client(Hashtable jndiEnvironment) throws Exception
   {
      initialContext = new InitialContext(jndiEnvironment);
   }

   // Public --------------------------------------------------------

   public void lookupConnectionFactory() throws Exception
   {
      if (connectionFactory != null)
      {
         System.out.println("ConnectionFactory already downloaded");
         return;
      }

      if (connectionFactory == null)
      {
         connectionFactory =
         (ConnectionFactory)initialContext.lookup("/messaging/ConnectionFactory");
      }
   }

   /**
    *
    * @param destination - the destination's fully qualified JNDI name
    */
   public Destination lookupDestination(String destination) throws Exception
   {
      return (Destination)initialContext.lookup(destination);
   }


   public void createConnection() throws Exception
   {
      if (connection != null)
      {
         System.out.println("Connection already created");
         return;
      }

      if (connectionFactory == null)
      {
         lookupConnectionFactory();
      }

      connection = connectionFactory.createConnection();
   }

   public void createSession() throws Exception
   {
      if (session != null)
      {
         System.out.println("Session already created");
         return;
      }

      if (connection == null)
      {
         createConnection();
      }

      session = connection.createSession(false, 0);
   }

   public void createProducer(String destination) throws Exception
   {

      if (producer != null)
      {
         System.out.println("Producer already created");
         return;
      }

      if (session == null)
      {
         createSession();
      }

      producer = session.createProducer(lookupDestination(destination));
   }

   public void createConsumer(String destination) throws Exception
   {

      if (consumer != null)
      {
         System.out.println("Producer already created");
         return;
      }

      if (session == null)
      {
         createSession();
      }

      consumer = session.createConsumer(lookupDestination(destination));
   }

   public void setMessageListener() throws Exception
   {
      if (consumer == null)
      {
         throw new Exception("You need to create a consumer first. " +
                             "Use createConsumer(destination)");
      }
      consumer.setMessageListener(new MessageListener()
      {
         public void onMessage(Message m)
         {
            System.out.println("MessageLister got message: " + m);
         }
      });
   }




   public void send() throws Exception
   {
      insureProducer();
      Message m = session.createMessage();
      producer.send(m);
   }

   public Object receive(long timeout) throws Exception
   {
      insureConsumer();
      return consumer.receive(timeout);
   }

   public Object receiveNoWait() throws Exception
   {
      insureConsumer();
      return consumer.receiveNoWait();
   }





   public void dump()
   {
      System.out.println("connectionFactory = " + connectionFactory);
      System.out.println("connection        = " + connection);
      System.out.println("session           = " + session);
      System.out.println("producer          = " + producer);
      System.out.println("consumer          = " + consumer);
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   private void insureConsumer()
   {
      if (consumer == null)
      {
         throw new RuntimeException("You need to create a consumer first. " +
                                    "Use createConsumer(destination)");
      }
   }

   private void insureProducer()
   {
      if (producer == null)
      {
         throw new RuntimeException("You need to create a producer first. " +
                                    "Use createProducer(destination)");
      }
   }


   // Inner classes -------------------------------------------------
}
