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
package org.jboss.test.messaging.tools.client;

import org.jboss.logging.Logger;

import javax.jms.ConnectionFactory;
import javax.jms.Connection;
import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.naming.InitialContext;

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

   private static final Logger log = Logger.getLogger(Client.class);

   // Static --------------------------------------------------------

   // Attributes ----------------------------------------------------

   private ConnectionFactory connectionFactory;
   private Connection connection;
   private Session session;
   private MessageProducer producer;
   private MessageConsumer consumer;

   // Constructors --------------------------------------------------

   public Client() throws Exception
   {
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
         InitialContext ic = new InitialContext();
         connectionFactory = (ConnectionFactory)ic.lookup("/ConnectionFactory");
         ic.close();
      }
   }

   /**
    *
    * @param destination - the destination's fully qualified JNDI name
    */
   public Destination lookupDestination(String destination) throws Exception
   {
      InitialContext ic = new InitialContext();
      Destination d = (Destination)ic.lookup(destination);
      ic.close();
      return d;
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


   public void startConnection() throws Exception
   {
      if (connection == null)
      {
         System.out.println("There is no connection. Create a connection first");
         return;
      }

      connection.start();
   }


   public void stopConnection() throws Exception
   {
      if (connection == null)
      {
         System.out.println("There is no connection. Create a connection first");
         return;
      }

      connection.stop();
   }


   public void createSession(boolean transacted, String acknowledgmentModeString) throws Exception
   {
      if (session != null)
      {
         System.out.println("Session already created");
         return;
      }

      int acknowledgmentMode = -1;
      if (!transacted)
      {
         acknowledgmentMode = acknowledgmentModeToInt(acknowledgmentModeString);
      }

      if (connection == null)
      {
         createConnection();
      }

      session = connection.createSession(transacted, acknowledgmentMode);
      displaySessionInfo(session);
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
         createSession(false, "AUTO_ACKNOWLEDGE");
      }

      producer = session.createProducer(lookupDestination(destination));

      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      log.info("The producer's delivery mode was set ot NON_PERSISTENT");
   }

   public void createConsumer(String destination) throws Exception
   {

      if (consumer != null)
      {
         System.out.println("Consumer already created");
         return;
      }

      if (session == null)
      {
         createSession(false, "AUTO_ACKNOWLEDGE");
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

   public void unsetMessageListener() throws Exception
   {
      if (consumer == null)
      {
         throw new Exception("You need to create a consumer first. " +
                             "Use createConsumer(destination)");
      }
      consumer.setMessageListener(null);
   }





   public void send() throws Exception
   {
      insureProducer();
      Message m = session.createMessage();
      producer.send(m);
   }

   /**
    * Sends a burst of messages.
    * @throws Exception
    */
   public void send(int count) throws Exception
   {
      insureProducer();
      Message m;
      for(int i = 0; i < count; i ++)
      {
         m = session.createMessage();
         producer.send(m);
      }
   }


   public Object receive(long timeout) throws Exception
   {
      insureConsumer();
      return consumer.receive(timeout);
   }

   public void receiveOnASeparateThread(final int workTimeMs)
   {
      insureConsumer();
      new Thread(new Runnable() {
         public void run()
         {
            int cnt = 1;
            while(true)
            {
               try
               {
                  consumer.receive();
                  System.out.println("received " + cnt++);

                  // process
                  Thread.sleep(workTimeMs);
               }
               catch(Exception e)
               {
                  e.printStackTrace();
               }
            }
         }}, "Consumer").start();
   }

   public Object receiveNoWait() throws Exception
   {
      insureConsumer();
      return consumer.receiveNoWait();
   }

   public void closeConnection() throws Exception
   {
      if (connection == null)
      {
         return;
      }
      connection.close();
   }

   public void closeSession() throws Exception
   {
      if (session == null)
      {
         return;
      }
      session.close();
   }

   public void closeProducer() throws Exception
   {
      if (producer == null)
      {
         return;
      }
      producer.close();
   }

   public void closeConsumer() throws Exception
   {
      if (consumer == null)
      {
         return;
      }
      consumer.close();
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

   private int acknowledgmentModeToInt(String ackModeString)
   {
      ackModeString = ackModeString.toUpperCase();
      if ("AUTO_ACKNOWLEDGE".equals(ackModeString))
      {
         return Session.AUTO_ACKNOWLEDGE;
      }
      else if ("CLIENT_ACKNOWLEDGE".equals(ackModeString))
      {
         return Session.CLIENT_ACKNOWLEDGE;
      }
      else if ("DUPS_OK_ACKNOWLEDGE".equals(ackModeString))
      {
         return Session.DUPS_OK_ACKNOWLEDGE;
      }
      throw new IllegalArgumentException("No such acknowledgment mode: " + ackModeString);
   }

   private String acknowledgmentModeToString(int ackMode)
   {

      if (ackMode == Session.AUTO_ACKNOWLEDGE)
      {
         return "AUTO_ACKNOWLEDGE";
      }
      else if (ackMode == Session.CLIENT_ACKNOWLEDGE)
      {
         return "CLIENT_ACKNOWLEDGE";
      }
      else if (ackMode == Session.DUPS_OK_ACKNOWLEDGE)
      {
         return "DUPS_OK_ACKNOWLEDGE";
      }
      return "UNKNOWN: " + ackMode;
   }


   public void displaySessionInfo(Session s) throws JMSException
   {
      if (s.getTransacted())
      {
         log.info("The session is TRANSACTED");
      }
      else
      {
         log.info("The session is NON-TRANSACTED, the delivery mode is " +
                  acknowledgmentModeToString(s.getAcknowledgeMode()));
      }
   }


   // Inner classes -------------------------------------------------
}
