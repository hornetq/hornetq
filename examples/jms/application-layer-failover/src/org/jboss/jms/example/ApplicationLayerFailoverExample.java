/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
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
package org.jboss.jms.example;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.common.example.JBMExample;

/**
 * A simple example that demonstrates application-layer failover of the JMS connection from one node to another
 * when the live server crashes
 *
 * @author <a href="tim.fox@jboss.com>Tim Fox</a>
 */
public class ApplicationLayerFailoverExample extends JBMExample
{
   public static void main(String[] args)
   {
      new ApplicationLayerFailoverExample().run(args);
   }

   private volatile InitialContext initialContext;

   private volatile Connection connection;

   private volatile Session session;

   private volatile MessageConsumer consumer;

   private volatile MessageProducer producer;

   private final CountDownLatch failoverLatch = new CountDownLatch(1);

   public boolean runExample() throws Exception
   {
      try
      {
         // Step 1. We create our JMS Connection, Session, MessageProducer and MessageConsumer on server 1.
         createJMSObjects(1);

         // Step 2. We set a JMS ExceptionListener on the connection. On failure this will be called and the connection,
         // session, etc. will be then recreated on the backup node.
         connection.setExceptionListener(new ExampleListener());

         System.out.println("The initial JMS objects have been created, and the ExceptionListener set");

         // Step 3. We send some messages to server 1, the live server

         final int numMessages = 10;

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage message = session.createTextMessage("This is text message " + i);

            producer.send(message);

            System.out.println("Sent message: " + message.getText());
         }

         // Step 4. We consume those messages on server 1.

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage message0 = (TextMessage)consumer.receive(5000);

            System.out.println("Got message: " + message0.getText());
         }

         // Step 5. We now cause server 1, the live server to crash. After a little while the connection's
         // ExceptionListener will register the failure and reconnection will occur.

         System.out.println("Killing the server");

         killServer(1);

         // Step 6. Wait for the client side to register the failure and reconnect

         boolean ok = failoverLatch.await(5000, TimeUnit.MILLISECONDS);

         if (!ok)
         {
            return false;
         }

         System.out.println("Reconnection has occurred. Now sending more messages.");

         // Step 8. We now send some more messages

         for (int i = numMessages; i < numMessages * 2; i++)
         {
            TextMessage message = session.createTextMessage("This is text message " + i);

            producer.send(message);

            System.out.println("Sent message: " + message.getText());
         }

         // Step 9. And consume them.

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage message0 = (TextMessage)consumer.receive(5000);

            System.out.println("Got message: " + message0.getText());
         }

         return true;
      }
      finally
      {
         // Step 14. Be sure to close our resources!

         closeResources();
      }
   }

   private void createJMSObjects(final int server) throws Exception
   {
      // Step 1. Get an initial context for looking up JNDI from the server
      initialContext = getContext(server);

      // Step 2. Look-up the JMS Queue object from JNDI
      Queue queue = (Queue)initialContext.lookup("/queue/exampleQueue");

      // Step 3. Look-up a JMS Connection Factory object from JNDI on server 1
      ConnectionFactory connectionFactory = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

      // Step 4. We create a JMS Connection connection
      connection = connectionFactory.createConnection();

      // Step 6. We start the connection to ensure delivery occurs
      connection.start();

      // Step 5. We create a JMS Session
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

      // Step 7. We create a JMS MessageConsumer object
      consumer = session.createConsumer(queue);

      // Step 8. We create a JMS MessageProducer object
      producer = session.createProducer(queue);
   }

   private void closeResources() throws Exception
   {
      if (initialContext != null)
      {
         initialContext.close();
      }

      if (connection != null)
      {
         connection.close();
      }
   }

   private class ExampleListener implements ExceptionListener
   {
      public void onException(JMSException exception)
      {
         try
         {
            // Step 7. The ExceptionListener gets called and we recreate the JMS objects on the new node

            System.out.println("Connection failure has been detected on a the client.");

            // Close the old resources

            // closeResources();

            System.out.println("The old resources have been closed.");

            // Create new JMS objects on the backup server

            createJMSObjects(0);

            System.out.println("The new resources have been created.");

            failoverLatch.countDown();
         }
         catch (Exception e)
         {
            System.err.println("Failed to handle failover");

            e.printStackTrace();
         }
      }
   }
}
