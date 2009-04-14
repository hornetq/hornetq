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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

/**
 * A simple example that demonstrates automatic failover of the JMS connection from one node to another
 * when the live server crashes
 *
 * @author <a href="tim.fox@jboss.com>Tim Fox</a>
 */
public class AutomaticFailoverExample extends JMSExample
{
   public static void main(String[] args)
   {
      new AutomaticFailoverExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection connection = null;

      InitialContext initialContext = null;

      try
      {
         // Step 1. Get an initial context for looking up JNDI from the server
         initialContext = getContext(1);

         // Step 2. Look-up the JMS Queue object from JNDI
         Queue queue = (Queue)initialContext.lookup("/queue/exampleQueue");

         // Step 3. Look-up a JMS Connection Factory object from JNDI on server 1
         ConnectionFactory connectionFactory = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 4. We create a JMS Connection connection
         connection = connectionFactory.createConnection();

         // Step 5. We create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. We start the connection to ensure delivery occurs
         connection.start();

         // Step 7. We create a JMS MessageConsumer object
         MessageConsumer consumer = session.createConsumer(queue);

         // Step 8. We create a JMS MessageProducer object
         MessageProducer producer = session.createProducer(queue);

         // Step 9. We send some messages to server 1, the live server

         final int numMessages = 10;

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage message = session.createTextMessage("This is text message " + i);

            producer.send(message);

            System.out.println("Sent message: " + message.getText());
         }

         // Step 10. We now cause server 1, the live server to crash, and wait a little while to make sure
         // it has really crashed

         killServer(1);
         
         Thread.sleep(2000);
         
         // Step 11. We consume the messages sent before the crash of the live server. We are now transparently
         // reconnected to server 0 - the backup server.

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage message0 = (TextMessage)consumer.receive(5000);

            System.out.println("Got message: " + message0.getText());
         }

         // Step 12. We now send some more messages

         for (int i = numMessages; i < numMessages * 2; i++)
         {
            TextMessage message = session.createTextMessage("This is text message " + i);

            producer.send(message);

            System.out.println("Sent message: " + message.getText());
         }

         // Step 13. And consume them.
         
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

         if (connection != null)
         {
            connection.close();
         }

         if (initialContext != null)
         {
            initialContext.close();
         }
      }
   }

}
