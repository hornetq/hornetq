/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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

import java.util.Enumeration;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

/**
 * A simple example which shows how to use a QueueBrowser to look at messages of a queue without removing them from the queue
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class QueueBrowserExample extends JMSExample
{
   public static void main(String[] args)
   {
      new QueueBrowserExample().run(args);
   }

   @Override
   public void runExample() throws Exception
   {
      Connection connection = null;
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         InitialContext initialContext = getContext();

         // Step 2. Perfom a lookup on the queue
         Queue queue = (Queue)initialContext.lookup("/queue/exampleQueue");

         // Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 4.Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         // Step 7. Create 2 Text Messages
         TextMessage message_1 = session.createTextMessage("this is the 1st message");
         TextMessage message_2 = session.createTextMessage("this is the 2nd message");

         // Step 8. Send the Message
         producer.send(message_1);
         producer.send(message_2);

         // Step 9. Create the JMS QueueBrowser
         QueueBrowser browser = session.createBrowser(queue);
         
         // Step 10. Browse the messages on the queue
         // Browsing a queue does not remove the messages from the queue
         Enumeration messageEnum = browser.getEnumeration();
         while (messageEnum.hasMoreElements())
         {
            TextMessage message = (TextMessage)messageEnum.nextElement();
            System.out.println("Browsing: " + message.getText());
         }

         // Step 11. Close the browser
         browser.close();
         
         // Step 12. Create a JMS Message Consumer
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 13. Start the Connection
         connection.start();

         // Step 14. Receive the 2 messages
         TextMessage messageReceived = (TextMessage)messageConsumer.receive(5000);
         System.out.println("Received message: " + messageReceived.getText());
         messageReceived = (TextMessage)messageConsumer.receive(5000);
         System.out.println("Received message: " + messageReceived.getText());
         
      }
      finally
      {
         // Step 15. Be sure to close our JMS resources!
         if (connection != null)
         {
            connection.close();
         }
      }
   }
}
