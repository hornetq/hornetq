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

import org.jboss.common.example.JBMExample;

/**
 * This example demonstrates how JBoss Messaging consumers can be configured to not buffer any messages from
 * the server.
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class NoConsumerBufferingExample extends JBMExample
{
   public static void main(String[] args)
   {
      new NoConsumerBufferingExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         // Step 2. Perfom a lookup on the queue
         Queue queue = (Queue)initialContext.lookup("/queue/exampleQueue");

         // Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 4. Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);
         
         // Step 7. Create a JMS MessageConsumer
         
         MessageConsumer consumer1 = session.createConsumer(queue);
         
         // Step 8. Start the connection
         
         connection.start();
         
         // Step 9. Send 10 messages to the queue
         
         final int numMessages = 10;
         
         for (int i = 0; i < numMessages; i++)
         {
            TextMessage message = session.createTextMessage("This is text message: " + i);

            producer.send(message);
         }
         
         System.out.println("Sent messages");
         
         //Step 10. Create another consumer on the same queue
         
         MessageConsumer consumer2 = session.createConsumer(queue);
         
         //Step 11. Consume three messages from consumer2
         
         for (int i = 0; i < 3; i++)
         {         
            TextMessage message = (TextMessage)consumer2.receive(2000);
            
            System.out.println("Consumed message from consumer2: " + message.getText());
         }
         
         //Step 12. Consume five messages from consumer1
         
         for (int i = 0; i < 5; i++)
         {         
            TextMessage message = (TextMessage)consumer1.receive(2000);
            
            System.out.println("Consumed message from consumer1: " + message.getText());
         }
         
         //Step 13. Consume another two messages from consumer2
         
         for (int i = 0; i < 2; i++)
         {         
            TextMessage message = (TextMessage)consumer2.receive(2000);
            
            System.out.println("Consumed message from consumer1: " + message.getText());
         }
         
         return true;
      }
      finally
      {
         // Step 9. Be sure to close our resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         
         if (connection != null)
         {
            connection.close();
         }
      }
   }

}
