/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.jms.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.hornetq.common.example.HornetQExample;

/**
 * A simple example that demonstrates automatic failover of the JMS connection from one node to another
 * when the live server crashes
 *
 * @author <a href="tim.fox@jboss.com>Tim Fox</a>
 */
public class AutomaticFailoverExample extends HornetQExample
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
