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
package org.hornetq.javaee.example;

import java.util.Date;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.hornetq.javaee.example.server.XARecoveryExampleService;

/**
 * An example which invokes an EJB. The EJB will be involved in a
 * transaction with a "buggy" XAResource to crash the server.
 * When the server is restarted, the recovery manager will recover the message
 * so that the consumer can receive it.
 * 
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class XARecoveryExample
{

   public static void main(final String[] args) throws Exception
   {
      InitialContext initialContext = null;
      try
      {
         // Step 1. Obtain an Initial Context
         initialContext = new InitialContext();

         // Step 2. Lookup the EJB
         XARecoveryExampleService service = (XARecoveryExampleService)initialContext.lookup("xarecovery-example/XARecoveryExampleBean/remote");

         // Step 3. Invoke the send method. This will crash the server
         String message = "This is a text message sent at " + new Date();
         System.out.println("invoking the EJB service with text: " + message);
         try
         {
            service.send(message);
         }
         catch (Exception e)
         {
            System.out.println("#########################");
            System.out.println("The server crashed: " + e.getMessage());
            System.out.println("#########################");
         }

         System.out.println("\n\n\nRestart the server by running 'ant restart' in a terminal");

         // Step 4. We will try to receive a message. Once the server is restarted, the message
         // will be recovered and the consumer will receive it
         System.out.println("Waiting for the server to restart and recover before receiving a message");

         boolean received = false;
         while (!received)
         {
            try
            {
               Thread.sleep(15000);
               XARecoveryExample.receiveMessage();
               received = true;
            }
            catch (Exception e)
            {
               System.out.println(".");
            }
         }
      }
      finally
      {
         // Step 4. Be sure to close the resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
      }
   }

   private static void receiveMessage() throws Exception
   {
      InitialContext initialContext = null;
      Connection connection = null;
      try
      {
         // Step 1. Obtain an Initial Context
         initialContext = new InitialContext();

         // Step 2. Lookup the JMS connection factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 3. Lookup the queue
         Queue queue = (Queue)initialContext.lookup("queue/testQueue");

         // Step 4. Create a connection, a session and a message consumer for the queue
         connection = cf.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(queue);

         // Step 5. Start the connection
         connection.start();

         // Step 6. Receive the message sent by the EJB
         System.out.println("\nwaiting to receive a message...");
         TextMessage messageReceived = (TextMessage)consumer.receive(3600 * 1000);
         System.out.format("Received message: %s \n\t(JMS MessageID: %s)\n",
                           messageReceived.getText(),
                           messageReceived.getJMSMessageID());
      }
      finally
      {
         // Step 7. Be sure to close the resources!
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
