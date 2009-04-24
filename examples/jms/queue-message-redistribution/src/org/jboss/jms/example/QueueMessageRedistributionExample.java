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
 * This example demonstrates a queue with the same name deployed on two nodes of a cluster.
 * Messages are initially round robin'd between both nodes of the cluster.
 * The consumer on one of the nodes is then closed, and we demonstrate that the "stranded" messages
 * are redistributed to the other node which has a consumer so they can be consumed.
 *
 * @author <a href="tim.fox@jboss.com>Tim Fox</a>
 */
public class QueueMessageRedistributionExample extends JMSExample
{
   public static void main(String[] args)
   {
      new QueueMessageRedistributionExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection connection0 = null;

      Connection connection1 = null;

      InitialContext ic0 = null;

      InitialContext ic1 = null;

      try
      {
         // Step 1. Get an initial context for looking up JNDI from server 0
         ic0 = getContext(0);

         // Step 2. Look-up the JMS Queue object from JNDI
         Queue queue = (Queue)ic0.lookup("/queue/exampleQueue");

         // Step 3. Look-up a JMS Connection Factory object from JNDI on server 0
         ConnectionFactory cf0 = (ConnectionFactory)ic0.lookup("/ConnectionFactory");

         // Step 4. Get an initial context for looking up JNDI from server 1
         ic1 = getContext(1);

         // Step 5. Look-up a JMS Connection Factory object from JNDI on server 1
         ConnectionFactory cf1 = (ConnectionFactory)ic1.lookup("/ConnectionFactory");

         // Step 6. We create a JMS Connection connection0 which is a connection to server 0
         connection0 = cf0.createConnection();

         // Step 7. We create a JMS Connection connection1 which is a connection to server 1
         connection1 = cf1.createConnection();

         // Step 8. We create a JMS Session on server 0, note the session is CLIENT_ACKNOWLEDGE
         Session session0 = connection0.createSession(false, Session.CLIENT_ACKNOWLEDGE);

         // Step 9. We create a JMS Session on server 1, note the session is CLIENT_ACKNOWLEDGE
         Session session1 = connection1.createSession(false, Session.CLIENT_ACKNOWLEDGE);

         // Step 10. We start the connections to ensure delivery occurs on them
         connection0.start();

         connection1.start();

         // Step 11. We create JMS MessageConsumer objects on server 0 and server 1
         MessageConsumer consumer0 = session0.createConsumer(queue);

         MessageConsumer consumer1 = session1.createConsumer(queue);

         Thread.sleep(1000);

         // Step 12. We create a JMS MessageProducer object on server 0
         MessageProducer producer = session0.createProducer(queue);

         // Step 13. We send some messages to server 0

         final int numMessages = 10;

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage message = session0.createTextMessage("This is text message " + i);

            producer.send(message);

            System.out.println("Sent message: " + message.getText());
         }

         // Step 14. We now consume those messages on *both* server 0 and server 1.
         // We note the messages have been distributed between servers in a round robin fashion
         // JMS Queues implement point-to-point message where each message is only ever consumed by a
         // maximum of one consumer

         TextMessage message0 = null;
         
         TextMessage message1 = null;
         
         for (int i = 0; i < numMessages; i += 2)
         {
            message0 = (TextMessage)consumer0.receive(5000);

            System.out.println("Got message: " + message0.getText() + " from node 0");

            message1 = (TextMessage)consumer1.receive(5000);

            System.out.println("Got message: " + message1.getText() + " from node 1");
         }
         
         // Step 15. We acknowledge the messages consumed on node 0. The sessions are CLIENT_ACKNOWLEDGE so
         // messages will not get acknowledged until they are explicitly acknowledged.
         // Note that we *do not* acknowledge the message consumed on node 1 yet.
         message0.acknowledge();
         
         // Step 16. We now close the session and consumer on node 1. (Closing the session automatically closes the consumer)
         session1.close();
         
         // Step 17. Since there is no more consumer on node 1, the messages on node 1 are now stranded (no local consumers)
         // so JBoss Messaging will redistribute them to node 0 so they can be consumed.
         
         for (int i = 0; i < numMessages; i += 2)
         {
            message0 = (TextMessage)consumer0.receive(5000);

            System.out.println("Got message: " + message0.getText() + " from node 0");           
         }
         
         // Step 18. We ack the messages.
         message0.acknowledge();
        
         return true;
      }
      finally
      {
         // Step 18. Be sure to close our resources!

         if (connection0 != null)
         {
            connection0.close();
         }

         if (connection1 != null)
         {
            connection1.close();
         }

         if (ic0 != null)
         {
            ic0.close();
         }

         if (ic1 != null)
         {
            ic1.close();
         }
      }
   }

}
