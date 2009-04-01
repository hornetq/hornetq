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

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.InitialContext;

/**
 * A simple JMS Queue example that creates a producer and consumer on a queue and sends then receives a message.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class PagingExample extends JMSExample
{
   public static void main(String[] args)
   {
      new PagingExample().run(args);
   }

   public void runExample() throws Exception
   {
      Connection connection = null;
      try
      {
         //Step 1. Create an initial context to perform the JNDI lookup.
         InitialContext initialContext = getContext();

         //Step 2. Perfom a lookup on the queue
         Queue queue = (Queue) initialContext.lookup("/queue/exampleQueue");

         //Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");

         //Step 4.Create a JMS Connection
         connection = cf.createConnection();

         //Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

         //Step 6. Create a JMS Message Producer
         MessageProducer messageProducer = session.createProducer(queue);
         
         //Step 7. We don't need persistent messages in order to use paging. (This step is optional)
         messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         //Step 8. Create a Binary Bytes Message with 10K arbitrary bytes
         BytesMessage message = session.createBytesMessage();
         message.writeBytes(new byte[10 * 1024]);
         
         //Step 9. Send the message for about 30K, which should be over the memory limit imposed by the server
         for (int i = 0; i < 30000; i++)
         {
            messageProducer.send(message);
         }
         
         
         //Step 10. Create a JMS Message Consumer
         MessageConsumer messageConsumer = session.createConsumer(queue);
         

         //Step 11.  Start the JMS Connection. This step will activate the subscribers to receive messages.
         connection.start();
         
         
         //Step 12. Receive the messages. 
         //         It's important to ACK for messages as JBM will not read messages from paging until messages are ACKed or that would lead the server to be OutOfMemory
         
         for (int i = 0; i < 30000; i++)
         {
            message = (BytesMessage)messageConsumer.receive(1000);

            if (i % 1000 == 0)
            {
               System.out.println("Received " + i + " messages");
               
               message.acknowledge();
            }
         }
         
         System.out.println("Received 30000 messages");

         initialContext.close();
      }
      finally
      {
         //Step 12. Be sure to close our JMS resources!
         if(connection != null)
         {
            connection.close();
         }
      }
   }

}
