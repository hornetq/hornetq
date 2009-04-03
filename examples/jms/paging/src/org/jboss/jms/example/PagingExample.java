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
 * @author <a href="csuconic@redhat.com">Clebert Suconic</a>
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
      
      InitialContext initialContext = null;
      try
      {
         //Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext();

         //Step 2. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");

         // Step 3. We look-up the JMS queue object from JNDI. pagingQueue is configured to hold a very limited number of bytes in memory
         Queue pageQueue = (Queue) initialContext.lookup("/queue/pagingQueue");
         
         // Step 4. Lookup for a JMS Queue 
         Queue queue = (Queue) initialContext.lookup("/queue/exampleQueue");

         // Step 5. Create a JMS Connection
         connection = cf.createConnection();
         
         //Step 6. Create a JMS Session
         Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

         //Step 7. Create a JMS Message Producer for pageQueueAddress
         MessageProducer pageMessageProducer = session.createProducer(pageQueue);
         
         //Step 8. We don't need persistent messages in order to use paging. (This step is optional)
         pageMessageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
         
         //Step 9. Create a Binary Bytes Message with 10K arbitrary bytes
         BytesMessage message = session.createBytesMessage();
         message.writeBytes(new byte[10 * 1024]);
         

         //Step 10. Send only 20 messages to the Queue. This will be already enough for pagingQueue. Look at ./paging/config/jbm-queues.xml for the config.
         for (int i = 0; i < 20; i++)
         {
            pageMessageProducer.send(message);
         }         
         
         //Step 11. Create a JMS Message Producer
         MessageProducer messageProducer = session.createProducer(queue);
         
         //Step 12. We don't need persistent messages in order to use paging. (This step is optional)
         messageProducer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         //Step 13. Send the message for about 30K, which should be over the memory limit imposed by the server
         for (int i = 0; i < 30000; i++)
         {
            messageProducer.send(message);
         }

         // Step 14. if you pause this example here, you will several files under ./build/data/paging
         // Thread.sleep(30000); // if you want to just our of curiosity, you can sleep here and inspect the created files just for 
         
         //Step 15. Create a JMS Message Consumer
         MessageConsumer messageConsumer = session.createConsumer(queue);
         

         //Step 16.  Start the JMS Connection. This step will activate the subscribers to receive messages.
         connection.start();
         
         
         //Step 17. Receive the messages. It's important to ACK for messages as JBM will not read messages from paging until messages are ACKed
         
         for (int i = 0; i < 30000; i++)
         {
            message = (BytesMessage)messageConsumer.receive(1000);

            if (i % 1000 == 0)
            {
               System.out.println("Received " + i + " messages");
               
               message.acknowledge();
            }
         }
         
         
         // Step 18. Receive the messages from the Queue names pageQueue. Create the proper consumer for that
         messageConsumer.close();
         messageConsumer = session.createConsumer(pageQueue);

         for (int i = 0; i < 20; i++)
         {
            message = (BytesMessage)messageConsumer.receive(1000);
            
            System.out.println("Received message " + i + " from pageQueue");

            message.acknowledge();
         }
         

         

      }
      finally
      {
         // And finally, always remember to close your JMS connections after use, in a finally block. Closing a JMS connection will automatically close all of its sessions, consumers, producer and browser objects
         
         if (initialContext != null)
         {
            initialContext.close();
         }
         
         if(connection != null)
         {
            connection.close();
         }
      }
   }

}
