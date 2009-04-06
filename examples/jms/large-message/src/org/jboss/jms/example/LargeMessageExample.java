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
public class LargeMessageExample extends JMSExample
{
   public static void main(String[] args)
   {
      new LargeMessageExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         //Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext();

         //Step 2. Perfom a lookup on the queue
         Queue queue = (Queue) initialContext.lookup("/queue/exampleQueue");

         //Step 3. Perform a lookup on the Connection Factory. This ConnectionFactory has a special set on this example. Messages with more than 10K are considered large
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");

         //Step 4.Create a JMS Connection
         connection = cf.createConnection();

         //Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         //Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         //Step 7. Create a BytesMessage with 1MB arbitrary bytes
         BytesMessage message = session.createBytesMessage();
         byte[] bytes = new byte[100 * 1024 * 1024];
         message.writeBytes(bytes);
         
         System.out.println("Sending message of " + bytes.length + " bytes");
         
         //Step 8. Send the Message
         producer.send(message);
         
         System.out.println("Large Message sent");
         
         // if you sleep the example and look at ./build/data/largeMessages you will see the largeMessage stored on disk

         //Step 9. Create a JMS Message Consumer
         MessageConsumer messageConsumer = session.createConsumer(queue);

         //Step 10. Start the Connection
         connection.start();

         //Step 11. Receive the message
         BytesMessage messageReceived = (BytesMessage) messageConsumer.receive(60000);

         System.out.println("Received message: " + messageReceived.getBodyLength() + " bytes");

         initialContext.close();
         
         return true;
      }
      finally
      {
         //Step 12. Be sure to close our JMS resources!
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
