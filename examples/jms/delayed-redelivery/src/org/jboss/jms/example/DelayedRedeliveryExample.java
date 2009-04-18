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
 * This example demonstrates how JBoss Messaging can be configured with a redelivery delay in the event a message
 * is redelivered.
 * 
 * Please see the readme.html for more information
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class DelayedRedeliveryExample extends JMSExample
{
   public static void main(String[] args)
   {
      new DelayedRedeliveryExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         // Step 2. Perform a lookup on the queue
         Queue queue = (Queue)initialContext.lookup("/queue/exampleQueue");

         // Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 4. Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Create a transacted JMS Session
         Session session = connection.createSession(true, 0);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         // Step 7. Create a Text Message
         TextMessage message = session.createTextMessage("this is a text message");

         // Step 8. Send the Message
         producer.send(message);
         
         System.out.println("Sent message to " + queue.getQueueName() + ": " + message.getText());

         // Step 9. Commit the session to effectively send the message
         session.commit();

         // Step 10. Create a JMS Message Consumer for the queue
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 11. Start the Connection
         connection.start();

         // Step 12. We receive a message...
         TextMessage messageReceived = (TextMessage)messageConsumer.receive(5000);
         System.out.println("1st delivery from " + queue.getQueueName() + ": " + messageReceived.getText());

         // Step 13. ... but we roll back the session. the message returns to the queue, but only after a 
         // 5 second delay
         session.rollback();
                  
         // Step 14. We try to receive the message but it's being delayed
         messageReceived = (TextMessage)messageConsumer.receive(3000);
         
         if (messageReceived != null)
         {
            return false;
         }
         
         System.out.println("Redelivery has been delayed so received message is " + messageReceived);
         
         // Step 15. We try and receive the message again, this time we should get it
         
         messageReceived = (TextMessage)messageConsumer.receive(3000);
         
         System.out.println("2nd delivery from " + queue.getQueueName() + ": " + messageReceived.getText());
         
         // Step 16. We rollback the session again to cause another redelivery, and we time how long this one takes
         
         long start = System.currentTimeMillis();
         
         session.rollback();
                         
         messageReceived = (TextMessage)messageConsumer.receive(8000);
         
         long end = System.currentTimeMillis();
         
         System.out.println("3nd delivery from " + queue.getQueueName() + ": " + messageReceived.getText() +
                            " after " + (end - start) + " milliseconds.");
                 
         return true;
      }
      finally
      {
         // Step 17. Be sure to close our JMS resources!
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
