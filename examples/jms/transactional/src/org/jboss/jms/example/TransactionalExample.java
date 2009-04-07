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
 * A simple JMS example that sends and consume message transactionally.
 *
 * @author <a href="hgao@redhat.com">Howard Gao</a>
 */
public class TransactionalExample extends JMSExample
{
   public static void main(String[] args)
   {
      new TransactionalExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         // Step 2. Look-up the JMS topic
         Queue queue = (Queue) initialContext.lookup("/queue/exampleQueue");

         // Step 3. Look-up the JMS connection factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");
         
         // Step 4. Create a JMS connection
         connection = cf.createConnection();

         // Step 5. Start the connection
         connection.start();

         // Step 6. Create a transactional JMS session
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         
         // Step 7. Create a JMS message producer
         MessageProducer messageProducer = session.createProducer(queue);

         // Step 8. Create a message consumer
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 9. Create 2 text messages
         TextMessage message1 = session.createTextMessage("This is a text message1");
         TextMessage message2 = session.createTextMessage("This is a text message2");

         // Step 10. Send the text messages to the queue
         messageProducer.send(message1);
         messageProducer.send(message2);

         System.out.println("Sent message: " + message1.getText());
         System.out.println("Sent message: " + message2.getText());

         // Step 11. Receive the message, it will return null as the transaction is not committed.
         TextMessage receivedMessage = (TextMessage) messageConsumer.receive(5000);
         
         System.out.println("Message received before send commit: " + receivedMessage);

         // Step 12. Commit the session
         session.commit();

         // Step 13. Receive the messages again
         receivedMessage = (TextMessage) messageConsumer.receive(5000);
         
         System.out.println("Message received after send commit: " + receivedMessage.getText());
         
         // Step 14. Roll back the session, this will cause the received message canceled and redelivered again.
         session.rollback();
         
         // Step 15. Receive the message again, we will get two messages
         receivedMessage = (TextMessage) messageConsumer.receive(5000);
         
         System.out.println("Message1 received after receive rollback: " + receivedMessage.getText());

         receivedMessage = (TextMessage) messageConsumer.receive(5000);
         
         System.out.println("Message2 received after receive rollback: " + receivedMessage.getText());
         
         receivedMessage = (TextMessage) messageConsumer.receive(5000);
         
         System.out.println("Message3 received after receive rollback: " + receivedMessage);
         
         //Step 16. Commit the session
         session.commit();
         
         //Step 17. Receive the message again. Nothing should be received.
         receivedMessage = (TextMessage) messageConsumer.receive(5000);
         
         if (receivedMessage != null)
         {
            // This was not supposed to happen
            return false;
         }
         
         System.out.println("Message received after receive commit: " + receivedMessage);
         
         return true;
         
      }
      finally
      {
         if(connection != null)
         {
            // Step 18. Be sure to close our JMS resources!
            connection.close();
         }
         if(initialContext != null)
         {
            // Step 19. Also close initial context!
            initialContext.close();
         }
      }
   }

}
