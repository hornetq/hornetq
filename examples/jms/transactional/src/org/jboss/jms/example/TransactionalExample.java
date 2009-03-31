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
import javax.jms.JMSException;
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

   public void runExample() throws Exception
   {
      Connection connection = null;
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         InitialContext initialContext = getContext();

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

         // Step 8. Create a text message
         TextMessage message = session.createTextMessage("This is a text message");

         // Step 9. Send the text message to the queue
         messageProducer.send(message);

         System.out.println("Sent message: " + message.getText());

         // Step 10. Commit the session
         session.commit();
         
         // Step 11. Create a message consumer
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 12. Receive the message from the queue
         message = (TextMessage) messageConsumer.receive(5000);

         System.out.println("Received message: " + message.getText());
         
         // Step 13. Commit the session again
         session.commit();
      }
      finally
      {
         if(connection != null)
         {
            try
            {
               // Step 19. Be sure to close our JMS resources!
               connection.close();
            }
            catch (JMSException e)
            {
               e.printStackTrace();
            }
         }
      }
   }

}
