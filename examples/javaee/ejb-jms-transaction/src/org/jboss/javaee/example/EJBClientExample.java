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
package org.jboss.javaee.example;

import org.jboss.javaee.example.server.SendMessageService;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

/**
 * An example showing how to invoke a EJB which sends a JMS message and update a JDBC table in the same transaction.
 * 
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class EJBClientExample
{
   public static void main(String[] args) throws Exception
   {
      InitialContext initialContext = null;
      Connection connection = null;
      try
      {
         // Step 1. Obtain an Initial Context
         initialContext = new InitialContext();

         // Step 2. Lookup the EJB
         SendMessageService service = (SendMessageService)initialContext.lookup("ejb-jms-transaction-example/SendMessageBean/remote");

         // Step 3. Create the DB table which will be updated
         service.createTable();

         // Step 4. Invoke the sendAndUpdate method
         service.sendAndUpdate("This is a text message");
         System.out.println("invoked the EJB service");

         // Step 5. Lookup the JMS connection factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 6. Lookup the queue
         Queue queue = (Queue)initialContext.lookup("queue/testQueue");

         // Step 7. Create a connection, a session and a message consumer for the queue
         connection = cf.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(queue);

         // Step 8. Start the connection
         connection.start();

         // Step 9. Receive the message sent by the EJB
         TextMessage messageReceived = (TextMessage)consumer.receive(5000);
         System.out.println("Received message: " + messageReceived.getText() +
                            " (" +
                            messageReceived.getJMSMessageID() +
                            ")");
      }
      finally
      {
         // Step 10. Be sure to close the resources!         
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
