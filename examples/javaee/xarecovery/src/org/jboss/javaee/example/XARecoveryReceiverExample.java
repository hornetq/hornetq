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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

/**
 * An example showing how to receive a JMS message.
 * The JMS message was recovered after the server was crashed and restarted.
 *  
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class XARecoveryReceiverExample
{
   public static void main(String[] args) throws Exception
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

         System.out.println("waiting to receive a message...");

         // Step 6. Receive the message sent by the EJB
         TextMessage messageReceived = (TextMessage)consumer.receive(3600 * 1000);
         System.out.format("Received message: %s (%s)\n", messageReceived.getText(), messageReceived.getJMSMessageID());
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
