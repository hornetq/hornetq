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
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

/**
 * An example which sends a message to a source queue and consume from a target queue.
 * The source and target queues are bridged by a JMS Bridge configured and running in JBoss AS 5.
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class JMSBridgeExample
{
   public static void main(String[] args) throws Exception
   {
      InitialContext initialContext = null;
      Connection sourceConnection = null;
      Connection targetConnection = null;
      try
      {
         // Step 1. Obtain an Initial Context
         initialContext = new InitialContext();

         // Step 2. Lookup the JMS connection factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 3. Lookup the source queue
         Queue sourceQueue = (Queue)initialContext.lookup("/queue/source");

         // Step 4. Create a connection, a session and a message producer for the *source* queue
         sourceConnection = cf.createConnection();
         Session sourceSession = sourceConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer sourceProducer = sourceSession.createProducer(sourceQueue);

         // Step 5. Create and send a text message to the *source* queue
         TextMessage message = sourceSession.createTextMessage("this is a text message");
         sourceProducer.send(message);
         System.out.format("Sent message to %s: %s\n",
                           ((Queue)message.getJMSDestination()).getQueueName(),
                           message.getText());
         System.out.format("Message ID : %s\n", message.getJMSMessageID());

         // Step 6. Close the *source* connection
         sourceConnection.close();

         // Step 7. Lookup the *target* queue
         Queue targetQueue = (Queue)initialContext.lookup("/queue/target");

         // Step 8. Create a connection, a session and a message consumer for the *target* queue
         targetConnection = cf.createConnection();
         Session targetSession = targetConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer targetConsumer = targetSession.createConsumer(targetQueue);

         // Step 9. Start the connection to receive messages from the *targe* queue
         targetConnection.start();

         // Step 10. Receive a message from the *target* queue
         TextMessage messageReceived = (TextMessage)targetConsumer.receive(5000);
         System.out.format("\nReceived from %s: %s\n",
                           ((Queue)messageReceived.getJMSDestination()).getQueueName(),
                           messageReceived.getText());
         
         // Step 11. Display the received message's ID
         System.out.format("Message ID         : %s\n", messageReceived.getJMSMessageID());

         // Step 12. Display the message ID of the message received by the *bridge*
         System.out.format("Bridged Message ID : %s\n", messageReceived.getStringProperty("JBM_BRIDGE_MSG_ID_LIST"));
      }
      finally
      {
         // Step 13. Be sure to close the resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         if (sourceConnection != null)
         {
            sourceConnection.close();
         }
         if (targetConnection != null)
         {
            targetConnection.close();
         }
      }
   }
}
