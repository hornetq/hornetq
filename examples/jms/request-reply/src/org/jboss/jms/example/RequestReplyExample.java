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
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

/**
 * A simple JMS example that shows how to use Request/Replay style messaging.
 *
 * @author <a href="hgao@redhat.com">Howard Gao</a>
 */
public class RequestReplyExample extends JMSExample
{
   public static void main(String[] args)
   {
      new RequestReplyExample().run(args);
   }

   public void runExample() throws Exception
   {
      Connection connection = null;
      try
      {
         
         //Step 1. Start the request server
         SimpleRequestServer server = new SimpleRequestServer();
         server.start();

         //Step 2. Create an initial context to perform the JNDI lookup.
         InitialContext initialContext = getContext();

         //Step 3. Lookup the queue for sending the request message
         Queue requestQueue = (Queue) initialContext.lookup("/queue/exampleQueue");         

         //Step 4. Lookup for the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");

         //Step 5. Create a JMS Connection
         connection = cf.createConnection();
         
         //Step 6. Start the connection.
         connection.start();

         //Step 7. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         //Step 8. Create a JMS Message Producer to send request message
         MessageProducer producer = session.createProducer(requestQueue);
         
         //Step 9. Create a temporary queue used to send reply message
         TemporaryQueue replyQueue = session.createTemporaryQueue();
         
         //Step 10. Create consumer to receive reply message
         MessageConsumer replyConsumer = session.createConsumer(replyQueue);

         //Step 11. Create a request Text Message
         TextMessage requestMsg = session.createTextMessage("A request message");
         
         //Step 12. Set the ReplyTo header so that the request receiver knows where to send the reply.
         requestMsg.setJMSReplyTo(replyQueue);
         
         //Step 13. Set the CorrelationID so that it can be linked with corresponding reply message
         requestMsg.setJMSCorrelationID("jbm-id: 0000001");
         
         //Step 14. Sent the request message
         producer.send(requestMsg);
         
         System.out.println("Request message sent.");
         
         //Step 15. Receive the reply message.
         TextMessage replyMessageReceived = (TextMessage)replyConsumer.receive();
         
         System.out.println("Received reply: " + replyMessageReceived.getText());
         System.out.println("CorrelatedId: " + replyMessageReceived.getJMSCorrelationID());
         
         //Step 17. close the consumer.
         replyConsumer.close();
         
         //Step 18. Delete the temporary queue
         replyQueue.delete();
         
         //Step 19. Shutdown the request server
         server.shutdown();
         
      }
      finally
      {
         //Step 19. Be sure to close our JMS resources!
         if(connection != null)
         {
            connection.close();
         }
      }
   }
   
   private class SimpleRequestServer implements MessageListener
   {
      private Connection connection;
      private Session session;
      MessageProducer replyProducer;
      MessageConsumer requestConsumer;
      
      public void start() throws Exception
      {
         //Get an initial context to perform the JNDI lookup.
         InitialContext initialContext = getContext();

         //Lookup the queue to receive the request message
         Queue requestQueue = (Queue) initialContext.lookup("/queue/exampleQueue");         

         //Lookup for the Connection Factory
         ConnectionFactory cfact = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");
         
         // Create a connection
         connection = cfact.createConnection();
         
         // Start the connection;
         connection.start();
         
         // Create a session
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         // Create a producer to send the reply message
         replyProducer = session.createProducer(null);
         
         //Create the request comsumer
         requestConsumer = session.createConsumer(requestQueue);
         
         //register the listener
         requestConsumer.setMessageListener(this);
      }

      public void onMessage(Message request)
      {
         try
         {
            System.out.println("Received request message: " + ((TextMessage)request).getText());

            // Extract the ReplyTo destination
            Destination replyDestination = request.getJMSReplyTo();

            System.out.println("Reply to queue: " + replyDestination);

            // Create the reply message
            TextMessage replyMessage = session.createTextMessage("A reply message");

            // Set the CorrelationID
            replyMessage.setJMSCorrelationID(request.getJMSCorrelationID());

            // Send out the reply message
            replyProducer.send(replyDestination, replyMessage);
            
            System.out.println("Reply sent");
         }
         catch (JMSException e)
         {
            e.printStackTrace();
         }
      }
      
      public void shutdown() throws JMSException
      {
         connection.close();
      }
   }

}
