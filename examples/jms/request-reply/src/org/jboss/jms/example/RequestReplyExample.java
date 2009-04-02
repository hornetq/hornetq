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
         //Step 1. Create an initial context to perform the JNDI lookup.
         InitialContext initialContext = getContext();

         //Step 2. Lookup the queue for sending the request message
         Queue requestQueue = (Queue) initialContext.lookup("/queue/exampleQueue");         

         //Step 3. Lookup for the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");

         //Step 4. Create a JMS Connection
         connection = cf.createConnection();
         
         //Step 5. Start the connection.
         connection.start();

         //Step 6. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         //Step 7. Create a JMS Message Producer to send request message
         MessageProducer producer = session.createProducer(requestQueue);
         
         //Step 8. Create a temporary queue used to send reply message
         TemporaryQueue replyQueue = session.createTemporaryQueue();
         
         
         //Step 9. Create consumer to receive reply message
         MessageConsumer replyConsumer = session.createConsumer(replyQueue);

         //Step 10. Create a JMS Message Consumer
         MessageConsumer messageConsumer = session.createConsumer(requestQueue);
         
         //Step 11. Register a message listener as request handler
         messageConsumer.setMessageListener(new SimpleRequestHandler(cf));

         //Step 12. Create a request Text Message
         TextMessage requestMsg = session.createTextMessage("A request message");
         
         //Step 13. Set the ReplyTo header so that the request receiver knows where to send the reply.
         requestMsg.setJMSReplyTo(replyQueue);
         
         //Step 14. Set the CorrelationID so that it can be linked with corresponding reply message
         requestMsg.setJMSCorrelationID("jbm-id: 0000001");
         
         //Step 15. Sent the request message
         producer.send(requestMsg);
         
         System.out.println("Request message sent.");
         
         //Step 16. Receive the reply message.
         TextMessage replyMessageReceived = (TextMessage)replyConsumer.receive();
         
         System.out.println("Received reply: " + replyMessageReceived.getText());
         System.out.println("CorrelatedId: " + replyMessageReceived.getJMSCorrelationID());
         
         //Step 17. close the consumer.
         replyConsumer.close();
         
         //Step 18. Delete the temporary queue
         replyQueue.delete();
         
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
   
   private class SimpleRequestHandler implements MessageListener
   {
      private ConnectionFactory cf;
      
      public SimpleRequestHandler(ConnectionFactory cfact)
      {
         cf = cfact;
      }
      
      public void onMessage(Message request)
      {
         Connection connection = null;
         try
         {
            System.out.println("Received request message: " + ((TextMessage)request).getText());

            // Create a connection
            connection = cf.createConnection();

            // Create a session
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Extract the ReplyTo destination
            Destination replyDestination = request.getJMSReplyTo();

            // Create a producer to send the reply message
            MessageProducer replyProducer = session.createProducer(replyDestination);

            System.out.println("Got reply queue: " + replyDestination);

            // Create the reply message
            TextMessage replyMessage = session.createTextMessage("A reply message");

            // Set the CorrelationID
            replyMessage.setJMSCorrelationID(request.getJMSCorrelationID());

            // Send out the reply message
            replyProducer.send(replyMessage);

            // Close the producer on the replyQueue
            replyProducer.close();
         }
         catch (JMSException e)
         {
            e.printStackTrace();
         }
         finally
         {
            if (connection != null)
            {
               try 
               {
                  //Close the connection
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

}
