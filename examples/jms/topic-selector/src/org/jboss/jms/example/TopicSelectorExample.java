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
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;

/**
 * A simple JMS Topic example that creates a producer and consumer on a queue and sends and receives a message.
 *
 * @author <a href="csuconic@redhat.com">Clebert Suconic</a>
 */
public class TopicSelectorExample extends JMSExample
{
   public static void main(String[] args)
   {
      new TopicSelectorExample().run(args);
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
         Topic topic = (Topic)initialContext.lookup("/topic/exampleTopic");

         // Step 3. Look-up the JMS connection factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 4. Create a JMS connection
         connection = cf.createConnection();

         // Step 5. Create a JMS session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS message producer
         MessageProducer producer = session.createProducer(topic);

         // Step 7. Create one subscription with a specific Filter for someID=1
         MessageConsumer messageConsumer1 = session.createConsumer(topic, "someID=1", false);

         // Step 8. Create another subscription with a specific Filter for someID=2
         MessageConsumer messageConsumer2 = session.createConsumer(topic, "someID=2", false);

         // Step 9. Create another subscription with no filters, which will receive every message sent to the topic
         MessageConsumer messageConsumer3 = session.createConsumer(topic);

         // Step 10. Send 20 messages, 10 with someID=1, 10 with someID=2

         for (int i = 1; i < 10; i++)
         {
            for (int someID = 1; someID <= 2; someID++)
            {
               // Step 10.1 Create a text message
               TextMessage message1 = session.createTextMessage("This is a text message " + i +
                                                                " sent for someID=" +
                                                                someID);

               // Step 10.1 Set a property
               message1.setIntProperty("someID", someID);

               // Step 10.2 Send the message
               producer.send(message1);

               System.out.println("Sent message: " + message1.getText());
            }
         }

         // Step 11. Start the JMS Connection. This step will activate the subscribers to receive messages.
         connection.start();

         // Step 12. Consume the messages from MessageConsumer1, filtering out someID=2

         System.out.println("*************************************************************");
         System.out.println("MessageConsumer1 will only receive messages where someID=1:");
         for (;;)
         {
            TextMessage messageReceivedA = (TextMessage)messageConsumer1.receive(1000);
            if (messageReceivedA == null)
            {
               break;
            }

            System.out.println("messageConsumer1 received " + messageReceivedA.getText() +
                               " someID = " +
                               messageReceivedA.getIntProperty("someID"));
         }

         // Step 13. Consume the messages from MessageConsumer2, filtering out someID=2
         System.out.println("*************************************************************");
         System.out.println("MessageConsumer2 will only receive messages where someID=2:");
         for (;;)
         {
            TextMessage messageReceivedB = (TextMessage)messageConsumer2.receive(1000);
            if (messageReceivedB == null)
            {
               break;
            }

            System.out.println("messageConsumer2 received " + messageReceivedB.getText() +
                               " someID = " +
                               messageReceivedB.getIntProperty("someID"));
         }

         // Step 14. Consume the messages from MessageConsumer3, receiving the complete set of messages
         System.out.println("*************************************************************");
         System.out.println("MessageConsumer3 will receive every message:");
         for (;;)
         {
            TextMessage messageReceivedC = (TextMessage)messageConsumer3.receive(1000);
            if (messageReceivedC == null)
            {
               break;
            }
            System.out.println("messageConsumer3 received " + messageReceivedC.getText() +
                               " someID = " +
                               messageReceivedC.getIntProperty("someID"));
         }

         // Step 15. Close the subscribers
         messageConsumer1.close();
         messageConsumer2.close();
         messageConsumer3.close();
         
         return true;

      }
      finally
      {
         // Step 15. Be sure to close our JMS resources!
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
