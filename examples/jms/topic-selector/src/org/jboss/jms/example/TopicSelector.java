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
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import javax.naming.InitialContext;

/**
 * A simple JMS Topic example that creates a producer and consumer on a queue and sends and receives a message.
 *
 * @author <a href="csuconic@redhat.com">Clebert Suconic</a>
 */
public class TopicSelector extends JMSExample
{
   public static void main(String[] args)
   {
      new TopicSelector().run(args);
   }
   
   
   
   

   public void runExample() throws Exception
   {
      Connection connection = null;
      try
      {

         // Step 1. Create an initial context to perform the JNDI lookup.
         InitialContext initialContext = getContext();

         // Step 2. Look-up the JMS topic
         Topic topic = (Topic)initialContext.lookup("/topic/exampleTopic");

         // Step 3. Look-up the JMS connection factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 4. Create a JMS connection
         connection = cf.createConnection();
         
         // Step 5. Create a JMS session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS message producer
         MessageProducer messageProducer = session.createProducer(topic);

         // Step 7. Create one consumer with a specific Filter
         TopicSubscriber subscriberA = session.createDurableSubscriber(topic, "sub-a1", "userId=1", false);

         // Step 8. Create another consuemr with a different Filter
         TopicSubscriber subscriberB = session.createDurableSubscriber(topic, "sub-a2", "userId=2", false);

         // Step 9. Send two messages
         
         for (int i = 1; i <= 2; i++)
         {
            // Step 9.1 Create a text message
            TextMessage message1 = session.createTextMessage("This is a text message " + i);
            
            // Step 9.1 Set a property
            message1.setIntProperty("userId", i);
   
            // Step 9.2 Send the message
            messageProducer.send(message1);

            System.out.println("Sent message: " + message1.getText());
         }

         // Step 10. Start the JMS Connection. This step will activate the subscribers to receive messages.
         connection.start();


         // Step 11. Consume the message from the durable subscription a

         TextMessage messageReceivedA = (TextMessage)subscriberA.receive();

         System.out.println("Received message: " + messageReceivedA.getText());


         // Step 12. Consume the message from the durable subscription b

         TextMessage messageReceivedB = (TextMessage)subscriberB.receive();

         System.out.println("Received message: " + messageReceivedB.getText());

         
         // Step 13. Close the subscribers
         subscriberA.close();
         subscriberA.close();

         // Step 14. Delete the durable subscription
         session.unsubscribe("sub-a1");
         session.unsubscribe("sub-a2");
      }
      finally
      {
         if (connection != null)
         {
            // Step 15. Be sure to close our JMS resources!
            connection.close();
         }
      }
   }
}
