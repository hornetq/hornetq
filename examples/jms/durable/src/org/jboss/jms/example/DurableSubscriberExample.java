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
 * A simple JMS example that shows how to use a durable subcriber.
 *
 * @author <a href="hgao@redhat.com">Howard Gao</a>
 */
public class DurableSubscriberExample extends JMSExample
{
   public static void main(String[] args)
   {
      new DurableSubscriberExample().run(args);
   }

   public void runExample() throws Exception
   {
      Connection connection = null;
      try
      {
         //Step 1. Create an initial context to perform the JNDI lookup.
         InitialContext initialContext = getContext();
         
         //Step 2. Look-up the JMS topic
         Topic topic = (Topic) initialContext.lookup("/topic/exampleTopic");
         
         //Step 3. Look-up the JMS connection factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");
         
         //Step 4. Create a JMS connection
         connection = cf.createConnection();
         
         //Step 5. Set the client-id on the connection
         connection.setClientID("durable-client");
         
         //Step 6. Start the connection
         connection.start();
         
         //Step 7. Create a JMS session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         //Step 8. Create a JMS message producer
         MessageProducer messageProducer = session.createProducer(topic);

         //Step 9. Create the subscription and the subscriber.
         TopicSubscriber subscriber = session.createDurableSubscriber(topic, "subscriber-1");
         
         //Step 10. Create a text message
         TextMessage message1 = session.createTextMessage("This is a text message 1");
         
         //Step 11. Send the text message to the topic
         messageProducer.send(message1);
         
         System.out.println("Sent message: " + message1.getText());
         
         //Step 12. Consume the message from the durable subscription
                           
         TextMessage messageReceived = (TextMessage)subscriber.receive();
         
         System.out.println("Received message: " + messageReceived.getText());
         
         //Step 13. Create and send another message
         
         TextMessage message2 = session.createTextMessage("This is a text message 2");
         
         messageProducer.send(message2);
         
         System.out.println("Sent message: " + message2.getText());
         
         //Step 14. Close the subscriber - the server could even be stopped at this point!
         subscriber.close();
         
         //Step 15. Create a new subscriber on the *same* durable subscription.
         
         subscriber = session.createDurableSubscriber(topic, "subscriber-1");
         
         //Step 16. Consume the message
         
         messageReceived = (TextMessage)subscriber.receive();
         
         System.out.println("Received message: " + messageReceived.getText());
         
         //Step 17. Close the subscriber
         subscriber.close();
         
         //Step 18. Delete the durable subscription
         session.unsubscribe("subscriber-1");
      }
      finally
      {
         if (connection != null)
         {
            // Step 19. Be sure to close our JMS resources!
            connection.close();
         }
      }
   }
}
