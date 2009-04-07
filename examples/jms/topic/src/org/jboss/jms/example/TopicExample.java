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
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class TopicExample extends JMSExample
{
   public static void main(String[] args)
   {
      new TopicExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         ///Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         //Step 2. perform a lookup on the topic
         Topic topic = (Topic) initialContext.lookup("/topic/exampleTopic");

         //Step 3. perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");

         //Step 4. Create a JMS Connection
         connection = cf.createConnection();

         //Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         //Step 6. Create a Message Producer
         MessageProducer producer = session.createProducer(topic);

         //Step 7. Create a JMS Message Consumer
         MessageConsumer messageConsumer1 = session.createConsumer(topic);

         //Step 8. Create a JMS Message Consumer
         MessageConsumer messageConsumer2 = session.createConsumer(topic);
         
         //Step 9. Create a Text Message
         TextMessage message = session.createTextMessage("This is a text message");

         System.out.println("Sent message: " + message.getText());

         //Step 10. Send the Message
         producer.send(message);
         
         //Step 11. Start the Connection
         connection.start();

         //Step 12. Receive the message
         TextMessage messageReceived = (TextMessage) messageConsumer1.receive();

         System.out.println("Consumer 1 Received message: " + messageReceived.getText());

         //Step 13. Receive the message
         messageReceived = (TextMessage) messageConsumer2.receive();

         System.out.println("Consumer 2 Received message: " + messageReceived.getText());
         
         return true;
      }
      finally
      {
         //Step 14. Be sure to close our JMS resources!
         if (connection != null)
         {
            connection.close();
         }
         
         // Also the initialContext
         if (initialContext != null)
         {
            initialContext.close();
         }
      }
   }
}
