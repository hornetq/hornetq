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
import javax.jms.JMSSecurityException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;

/**
 * A simple JMS example that shows how to access messaging with security configured.
 *
 * @author <a href="hgao@redhat.com">Howard Gao</a>
 */
public class SecurityExample extends JMSExample
{
   private boolean result = true;
   
   public static void main(String[] args)
   {
      new SecurityExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection connection1 = null;
      Connection connection2 = null;
      InitialContext initialContext = null;
      try
      {
         ///Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         //Step 2. perform a lookup on the topic
         Topic topic = (Topic) initialContext.lookup("/topic/exampleTopic");

         //Step 3. perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");

         //Step 4. Try to create a JMS Connection without user/password. It will fail.
         try
         {
            connection1 = cf.createConnection();
            result = false;
         }
         catch (JMSSecurityException e)
         {
            System.out.println("Error creating connection, detail: " + e.getMessage());
         }

         //Step 5. Create a Connection using wrong password, it will fail.
         try
         {
            connection1 = cf.createConnection("jbm-sender", "wrong-password");
            result = false;
         }
         catch (JMSSecurityException e)
         {
            System.out.println("Error creating connection, detail: " + e.getMessage());
         }
         
         //Step 6. Now create two connections with correct credentials. connection1 is used for sending, connection2 receiving
         connection1 = cf.createConnection("jbm-sender", "jbossmessaging1");
         connection2 = cf.createConnection("jbm-consumer", "jbossmessaging2");

         //Step 7. Create 2 JMS Sessions
         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         //Step 8. Create 2 Message Producers, where producer2 has no right to send
         MessageProducer producer1 = session1.createProducer(topic);
         MessageProducer producer2 = session2.createProducer(topic);

         //Step 9. Create 2 JMS Message Consumers
         MessageConsumer messageConsumer1 = session2.createConsumer(topic);
         MessageConsumer messageConsumer2 = session2.createConsumer(topic);
                  
         //Step 10. Start the Connections
         connection1.start();
         connection2.start();
         
         //Step 11. Create a Text Message
         TextMessage message = session1.createTextMessage("This is a text message");

         //Step 12. Send the Message by producer2
         producer2.send(message);
         System.out.println("Producer2 sent message: " + message.getText());
         
         //Step 13. Check no messages are received by either consumer.
         TextMessage messageReceived1 = (TextMessage) messageConsumer1.receive(2000);
         TextMessage messageReceived2 = (TextMessage) messageConsumer2.receive(2000);
         if (messageReceived1 != null) 
         {
            System.out.println("Message received! " + messageReceived1.getText());
            result = false;
         }
         if (messageReceived2 != null) 
         {
            System.out.println("Message received! " + messageReceived2.getText());
            result = false;
         }
         
         //Step 14. Send the message by producer1
         producer1.send(message);

         System.out.println("Producer1 sent message: " + message.getText());

         //Step 15. Receive the message
         messageReceived1 = (TextMessage) messageConsumer1.receive(1000);
         messageReceived2 = (TextMessage) messageConsumer2.receive(1000);
         System.out.println("Consumer 1 Received message: " + messageReceived1.getText());
         System.out.println("Consumer 2 Received message: " + messageReceived2.getText());
         
         return result;
      }
      finally
      {
         //Step 16. Be sure to close our JMS resources!
         if (connection1 != null)
         {
            connection1.close();
         }
         if (connection2 != null)
         {
            connection2.close();
         }
         
         // Also the initialContext
         if (initialContext != null)
         {
            initialContext.close();
         }
      }
   }
}
