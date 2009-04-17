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
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

/**
 * A simple JMS example that shows how static message selectors work.
 *
 * @author <a href="hgao@redhat.com">Howard Gao</a>
 */
public class StaticSelectorExample extends JMSExample
{
   private volatile boolean result = true;

   public static void main(String[] args)
   {
      new StaticSelectorExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         //Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         //Step 2. look-up the JMS queue object from JNDI, this is the queue that has filter configured with it.
         Queue queue = (Queue) initialContext.lookup("/queue/selectorQueue");

         //Step 3. look-up the JMS connection factory object from JNDI
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");

         //Step 4. Create a JMS Connection
         connection = cf.createConnection();
         
         //Step 5. Start the connection
         connection.start();

         //Step 6. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         //Step 7. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         //Step 8. Create a JMS Message Consumer that receives 'red' messages
         MessageConsumer redConsumer = session.createConsumer(queue);
         redConsumer.setMessageListener(new SimpleMessageListener("red"));
         
         //Step 9. Create five messages with different 'color' properties
         TextMessage redMessage1 = session.createTextMessage("Red-1");
         redMessage1.setStringProperty("color", "red");
         TextMessage redMessage2 = session.createTextMessage("Red-2");
         redMessage2.setStringProperty("color", "red");
         TextMessage greenMessage = session.createTextMessage("Green");
         greenMessage.setStringProperty("color", "green");
         TextMessage blueMessage = session.createTextMessage("Blue");
         blueMessage.setStringProperty("color", "blue");
         TextMessage normalMessage = session.createTextMessage("No color");

         //Step 10. Send the Messages
         producer.send(redMessage1);
         System.out.println("Message sent: " + redMessage1.getText());
         producer.send(greenMessage);
         System.out.println("Message sent: " + greenMessage.getText());
         producer.send(blueMessage);
         System.out.println("Message sent: " + blueMessage.getText());
         producer.send(redMessage2);
         System.out.println("Message sent: " + redMessage2.getText());
         producer.send(normalMessage);
         System.out.println("Message sent: " + normalMessage.getText());
         
         //Step 11. Waiting for the message listener to check the received messages.
         Thread.sleep(5000);
         
         return result;
      }
      finally
      {
         //Step 12. Be sure to close our JMS resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         if(connection != null)
         {
            connection.close();
         }
      }
   }
   
   public class SimpleMessageListener implements MessageListener
   {
      private String name;
      
      public SimpleMessageListener(String listener)
      {
         name = listener;
      }

      public void onMessage(Message msg)
      {
         TextMessage textMessage = (TextMessage)msg;
         try
         {
            String colorProp = msg.getStringProperty("color");
            System.out.println("Receiver " + name + " receives message [" + textMessage.getText() + "] with color property: " + colorProp);
            if (!colorProp.equals(name))
            {
               result = false;
            }
         }
         catch (JMSException e)
         {
            e.printStackTrace();
            result = false;
         }
      }
   }

}
