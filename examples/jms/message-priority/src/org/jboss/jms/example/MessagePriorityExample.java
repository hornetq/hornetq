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

import java.util.ArrayList;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.common.example.JBMExample;

/**
 * A simple JMS example that shows the delivery order of messages with priorities.
 *
 * @author <a href="hgao@redhat.com">Howard Gao</a>
 */
public class MessagePriorityExample extends JBMExample
{
   private volatile boolean result = true;
   private ArrayList<TextMessage> msgReceived = new ArrayList<TextMessage>();

   public static void main(String[] args)
   {
      new MessagePriorityExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         //Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         //Step 2. look-up the JMS queue object from JNDI
         Queue queue = (Queue) initialContext.lookup("/queue/exampleQueue");

         //Step 3. look-up the JMS connection factory object from JNDI
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");

         //Step 4. Create a JMS Connection
         connection = cf.createConnection();

         //Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         //Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);
         
         //Step 7. Create a JMS Message Consumer
         MessageConsumer redConsumer = session.createConsumer(queue);
         redConsumer.setMessageListener(new SimpleMessageListener());
         
         //Step 8. Create three messages
         TextMessage[] sentMessages = new TextMessage[3];
         sentMessages[0] = session.createTextMessage("first message");
         sentMessages[1] = session.createTextMessage("second message");
         sentMessages[2] = session.createTextMessage("third message");

         //Step 9. Send the Messages, each has a different priority
         producer.send(sentMessages[0]);
         System.out.println("Message sent: " + sentMessages[0].getText() + " with priority: " + sentMessages[0].getJMSPriority());
         producer.send(sentMessages[1], DeliveryMode.NON_PERSISTENT, 5, 0);
         System.out.println("Message sent: " + sentMessages[1].getText() + "with priority: " + sentMessages[1].getJMSPriority());
         producer.send(sentMessages[2], DeliveryMode.NON_PERSISTENT, 9, 0);
         System.out.println("Message sent: " + sentMessages[2].getText() + "with priority: " + sentMessages[2].getJMSPriority());
         
         //Step 10. Start the connection now.
         connection.start();
         
         //Step 11. Wait for message delivery completion
         Thread.sleep(5000);
         
         //Step 12. Examine the order
         for (int i = 0; i < 3; i++)
         {
            TextMessage rm = msgReceived.get(i);
            if (!rm.getText().equals(sentMessages[2-i].getText()))
            {
               System.err.println("Priority is broken!");
               result = false;
            }
         }
         
         return result;
      }
      finally
      {
         //Step 13. Be sure to close our JMS resources!
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
      
      public SimpleMessageListener()
      {
      }

      public void onMessage(Message msg)
      {
         TextMessage textMessage = (TextMessage)msg;
         try
         {
            System.out.println("Received message : [" + textMessage.getText() + "]");
         }
         catch (JMSException e)
         {
            result = false;
         }
         msgReceived.add(textMessage);
      }
      
   }

}
