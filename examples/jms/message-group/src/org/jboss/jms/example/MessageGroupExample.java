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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

import org.jboss.common.example.JMSExample;
import org.jboss.messaging.jms.client.JBossMessage;

/**
 * A simple JMS Queue example that sends and receives message groups.
 *
 * @author <a href="hgao@redhat.com">Howard Gao</a>
 */
public class MessageGroupExample extends JMSExample
{
   private Map<String, String> messageReceiverMap = new ConcurrentHashMap<String, String>();
   private boolean result = true;
   
   public static void main(String[] args)
   {
      new MessageGroupExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         //Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         //Step 2. Perform a lookup on the queue
         Queue queue = (Queue) initialContext.lookup("/queue/exampleQueue");

         //Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");

         //Step 4. Create a JMS Connection
         connection = cf.createConnection();
         
         //Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         //Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);
         
         //Step 7. Create two consumers
         MessageConsumer consumer1 = session.createConsumer(queue);
         consumer1.setMessageListener(new SimpleMessageListener("consumer-1"));
         MessageConsumer consumer2 = session.createConsumer(queue);
         consumer2.setMessageListener(new SimpleMessageListener("consumer-2"));
         
         //Step 8. Create and send 10 text messages with group id 'Group-0'
         int msgCount = 10;
         TextMessage[] groupMessages = new TextMessage[msgCount];
         for (int i = 0; i < msgCount; i++)
         {
            groupMessages[i] = session.createTextMessage("Group-0 message " + i);
            groupMessages[i].setStringProperty(JBossMessage.JMSXGROUPID, "Group-0");
            groupMessages[i].setIntProperty("JMSXGroupSeq", i + 1);
            producer.send(groupMessages[i]);
            System.out.println("Sent message: " + groupMessages[i].getText());
         }

         System.out.println("all messages are sent");
         
         //Step 9. Start the connection
         connection.start();
         
         Thread.sleep(2000);
         
         //Step 10. check the group messages are received by only one consumer
         String trueReceiver = messageReceiverMap.get(groupMessages[0].getText());
         for (TextMessage grpMsg : groupMessages)
         {
            String receiver = messageReceiverMap.get(grpMsg.getText());
            if (!trueReceiver.equals(receiver))
            {
               System.out.println("Group message [" + grpMsg.getText() + "[ went to wrong receiver: " + receiver);
               result = false;
            }
         }

         return result;
      }
      finally
      {
         //Step 11. Be sure to close our JMS resources!
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
   
   private class SimpleMessageListener implements MessageListener
   {
      private String name;
      
      public SimpleMessageListener(String listenerName)
      {
         name = listenerName;
      }

      public void onMessage(Message message)
      {
         try
         {
            TextMessage msg = (TextMessage)message;
            System.out.format("Message: [%s] received by %s, (%s in the group)\n",
                              msg.getText(),
                              name,
                              msg.getIntProperty("JMSXGroupSeq"));
            messageReceiverMap.put(msg.getText(), name);
         }
         catch (JMSException e)
         {
            e.printStackTrace();
         }
      }
   }

}
