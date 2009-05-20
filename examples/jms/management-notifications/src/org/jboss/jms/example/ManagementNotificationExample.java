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

import java.util.Enumeration;

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
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.jboss.common.example.JMSExample;
import org.jboss.messaging.jms.JBossTopic;

/**
 * An example that shows how to receive management notifications using JMS messages.
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ManagementNotificationExample extends JMSExample
{
   public static void main(String[] args)
   {
      new ManagementNotificationExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         // Step 2. Perfom a lookup on the queue
         Queue queue = (Queue) initialContext.lookup("/queue/exampleQueue");
         
         // Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");

         // Step 4.Create a JMS connection, a session and a producer for the queue
         connection = cf.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(queue);

         // Step 5. create the JMS notification topic.
         // It is a "special" topic and it is not looked up from JNDI but constructed directly
         Topic notificationsTopic = new JBossTopic("example.notifications", "example.notifications");
         
         // Step 6. Create a JMS message consumer for the notification topic and set its message listener
         // It will display all the properties of the JMS Message
         MessageConsumer notificationConsumer = session.createConsumer(notificationsTopic);
         notificationConsumer.setMessageListener(new MessageListener()
         {
            public void onMessage(Message notif)
            {
               System.out.println("------------------------");
               System.out.println("Received notification:");
               try
               {
                  Enumeration propertyNames = notif.getPropertyNames();
                  while (propertyNames.hasMoreElements())
                  {
                     String propertyName = (String)propertyNames.nextElement();
                     System.out.format("  %s: %s\n", propertyName, notif.getObjectProperty(propertyName));
                  }
               }
               catch (JMSException e)
               {
               }
               System.out.println("------------------------");
            }            
         });

         // Step 7. Start the Connection to allow the consumers to receive messages
         connection.start();

         // Step 8. Create a JMS Message Consumer on the queue         
         MessageConsumer consumer = session.createConsumer(queue);

         // Step 9. Close the consumer
         consumer.close();
         
         // Step 10. Try to create a connection with unknown user
         try
         {
            cf.createConnection("not.a.valid.user", "not.a.valid.password");
         } catch (JMSException e)
         {            
         }
         
         // sleep a little bit to be sure to receive the notification for the security
         // authentication violation before leaving the example
         Thread.sleep(2000);
         
         return true;
      }
      finally
      {
         //Step 11. Be sure to close the resources!
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
}
