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

import java.text.SimpleDateFormat;
import java.util.Date;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.common.example.JMSExample;
import org.jboss.messaging.core.message.impl.MessageImpl;

/**
 * A simple JMS scheduled delivery example that delivers a message in 5 seconds.
 *
 * @author <a href="hgao@redhat.com">Howard Gao</a>
 */
public class ScheduledMessageExample extends JMSExample
{
   public static void main(String[] args)
   {
      new ScheduledMessageExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         //Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         //Step 2. Perfom a lookup on the queue
         Queue queue = (Queue) initialContext.lookup("/queue/exampleQueue");

         //Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");

         //Step 4.Create a JMS Connection
         connection = cf.createConnection();

         //Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         //Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         //Step 7. Create a Text Message
         TextMessage message = session.createTextMessage("This is a scheduled message message which will be delivered in 5 sec.");

         //Step 8. Set the delivery time to be 5 sec later.
         long time = System.currentTimeMillis();
         time += 5000;
         message.setLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME.toString(), time);
         
         //Step 9. Send the Message
         producer.send(message);
         
         System.out.println("Sent message: " + message.getText());
         SimpleDateFormat formatter = new SimpleDateFormat("EEE, d MMM yyyy HH:mm:ss");
         System.out.println("Time of send: " + formatter.format(new Date()));

         //Step 10. Create a JMS Message Consumer
         MessageConsumer messageConsumer = session.createConsumer(queue);

         //Step 11. Start the Connection
         connection.start();

         //Step 12. Receive the message
         TextMessage messageReceived = (TextMessage) messageConsumer.receive();
         
         System.out.println("Received message: " + messageReceived.getText());
         System.out.println("Time of receive: " + formatter.format(new Date()));
         
         return true;
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

}
