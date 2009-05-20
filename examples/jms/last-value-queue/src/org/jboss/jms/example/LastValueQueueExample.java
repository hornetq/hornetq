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
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.common.example.JMSExample;

/**
 * This example shows how to configure and use a <em>Last-Value</em> queues.
 * Only the last message with a well-defined property is hold by the queue.
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class LastValueQueueExample extends JMSExample
{
   public static void main(String[] args)
   {
      new LastValueQueueExample().run(args);
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
         Queue queue = (Queue)initialContext.lookup("/queue/lastValueQueue");

         // Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 4.Create a JMS Connection, session and producer on the queue
         connection = cf.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(queue);

         // Step 5. Create and send a text message with the Last-Value header set
         TextMessage message = session.createTextMessage("1st message with Last-Value property set");
         message.setStringProperty("_JBM_LVQ_NAME", "STOCK_NAME");
         producer.send(message);
         System.out.format("Sent message: %s\n", message.getText());

         // Step 6. Create and send a second text message with the Last-Value header set
         message = session.createTextMessage("2nd message with Last-Value property set");
         message.setStringProperty("_JBM_LVQ_NAME", "STOCK_NAME");
         producer.send(message);
         System.out.format("Sent message: %s\n", message.getText());

         // Step 7. Create and send a third text message with the Last-Value header set
         message = session.createTextMessage("3rd message with Last-Value property set");
         message.setStringProperty("_JBM_LVQ_NAME", "STOCK_NAME");
         producer.send(message);
         System.out.format("Sent message: %s\n", message.getText());

         // Step 8. Browse the queue. There is only 1 message in it, the last sent
         QueueBrowser browser = session.createBrowser(queue);
         Enumeration enumeration = browser.getEnumeration();
         while (enumeration.hasMoreElements())
         {
            TextMessage messageInTheQueue = (TextMessage)enumeration.nextElement();
            System.out.format("Message in the queue: %s\n", messageInTheQueue.getText());
         }
         browser.close();

         // Step 9. Create a JMS Message Consumer for the queue
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 10. Start the Connection
         connection.start();

         // Step 11. Trying to receive a message. Since the queue is configured to keep only the
         // last message with the Last-Value header set, the message received is the last sent
         TextMessage messageReceived = (TextMessage)messageConsumer.receive(5000);
         System.out.format("Received message: %s\n", messageReceived.getText());

         // Step 12. Trying to receive another message but there will be none.
         // The 1st message was discarded when the 2nd was sent to the queue.
         // The 2nd message was in turn discarded when the 3trd was sent to the queue
         messageReceived = (TextMessage)messageConsumer.receive(5000);
         System.out.format("Received message: %s\n", messageReceived);

         initialContext.close();
         
         return true;
      }
      finally
      {
         // Step 13. Be sure to close our JMS resources!
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
