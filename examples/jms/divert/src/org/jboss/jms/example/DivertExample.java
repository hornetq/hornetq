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
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;

/**
 * This examples demonstrates the use of JBoss Messaging "Diverts" to transparently divert or copy messages
 * from one address to another.
 * 
 * Please see the readme.html for more information.
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class DivertExample extends JMSExample
{
   public static void main(String[] args)
   {
      new DivertExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection connection0 = null;

      Connection connection1 = null;

      InitialContext initialContext0 = null;

      InitialContext initialContext1 = null;
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup on the London server
         initialContext0 = getContext(0);

         // Step 2. Look-up the queue orderQueue on the London server - this is the queue any orders are sent to
         Queue orderQueue0 = (Queue)initialContext0.lookup("/queue/orders");

         // Step 3. Look-up the topic priceUpdates on the London server- this is the topic that any price updates are sent to
         Topic priceUpdates0 = (Topic)initialContext0.lookup("/topic/priceUpdates");

         // Step 4. Look-up the spy topic on the London server- this is what we will use to snoop on any orders
         Topic spyTopic0 = (Topic)initialContext0.lookup("/topic/spyTopic");

         // Step 6. Create an initial context to perform the JNDI lookup on the New York server
         initialContext1 = getContext(1);

         // Step 7. Look-up the topic newYorkPriceUpdates on the New York server - any price updates sent to priceUpdates on the London server will
         // be diverted to the queue priceForward on the London server, and a bridge will consume from that queue and forward
         // them to the address newYorkPriceUpdates on the New York server where they will be distributed to the topic subscribers on
         // the New York server
         Topic newYorkPriceUpdates = (Topic)initialContext1.lookup("/topic/newYorkPriceUpdates");

         // Step 8. Perform a lookup on the Connection Factory on the London server
         ConnectionFactory cf0 = (ConnectionFactory)initialContext0.lookup("/ConnectionFactory");

         // Step 9. Perform a lookup on the Connection Factory on the New York server
         ConnectionFactory cf1 = (ConnectionFactory)initialContext1.lookup("/ConnectionFactory");

         // Step 10. Create a JMS Connection on the London server
         connection0 = cf0.createConnection();

         // Step 11. Create a JMS Connection on the New York server
         connection1 = cf1.createConnection();

         // Step 12. Create a JMS Session on the London server
         Session session0 = connection0.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 13. Create a JMS Session on the New York server
         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 14. Create a JMS MessageProducer orderProducer that sends to the queue orderQueue on the London server
         MessageProducer orderProducer = session0.createProducer(orderQueue0);

         // Step 15. Create a JMS MessageProducer priceProducer that sends to the topic priceUpdates on the London server
         MessageProducer priceProducer = session0.createProducer(priceUpdates0);

         // Step 15. Create a JMS subscriber which subscribes to the spyTopic on the London server
         MessageConsumer spySubscriberA = session0.createConsumer(spyTopic0);

         // Step 16. Create another JMS subscriber which also subscribes to the spyTopic on the London server
         MessageConsumer spySubscriberB = session0.createConsumer(spyTopic0);

         // Step 17. Create a JMS MessageConsumer which consumes orders from the order queue on the London server
         MessageConsumer orderConsumer = session0.createConsumer(orderQueue0);

         // Step 18. Create a JMS subscriber which subscribes to the priceUpdates topic on the London server
         MessageConsumer priceUpdatesSubscriber0 = session0.createConsumer(priceUpdates0);

         // Step 19. Create a JMS subscriber which subscribes to the newYorkPriceUpdates topic on the New York server
         MessageConsumer newYorkPriceUpdatesSubscriberA = session1.createConsumer(newYorkPriceUpdates);

         // Step 20. Create another JMS subscriber which also subscribes to the newYorkPriceUpdates topic on the New York server
         MessageConsumer newYorkPriceUpdatesSubscriberB = session1.createConsumer(newYorkPriceUpdates);

         // Step 21. Start the connections

         connection0.start();

         connection1.start();

         // Step 22. Create an order message
         TextMessage orderMessage = session0.createTextMessage("This is an order");

         // Step 23. Send the order message to the order queue on the London server
         orderProducer.send(orderMessage);

         System.out.println("Sent message: " + orderMessage.getText());

         // Step 24. The order message is consumed by the orderConsumer on the London server
         TextMessage receivedOrder = (TextMessage)orderConsumer.receive(5000);

         System.out.println("Received order: " + receivedOrder.getText());

         // Step 25. A copy of the order is also received by the spyTopic subscribers on the London server
         TextMessage spiedOrder1 = (TextMessage)spySubscriberA.receive(5000);

         System.out.println("Snooped on order: " + spiedOrder1.getText());

         TextMessage spiedOrder2 = (TextMessage)spySubscriberB.receive(5000);

         System.out.println("Snooped on order: " + spiedOrder2.getText());

         // Step 26. Create a price update message, destined for London
         TextMessage priceUpdateMessageLondon = session0.createTextMessage("This is a price update for London");
                 
         priceUpdateMessageLondon.setStringProperty("office", "London");
         
         priceProducer.send(priceUpdateMessageLondon);
         
         // Step 27. The price update *should* be received by the local subscriber since we only divert messages
         // where office = New York
         TextMessage receivedUpdate = (TextMessage)priceUpdatesSubscriber0.receive(2000);

         System.out.println("Received price update locally: " + receivedUpdate.getText());
         
         // Step 28. The price update *should not* be received in New York
         
         TextMessage priceUpdate1 = (TextMessage)newYorkPriceUpdatesSubscriberA.receive(1000);

         if (priceUpdate1 != null)
         {
            return false;
         }
         
         System.out.println("Did not received price update in New York, look it's: " + priceUpdate1);
         
         TextMessage priceUpdate2 = (TextMessage)newYorkPriceUpdatesSubscriberB.receive(1000);

         if (priceUpdate2 != null)
         {
            return false;
         }
         
         System.out.println("Did not received price update in New York, look it's: " + priceUpdate2);

         // Step 29. Create a price update message, destined for New York
         
         TextMessage priceUpdateMessageNewYork = session0.createTextMessage("This is a price update for New York");
         
         priceUpdateMessageNewYork.setStringProperty("office", "New York");
      
         // Step 30. Send the price update message to the priceUpdates topic on the London server
         priceProducer.send(priceUpdateMessageNewYork);

         // Step 31. The price update *should not* be received by the local subscriber to the priceUpdates topic
         // since it has been *exclusively* diverted to the priceForward queue, because it has a header saying
         // it is destined for the New York office
         Message message = priceUpdatesSubscriber0.receive(1000);

         if (message != null)
         {
            return false;
         }

         System.out.println("Didn't receive local price update, look, it's: " + message);

         // Step 32. The remote subscribers on server 1 *should* receive a copy of the price update since
         // it has been diverted to a local priceForward queue which has a bridge consuming from it and which
         // forwards it to the same address on server 1.
         // We notice how the forwarded messages have had a special header added by our custom transformer that
         // we told the divert to use

         priceUpdate1 = (TextMessage)newYorkPriceUpdatesSubscriberA.receive(5000);

         System.out.println("Received forwarded price update on server 1: " + priceUpdate1.getText());
         System.out.println("Time of forward: " + priceUpdate1.getLongProperty("time_of_forward"));

         priceUpdate2 = (TextMessage)newYorkPriceUpdatesSubscriberB.receive(5000);

         System.out.println("Received forwarded price update on server 2: " + priceUpdate2.getText());
         System.out.println("Time of forward: " + priceUpdate2.getLongProperty("time_of_forward"));

         return true;
      }
      finally
      {
         // Step 12. Be sure to close our resources!
         if (initialContext0 != null)
         {
            initialContext0.close();
         }
         if (initialContext1 != null)
         {
            initialContext1.close();
         }
         if (connection0 != null)
         {
            connection0.close();
         }
         if (connection1 != null)
         {
            connection1.close();
         }
      }
   }

}
