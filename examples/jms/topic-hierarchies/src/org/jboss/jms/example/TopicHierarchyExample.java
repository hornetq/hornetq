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
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.jboss.common.example.JMSExample;
import org.jboss.messaging.jms.JBossTopic;

/**
 * This example demonstrates how a JMS TopicSubscriber can be created to subscribe to a wild-card Topic.
 * 
 * For more information please see the readme.html
 *
 * @author <a href="tim.fox@jboss.com">Tim Fox</a>
 */
public class TopicHierarchyExample extends JMSExample
{
   public static void main(String[] args)
   {
      new TopicHierarchyExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         //Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         //Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");

         //Step 4. Create a JMS Connection
         connection = cf.createConnection();

         //Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         //Step 6. Instantiate a topic representing the wildcard we're going to subscribe to 
         Topic topicSubscribe = new JBossTopic("news.europe.#");
         
         //Step 7. Create a consumer (topic subscriber) that will consume using that wildcard
         // The consumer will receive any messages sent to any topic that starts with news.europe
         MessageConsumer messageConsumer = session.createConsumer(topicSubscribe);
 
         //Step 8. Create an anonymous producer
         MessageProducer producer = session.createProducer(null);
         
         //Step 9. Instantiate some more topic objects corresponding to the individual topics
         //we're going to send messages to
         Topic topicNewsUsaWrestling = new JBossTopic("news.usa.wrestling");
         
         Topic topicNewsEuropeSport = new JBossTopic("news.europe.sport");
         
         Topic topicNewsEuropeEntertainment = new JBossTopic("news.europe.entertainment");
        
         //Step 10. Send a message destined for the usa wrestling topic
         TextMessage messageWrestlingNews = session.createTextMessage("Hulk Hogan starts ballet classes");
         
         producer.send(topicNewsUsaWrestling, messageWrestlingNews);
         
         //Step 11. Send a message destined for the europe sport topic
         TextMessage messageEuropeSport = session.createTextMessage("Lewis Hamilton joins European synchronized swimming team");
         
         producer.send(topicNewsEuropeSport, messageEuropeSport);
         
         //Step 12. Send a message destined for the europe entertainment topic
         TextMessage messageEuropeEntertainment = session.createTextMessage("John Lennon resurrected from dead");
         
         producer.send(topicNewsEuropeEntertainment, messageEuropeEntertainment);
         
         //Step 9. Start the connection
         
         connection.start();
         
         //Step 10. We don't receive the usa wrestling message since we subscribed to news.europe.# and
         //that doesn't match news.usa.wrestling. However we do receive the Europe sport message, and the
         //europe entertainment message, since these match the wildcard.
                           
         TextMessage messageReceived1 = (TextMessage)messageConsumer.receive(5000);
         
         System.out.println("Received message: " + messageReceived1.getText());
         
         TextMessage messageReceived2 = (TextMessage)messageConsumer.receive(5000);
         
         System.out.println("Received message: " + messageReceived2.getText());
         
         Message message = messageConsumer.receive(1000);
         
         if (message != null)
         {
            return false;
         }
         
         System.out.println("Didn't received any more message: " + message);

         return true;
      }
      finally
      {
         //Step 12. Be sure to close our resources!
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
