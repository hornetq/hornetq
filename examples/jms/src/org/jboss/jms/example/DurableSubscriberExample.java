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
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import javax.naming.InitialContext;

/**
 * A simple JMS Durable Subscriber example.
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class DurableSubscriberExample
{
   public static void main(final String[] args)
   {
      Connection connection = null;
      try
      {
         //create an initial context, env will be picked up from jndi.properties
         InitialContext initialContext = new InitialContext();
         Topic topic = (Topic) initialContext.lookup("/topic/testTopic");
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");
         connection = cf.createConnection();
         connection.setClientID("myclientid");
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         session.createDurableSubscriber(topic, "myuniqueid");
         connection.close();

         connection = cf.createConnection();
         connection.setClientID("myclientid");
         session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer messageProducer = session.createProducer(topic);
         Message message = session.createTextMessage("This is a text message!");
         messageProducer.send(message);
         TopicSubscriber topicSubscriber =  session.createDurableSubscriber(topic, "myuniqueid");
         connection.start();
         TextMessage message2 = (TextMessage) topicSubscriber.receive();
         System.out.println("message = " + message2.getText());
         topicSubscriber.close();
         session.unsubscribe("myuniqueid");
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
         if (connection != null)
         {
            try
            {
               connection.close();
            }
            catch (JMSException e)
            {
               e.printStackTrace();
            }
         }
      }
   }
}
