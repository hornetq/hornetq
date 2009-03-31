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
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;
import javax.naming.InitialContext;
import java.util.concurrent.CountDownLatch;

/**
 * A simple JMS example that shows how to use a durable subcriber.
 *
 * @author <a href="hgao@redhat.com">Howard Gao</a>
 */
public class DurableSubscriberExample extends JMSExample
{
   public static void main(String[] args)
   {
      new DurableSubscriberExample().run(args);
   }

   public void runExample()
   {
      Connection connection = null;
      try
      {
         //create an initial context, env will be picked up from client-jndi.properties
         InitialContext initialContext = getContext();
         Topic topic = (Topic) initialContext.lookup("/topic/exampleTopic");
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");
         connection = cf.createConnection();
         connection.setClientID("durable-client");
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         
         MessageProducer messageProducer = session.createProducer(topic);

         TopicSubscriber subscriber1 = session.createDurableSubscriber(topic, "subscriber-1");
         TopicSubscriber subscriber2 = session.createDurableSubscriber(topic, "subscriber-2");
         Message message = session.createTextMessage("This is a text message!");
         final CountDownLatch latch = new CountDownLatch(2);
         subscriber1.setMessageListener(new MessageListener()
         {
            public void onMessage(Message message)
            {
               try
               {
                  log.info("message received by subscriber1 from topic");
                  TextMessage textMessage = (TextMessage) message;
                  log.info("message = " + textMessage.getText());
               }
               catch (JMSException e)
               {
                  e.printStackTrace();
               }
               latch.countDown();
            }
         });
         subscriber2.setMessageListener(new MessageListener()
         {
            public void onMessage(Message message)
            {
               try
               {
                  log.info("message received by subscriber2 from topic");
                  TextMessage textMessage = (TextMessage) message;
                  log.info("message = " + textMessage.getText());
               }
               catch (JMSException e)
               {
                  e.printStackTrace();
               }
               latch.countDown();
            }
         });
         connection.start();

         log.info("publishing message to topic");
         messageProducer.send(message);

         try
         {
            latch.await();
         }
         catch (InterruptedException e)
         {
         }
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
