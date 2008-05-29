/*
   * JBoss, Home of Professional Open Source
   * Copyright 2005, JBoss Inc., and individual contributors as indicated
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

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.jms.*;

import org.jboss.messaging.core.logging.Logger;

import java.util.concurrent.CountDownLatch;

/**
 * A simple JMS Topic example that creates a producer and consumer on a queue and sends and receives a message via a
 * Message Listener..
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class TopicExample
{
   final static Logger log = Logger.getLogger(TopicExample.class);

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
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer messageProducer = session.createProducer(topic);

         MessageConsumer messageConsumer = session.createConsumer(topic);
         Message message = session.createTextMessage("This is a text message!");
         final CountDownLatch latch = new CountDownLatch(1);
         messageConsumer.setMessageListener(new MessageListener()
         {
            public void onMessage(Message message)
            {
               try
               {
                  log.info("message received from topic");
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
