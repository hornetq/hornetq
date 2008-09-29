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

import org.jboss.messaging.core.logging.Logger;

import javax.jms.*;
import javax.naming.InitialContext;
import java.util.concurrent.CountDownLatch;

/**
 * This example sends 20 messages setting the groupid so that a specific consumer will receive each message.
 * setting the property 'JMSXGroupID' will bind a consumer to the value given, from this point on the same consumer will
 * receive any message that has the same JMSXGroupID value. setting the property 'JMSXGroupSeq' to 0 will release the
 * binding after that message has been delivered.  
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class MessageGroupingExample
{
   final static Logger log = Logger.getLogger(MessageGroupingExample.class);

   public static void main(final String[] args)
   {
      Connection connection = null;

      try
      {
         //create an initial context, env will be picked up from jndi.properties
         InitialContext initialContext = new InitialContext();
         Queue queue = (Queue) initialContext.lookup("/queue/testGroupQueue");
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");

         connection = cf.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(queue);
         Message[] messages = new Message[20];
         for (int i = 0; i < messages.length; i++)
         {
            if (i < messages.length/2)
            {
               messages[i] = session.createTextMessage("This is a text message from groupid1!");
               messages[i].setStringProperty("JMSXGroupID", "groupid1");
               if(i == 4)
               {
                  messages[i].setIntProperty("JMSXGroupSeq", 0);  
               }
            }
            else
            {
               messages[i] = session.createTextMessage("This is a text message from groupid2!");
               messages[i].setStringProperty("JMSXGroupID", "groupid2");
            }
         }

         final CountDownLatch latch = new CountDownLatch(20);
         MessageConsumer messageConsumer = session.createConsumer(queue);
         messageConsumer.setMessageListener(new MessageListener()
         {
            public void onMessage(Message message)
            {
               TextMessage m = (TextMessage) message;
               try
               {
                  log.info("message received for consumer 1 = " + m.getText());
               }
               catch (JMSException e)
               {
                  e.printStackTrace();
               }
               latch.countDown();
            }
         });
         MessageConsumer messageConsumer2 = session.createConsumer(queue);
         messageConsumer2.setMessageListener(new MessageListener()
         {
            public void onMessage(Message message)
            {
              TextMessage m = (TextMessage) message;
               try
               {
                  log.info("message received for consumer 2 = " + m.getText());
               }
               catch (JMSException e)
               {
                  e.printStackTrace();
               }
               latch.countDown();
            }
         });
         MessageConsumer messageConsumer3 = session.createConsumer(queue);
         messageConsumer3.setMessageListener(new MessageListener()
         {
            public void onMessage(Message message)
            {
              TextMessage m = (TextMessage) message;
               try
               {
                  log.info("message received for consumer 3 = " + m.getText());
               }
               catch (JMSException e)
               {
                  e.printStackTrace();
               }
               latch.countDown();
            }
         });
         connection.start();

         log.info("sending messages to queue");
         for (Message message : messages)
         {
            producer.send(message);
         }
         latch.await();
      }
      catch (Exception e)
      {
         e.printStackTrace();
      }
      finally
      {
         if(connection != null)
         {
            try
            {
               connection.close();
            }
            catch (JMSException e)
            {
            }
         }
      }
   }
}