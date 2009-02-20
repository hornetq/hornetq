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
import javax.jms.Queue;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;
import javax.jms.Session;
import javax.jms.MessageListener;
import javax.jms.Message;
import javax.jms.JMSException;
import javax.naming.InitialContext;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class Sender
{
   public static void main(String[] args) throws Exception
   {
      String destinationName = "/queue/testQueue";
      InitialContext ic = new InitialContext();
      ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
      Queue queue = (Queue)ic.lookup(destinationName);

      Connection connection = cf.createConnection();
      Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      MessageProducer sender = session.createProducer(queue);

      Queue temporaryQueue = session.createTemporaryQueue();
      MessageConsumer consumer = session.createConsumer(temporaryQueue);
      final CountDownLatch latch = new CountDownLatch(30);
      consumer.setMessageListener(new MessageListener()
      {
         public void onMessage(Message message)
         {
            try
            {
               TextMessage m = (TextMessage)message;
               System.out.println("received " + m.getText());
               latch.countDown();
            }
            catch (JMSException e)
            {
               e.printStackTrace();
            }
         }
      });

      connection.start();

      for(int i = 1; i <= 30; i++)
      {
         TextMessage message = session.createTextMessage("Message " + i);
         message.setJMSReplyTo(temporaryQueue);
         sender.send(message);
      }
      latch.await(60, TimeUnit.SECONDS);
      connection.close();
   }
}
