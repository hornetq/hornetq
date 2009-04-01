/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2009, Red Hat Middleware LLC, and individual contributors
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
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * A TextReverserService is a MessageListener which consume text messages from a destination
 * and replies with text messages containing the reversed text.
 * It sends replies to the destination specified by the JMS ReplyTo header of the consumed messages.
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 *
 */
public class TextReverserService implements MessageListener
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private Session session;

   private Connection connection;

   // Static --------------------------------------------------------

   private static String reverse(String text)
   {
      return new StringBuffer(text).reverse().toString();
   }

   // Constructors --------------------------------------------------

   public TextReverserService(ConnectionFactory cf, Destination destination) throws JMSException
   {
      // create a JMS connection
      connection = cf.createConnection();
      // create a JMS session
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      // create a JMS MessageConsumer to consume message from the destination
      MessageConsumer consumer = session.createConsumer(destination);
      // let TextReverter implement MessageListener to consume messages
      consumer.setMessageListener(this);

      // start the connection to start consuming messages
      connection.start();
   }

   // MessageListener implementation --------------------------------

   public void onMessage(Message request)
   {
      TextMessage textMessage = (TextMessage)request;
      try
      {
         // retrieve the request's text
         String text = textMessage.getText();
         // create a reply containing the reversed text
         TextMessage reply = session.createTextMessage(reverse(text));

         // retrieve the destination to reply to
         Destination replyTo = request.getJMSReplyTo();
         // create a producer to send the reply
         MessageProducer producer = session.createProducer(replyTo);
         // send the reply
         producer.send(reply);
         // close the producer
         producer.close();
      }
      catch (JMSException e)
      {
         e.printStackTrace();
      }
   }

   // Public --------------------------------------------------------

   public void close()
   {
      if (connection != null)
      {
         try
         {
            // be sure to close the JMS resources
            connection.close();
         }
         catch (JMSException e)
         {
            e.printStackTrace();
         }
      }
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
