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

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class WildcardExample
{
   final static Logger log = Logger.getLogger(WildcardExample.class);

   public static void main(final String[] args)
   {
      Connection connection = null;
      try
      {
         //create an initial context, env will be picked up from jndi.properties
         InitialContext initialContext = new InitialContext();
         Topic topicA = (Topic) initialContext.lookup("/topic/topicA");
         Topic topicB = (Topic) initialContext.lookup("/topic/topicB");
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");
         connection = cf.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer messageProducerA = session.createProducer(topicA);
         MessageProducer messageProducerB = session.createProducer(topicB);

         Topic wildCardTopic = session.createTopic("topic.*");
         //or alternatively new JBossTopic("topic.*");
         MessageConsumer messageConsumer = session.createConsumer(wildCardTopic);
         Message messageA = session.createTextMessage("This is a text message from TopicA!");
         Message messageB = session.createTextMessage("This is a text message from TopicB!");
         connection.start();

         log.info("publishing message to topicA");
         messageProducerA.send(messageA);
         log.info("publishing message to topicB");
         messageProducerB.send(messageB);

         TextMessage msg1 = (TextMessage) messageConsumer.receive(5000);
         log.info("received message = " + msg1.getText());
         TextMessage msg2 = (TextMessage) messageConsumer.receive(5000);
         log.info("received message = " + msg2.getText());
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
