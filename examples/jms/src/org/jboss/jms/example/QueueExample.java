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

import org.jboss.messaging.core.logging.Logger;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.jms.*;
import java.util.ResourceBundle;
import java.util.Properties;

/**
 * A simple JMS Queue example that creates a producer and consumer on a queue and sends a message.
 *
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class QueueExample
{
   final static Logger log = Logger.getLogger(QueueExample.class);
   public static void main(String[] args)
   {
      Connection connection = null;
      Connection connection2 = null;
      try
      {
         //create an initial context, env will be picked up from jndi.properties
         InitialContext initialContext = new InitialContext();
         Queue queue = (Queue) initialContext.lookup("/queue/testQueue");
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");
         connection = cf.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(queue);
         Message message = session.createTextMessage("This is a text message!");
         log.info("sending message to queue");
         producer.send(message);
         MessageConsumer messageConsumer = session.createConsumer(queue);
         connection.start();
         TextMessage message2 = (TextMessage) messageConsumer.receive(5000);
         log.info("message received from queue");
         log.info("message = " + message2.getText());
      }
      catch (NamingException e)
      {
         e.printStackTrace();
      }
      catch (JMSException e)
      {
         e.printStackTrace();
      }
      finally
      {
         if(connection != null)
            try
            {
               connection.close();
               connection2.close();
            }
            catch (JMSException e)
            {
               e.printStackTrace();
            }
      }

   }
}
