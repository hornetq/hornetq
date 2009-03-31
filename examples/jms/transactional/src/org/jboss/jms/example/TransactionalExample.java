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
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

/**
 * A simple JMS example that sends and consume message transactionally.
 *
 * @author <a href="hgao@redhat.com">Howard Gao</a>
 */
public class TransactionalExample extends JMSExample
{
   public static void main(String[] args)
   {
      new TransactionalExample().run(args);
   }

   public void runExample()
   {
      Connection connection = null;
      try
      {
         //create an initial context, env will be picked up from client-jndi.properties
         InitialContext initialContext = getContext();
         Queue queue = (Queue) initialContext.lookup("/queue/exampleQueue");
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");
         
         connection = cf.createConnection();
         Session session = connection.createSession(true, Session.SESSION_TRANSACTED);
         
         MessageProducer producer = session.createProducer(queue);
         Message message = session.createTextMessage("This is a text message!");
         
         log.info("sending message to queue");
         producer.send(message);
         
         session.commit();
         
         MessageConsumer messageConsumer = session.createConsumer(queue);
         connection.start();
         TextMessage message2 = (TextMessage) messageConsumer.receive(5000);
         
         session.commit();

         log.info("message received from queue");
         log.info("message = " + message2.getText());

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
               e.printStackTrace();
            }
         }
      }
   }

}
