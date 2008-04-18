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
import java.util.ResourceBundle;
import java.util.Properties;
import java.io.IOException;

/**
 * A simple JMS Queue example that creates a producer and consumer on a queue and sends a message.
 * 
 * @author <a href="ataylor@redhat.com">Andy Taylor</a>
 */
public class QueueExample
{
   public static void main(String[] args)
   {
      Connection connection = null;
      try
      {
         //create an initial context, env will be picked up from jndi.properties
         InitialContext initialContext = new InitialContext();
         Queue queue = (Queue) initialContext.lookup("/queue/testQueue");
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");
         connection = cf.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(queue);


         MessageConsumer messageConsumer = session.createConsumer(queue, "MYHEADER='one'");
         messageConsumer.setMessageListener(new MessageListener()
         {
            public void onMessage(Message message)
            {
               System.out.println("ml1");
            }
         });

         MessageConsumer messageConsumer2 = session.createConsumer(queue,"MYHEADER='two'");
         messageConsumer2.setMessageListener(new MessageListener()
         {
            public void onMessage(Message message)
            {
               System.out.println("ml2");
            }
         });
         for(int i = 0; i < 10; i++)
         {
            Message message = session.createTextMessage("This is a text message " + i);
            if(i == 0 || i==2 || i==4 || i==6 || i==8 )
            {
               message.setStringProperty("MYHEADER", "one");
            }
            else
            {
               message.setStringProperty("MYHEADER", "two");
            }
            producer.send(message);
         }
         connection.start();
         try
         {
            System.in.read();
         }
         catch (IOException e)
         {
            e.printStackTrace();
         }
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
            }
            catch (JMSException e)
            {
               e.printStackTrace();
            }
      }

   }
}
