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
package org.hornetq.jms.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.hornetq.common.example.JBMExample;

/**
 * 
 * This example demonstrates a distributed topic, and needs three servers to be started before the example is run.
 * 
 * The example will not spawn the servers itself.
 * 
 * The servers should be started using ./run.sh ../config/stand-alone/clustered
 * 
 * If running on the same physical box, make sure that each server:
 * 
 * a) uses a different data directory
 * b) uses different ports for the netty acceptor
 * c) uses different ports for JNDI
 * 
 * Update server[0|1|2]/client-jndi.properties to the correct ports and hosts for the 3 servers
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class ClusteredStandaloneExample extends JBMExample
{
   public static void main(String[] args)
   {
      new ClusteredStandaloneExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection connection0 = null;

      Connection connection1 = null;

      Connection connection2 = null;

      InitialContext initialContext0 = null;
      InitialContext initialContext1 = null;
      InitialContext initialContext2 = null;

      try
      {
         initialContext0 = getContext(0);

         initialContext1 = getContext(1);

         initialContext2 = getContext(2);
         
         // First we demonstrate a distributed topic.
         // We create a connection on each node, create a consumer on each connection and send some
         // messages at a node and verify they are all received by all consumers

         ConnectionFactory cf0 = (ConnectionFactory)initialContext0.lookup("/ConnectionFactory");
         
         System.out.println("Got cf " + cf0);

         ConnectionFactory cf1 = (ConnectionFactory)initialContext1.lookup("/ConnectionFactory");
         
         System.out.println("Got cf " + cf1);

         ConnectionFactory cf2 = (ConnectionFactory)initialContext2.lookup("/ConnectionFactory");
         
         System.out.println("Got cf " + cf2);
         
         Topic topic = (Topic)initialContext0.lookup("/topic/ExampleTopic");


         connection0 = cf0.createConnection();

         connection1 = cf1.createConnection();

         connection2 = cf2.createConnection();

         connection0.start();

         connection1.start();

         connection2.start();

         Session session0 = connection0.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer messageConsumer0 = session0.createConsumer(topic);

         MessageConsumer messageConsumer1 = session1.createConsumer(topic);

         MessageConsumer messageConsumer2 = session2.createConsumer(topic);

         MessageProducer producer = session0.createProducer(topic);

         final int numMessages = 10;

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage message = session0.createTextMessage("Message " + i);

            producer.send(message);
         }

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage message0 = (TextMessage)messageConsumer0.receive(2000);

            if (message0 == null)
            {
               return false;
            }

            System.out.println("Received message " + message0.getText());

            TextMessage message1 = (TextMessage)messageConsumer1.receive(2000);

            if (message1 == null)
            {
               return false;
            }

            System.out.println("Received message " + message1.getText());

            TextMessage message2 = (TextMessage)messageConsumer2.receive(2000);

            if (message2 == null)
            {
               return false;
            }

            System.out.println("Received message " + message2.getText());
         }
         
         producer.close();
         
         messageConsumer0.close();
         
         messageConsumer1.close();
         
         messageConsumer2.close();
                  
         return true;
      }
      finally
      {
         // Step 12. Be sure to close our JMS resources!
         if (initialContext0 != null)
         {
            initialContext0.close();
         }
         if (initialContext1 != null)
         {
            initialContext1.close();
         }
         if (initialContext2 != null)
         {
            initialContext2.close();
         }
         if (connection0 != null)
         {
            connection0.close();
         }
         if (connection1 != null)
         {
            connection1.close();
         }
         if (connection2 != null)
         {
            connection2.close();
         }
      }
   }

}
