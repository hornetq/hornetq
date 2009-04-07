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

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.integration.transports.netty.TransportConstants;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.client.JBossConnectionFactory;

/**
 * A simple example that demonstrates server side load-balancing of messages between the queue instances on different 
 * nodes of the cluster.
 *
 * @author <a href="tim.fox@jboss.com>Tim Fox</a>
 */
public class ClusteredQueueExample extends JMSExample
{
   public static void main(String[] args)
   {
      new ClusteredQueueExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection connection0 = null;

      Connection connection1 = null;
      try
      {
         //Step 1. We directly instantiate a JMS Queue object. (Alternatively you could look up from JNDI)
         Queue queue = new JBossQueue("exampleQueue");

         //Step 2. We create some objects with the connection details of server 0
         Map<String, Object> params0 = new HashMap<String, Object>();
         params0.put(TransportConstants.PORT_PROP_NAME, 5445);
         TransportConfiguration tc0 = new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory",
                                                                 params0);
         
         //Step 3. We directly instantiate a JMS ConnectionFactory with those connection details. This connection factory will
         //create connections to server 0
         ConnectionFactory cf0 = new JBossConnectionFactory(tc0);

         //Step 4. We create some objects with the connection details of server 1
         Map<String, Object> params1 = new HashMap<String, Object>();
         params1.put(TransportConstants.PORT_PROP_NAME, 5446);
         TransportConfiguration tc1 = new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory",
                                                                 params1);
         
         //Step 5. We directly instantiate a JMS ConnectionFactory with those connection details. This connection factory will
         //create connections to server 1
         ConnectionFactory cf1 = new JBossConnectionFactory(tc1);

         //Step 6. We create a JMS Connection connection0 which is a connection to server 0
         connection0 = cf0.createConnection();

         //Step 7. We create a JMS Connection connection1 which is a connection to server 1
         connection1 = cf1.createConnection();

         //Step 8. We create a JMS Session on server 0
         Session session0 = connection0.createSession(false, Session.AUTO_ACKNOWLEDGE);

         //Step 9. We create a JMS Session on server 1
         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         //Step 10. We start the connections to ensure delivery occurs on them
         connection0.start();

         connection1.start();

         //Step 11. We create JMS MessageConsumer objects on server 0 and server 1
         MessageConsumer consumer0 = session0.createConsumer(queue);

         MessageConsumer consumer1 = session1.createConsumer(queue);
         
         Thread.sleep(1000);

         //Step 12. We create a JMS MessageProducer object on server 0
         MessageProducer producer = session0.createProducer(queue);

         //Step 13. We send some messages to server 0
         
         final int numMessages = 10;

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage message = session0.createTextMessage("This is text message " + i);
            
            producer.send(message);

            System.out.println("Sent message: " + message.getText());
         }
         
         //Step 14. We now consume those messages on *both* server 0 and server 1.
         //We note the messages have been distributed between servers in a round robin fashion

         for (int i = 0; i < numMessages; i += 2)
         {
            TextMessage message0 = (TextMessage)consumer0.receive(5000);

            System.out.println("Got message: " + message0.getText() + " from node 0");

            TextMessage message1 = (TextMessage)consumer1.receive(5000);

            System.out.println("Got message: " + message1.getText() + " from node 1");
         }

         return true;
      }
      finally
      {
         // Step 15. Be sure to close our JMS resources!
         if (connection0 != null)
         {
            connection0.close();
         }
         
         if (connection1 != null)
         {
            connection1.close();
         }

      }
   }

}
