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
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.integration.transports.netty.TransportConstants;
import org.jboss.messaging.jms.JBossTopic;
import org.jboss.messaging.jms.client.JBossConnectionFactory;

/**
 * A simple JMS Topic example
 *
 */
public class ClusteredTopicExample extends JMSExample
{
   public static void main(String[] args)
   {
      new ClusteredTopicExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection connection0 = null;

      Connection connection1 = null;
      try
      {
         Topic topic = new JBossTopic("exampleTopic");

         Map<String, Object> params0 = new HashMap<String, Object>();
         params0.put(TransportConstants.PORT_PROP_NAME, 5445);
         TransportConfiguration tc0 = new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory",
                                                                 params0);
         ConnectionFactory cf0 = new JBossConnectionFactory(tc0);

         Map<String, Object> params1 = new HashMap<String, Object>();
         params1.put(TransportConstants.PORT_PROP_NAME, 5446);
         TransportConfiguration tc1 = new TransportConfiguration("org.jboss.messaging.integration.transports.netty.NettyConnectorFactory",
                                                                 params1);
         ConnectionFactory cf1 = new JBossConnectionFactory(tc1);

         connection0 = cf0.createConnection();

         connection1 = cf1.createConnection();

         Session session0 = connection0.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         connection0.start();

         connection1.start();

         MessageConsumer consumer0 = session0.createConsumer(topic);

         MessageConsumer consumer1 = session1.createConsumer(topic);
         
         Thread.sleep(1000);

         MessageProducer producer = session0.createProducer(topic);

         final int numMessages = 10;

         for (int i = 0; i < numMessages; i++)
         {
            TextMessage message = session0.createTextMessage("This is text message " + i);
            
            producer.send(message);

            System.out.println("Sent message: " + message.getText());
         }

         for (int i = 0; i < numMessages; i++)
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
         // Step 12. Be sure to close our JMS resources!
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
