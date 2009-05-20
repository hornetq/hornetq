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

import static org.jboss.messaging.integration.transports.netty.TransportConstants.PORT_PROP_NAME;

import java.util.HashMap;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.jboss.common.example.JBMExample;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.client.JBossConnectionFactory;

/**
 * 
 * This example demonstrates how a JMS client can directly instantiate it's JMS Objects like
 * Queue, ConnectionFactory, etc. without having to use JNDI at all.
 * 
 * For more information please see the readme.html file.
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class InstantiateConnectionFactoryExample extends JBMExample
{
   public static void main(String[] args)
   {
      new InstantiateConnectionFactoryExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection connection = null;
      try
      {
         // Step 1. Directly instantiate the JMS Queue object.
         Queue queue = new JBossQueue("exampleQueue");

         // Step 2. Instantiate the TransportConfiguration object which contains the knowledge of what transport to use,
         // The server port etc.

         Map<String, Object> connectionParams = new HashMap<String, Object>();
         connectionParams.put(PORT_PROP_NAME, 5446);

         TransportConfiguration transportConfiguration = new TransportConfiguration(NettyConnectorFactory.class.getName(),
                                                                                    connectionParams);

         // Step 3 Directly instantiate the JMS ConnectionFactory object using that TransportConfiguration
         ConnectionFactory cf = new JBossConnectionFactory(transportConfiguration);

         // Step 4.Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         // Step 7. Create a Text Message
         TextMessage message = session.createTextMessage("This is a text message");

         System.out.println("Sent message: " + message.getText());

         // Step 8. Send the Message
         producer.send(message);

         // Step 9. Create a JMS Message Consumer
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 10. Start the Connection
         connection.start();

         // Step 11. Receive the message
         TextMessage messageReceived = (TextMessage)messageConsumer.receive(5000);

         System.out.println("Received message: " + messageReceived.getText());

         return true;
      }
      finally
      {
         if (connection != null)
         {
            connection.close();
         }
      }
   }

}
