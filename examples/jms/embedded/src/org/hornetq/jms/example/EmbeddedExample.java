/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */
package org.hornetq.jms.example;

import java.util.Date;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.hornetq.core.client.ClientSession;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.HornetQQueue;
import org.hornetq.jms.client.HornetQConnectionFactory;

/**
 * This example demonstrates how to run a HornetQ embedded with JMS
 * 
 * @author <a href="clebert.suconic@jboss.com">Clebert Suconic</a>
 */
public class EmbeddedExample
{

   public static void main(String[] args)
   {
      try
      {
         
         // Step 1. Create the Configuration, and set the properties accordingly
         Configuration configuration = new ConfigurationImpl();
         configuration.setPersistenceEnabled(false);
         configuration.setSecurityEnabled(false);
         configuration.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));
         
         // Step 2. Create and start the server
         HornetQServer server = HornetQ.newMessagingServer(configuration);
         server.start();
   
   
         // Step 3. As we are not using a JNDI environment we instantiate the objects directly
         Queue queue = new HornetQQueue("exampleQueue");
         HornetQConnectionFactory cf = new HornetQConnectionFactory (new TransportConfiguration(InVMConnectorFactory.class.getName()));
         
         // Step 4. Create a JMS Destination by using the Core API
         ClientSession coreSession = cf.getCoreFactory().createSession(false, false, false);
         coreSession.createQueue("jms.queue.exampleQueue", "jms.queue.exampleQueue", true);
         coreSession.close();
         
         
         Connection connection = null;
   
         try
         {
   
            // Step 5. Create the JMS objects
            connection = cf.createConnection();
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(queue);
   
            // Step 6. Create and send a TextMessage
            TextMessage message = session.createTextMessage("Hello sent at " + new Date());
            System.out.println("Sending the message.");
            producer.send(message);

            // Step 7. Create the message consumer and start the connection
            MessageConsumer messageConsumer = session.createConsumer(queue);
            connection.start();
   
            // Step 8. Receive the message. 
            TextMessage messageReceived = (TextMessage)messageConsumer.receive(1000);
            System.out.println("Received TextMessage:" + messageReceived.getText());
         }
         finally
         {
            // Step 9. Be sure to close our resources!
            if (connection != null)
            {
               connection.close();
            }
            
            // Step 10. Stop the server
            server.stop();
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
         System.exit(-1);
      }
   }

}
