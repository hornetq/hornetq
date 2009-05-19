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
package org.jboss.core.example;

import java.util.Date;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.config.impl.ConfigurationImpl;
import org.jboss.messaging.core.remoting.impl.invm.InVMConnectorFactory;
import org.jboss.messaging.core.server.Messaging;
import org.jboss.messaging.core.server.MessagingServer;

/**
 * 
 * This exammple shows how to run a JBoss Messaging core client and server embedded in your
 * own application
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class EmbeddedExample
{

   public static void main(String[] args)
   {
      try
      {
         
         // Step 1. Create the Configuration, and set the properties accordingly
         Configuration configuration = new ConfigurationImpl();
         configuration.setEnablePersistence(false);
         configuration.setSecurityEnabled(false);
         
         // Step 2. Create and start the server
         MessagingServer server = Messaging.newMessagingServer(configuration);
         server.start();
   
   
         // Step 3. As we are not using a JNDI environment we instantiate the objects directly         
         ClientSessionFactory sf = new ClientSessionFactoryImpl (new TransportConfiguration(InVMConnectorFactory.class.getName()));
         
         // Step 4. Create a core queue
         ClientSession coreSession = sf.createSession(false, false, false);
         
         final String queueName = "queue.exampleQueue";
         
         coreSession.createQueue(queueName, queueName, true);
         
         coreSession.close();
                  
         ClientSession session = null;
   
         try
         {
   
            // Step 5. Create the session, and producer
            session = sf.createSession();
                                   
            ClientProducer producer = session.createProducer(queueName);
   
            // Step 6. Create and send a message
            ClientMessage message = session.createClientMessage(false);
            
            final String propName = "myprop";
            
            message.putStringProperty(propName, "Hello sent at " + new Date());
            
            System.out.println("Sending the message.");
            
            producer.send(message);

            // Step 7. Create the message consumer and start the connection
            ClientConsumer messageConsumer = session.createConsumer(queueName);
            session.start();
   
            // Step 8. Receive the message. 
            ClientMessage messageReceived = messageConsumer.receive(1000);
            System.out.println("Received TextMessage:" + messageReceived.getProperty(propName));
         }
         finally
         {
            // Step 9. Be sure to close our resources!
            if (session != null)
            {
               session.close();
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
