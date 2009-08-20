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
package org.hornetq.core.example;

import java.util.Date;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.integration.bootstrap.HornetQBootstrapServer;
import org.hornetq.integration.transports.netty.NettyConnectorFactory;


/**
 * 
 * This exammple shows how to run a JBoss Messaging core client and server embedded in your
 * own application
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class EmbeddedMicroContainerExample
{

   public static void main(String[] args)
   {

      HornetQBootstrapServer hornetQ = null;
      try
      {
         
         // Step 1. Start the server         
         hornetQ = new HornetQBootstrapServer("./server0/jbm-jboss-beans.xml");
         hornetQ.run();
         
         // Step 2. As we are not using a JNDI environment we instantiate the objects directly         
         ClientSessionFactory sf = new ClientSessionFactoryImpl (new TransportConfiguration(NettyConnectorFactory.class.getName()));
         
         // Step 3. Create a core queue
         ClientSession coreSession = sf.createSession(false, false, false);
         
         final String queueName = "queue.exampleQueue";
         
         coreSession.createQueue(queueName, queueName, true);
         
         coreSession.close();
                  
         ClientSession session = null;
   
         try
         {
   
            // Step 4. Create the session, and producer
            session = sf.createSession();
                                   
            ClientProducer producer = session.createProducer(queueName);
   
            // Step 5. Create and send a message
            ClientMessage message = session.createClientMessage(false);
            
            final String propName = "myprop";
            
            message.putStringProperty(propName, "Hello sent at " + new Date());
            
            System.out.println("Sending the message.");
            
            producer.send(message);

            // Step 6. Create the message consumer and start the connection
            ClientConsumer messageConsumer = session.createConsumer(queueName);
            session.start();
   
            // Step 7. Receive the message. 
            ClientMessage messageReceived = messageConsumer.receive(1000);
            
            System.out.println("Received TextMessage:" + messageReceived.getProperty(propName));
         }
         finally
         {
            // Step 8. Be sure to close our resources!
            if (session != null)
            {
               session.close();
            }

            // Step 9. Shutdown the container
            if (hornetQ != null)
            {
               hornetQ.shutDown();
            }
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
         System.exit(-1);
      }
   }

 
}
