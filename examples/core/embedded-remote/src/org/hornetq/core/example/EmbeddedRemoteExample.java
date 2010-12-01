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

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.common.example.SpawnedVMSupport;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;

/**
 * 
 * This example shows how to run a HornetQ core client and server embedded in your
 * own application
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class EmbeddedRemoteExample
{

   public static void main(final String[] args)
   {

      Process process = null;
      try
      {

         // Step 1. start a server remotely
         // Step 2 and 3 on EmbeddedServer
         process = EmbeddedRemoteExample.startRemoteEmbedded();

         // Step 4. As we are not using a JNDI environment we instantiate the objects directly
         ServerLocator serverLocator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(NettyConnectorFactory.class.getName()));
         ClientSessionFactory sf = serverLocator.createSessionFactory();

         // Step 5. Create a core queue
         ClientSession coreSession = sf.createSession(false, false, false);

         final String queueName = "queue.exampleQueue";

         coreSession.createQueue(queueName, queueName, true);

         coreSession.close();

         ClientSession session = null;

         try
         {

            // Step 6. Create the session, and producer
            session = sf.createSession();

            ClientProducer producer = session.createProducer(queueName);

            // Step 7. Create and send a message
            ClientMessage message = session.createMessage(false);

            final String propName = "myprop";

            message.putStringProperty(propName, "Hello sent at " + new Date());

            System.out.println("Sending the message.");

            producer.send(message);

            // Step 8. Create the message consumer and start the connection
            ClientConsumer messageConsumer = session.createConsumer(queueName);
            session.start();

            // Step 9. Receive the message.
            ClientMessage messageReceived = messageConsumer.receive(1000);
            System.out.println("Received TextMessage:" + messageReceived.getStringProperty(propName));
         }
         finally
         {
            // Step 10. Be sure to close our resources!
            if (sf != null)
            {
               sf.close();
            }

            if (process != null)
            {
               process.destroy();
            }
         }
      }
      catch (Exception e)
      {
         e.printStackTrace();
         System.exit(-1);
      }
   }

   /**
    * @return
    * @throws Exception
    */
   private static Process startRemoteEmbedded() throws Exception
   {
      Process process;
      String remoteClasspath = System.getProperty("remote-classpath");

      if (remoteClasspath == null)
      {
         System.out.println("remote-classpath system property needs to be specified");
         System.exit(-1);
      }

      process = SpawnedVMSupport.spawnVM(remoteClasspath,
                                         EmbeddedServer.class.getName(),
                                         "",
                                         true,
                                         "STARTED::",
                                         "FAILED::",
                                         ".",
                                         new String[] {});
      return process;
   }

}
