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

import org.jboss.common.example.SpawnedVMSupport;
import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.integration.transports.netty.NettyConnectorFactory;


/**
 * 
 * This exammple shows how to run a JBoss Messaging core client and server embedded in your
 * own application
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class EmbeddedRemoteExample
{

   public static void main(String[] args)
   {

      Process process = null;
      try
      {
         
         // Step 1. start a server remotely
         // Step 2 and 3 on EmbeddedServer
         process = startRemoteEmbedded();
         
         // Step 4. As we are not using a JNDI environment we instantiate the objects directly         
         ClientSessionFactory sf = new ClientSessionFactoryImpl (new TransportConfiguration(NettyConnectorFactory.class.getName()));
         
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
            ClientMessage message = session.createClientMessage(false);
            
            final String propName = "myprop";
            
            message.putStringProperty(propName, "Hello sent at " + new Date());
            
            System.out.println("Sending the message.");
            
            producer.send(message);

            // Step 8. Create the message consumer and start the connection
            ClientConsumer messageConsumer = session.createConsumer(queueName);
            session.start();
   
            // Step 9. Receive the message. 
            ClientMessage messageReceived = messageConsumer.receive(1000);
            System.out.println("Received TextMessage:" + messageReceived.getProperty(propName));
         }
         finally
         {
            // Step 10. Be sure to close our resources!
            if (session != null)
            {
               session.close();
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
      String remoteClasspath= System.getProperty("remote-classpath");
      
      if (remoteClasspath == null)
      {
         System.out.println("remote-classpath system property needs to be specified");
         System.exit(-1);
      }
      
      process = SpawnedVMSupport.spawnVM(remoteClasspath, EmbeddedServer.class.getName(), new String[]{}, true, 
                               "STARTED::",
                               "FAILED::",
                               ".",
                               new String[]{});
      return process;
   }

}
