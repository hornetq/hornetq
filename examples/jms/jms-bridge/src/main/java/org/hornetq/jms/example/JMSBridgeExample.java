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

import java.util.Hashtable;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;

/**
 * An example which sends a message to a source topic and consume from a target queue.
 * The source and target destinations are located on 2 different HornetQ server.
 * The source and target queues are bridged by a JMS Bridge configured and running on the "target" server.
 * 
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class JMSBridgeExample
{
   public static void main(final String[] args) throws Exception
   {
      if (args.length != 2)
      {
         throw new IllegalArgumentException("JMSBridgeExample needs 2 arguments: <source server> <target server>");
      }
      String sourceServer = args[0];
      String targetServer = args[1];

      System.out.println("client will publish messages to " + sourceServer +
                         " and receives message from " +
                         targetServer);

      // Step 1. Create JNDI contexts for source and target servers
      InitialContext sourceContext = JMSBridgeExample.createContext(sourceServer);
      InitialContext targetContext = JMSBridgeExample.createContext(targetServer);

      Connection sourceConnection = null;
      Connection targetConnection = null;
      try
      {
         // Step 2. Lookup the *source* JMS resources
         ConnectionFactory sourceConnectionFactory = (ConnectionFactory)sourceContext.lookup("/client/ConnectionFactory");
         Topic sourceTopic = (Topic)sourceContext.lookup("/source/topic");

         // Step 3. Create a connection, a session and a message producer for the *source* topic
         sourceConnection = sourceConnectionFactory.createConnection();
         Session sourceSession = sourceConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer sourceProducer = sourceSession.createProducer(sourceTopic);

         // Step 4. Create and send a text message to the *source* queue
         TextMessage message = sourceSession.createTextMessage("this is a text message sent at " + System.currentTimeMillis());
         sourceProducer.send(message);
         System.out.format("Sent message to %s: %s\n",
                           ((Topic)message.getJMSDestination()).getTopicName(),
                           message.getText());
         System.out.format("Message ID : %s\n", message.getJMSMessageID());

         // Step 5. Close the *source* connection
         sourceConnection.close();

         // Step 6. Lookup the *target* JMS resources
         ConnectionFactory targetConnectionFactory = (ConnectionFactory)targetContext.lookup("/client/ConnectionFactory");
         Queue targetQueue = (Queue)targetContext.lookup("/target/queue");

         // Step 7. Create a connection, a session and a message consumer for the *target* queue
         targetConnection = targetConnectionFactory.createConnection();
         Session targetSession = targetConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer targetConsumer = targetSession.createConsumer(targetQueue);

         // Step 8. Start the connection to receive messages from the *target* queue
         targetConnection.start();

         // Step 9. Receive a message from the *target* queue
         TextMessage messageReceived = (TextMessage)targetConsumer.receive(5000);
         System.out.format("\nReceived from %s: %s\n",
                           ((Queue)messageReceived.getJMSDestination()).getQueueName(),
                           messageReceived.getText());

         // Step 10. Display the received message's ID and this "bridged" message ID
         System.out.format("Message ID         : %s\n", messageReceived.getJMSMessageID());
         System.out.format("Bridged Message ID : %s\n", messageReceived.getStringProperty("HQ_BRIDGE_MSG_ID_LIST"));
      }
      finally
      {
         // Step 11. Be sure to close the resources!
         if (sourceContext != null)
         {
            sourceContext.close();
         }
         if (targetContext != null)
         {
            targetContext.close();
         }
         if (sourceConnection != null)
         {
            sourceConnection.close();
         }
         if (targetConnection != null)
         {
            targetConnection.close();
         }
      }
   }

   private static InitialContext createContext(final String server) throws Exception
   {
      String jndiURL = "jnp://" + server + ":1099";
      Hashtable<String, String> jndiProps = new Hashtable<String, String>();
      jndiProps.put("java.naming.provider.url", jndiURL);
      jndiProps.put("java.naming.factory.initial", "org.jnp.interfaces.NamingContextFactory");
      jndiProps.put("java.naming.factory.url.pkgs", "org.jboss.naming:org.jnp.interfaces");
      return new InitialContext(jndiProps);
   }
}
