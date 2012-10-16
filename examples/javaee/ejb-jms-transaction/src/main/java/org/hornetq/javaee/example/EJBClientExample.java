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
package org.hornetq.javaee.example;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.Context;
import javax.naming.InitialContext;

import org.hornetq.javaee.example.server.SendMessageService;

import java.util.Properties;

/**
 * An example showing how to invoke a EJB which sends a JMS message and update a JDBC table in the same transaction.
 *
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class EJBClientExample
{
   public static void main(final String[] args) throws Exception
   {
      InitialContext initialContext = null;
      Connection connection = null;
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         final Properties env = new Properties();

         env.put(Context.INITIAL_CONTEXT_FACTORY, "org.jboss.naming.remote.client.InitialContextFactory");

         env.put(Context.PROVIDER_URL, "remote://localhost:4447");

         env.put(Context.SECURITY_PRINCIPAL, "guest");

         env.put(Context.SECURITY_CREDENTIALS, "password");

         env.put("jboss.naming.client.ejb.context", true);

         initialContext = new InitialContext(env);

         // Step 2. Lookup the EJB
         SendMessageService service = (SendMessageService)initialContext.lookup("java:ejb/SendMessageBean!org.hornetq.javaee.example.server.SendMessageService");

         // Step 3. Create the DB table which will be updated
         service.createTable();

         // Step 4. Invoke the sendAndUpdate method
         service.sendAndUpdate("This is a text message");
         System.out.println("invoked the EJB service");

         // Step 5. Lookup the JMS connection factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("jms/RemoteConnectionFactory");

         // Step 6. Lookup the queue
         Queue queue = (Queue)initialContext.lookup("jms/queues/testQueue");

         // Step 7. Create a connection, a session and a message consumer for the queue
         connection = cf.createConnection("guest", "password");
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageConsumer consumer = session.createConsumer(queue);

         // Step 8. Start the connection
         connection.start();

         // Step 9. Receive the message sent by the EJB
         TextMessage messageReceived = (TextMessage)consumer.receive(5000);
         System.out.println("Received message: " + messageReceived.getText() +
                            " (" +
                            messageReceived.getJMSMessageID() +
                            ")");
      }
      finally
      {
         // Step 10. Be sure to close the resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         if (connection != null)
         {
            connection.close();
         }
      }
   }
}
