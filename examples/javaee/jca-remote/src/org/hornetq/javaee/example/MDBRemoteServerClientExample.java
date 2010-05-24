/*
 * Copyright 2009 Red Hat, Inc.
 *  Red Hat licenses this file to you under the Apache License, version
 *  2.0 (the "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 *  implied.  See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package org.hornetq.javaee.example;


import org.hornetq.common.example.HornetQExample;

import javax.jms.*;
import javax.naming.InitialContext;

/**
 *
 * MDB Remote & JCA Configuration Example.
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public class MDBRemoteServerClientExample extends HornetQExample
{
   public static void main(String[] args) throws Exception
   {
      new MDBRemoteServerClientExample().run(args);
   }

   @Override
   public boolean runExample() throws Exception
   {
      InitialContext initialContext = null;
      Connection connection = null;
      try
      {
         System.out.println("Please start the Application Server by running './build.sh deploy' (build.bat on windows)");
         System.in.read();

         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         // Step 2. Look up the MDB's queue
         Queue queue = (Queue) initialContext.lookup("queue/mdbQueue");

         // Step 3. Look up a Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("ConnectionFactory");

         //Step 4. Create a connection
         connection = cf.createConnection();

         //Step 5. Create a Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a message producer to send the message
         MessageProducer producer = session.createProducer(queue);

         // Step 7. Create and send a message
         producer.send(session.createTextMessage("a message"));

         // Step 15. Look up the reply queue
         Queue replyQueue = (Queue) initialContext.lookup("queue/mdbReplyQueue");

         // Step 16. Create a message consumer to receive the message
         MessageConsumer consumer = session.createConsumer(replyQueue);

         // Step 17. Start the connection so delivery starts
         connection.start();

         // Step 18. Receive the text message
         TextMessage textMessage = (TextMessage) consumer.receive(5000);

         System.out.println("Message received from reply queue. Message = \"" + textMessage.getText() + "\"" );

         System.out.println("stop the Application Server and press enter");

         System.in.read();

         return true;
      }
      finally
      {
         // Step 19. Be sure to close our JMS resources!
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