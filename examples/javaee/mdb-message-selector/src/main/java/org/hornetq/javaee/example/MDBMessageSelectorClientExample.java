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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class MDBMessageSelectorClientExample
{
   public static void main(String[] args) throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         //Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = new InitialContext();

         //Step 2. Perfom a lookup on the queue
         Queue queue = (Queue) initialContext.lookup("/queue/testQueue");

         //Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory) initialContext.lookup("/ConnectionFactory");

         //Step 4.Create a JMS Connection
         connection = cf.createConnection();

         //Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         //Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         //Step 7. Create a Text Message and set the color property to blue
         TextMessage blueMessage = session.createTextMessage("This is a text message");

         blueMessage.setStringProperty("color", "BLUE");

         System.out.println("Sent message: " + blueMessage.getText() + " color=BLUE");

         //Step 8. Send the Message
         producer.send(blueMessage);

         //Step 9. create another message and set the color property to red
         TextMessage redMessage = session.createTextMessage("This is a text message");

         redMessage.setStringProperty("color", "RED");

         System.out.println("Sent message: " + redMessage.getText() + " color=RED");

         //Step 10. Send the Message
         producer.send(redMessage);
          //Step 10,11 and 12 in MDBMessageSelectorExample
      }
      finally
      {
         //Step 13. Be sure to close our JMS resources!
         if (initialContext != null)
         {
            initialContext.close();
         }
         if(connection != null)
         {
            connection.close();
         }
      }
   }
}