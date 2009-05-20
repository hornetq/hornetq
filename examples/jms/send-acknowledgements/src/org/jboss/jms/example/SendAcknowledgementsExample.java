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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.naming.InitialContext;

import org.jboss.common.example.JMSExample;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.SendAcknowledgementHandler;
import org.jboss.messaging.core.message.Message;
import org.jboss.messaging.jms.client.JBossSession;

/**
 * 
 * Asynchronous Send Acknowledgements are an advanced feature of JBoss Messaging which allow you to
 * receive acknowledgements that messages were successfully received at the server in a separate stream
 * to the stream of messages being sent to the server.
 * For more information please see the readme.html file
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class SendAcknowledgementsExample extends JMSExample
{
   public static void main(String[] args)
   {
      new SendAcknowledgementsExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         // Step 2. Perfom a lookup on the queue
         Queue queue = (Queue)initialContext.lookup("/queue/exampleQueue");

         // Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 4. Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Define a SendAcknowledgementHandler which will receive asynchronous acknowledgements
         class MySendAcknowledgementsHandler implements SendAcknowledgementHandler
         {
            int count = 0;

            public void sendAcknowledged(final Message message)
            {
               System.out.println("Received send acknowledgement for message " + count++);
            }
         }

         // Step 6. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 7. Set the handler on the underlying core session

         ClientSession coreSession = ((JBossSession)session).getCoreSession();

         coreSession.setSendAcknowledgementHandler(new MySendAcknowledgementsHandler());

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

         // Step 7. Send 5000 messages, the handler will get called asynchronously some time later after the messages
         // are sent.

         final int numMessages = 5000;

         for (int i = 0; i < numMessages; i++)
         {
            javax.jms.Message jmsMessage = session.createMessage();

            producer.send(jmsMessage);

            System.out.println("Sent message " + i);
         }

         return true;
      }
      finally
      {
         // Step 12. Be sure to close our JMS resources!
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
