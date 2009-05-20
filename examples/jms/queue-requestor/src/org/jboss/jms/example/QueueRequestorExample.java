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

import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueRequestor;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.common.example.JMSExample;

/**
 * A simple JMS example that shows how to use queues requestors.
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class QueueRequestorExample extends JMSExample
{
   public static void main(String[] args)
   {
      new QueueRequestorExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      QueueConnection connection = null;
      InitialContext initialContext = null;
      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         // Step 2. Perfom a lookup on the queue
         Queue queue = (Queue)initialContext.lookup("/queue/exampleQueue");

         // Step 3. Look-up the JMS queue connection factory
         QueueConnectionFactory cf = (QueueConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 4. Create a TextReverserService which consumes messages from the queue and sends message with reversed text
         TextReverserService reverserService = new TextReverserService(cf, queue);

         // Step 5. Create a JMS QueueConnection
         connection = cf.createQueueConnection();

         // Step 6. Start the connection
         connection.start();

         // Step 7. Create a JMS queue session with AUTO_ACKNOWLEDGE mode
         QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 8. Create a JMS queue requestor to send requests to the queue
         QueueRequestor queueRequestor = new QueueRequestor(session, queue);

         // Step 9. Create a JMS message to send as a request
         TextMessage request = session.createTextMessage("Hello, World!");

         // Step 10. Use the requestor to send the request and wait to receive a reply
         TextMessage reply = (TextMessage)queueRequestor.request(request);

         // Step 11. The reply's text contains the reversed request's text
         System.out.println("Send request: " + request.getText());
         System.out.println("Received reply:" + reply.getText());

         // Step.12 close the queue requestor
         queueRequestor.close();

         // Step 13. close the text reverser service
         reverserService.close();
         
         return true;
      }
      finally
      {
         if (connection != null)
         {
            try
            {
               // Step 14. Be sure to close the JMS resources!
               connection.close();
            }
            catch (JMSException e)
            {
               e.printStackTrace();
            }
         }
         
         if (initialContext != null)
         {
            // Also the InitialContext
            initialContext.close();
         }
      }
   }
}
