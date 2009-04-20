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

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueRequestor;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.server.management.impl.JMSManagementHelper;

/**
 * An example that shows how to manage JBoss Messaging using JMS messages.
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class ManagementExample extends JMSExample
{
   public static void main(String[] args)
   {
      new ManagementExample().run(args);
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
         Queue queue = (Queue) initialContext.lookup("/queue/exampleQueue");
         
         // Step 3. Perform a lookup on the Connection Factory
         QueueConnectionFactory cf = (QueueConnectionFactory) initialContext.lookup("/ConnectionFactory");

         // Step 4.Create a JMS Connection
         connection = cf.createQueueConnection();

         // Step 5. Create a JMS Session
         QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         // Step 7. Create a Text Message
         TextMessage message = session.createTextMessage("This is a text message");         
         System.out.println("Sent message: " + message.getText());

         // Step 8. Send the Message
         producer.send(message);

         // Step 9. create the JMS management queue.
         // It is a "special" queue and it is not looked up from JNDI but constructed directly
         Queue managementQueue = new JBossQueue("jbm.management", "jbm.management");
         
         // Step 10. Create a QueueRequestor for the management queue (see queue-requestor example)
         QueueRequestor requestor = new QueueRequestor(session, managementQueue);
         
         // Step 11. Start the Connection to allow the queue requestor to receive replies
         connection.start();

         // Step 12. Create a JMS message which is used to send a management message
         Message m = session.createMessage();
                  
         // Step 13. Use a helper class to fill the JMS message with management information:
         // * the name of the resource to manage
         // * in this case, we want to retrieve the value of the MessageCount of the queue
         JMSManagementHelper.putAttribute(m, "jms.queue.exampleQueue", "MessageCount");
         
         // Step 14. Use the requestor to send the request and wait for the reply
         ObjectMessage reply = (ObjectMessage)requestor.request(m);

         // Step 15. The attribute value is returned as the body of an ObjectMessage (in this case, an integer)
         int messageCount = (Integer) reply.getObject();
         System.out.println(queue.getQueueName() + " contains " + messageCount + " messages");
         
         // Step 16. Create another JMS message to use as a management message
         m = session.createMessage();
         
         // Step 17. Use a helper class to fill the JMS message with management information:
         // * the object name of the resource to manage (i.e. the queue)
         // * in this case, we want to call the "removeMessage" operation with the JMS MessageID
         //   of the message sent to the queue in step 8.
         JMSManagementHelper.putOperationInvocation(m, "jms.queue.exampleQueue", "removeMessage", message.getJMSMessageID());

         // Step 18 Use the requestor to send the request and wait for the reply
         reply = (ObjectMessage)requestor.request(m);
         
         // Step 19. Use a helper class to check that the operation has succeeded
         boolean success = JMSManagementHelper.hasOperationSucceeded(reply);
         System.out.println("operation invocation has succeeded: " + success);

         // Step 20. The return value of the operation invocation is returned as the body of an ObjectMessage
         // in that case, a boolean which is true if the message was removed, false else
         boolean messageRemoved = (Boolean) reply.getObject();
         System.out.println("message has been removed: " + messageRemoved);
         
         // Step 21. Create a JMS Message Consumer on the queue
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 22. Trying to receive a message. Since the only message in the queue was removed by a management operation,
         // there is none to consume. The call will timeout after 5000ms and messageReceived will be null
         TextMessage messageReceived = (TextMessage) messageConsumer.receive(5000);
         System.out.println("Received message: " + messageReceived);

         return true;
      }
      finally
      {
         //Step 23. Be sure to close the resources!
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
