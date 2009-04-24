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

import java.util.HashMap;

import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.QueueConnection;
import javax.jms.QueueConnectionFactory;
import javax.jms.QueueSession;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.management.MBeanServerConnection;
import javax.management.MBeanServerInvocationHandler;
import javax.management.ObjectName;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.naming.InitialContext;

import org.jboss.messaging.core.management.ObjectNames;
import org.jboss.messaging.jms.server.management.JMSQueueControlMBean;

/**
 * An example that shows how to manage JBoss Messaging using JMX.
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class JMXExample extends JMSExample
{
   private static final String JMX_URL = "service:jmx:rmi:///jndi/rmi://localhost:3000/jmxrmi";

   public static void main(String[] args)
   {
      String[] serverJMXArgs = new String[] { "-Dcom.sun.management.jmxremote",
                                             "-Dcom.sun.management.jmxremote.port=3000",
                                             "-Dcom.sun.management.jmxremote.ssl=false",
                                             "-Dcom.sun.management.jmxremote.authenticate=false"
      };
      new JMXExample().run(serverJMXArgs, args);
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

         // Step 3. Perform a lookup on the Connection Factory
         QueueConnectionFactory cf = (QueueConnectionFactory)initialContext.lookup("/ConnectionFactory");

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

         // Step 9. Retrieve the ObjectName of the queue. This is used to identify the server resources to manage
         ObjectName on = ObjectNames.getJMSQueueObjectName(queue.getQueueName());

         // Step 10. Create JMX Connector to connect to the server's MBeanServer
         JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(JMX_URL), new HashMap());

         // Step 11. Retrieve the MBeanServerConnection
         MBeanServerConnection mbsc = connector.getMBeanServerConnection();

         // Step 12. Create a JMSQueueControlMBean proxy to manage the queue on the server
         JMSQueueControlMBean queueControl = (JMSQueueControlMBean)MBeanServerInvocationHandler.newProxyInstance(mbsc,
                                                                                                                 on,
                                                                                                                 JMSQueueControlMBean.class,
                                                                                                                 false);
         // Step 13. Display the number of messages in the queue
         System.out.println(queueControl.getName() + " contains " + queueControl.getMessageCount() + " messages");

         // Step 14. Remove the message sent at step #8
         System.out.println("message has been removed: " + queueControl.removeMessage(message.getJMSMessageID()));

         // Step 15. Display the number of messages in the queue
         System.out.println(queueControl.getName() + " contains " + queueControl.getMessageCount() + " messages");

         // Step 16. We close the JMX connector
         connector.close();

         // Step 17. Create a JMS Message Consumer on the queue
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 18. Start the Connection
         connection.start();

         // Step 19. Trying to receive a message. Since the only message in the queue was removed by a management
         // operation, there is none to consume.
         // The call will timeout after 5000ms and messageReceived will be null
         TextMessage messageReceived = (TextMessage)messageConsumer.receive(5000);
         System.out.println("Received message: " + messageReceived);

         return true;
      }
      finally
      {
         // Step 20. Be sure to close the resources!
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
