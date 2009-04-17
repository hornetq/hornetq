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
import javax.management.openmbean.CompositeData;
import javax.management.remote.JMXConnector;
import javax.management.remote.JMXConnectorFactory;
import javax.management.remote.JMXServiceURL;
import javax.naming.InitialContext;

import org.jboss.messaging.core.management.MessageCounterInfo;
import org.jboss.messaging.core.management.ObjectNames;
import org.jboss.messaging.jms.server.management.JMSQueueControlMBean;

/**
 * An example showing how to use message counters to have information on a queue.
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class MessageCounterExample extends JMSExample
{
   private String JMX_URL = "service:jmx:rmi:///jndi/rmi://localhost:3001/jmxrmi";

   public static void main(String[] args)
   {
      String[] serverJMXArgs = new String[] { "-Dcom.sun.management.jmxremote",
                                             "-Dcom.sun.management.jmxremote.port=3001",
                                             "-Dcom.sun.management.jmxremote.ssl=false",
                                             "-Dcom.sun.management.jmxremote.authenticate=false" };
      new MessageCounterExample().run(serverJMXArgs, args);
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

         // Step 4.Create a JMS Connection, session and a producer for the queue
         connection = cf.createQueueConnection();
         QueueSession session = connection.createQueueSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer producer = session.createProducer(queue);

         // Step 7. Create and send a Text Message
         TextMessage message = session.createTextMessage("This is a text message");
         producer.send(message);
         System.out.println("Sent message: " + message.getText());

         System.out.println("Sleep a little bit to have the message counters sampled...");
         Thread.sleep(12000);
         
         ObjectName on = ObjectNames.getJMSQueueObjectName(queue.getQueueName());
         JMXConnector connector = JMXConnectorFactory.connect(new JMXServiceURL(JMX_URL), new HashMap());
         MBeanServerConnection mbsc = connector.getMBeanServerConnection();
         JMSQueueControlMBean queueControl = (JMSQueueControlMBean)MBeanServerInvocationHandler.newProxyInstance(mbsc,
                                                                                                                 on,
                                                                                                                 JMSQueueControlMBean.class,
                                                                                                                 false);

         CompositeData compositeData = queueControl.listMessageCounter();
         MessageCounterInfo messageCounter = MessageCounterInfo.from(compositeData);
         displayMessageCounter(messageCounter);

         System.out.println("Sleep a little bit again...");
         Thread.sleep(12000);
         
         compositeData = queueControl.listMessageCounter();
         messageCounter = MessageCounterInfo.from(compositeData);

         displayMessageCounter(messageCounter);

         MessageConsumer consumer = session.createConsumer(queue);
         
         connection.start();
         
         TextMessage messageReceived = (TextMessage)consumer.receive(5000);
         System.out.format("Received message: %s\n\n", messageReceived.getText());
         
         System.out.println("Sleep a little bit on last time...");
         Thread.sleep(12000);
         
         compositeData = queueControl.listMessageCounter();
         messageCounter = MessageCounterInfo.from(compositeData);

         displayMessageCounter(messageCounter);
         
         return true;
      }
      finally
      {
         // Step 20. Be sure to close our JMS resources!
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
   
   private void displayMessageCounter(MessageCounterInfo counter)
   {
      System.out.format("%s (sample updated at %s)\n",  counter.getName(), counter.getUdpateTimestamp());
      System.out.format("   %s message(s) added to the queue (since last sample: %s)\n", counter.getCount(), counter.getCountDelta());
      System.out.format("   %s message(s) in the queue (since last sample: %s)\n", counter.getDepth(), counter.getDepthDelta());
      System.out.format("   last message added at %s\n\n", counter.getLastAddTimestamp());
   }

}
