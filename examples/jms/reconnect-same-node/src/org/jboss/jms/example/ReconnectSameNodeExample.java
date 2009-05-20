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
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.common.example.JBMExample;
import org.jboss.messaging.jms.JBossQueue;
import org.jboss.messaging.jms.server.management.impl.JMSManagementHelper;

/**
 * This examples demonstrates a connection created to a server. Failure of the network connection is then simulated
 * 
 * The network is brought back up and the client reconnects and resumes transparently.
 *
 * @author <a href="tim.fox@jboss.com>Tim Fox</a>
 */
public class ReconnectSameNodeExample extends JBMExample
{
   public static void main(String[] args)
   {
      new ReconnectSameNodeExample().run(args);
   }

   public boolean runExample() throws Exception
   {
      Connection connection = null;
      InitialContext initialContext = null;

      try
      {
         // Step 1. Create an initial context to perform the JNDI lookup.
         initialContext = getContext(0);

         // Step 2. Perform a lookup on the queue
         Queue queue = (Queue)initialContext.lookup("/queue/exampleQueue");

         // Step 3. Perform a lookup on the Connection Factory
         ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory");

         // Step 4. Create a JMS Connection
         connection = cf.createConnection();

         // Step 5. Create a JMS Session
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         // Step 6. Create a JMS Message Producer
         MessageProducer producer = session.createProducer(queue);

         // Step 7. Create a Text Message
         TextMessage message = session.createTextMessage("This is a text message");

         System.out.println("Sent message: " + message.getText());

         // Step 8. Send the Message
         producer.send(message);

         // Step 9. Create a JMS Message Consumer
         MessageConsumer messageConsumer = session.createConsumer(queue);

         // Step 10. Start the Connection
         connection.start();

         // Step 11. To simulate a temporary problem on the network, we stop the remoting acceptor on the
         // server which will close all connections
         stopAcceptor(initialContext);

         System.out.println("Acceptor now stopped, will wait for 10 seconds. This simulates the network connection failing for a while");

         // Step 12. Wait a while then restart the acceptor
         Thread.sleep(10000);

         System.out.println("Re-starting acceptor");

         startAcceptor(initialContext);

         System.out.println("Restarted acceptor. The client will now reconnect.");

         // Step 13. We receive the message
         TextMessage messageReceived = (TextMessage)messageConsumer.receive(5000);

         System.out.println("Received message: " + messageReceived.getText());

         return true;
      }
      finally
      {
         // Step 14. Be sure to close our JMS resources!
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

   private void stopAcceptor(InitialContext ic) throws Exception
   {
      this.stopStartAcceptor(ic, true);
   }

   private void startAcceptor(InitialContext ic) throws Exception
   {
      this.stopStartAcceptor(ic, false);
   }

   // To do this we send a management message to close the acceptor, we do this on a different
   // connection factory which uses a different remoting connection so we can still send messages
   // when the main connection has been stopped
   private void stopStartAcceptor(InitialContext initialContext, boolean stop) throws Exception
   {
      ConnectionFactory cf = (ConnectionFactory)initialContext.lookup("/ConnectionFactory2");

      Connection connection = null;
      try
      {
         connection = cf.createConnection();

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         Queue managementQueue = new JBossQueue("jbm.management", "jbm.management");

         MessageProducer producer = session.createProducer(managementQueue);

         connection.start();

         Message m = session.createMessage();

         String oper = stop ? "stop" : "start";

         JMSManagementHelper.putOperationInvocation(m, "core.acceptor.netty-acceptor", oper);

         producer.send(m);
      }
      finally
      {
         if (connection != null)
         {
            connection.close();
         }
      }
   }

}
