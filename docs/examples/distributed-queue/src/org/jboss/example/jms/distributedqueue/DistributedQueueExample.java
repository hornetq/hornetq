/*
* JBoss, Home of Professional Open Source
* Copyright 2005, JBoss Inc., and individual contributors as indicated
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
package org.jboss.example.jms.distributedqueue;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Queue;
import javax.naming.InitialContext;

import org.jboss.example.jms.common.ExampleSupport;

/**
 * The example creates two connections to two distinct cluster nodes on which we have previously
 * deployed a distributed queue. The example sends and receives messages using both connections.
 *
 * NOTE: This is an example that assumes a NullMessagePullPolicy. This is the default configuration
 *       option a release ships with, and also is a very boring options, since messages are *NOT*
 *       redistributed among nodes. An example that assumes a DefaultMessagePullPolicy is coming
 *       soon (http://jira.jboss.org/jira/browse/JBMESSAGING-907).
 *
 * Since this example is also used as a smoke test, it is essential that the VM exits with exit
 * code 0 in case of successful execution and a non-zero value on failure.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 1001 $</tt>
 *
 * $Id: TopicExample.java 1001 2006-06-24 09:05:40Z timfox $
 */
public class DistributedQueueExample extends ExampleSupport
{
   public void example() throws Exception
   {
      String destinationName = getDestinationJNDIName();

      InitialContext ic = null;

      Connection connection0 = null;
      Connection connection1 = null;

      try
      {
         // connecting to the first node

         ic = new InitialContext();

         ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ClusteredConnectionFactory");
         Queue distributedQueue = (Queue)ic.lookup(destinationName);
         log("Distributed queue " + destinationName + " exists");


         // When connecting to a messaging cluster, the ConnectionFactory has the capability of
         // transparently creating physical connections to different cluster nodes, in a round
         // robin fashion ...

         // ... so this is a connection to a cluster node
         connection0 = cf.createConnection();

         // ... and this is a connection to a different cluster node
         connection1 = cf.createConnection();

         // Let's make sure that (this example is also a smoke test)
         assertNotEquals(getServerID(connection0), getServerID(connection1));

         // Create a session, a producer and a consumer on the first connection

         Session session0 = connection0.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer publisher0 = session0.createProducer(distributedQueue);
         MessageConsumer consumer0 = session0.createConsumer(distributedQueue);
         ExampleListener messageListener0 = new ExampleListener("MessageListener0");
         consumer0.setMessageListener(messageListener0);

         // Create another session, producer and consumer on the second connection

         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer publisher1 = session1.createProducer(distributedQueue);
         MessageConsumer consumer1 = session1.createConsumer(distributedQueue);
         ExampleListener messageListener1 = new ExampleListener("MessageListener1");
         consumer1.setMessageListener(messageListener1);

         // Start connections, so we can receive the message

         connection0.start();
         connection1.start();

         // Send the message

         TextMessage message = session0.createTextMessage("Hello!");
         publisher0.send(message);

         message = session1.createTextMessage("Another Hello!");
         publisher1.send(message);

         log("The messages were successfully sent to the distributed queue");

         // NOTE: We know that this example is extremely boring, but so it's NullMessagePullPolicy.
         //       However, this is the default configuration option a release ships with.

         messageListener0.waitForMessage(3000);

         message = (TextMessage)messageListener0.getMessage();
         log(messageListener0.getName() + " received message: " + message.getText());
         assertEquals("Hello!", message.getText());

         messageListener1.waitForMessage(3000);

         message = (TextMessage)messageListener1.getMessage();
         log(messageListener1.getName() + " received message: " + message.getText());
         assertEquals("Another Hello!", message.getText());

         displayProviderInfo(connection0.getMetaData());

      }
      finally
      {
         if (ic != null)
         {
            try
            {
               ic.close();
            }
            catch(Exception e)
            {
               throw e;
            }
         }

         try
         {
            if (connection0 != null)
            {
               connection0.close();
            }
         }
         catch(JMSException e)
         {
            log("Could not close connection " + connection0 + ", exception was " + e);
            throw e;
         }

         try
         {
            if (connection1 != null)
            {
               connection1.close();
            }
         }
         catch(JMSException e)
         {
            log("Could not close connection " + connection1 + ", exception was " + e);
            throw e;
         }
      }
   }

   protected boolean isQueueExample()
   {
      return true;
   }

   public static void main(String[] args)
   {
      new DistributedQueueExample().run();
   }

}
