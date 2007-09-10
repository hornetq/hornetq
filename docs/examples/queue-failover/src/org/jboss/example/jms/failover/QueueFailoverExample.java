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
package org.jboss.example.jms.failover;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.naming.InitialContext;

import org.jboss.example.jms.common.ExampleSupport;

/**
 * The example creates a connection to a clustered messaging instance and sends a message to a
 * clustered queue. The example then kills the cluster node the connection was created to and then
 * shows how the client code can receive the message with using the *same* connection (that, in
 * the mean time, have transparently failed over to another cluster node).
 *
 * Since this example is also used as a smoke test, it is essential that the VM exits with exit
 * code 0 in case of successful execution and a non-zero value on failure.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 1001 $</tt>
 *
 * $Id: TopicExample.java 1001 2006-06-24 09:05:40Z timfox $
 */
public class QueueFailoverExample extends ExampleSupport
{
   public void example() throws Exception
   {
      String destinationName = getDestinationJNDIName();

      InitialContext ic = null;

      Connection connection = null;


      ConnectionFactory cf = null;
      
      try
      {
         // Create a connection to the clustered messaging instance

         ic = new InitialContext();

         cf = (ConnectionFactory)ic.lookup("/ClusteredConnectionFactory");

         Queue distributedQueue = (Queue)ic.lookup(destinationName);
         log("Distributed queue " + destinationName + " exists");

         // When connecting to a messaging cluster, the ConnectionFactory has the capability of
         // transparently creating physical connections to different cluster nodes, in a round
         // robin fashion ...

         connection = cf.createConnection();

         connection.start();

         // Send 2 messages to the queue

         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageProducer producer = session.createProducer(distributedQueue);

         TextMessage message1 = session.createTextMessage("Hello1!");

         producer.send(message1);

         TextMessage message2 = session.createTextMessage("Hello2!");

         producer.send(message2);

         log("The messages were successfully sent to the distributed queue");

         // Now receive one of the messages

         MessageConsumer consumer = session.createConsumer(distributedQueue);

         TextMessage rm1 = (TextMessage)consumer.receive(2000);

         log("Received message: " + rm1.getText());

         assertEquals("Hello1!", rm1.getText());

         // Kill the active node
         killActiveNode();
         
         Thread.sleep(5000);

         // receive the second message on the failed over node

         TextMessage rm2 = (TextMessage)consumer.receive(2000);
         log("Received message: " + rm2.getText());


         assertEquals("Hello2!", rm2.getText());

         displayProviderInfo(connection.getMetaData());
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
            if (connection != null)
            {
               connection.close();
            }
         }
         catch(JMSException e)
         {
            log("Could not close connection " + connection + ", exception was " + e);
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
      new QueueFailoverExample().run();
   }
}
