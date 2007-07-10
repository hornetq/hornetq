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
package org.jboss.example.jms.distributedtopic;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.naming.InitialContext;

import org.jboss.example.jms.common.ExampleSupport;

/**
 * The example sends a message to a distributed topic deployed on the JMS cluster. The message is
 * subsequently received by two different subscribers, connected to two distinct cluster nodes.
 *
 * Since this example is also used as a smoke test, it is essential that the VM exits with exit
 * code 0 in case of successful execution and a non-zero value on failure.
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class DistributedTopicExample extends ExampleSupport
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
         Topic distributedTopic = (Topic)ic.lookup(destinationName);
         log("Distributed topic " + destinationName + " exists");


         // When connecting to a messaging cluster, the ConnectionFactory has the capability of
         // transparently creating physical connections to different cluster nodes, in a round
         // robin fashion ...

         // ... so this is a connection to a cluster node
         connection0 = cf.createConnection();

         // ... and this is a connection to a different cluster node
         connection1 = cf.createConnection();

         // Let's make sure that (this example is also a smoke test)
         assertNotEquals(getServerID(connection0), getServerID(connection1));

         // Create a session, a producer and consumer for the distributed topic, using connection0

         Session session0 = connection0.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer subscriber0 = session0.createConsumer(distributedTopic);
         ExampleListener messageListener0 = new ExampleListener("MessageListener 0");
         subscriber0.setMessageListener(messageListener0);

         MessageProducer publisher = session0.createProducer(distributedTopic);


         // Create a session and a consumer for the distributed topic, using connection1

         Session session1 = connection1.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer subscriber1 = session1.createConsumer(distributedTopic);

         ExampleListener messageListener1 = new ExampleListener("MessageListener 1");
         subscriber1.setMessageListener(messageListener1);

         // Starting the connections

         connection0.start();
         connection1.start();

         // Sending the message

         TextMessage message = session0.createTextMessage("Hello!");
         publisher.send(message);

         log("The message was successfully published on the distributed topic");

         messageListener0.waitForMessage();
         messageListener1.waitForMessage();

         message = (TextMessage)messageListener0.getMessage();
         log(messageListener0.getName() + " received message: " + message.getText());
         assertEquals("Hello!", message.getText());

         message = (TextMessage)messageListener1.getMessage();
         log(messageListener1.getName() + " received message: " + message.getText());
         assertEquals("Hello!", message.getText());

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
      return false;
   }

   public static void main(String[] args)
   {
      new DistributedTopicExample().run();
   }

}
