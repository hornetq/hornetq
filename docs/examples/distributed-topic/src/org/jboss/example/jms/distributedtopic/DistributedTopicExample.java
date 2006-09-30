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

import javax.naming.InitialContext;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Topic;
import javax.jms.Session;
import javax.jms.MessageProducer;
import javax.jms.MessageConsumer;
import javax.jms.TextMessage;
import javax.jms.JMSException;

import org.jboss.example.jms.common.ExampleSupport;

import java.util.Hashtable;

/**
 * The example sends a message to a distributed topic depolyed on the JMS cluster. The message is
 * subsequently received by two different subscribers, connected to two distinct cluster nodes.
 *
 * Since this example is also used as a smoke test, it is essential that the VM exits with exit
 * code 0 in case of successful execution and a non-zero value on failure.
 *
 * @author <a href="mailto:ovidiu@jboss.org">Ovidiu Feodorov</a>
 * @version <tt>$Revision: 1001 $</tt>
 *
 * $Id: TopicExample.java 1001 2006-06-24 09:05:40Z timfox $
 */
public class DistributedTopicExample extends ExampleSupport
{
   public void example() throws Exception
   {
      String destinationName = getDestinationJNDIName();

      InitialContext ic = null;
      InitialContext ic2 = null;

      Connection connection = null;
      Connection connection2 = null;

      try
      {
         // connecting to the first node

         ic = new InitialContext();

         ConnectionFactory cf = (ConnectionFactory)ic.lookup("/ConnectionFactory");
         Topic distributedTopic = (Topic)ic.lookup(destinationName);
         log("Distributed topic " + destinationName + " exists");

         connection = cf.createConnection();
         Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
         MessageProducer publisher = session.createProducer(distributedTopic);

         MessageConsumer subscriber = session.createConsumer(distributedTopic);

         ExampleListener messageListener = new ExampleListener("MessageListener 1");
         subscriber.setMessageListener(messageListener);

         // connecting to the second node

         Hashtable environment = new Hashtable();
         environment.put("java.naming.factory.initial", "org.jnp.interfaces.NamingContextFactory");
         environment.put("java.naming.provider.url", "jnp://localhost:1199");
         environment.put("java.naming.factory.url.pkgs", "org.jboss.naming:org.jnp.interfaces");

         ic2 = new InitialContext(environment);

         ConnectionFactory cf2 = (ConnectionFactory)ic2.lookup("/ConnectionFactory");

         connection2 = cf2.createConnection();
         Session session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);

         MessageConsumer subscriber2 = session2.createConsumer(distributedTopic);

         ExampleListener messageListener2 = new ExampleListener("MessageListener 2");
         subscriber2.setMessageListener(messageListener2);

         // starting the connections

         connection.start();
         connection2.start();

         // sending the message

         TextMessage message = session.createTextMessage("Hello!");
         publisher.send(message);

         log("The message was successfully published on the distributed topic");

         messageListener.waitForMessage();
         messageListener2.waitForMessage();

         message = (TextMessage)messageListener.getMessage();
         log(messageListener.getName() + " received message: " + message.getText());
         assertEquals("Hello!", message.getText());

         message = (TextMessage)messageListener2.getMessage();
         log(messageListener2.getName() + " received message: " + message.getText());
         assertEquals("Hello!", message.getText());

         displayProviderInfo(connection.getMetaData());

      }
      finally
      {
         if(ic != null)
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

         if(ic2 != null)
         {
            try
            {
               ic2.close();
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

         try
         {
            if (connection2 != null)
            {
               connection2.close();
            }
         }
         catch(JMSException e)
         {
            log("Could not close connection " + connection2 + ", exception was " + e);
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
