/*
 * Copyright 2009 Red Hat, Inc.
 * Red Hat licenses this file to you under the Apache License, version
 * 2.0 (the "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 */

package org.hornetq.tests.integration.security;

import static org.hornetq.tests.util.RandomUtil.randomString;

import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.logging.Logger;
import org.hornetq.jms.client.HornetQTextMessage;

/**
 * Code to be run in an external VM, via main().
 *
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class SimpleClient
{
   // Constants ------------------------------------------------------------------------------------

   private static final Logger log = Logger.getLogger(SimpleClient.class);

   // Static ---------------------------------------------------------------------------------------

   public static void main(String[] args) throws Exception
   {
      try
      {

         if (args.length != 1)
         {
            throw new Exception("require 1 argument: connector factory class name");
         }
         String connectorFactoryClassName = args[0];

         String queueName = randomString();
         String messageText = randomString();

         ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(connectorFactoryClassName));
         ClientSession session = sf.createSession(false, true, true);
         
         session.createQueue(queueName, queueName, null, false);
         ClientProducer producer = session.createProducer(queueName);
         ClientConsumer consumer = session.createConsumer(queueName);

         ClientMessage message = session.createClientMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.getBodyBuffer().writeString(messageText);
         producer.send(message);

         session.start();

         ClientMessage receivedMsg = consumer.receive(5000);
         if (receivedMsg == null)
         {
            throw new Exception("did not receive the message");
         }
         
         String text = receivedMsg.getBodyBuffer().readString();
         if (text == null || !text.equals(messageText))
         {
            throw new Exception("received " + text + ", was expecting " + messageText);
         }

         // clean all resources to exit cleanly
         consumer.close();
         session.deleteQueue(queueName);         
         session.close();
         
         System.out.println("OK");
      }
      catch (Throwable t)
      {
         String allStack = t.getMessage() + "|";
         StackTraceElement[] stackTrace = t.getStackTrace();
         for (StackTraceElement stackTraceElement : stackTrace)
         {
            allStack += stackTraceElement.toString() + "|";
         }
         System.out.println(allStack);
         System.exit(1);
      }
   }

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
