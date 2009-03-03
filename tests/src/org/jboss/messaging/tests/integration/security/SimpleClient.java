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

package org.jboss.messaging.tests.integration.security;

import static org.jboss.messaging.tests.util.RandomUtil.randomString;

import org.jboss.messaging.core.client.ClientConsumer;
import org.jboss.messaging.core.client.ClientMessage;
import org.jboss.messaging.core.client.ClientProducer;
import org.jboss.messaging.core.client.ClientSession;
import org.jboss.messaging.core.client.ClientSessionFactory;
import org.jboss.messaging.core.client.impl.ClientSessionFactoryImpl;
import org.jboss.messaging.core.config.TransportConfiguration;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.jms.client.JBossTextMessage;

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
         
         session.createQueue(queueName, queueName, null, false, false);
         ClientProducer producer = session.createProducer(queueName);
         ClientConsumer consumer = session.createConsumer(queueName);

         ClientMessage message = session.createClientMessage(JBossTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.getBody().writeString(messageText);
         producer.send(message);

         session.start();

         ClientMessage receivedMsg = consumer.receive(5000);
         if (receivedMsg == null)
         {
            throw new Exception("did not receive the message");
         }
         
         String text = receivedMsg.getBody().readString();
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
