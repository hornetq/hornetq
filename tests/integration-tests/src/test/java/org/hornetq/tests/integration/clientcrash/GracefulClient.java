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

package org.hornetq.tests.integration.clientcrash;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.tests.integration.IntegrationTestLogger;

/**
 * Code to be run in an external VM, via main().
 *
 * @author <a href="mailto:ovidiu@feodorov.com">Ovidiu Feodorov</a>
 * @author <a href="mailto:jmesnil@redhat.com">Jeff Mesnil</a>
 *
 * @version <tt>$Revision$</tt>
 *
 * $Id$
 */
public class GracefulClient
{
   // Constants ------------------------------------------------------------------------------------

   private static final IntegrationTestLogger log = IntegrationTestLogger.LOGGER;

   // Static ---------------------------------------------------------------------------------------

   public static void main(final String[] args) throws Exception
   {
      if (args.length != 2)
      {
         throw new Exception("require 2 arguments: queue name + message text");
      }
      String queueName = args[0];
      String messageText = args[1];

      try
      {
         ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(NettyConnectorFactory.class.getName()));
         ClientSessionFactory sf = locator.createSessionFactory();
         ClientSession session = sf.createSession(false, true, true);
         ClientProducer producer = session.createProducer(queueName);
         ClientConsumer consumer = session.createConsumer(queueName);

         ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.getBodyBuffer().writeString(messageText);
         producer.send(message);

         session.start();

         // block in receiving for 5 secs, we won't receive anything
         consumer.receive(5000);

         // this should silence any non-daemon thread and allow for graceful exit
         session.close();
      }
      catch (Throwable t)
      {
         GracefulClient.log.error(t.getMessage(), t);
         System.exit(1);
      }
   }

   // Attributes -----------------------------------------------------------------------------------

   // Constructors ---------------------------------------------------------------------------------

   // Command implementation -----------------------------------------------------------------------

   // Public ---------------------------------------------------------------------------------------

   // Package protected ----------------------------------------------------------------------------

   // Protected ------------------------------------------------------------------------------------

   // Private --------------------------------------------------------------------------------------

   // Inner classes --------------------------------------------------------------------------------

}
