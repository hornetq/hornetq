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

package org.hornetq.tests.integration.client;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.tests.util.ServiceTestBase;

public class CoreClientTest extends ServiceTestBase
{
   private static final Logger log = Logger.getLogger(CoreClientTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCoreClientNetty() throws Exception
   {
      testCoreClient("org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory",
                     "org.hornetq.core.remoting.impl.netty.NettyConnectorFactory");
   }

   public void testCoreClientInVM() throws Exception
   {
      testCoreClient("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory",
                     "org.hornetq.core.remoting.impl.invm.InVMConnectorFactory");
   }

   private void testCoreClient(final String acceptorFactoryClassName, final String connectorFactoryClassName) throws Exception
   {
      final SimpleString QUEUE = new SimpleString("CoreClientTestQueue");

      Configuration conf = createDefaultConfig();

      conf.setSecurityEnabled(false);

      conf.getAcceptorConfigurations().add(new TransportConfiguration(acceptorFactoryClassName));

      HornetQServer server = HornetQServers.newHornetQServer(conf, false);

      server.start();
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(connectorFactoryClassName));

      ClientSessionFactory sf = locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);

         message.putStringProperty("foo", "bar");

         // One way around the setting destination problem is as follows -
         // Remove destination as an attribute from client producer.
         // The destination always has to be set explicity before sending a message

         message.setAddress(QUEUE);

         message.getBodyBuffer().writeString("testINVMCoreClient");

         producer.send(message);
      }

      CoreClientTest.log.info("sent messages");

      ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         HornetQBuffer buffer = message2.getBodyBuffer();

         Assert.assertEquals("testINVMCoreClient", buffer.readString());

         message2.acknowledge();
      }

      session.close();

      locator.close();

      server.stop();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
