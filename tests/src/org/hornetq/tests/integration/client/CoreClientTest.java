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

import org.hornetq.core.buffers.HornetQBuffer;
import org.hornetq.core.client.ClientConsumer;
import org.hornetq.core.client.ClientMessage;
import org.hornetq.core.client.ClientProducer;
import org.hornetq.core.client.ClientSession;
import org.hornetq.core.client.ClientSessionFactory;
import org.hornetq.core.client.impl.ClientSessionFactoryImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.tests.util.UnitTestCase;
import org.hornetq.utils.SimpleString;

public class CoreClientTest extends UnitTestCase
{
   private static final Logger log = Logger.getLogger(CoreClientTest.class);

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testCoreClientNetty() throws Exception
   {
      testCoreClient("org.hornetq.integration.transports.netty.NettyAcceptorFactory",
                     "org.hornetq.integration.transports.netty.NettyConnectorFactory");
   }

   public void testCoreClientInVM() throws Exception
   {
      testCoreClient("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory",
                     "org.hornetq.core.remoting.impl.invm.InVMConnectorFactory");
   }

   private void testCoreClient(final String acceptorFactoryClassName, final String connectorFactoryClassName) throws Exception
   {
      final SimpleString QUEUE = new SimpleString("CoreClientTestQueue");

      Configuration conf = new ConfigurationImpl();

      conf.setSecurityEnabled(false);

      conf.getAcceptorConfigurations().add(new TransportConfiguration(acceptorFactoryClassName));

      HornetQServer server = HornetQ.newHornetQServer(conf, false);

      server.start();

      ClientSessionFactory sf = new ClientSessionFactoryImpl(new TransportConfiguration(connectorFactoryClassName));
     // sf.setConsumerWindowSize(0);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {         
         ClientMessage message = session.createClientMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);

         message.putStringProperty("foo", "bar");

         //One way around the setting destination problem is as follows -
         //Remove destination as an attribute from client producer.
         //The destination always has to be set explicity before sending a message
         
         message.setDestination(QUEUE);
         
         message.getBodyBuffer().writeString("testINVMCoreClient");

         producer.send(message);
      }

      log.info("sent messages");
      
      ClientConsumer consumer = session.createConsumer(QUEUE);
      
      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();
         
        // log.info("got message " + i);
         
         HornetQBuffer buffer = message2.getBodyBuffer();
         
         assertEquals("testINVMCoreClient", buffer.readString());

         message2.acknowledge();
      }

      session.close();

      server.stop();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
}
