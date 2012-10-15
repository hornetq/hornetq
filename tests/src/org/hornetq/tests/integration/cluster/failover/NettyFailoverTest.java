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

package org.hornetq.tests.integration.cluster.failover;

import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.remoting.impl.netty.TransportConstants;

/**
 * A NettyFailoverTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class NettyFailoverTest extends FailoverTest
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live)
   {
      if (live)
      {
         return new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory");
      }
      else
      {
         Map<String, Object> server1Params = new HashMap<String, Object>();

         server1Params.put(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_PORT + 1);

         return new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory",
                                           server1Params);
      }
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live)
   {
      if (live)
      {
         return new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyConnectorFactory");
      }
      else
      {
         Map<String, Object> server1Params = new HashMap<String, Object>();

         server1Params.put(TransportConstants.PORT_PROP_NAME, TransportConstants.DEFAULT_PORT + 1);

         return new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyConnectorFactory",
                                           server1Params);
      }
   }


   public void testFailoverWithHostAlias() throws Exception
   {
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.HOST_PROP_NAME, "127.0.0.1");
      TransportConfiguration tc = new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyConnectorFactory", params);

      ServerLocator locator = HornetQClient.createServerLocatorWithHA(tc);

      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(true);
      locator.setReconnectAttempts(-1);
      ClientSessionFactoryInternal sf = createSessionFactoryAndWaitForTopology(locator, 2);

      ClientSession session = createSession(sf, true, true, 0);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      ClientProducer producer = session.createProducer(FailoverTestBase.ADDRESS);

      final int numMessages = 10;

      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);

      session.start();

      crash(session);

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(i % 2 == 0);

         setBody(i, message);

         message.putIntProperty("counter", i);

         producer.send(message);
      }

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = consumer.receive(1000);

         Assert.assertNotNull(message);

         assertMessageBody(i, message);

         Assert.assertEquals(i, message.getIntProperty("counter").intValue());

         message.acknowledge();
      }

      session.close();

      sf.close();

      Assert.assertEquals(0, sf.numSessions());

      Assert.assertEquals(0, sf.numConnections());
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
