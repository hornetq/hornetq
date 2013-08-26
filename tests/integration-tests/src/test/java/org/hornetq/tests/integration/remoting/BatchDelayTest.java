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

package org.hornetq.tests.integration.remoting;
import org.junit.Before;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.tests.util.ServiceTestBase;

/**
 *
 * A BatchDelayTest
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 *
 */
public class BatchDelayTest extends ServiceTestBase
{

   private static final int N = 1000;
   private static final long DELAY = 500;
   private HornetQServer server;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.BATCH_DELAY, DELAY);

      TransportConfiguration tc = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params);

      Configuration config = createBasicConfig();
      config.getAcceptorConfigurations().add(tc);

      config.setSecurityEnabled(false);
      server = createServer(false, config);
      server.start();
   }

   protected ClientSessionFactory createSessionFactory() throws Exception
   {
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.BATCH_DELAY, DELAY);
      ServerLocator locator =
               HornetQClient.createServerLocatorWithoutHA(createTransportConfiguration(true, false, params));
      addServerLocator(locator);
      ClientSessionFactory sf = createSessionFactory(locator);
      return addSessionFactory(sf);
   }

   @Test
   public void testSendReceiveMany() throws Exception
   {
      ClientSessionFactory sf = createSessionFactory();

      ClientSession session = sf.createSession();

      final String foo = "foo";

      session.createQueue(foo, foo);

      ClientProducer prod = session.createProducer(foo);

      ClientConsumer cons = session.createConsumer(foo);

      session.start();

      sendMessages(session, prod, N);
      receiveMessages(cons, 0, N, true);
   }

   @Test
   public void testSendReceiveOne() throws Exception
   {
      ClientSessionFactory sf = createSessionFactory();

      ClientSession session = sf.createSession();

      final String foo = "foo";

      session.createQueue(foo, foo);

      ClientProducer prod = session.createProducer(foo);

      ClientConsumer cons = session.createConsumer(foo);

      session.start();

      ClientMessage msg = session.createMessage(false);

      prod.send(msg);

      msg = cons.receive(10000);

      assertNotNull(msg);

      msg.acknowledge();
   }
}
