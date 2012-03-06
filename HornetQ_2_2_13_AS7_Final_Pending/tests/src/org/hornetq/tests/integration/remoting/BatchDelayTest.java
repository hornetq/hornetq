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

import java.util.HashMap;
import java.util.Map;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
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

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(BatchDelayTest.class);

   private static final long DELAY = 500;
   
   // Attributes ----------------------------------------------------

   private HornetQServer server;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.BATCH_DELAY, DELAY);

      TransportConfiguration tc = new TransportConfiguration(NettyAcceptorFactory.class.getName(), params);

      Configuration config = createBasicConfig();
      config.getAcceptorConfigurations().add(tc);

      config.setSecurityEnabled(false);
      server = createServer(false, config);
      server.start();
   }

   @Override
   protected void tearDown() throws Exception
   {
      server.stop();

      server = null;

      super.tearDown();
   }

   protected ClientSessionFactory createSessionFactory() throws Exception
   {
      Map<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.BATCH_DELAY, DELAY);
      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(ServiceTestBase.NETTY_CONNECTOR_FACTORY, params));

      ClientSessionFactory sf = locator.createSessionFactory();

      return sf;
   }

   public void testSendReceiveMany() throws Exception
   {
      ClientSessionFactory sf = createSessionFactory();

      ClientSession session = sf.createSession();

      final String foo = "foo";

      session.createQueue(foo, foo);

      ClientProducer prod = session.createProducer(foo);

      ClientConsumer cons = session.createConsumer(foo);

      session.start();

      final int numMessages = 1000;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage msg = session.createMessage(false);

         prod.send(msg);
      }

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage msg = cons.receive(10000);

         assertNotNull(msg);

         msg.acknowledge();
      }

      sf.close();
   }

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

      sf.close();
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
