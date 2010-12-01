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
package org.hornetq.tests.integration.http;

import java.util.HashMap;

import junit.framework.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.*;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.remoting.impl.netty.NettyConnectorFactory;
import org.hornetq.core.remoting.impl.netty.TransportConstants;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.jms.client.HornetQTextMessage;
import org.hornetq.tests.util.UnitTestCase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 */
public class CoreClientOverHttpTest extends UnitTestCase
{
   public void testCoreHttpClient() throws Exception
   {
      final SimpleString QUEUE = new SimpleString("CoreClientOverHttpTestQueue");

      Configuration conf = new ConfigurationImpl();

      conf.setSecurityEnabled(false);

      HashMap<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.HTTP_ENABLED_PROP_NAME, true);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(NettyAcceptorFactory.class.getName(), params));

      HornetQServer server = HornetQServers.newHornetQServer(conf, false);

      server.start();

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(NettyConnectorFactory.class.getName(), params));
      ClientSessionFactory sf = locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
                                                             false,
                                                             0,
                                                             System.currentTimeMillis(),
                                                             (byte)1);
         message.getBodyBuffer().writeString("CoreClientOverHttpTest");
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         Assert.assertEquals("CoreClientOverHttpTest", message2.getBodyBuffer().readString());

         message2.acknowledge();
      }

      session.close();

      locator.close();


      server.stop();
   }

   public void testCoreHttpClientIdle() throws Exception
   {
      final SimpleString QUEUE = new SimpleString("CoreClientOverHttpTestQueue");

      Configuration conf = new ConfigurationImpl();

      conf.setSecurityEnabled(false);

      HashMap<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.HTTP_ENABLED_PROP_NAME, true);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(NettyAcceptorFactory.class.getName(), params));

      HornetQServer server = HornetQServers.newHornetQServer(conf, false);

      server.start();

      ServerLocator locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(NettyConnectorFactory.class.getName(), params));
      locator.setConnectionTTL(500);
      ClientSessionFactory sf = locator.createSessionFactory();

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      Thread.sleep(500 * 5);

      session.close();

      locator.close();

      server.stop();
   }
}
