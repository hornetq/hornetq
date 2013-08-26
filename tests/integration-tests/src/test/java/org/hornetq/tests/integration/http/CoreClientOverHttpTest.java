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
import org.junit.Before;

import org.junit.Test;

import java.util.HashMap;
import java.util.Random;

import org.junit.Assert;

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
   private static final SimpleString QUEUE = new SimpleString("CoreClientOverHttpTestQueue");
   private Configuration conf;
   private HornetQServer server;
   private ServerLocator locator;

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      conf = createDefaultConfig();

      conf.setSecurityEnabled(false);
      HashMap<String, Object> params = new HashMap<String, Object>();
      params.put(TransportConstants.HTTP_ENABLED_PROP_NAME, true);
      conf.getAcceptorConfigurations().add(new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params));

      server = addServer(HornetQServers.newHornetQServer(conf, false));

      server.start();
      locator = HornetQClient.createServerLocatorWithoutHA(new TransportConfiguration(NETTY_CONNECTOR_FACTORY, params));
      addServerLocator(locator);
   }

   @Test
   public void testCoreHttpClient() throws Exception
   {
      ClientSessionFactory sf = createSessionFactory(locator);
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
   }

   @Test
   public void testCoreHttpClientIdle() throws Exception
   {
      locator.setConnectionTTL(500);
      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      Thread.sleep(500 * 5);

      session.close();
   }

   // https://issues.jboss.org/browse/JBPAPP-5542
   @Test
   public void testCoreHttpClient8kPlus() throws Exception
   {
      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(false, true, true);

      session.createQueue(QUEUE, QUEUE, null, false);

      ClientProducer producer = session.createProducer(QUEUE);

      final int numMessages = 100;

      String[] content = new String[numMessages];

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message = session.createMessage(HornetQTextMessage.TYPE,
                                                       false,
                                                       0,
                                                       System.currentTimeMillis(),
                                                       (byte)1);
         content[i] = this.getFixedSizeString(((i % 5) + 1) * 1024 * 8);
         message.getBodyBuffer().writeString(content[i]);
         producer.send(message);
      }

      ClientConsumer consumer = session.createConsumer(QUEUE);

      session.start();

      for (int i = 0; i < numMessages; i++)
      {
         ClientMessage message2 = consumer.receive();

         Assert.assertEquals(content[i], message2.getBodyBuffer().readString());

         message2.acknowledge();
      }

      session.close();
   }

   private String getFixedSizeString(int size)
   {
      StringBuffer sb = new StringBuffer();
      Random r = new Random();
      for (int i = 0; i < size; i++)
      {
         char chr = (char)r.nextInt(256);
         sb.append(chr);
      }
      String result = sb.toString();
      return result;
   }

}
