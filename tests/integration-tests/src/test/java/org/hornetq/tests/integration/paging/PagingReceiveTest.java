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

package org.hornetq.tests.integration.paging;

import junit.framework.Assert;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.Queue;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;
import org.junit.Before;
import org.junit.Test;

public class PagingReceiveTest extends ServiceTestBase
{

   private static final SimpleString ADDRESS = new SimpleString("jms.queue.catalog-service.price.change.bm");

   private HornetQServer server;

   private ServerLocator locator;

   protected boolean isNetty()
   {
      return false;
   }


   @Test
   public void testReceive() throws Exception
   {
      ClientMessage message = receiveMessage();
      System.out.println("message received:" + message);

      Assert.assertNotNull("Message not found.", message);
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      server = internalCreateServer();

      Queue queue = server.createQueue(ADDRESS, ADDRESS, null, true, false);
      queue.getPageSubscription().getPagingStore().startPaging();

      for (int i = 0; i < 10; i++)
      {
         queue.getPageSubscription().getPagingStore().forceAnotherPage();
      }

      final ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(null, null, false, true, true, false, 0);
      ClientProducer prod = session.createProducer(ADDRESS);

      for (int i = 0; i < 500; i++)
      {
         ClientMessage msg = session.createMessage(true);
         msg.putIntProperty("key", i);
         prod.send(msg);
         if (i > 0 && i % 10 == 0)
         {
            session.commit();
         }
      }

      session.close();
      locator.close();

      server.stop();

      internalCreateServer();


   }

   private HornetQServer internalCreateServer() throws Exception
   {
      final HornetQServer server = newHornetQServer();

      server.start();

      waitForServer(server);

      locator = createFactory(isNetty());
      return server;
   }

   private ClientMessage receiveMessage() throws Exception
   {
      final ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(null, null, false, true, true, false, 0);

      session.start();
      ClientConsumer consumer = session.createConsumer(ADDRESS);

      ClientMessage message = consumer.receive(1000);

      session.commit();

      if (message != null)
      {
         message.acknowledge();
      }

      consumer.close();

      session.close();

      return message;
   }

   private HornetQServer newHornetQServer() throws Exception
   {
      final HornetQServer server = createServer(true, isNetty());

      final AddressSettings settings = new AddressSettings();
      settings.setMaxSizeBytes(67108864);
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      settings.setMaxRedeliveryDelay(3600000);
      settings.setRedeliveryMultiplier(2.0);
      settings.setRedeliveryDelay(500);

      server.getAddressSettingsRepository().addMatch("#", settings);

      return server;
   }


}
