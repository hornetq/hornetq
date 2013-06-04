/*
 * Copyright 2010 Red Hat, Inc.
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

/**
 * A ExpireTestOnRestartTest
 *
 * @author clebertsuconic
 *
 *
 */
public class ExpireTestOnRestartTest extends ServiceTestBase
{

   HornetQServer server;


   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      server = createServer(true);
      AddressSettings setting = new AddressSettings();
      setting.setExpiryAddress(SimpleString.toSimpleString("exp"));
      setting.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      setting.setPageSizeBytes(100 * 1024);
      setting.setMaxSizeBytes(200 * 1024);
      server.getConfiguration().setJournalSyncNonTransactional(false);
      server.getConfiguration().setMessageExpiryScanPeriod(-1);
      server.getConfiguration().setJournalSyncTransactional(false);
      server.getAddressSettingsRepository().addMatch("#", setting);
      server.start();
   }

   // The biggest problem on this test was the exceptions that happened. I couldn't find any wrong state beyond the exceptions
   @Test
   public void testRestartWithExpire() throws Exception
   {
      int NUMBER_OF_EXPIRED_MESSAGES = 1000;
      ServerLocator locator = createInVMNonHALocator();
      locator.setBlockOnDurableSend(false);
      ClientSessionFactory factory = locator.createSessionFactory();
      ClientSession session = factory.createSession(true, true);

      session.createQueue("test", "test", true);
      session.createQueue("exp", "exp", true);
      ClientProducer prod = session.createProducer("test");

      for (int i = 0 ; i < 10; i++)
      {
         ClientMessage message = session.createMessage(true);
         message.getBodyBuffer().writeBytes(new byte[1024 * 10]);
         prod.send(message);
      }

      for (int i = 0 ; i < NUMBER_OF_EXPIRED_MESSAGES; i++)
      {
         ClientMessage message = session.createMessage(true);
         message.putIntProperty("i", i);
         message.getBodyBuffer().writeBytes(new byte[1024 * 10]);
         message.setExpiration(System.currentTimeMillis() + 5000);
         prod.send(message);
      }

      session.commit();

      session.close();

      server.stop();
      server.getConfiguration().setMessageExpiryScanPeriod(1);

      Thread.sleep(5500); // enough time for expiration of the messages

      server.start();

      Queue queue = server.locateQueue(SimpleString.toSimpleString("test"));

      factory = locator.createSessionFactory();
      session = factory.createSession(false, false);


      ClientConsumer cons = session.createConsumer("test");
      session.start();
      for (int i = 0 ; i < 10; i++)
      {
         ClientMessage msg = cons.receive(5000);
         assertNotNull(msg);
         msg.acknowledge();
      }

      assertNull(cons.receiveImmediate());
      cons.close();

      long timeout = System.currentTimeMillis() + 60000;
      while (queue.getPageSubscription().getPagingStore().isPaging() && timeout > System.currentTimeMillis())
      {
         Thread.sleep(1);
      }
      assertFalse(queue.getPageSubscription().getPagingStore().isPaging());


      cons = session.createConsumer("exp");
      for (int i  = 0 ; i < NUMBER_OF_EXPIRED_MESSAGES; i++)
      {
         ClientMessage msg = cons.receive(5000);
         assertNotNull(msg);
         msg.acknowledge();
      }

      session.commit();

      int extras = 0;
      ClientMessage msg;
      while ((msg = cons.receiveImmediate()) != null)
      {
         System.out.println(msg);
         extras++;
      }

      assertEquals("Received extra messages on expire address", 0, extras);


      session.commit();

      session.close();

      locator.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
