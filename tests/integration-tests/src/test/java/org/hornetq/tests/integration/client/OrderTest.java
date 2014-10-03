/*
 * Copyright 2005-2014 Red Hat, Inc.
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

import java.util.Arrays;
import java.util.Collection;

import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * A OrderTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
@RunWith(Parameterized.class)
public class OrderTest extends ServiceTestBase
{

   private boolean persistent;

   private HornetQServer server;

   private ServerLocator locator;

   public OrderTest(boolean persistent)
   {
      this.persistent = persistent;
   }
   @Parameterized.Parameters(name = "persistent={0}")
   public static Collection<Object[]> getParams()
   {
      return Arrays.asList(new Object[][]{
         {true},
         {false}
      });
   }


   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      locator = createNettyNonHALocator();
   }


   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   @Test
   public void testSimpleStorage() throws Exception
   {
      server = createServer(persistent, true);
      server.start();

      locator.setBlockOnNonDurableSend(false);
      locator.setBlockOnDurableSend(false);
      locator.setBlockOnAcknowledge(true);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(true, true, 0);

      session.createQueue("queue", "queue", true);

      ClientProducer prod = session.createProducer("queue");

      for (int i = 0; i < 100; i++)
      {
         ClientMessage msg = session.createMessage(i % 2 == 0);
         msg.putIntProperty("id", i);
         prod.send(msg);
      }

      session.close();

      boolean started = false;

      for (int start = 0; start < 2; start++)
      {

         if (persistent && start == 1)
         {
            started = true;
            server.stop();
            server.start();
            sf = createSessionFactory(locator);
         }

         session = sf.createSession(true, true);

         session.start();

         ClientConsumer cons = session.createConsumer("queue");

         for (int i = 0; i < 100; i++)
         {
            if (!started || started && i % 2 == 0)
            {
               ClientMessage msg = cons.receive(10000);

               Assert.assertEquals(i, msg.getIntProperty("id").intValue());
            }
         }

         cons.close();

         cons = session.createConsumer("queue");

         for (int i = 0; i < 100; i++)
         {
            if (!started || started && i % 2 == 0)
            {
               ClientMessage msg = cons.receive(10000);

               Assert.assertEquals(i, msg.getIntProperty("id").intValue());
            }
         }

         session.close();
      }
   }

   @Test
   public void testOrderOverSessionClose() throws Exception
   {
      server = createServer(persistent, true);

      server.start();

      locator.setBlockOnNonDurableSend(false);
      locator.setBlockOnDurableSend(false);
      locator.setBlockOnAcknowledge(false);

      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(true, true, 0);

      int numberOfMessages = 500;
      session.createQueue("queue", "queue", true);

      ClientProducer prod = session.createProducer("queue");

      for (int i = 0; i < numberOfMessages; i++)
      {
         ClientMessage msg = session.createMessage(i % 2 == 0);
         msg.putIntProperty("id", i);
         prod.send(msg);
      }

      session.close();

      for (int i = 0; i < numberOfMessages; )
      {
         session = sf.createSession();

         session.start();

         ClientConsumer consumer = session.createConsumer("queue");

         int max = i + 10;

         for (; i < max; i++)
         {
            ClientMessage msg = consumer.receive(1000);

            msg.acknowledge();

            Assert.assertEquals(i, msg.getIntProperty("id").intValue());
         }

         // Receive a few more messages but don't consume them
         for (int j = 0; j < 10 && i < numberOfMessages; j++)
         {
            ClientMessage msg = consumer.receiveImmediate();
            if (msg == null)
            {
               break;
            }
         }
         session.close();

      }
   }

   @Test
   public void testOrderOverSessionCloseWithRedeliveryDelay() throws Exception
   {
      server = createServer(persistent, true);

      server.getAddressSettingsRepository().clear();
      AddressSettings setting = new AddressSettings();
      setting.setRedeliveryDelay(500);
      server.getAddressSettingsRepository().addMatch("#", setting);

      server.start();

      locator.setBlockOnNonDurableSend(false);
      locator.setBlockOnDurableSend(false);
      locator.setBlockOnAcknowledge(false);

      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession(true, true, 0);

      int numberOfMessages = 500;

      session.createQueue("queue", "queue", true);

      ClientProducer prod = session.createProducer("queue");

      for (int i = 0; i < numberOfMessages; i++)
      {
         ClientMessage msg = session.createMessage(i % 2 == 0);
         msg.putIntProperty("id", i);
         prod.send(msg);
      }

      session.close();

      session = sf.createSession(false, false);

      session.start();

      ClientConsumer cons = session.createConsumer("queue");

      for (int i = 0; i < numberOfMessages; i++)
      {
         ClientMessage msg = cons.receive(5000);
         msg.acknowledge();
         assertEquals(i, msg.getIntProperty("id").intValue());
      }
      session.close();


      session = sf.createSession(false, false);

      session.start();

      cons = session.createConsumer("queue");


      for (int i = 0; i < numberOfMessages; i++)
      {
         ClientMessage msg = cons.receive(5000);
         assertNotNull(msg);
         msg.acknowledge();
         assertEquals(i, msg.getIntProperty("id").intValue());
      }

      // receive again
      session.commit();
      session.close();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
