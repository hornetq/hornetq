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

package org.hornetq.tests.soak.client;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * A ClientSoakTest
 *
 * @author <a href="mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ClientNonDivertedSoakTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("ADD");

   private static final boolean IS_NETTY = false;

   private static final boolean IS_JOURNAL = false;

   public static final int MIN_MESSAGES_ON_QUEUE = 5000;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   private HornetQServer server;

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      clearData();

      Configuration config = createDefaultConfig(ClientNonDivertedSoakTest.IS_NETTY);

      config.setJournalFileSize(10 * 1024 * 1024);

      server = createServer(IS_JOURNAL, config, -1, -1, new HashMap<String, AddressSettings>());

      server.start();

      ServerLocator locator = createFactory(ClientNonDivertedSoakTest.IS_NETTY);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession();

      session.createQueue(ClientNonDivertedSoakTest.ADDRESS, ClientNonDivertedSoakTest.ADDRESS, true);

      session.close();

      sf.close();

      locator.close();

   }

   public void testSoakClient() throws Exception
   {
      ServerLocator locator = createFactory(ClientNonDivertedSoakTest.IS_NETTY);

      final ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession(false, false);

      ClientProducer producer = session.createProducer(ADDRESS);

      for (int i = 0; i < MIN_MESSAGES_ON_QUEUE; i++)
      {
         ClientMessage msg = session.createMessage(true);
         msg.putLongProperty("count", i);
         msg.getBodyBuffer().writeBytes(new byte[10 * 1024]);
         producer.send(msg);

         if (i % 1000 == 0)
         {
            System.out.println("Sent " + i + " messages");
            session.commit();
         }
      }

      session.commit();

      session.close();
      sf.close();

      Receiver rec1 = new Receiver(createSessionFactory(locator), ADDRESS.toString());

      Sender send = new Sender(createSessionFactory(locator), ADDRESS.toString(), new Receiver[] { rec1 });

      send.start();
      rec1.start();

      long timeEnd = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);
      while (timeEnd > System.currentTimeMillis())
      {
         if (send.getErrorsCount() != 0 || rec1.getErrorsCount() != 0 )
         {
            System.out.println("There are sequence errors in some of the clients, please look at the logs");
            break;
         }

         System.out.println("count = " + send.msgs);
         Thread.sleep(10000);
      }

      send.setRunning(false);
      rec1.setRunning(false);

      send.join();
      rec1.join();

      assertEquals(0, send.getErrorsCount());
      assertEquals(0, rec1.getErrorsCount());

      locator.close();

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
