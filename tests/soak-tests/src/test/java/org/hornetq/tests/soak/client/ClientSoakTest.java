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
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.DivertConfiguration;
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
public class ClientSoakTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private static final SimpleString ADDRESS = new SimpleString("ADD");

   private static final SimpleString DIVERTED_AD1 = ClientSoakTest.ADDRESS.concat("-1");

   private static final SimpleString DIVERTED_AD2 = ClientSoakTest.ADDRESS.concat("-2");

   private static final boolean IS_NETTY = true;

   private static final boolean IS_JOURNAL = true;

   public static final int MIN_MESSAGES_ON_QUEUE = 5000;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   private HornetQServer server;

   @Override
   @Before
   public void setUp() throws Exception
   {
      clearData();

      Configuration config = createDefaultConfig(ClientSoakTest.IS_NETTY);

      config.setJournalFileSize(10 * 1024 * 1024);

      server = createServer(IS_JOURNAL, config, -1, -1, new HashMap<String, AddressSettings>());

      DivertConfiguration divert1 = new DivertConfiguration("dv1",
                                                            "nm1",
                                                            ClientSoakTest.ADDRESS.toString(),
                                                            ClientSoakTest.DIVERTED_AD1.toString(),
                                                            true,
                                                            null,
                                                            null);

      DivertConfiguration divert2 = new DivertConfiguration("dv2",
                                                            "nm2",
                                                            ClientSoakTest.ADDRESS.toString(),
                                                            ClientSoakTest.DIVERTED_AD2.toString(),
                                                            true,
                                                            null,
                                                            null);

      ArrayList<DivertConfiguration> divertList = new ArrayList<DivertConfiguration>();
      divertList.add(divert1);
      divertList.add(divert2);

      config.setDivertConfigurations(divertList);

      server.start();

      ServerLocator locator = createFactory(IS_NETTY);

      ClientSessionFactory sf = createSessionFactory(locator);

      ClientSession session = sf.createSession();

      session.createQueue(ClientSoakTest.ADDRESS, ClientSoakTest.ADDRESS, true);

      session.createQueue(ClientSoakTest.DIVERTED_AD1, ClientSoakTest.DIVERTED_AD1, true);

      session.createQueue(ClientSoakTest.DIVERTED_AD2, ClientSoakTest.DIVERTED_AD2, true);

      session.close();

      sf.close();

      locator.close();

   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      server.stop();
      server = null;
   }

   @Test
   public void testSoakClient() throws Exception
   {
      final ServerLocator locator = createFactory(IS_NETTY);
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

      Receiver rec1 = new Receiver(createSessionFactory(locator), DIVERTED_AD1.toString());
      Receiver rec2 = new Receiver(createSessionFactory(locator), DIVERTED_AD2.toString());

      Sender send = new Sender(createSessionFactory(locator), ADDRESS.toString(), new Receiver[] { rec1, rec2 });

      send.start();
      rec1.start();
      rec2.start();

      long timeEnd = System.currentTimeMillis() + TimeUnit.HOURS.toMillis(1);
      while (timeEnd > System.currentTimeMillis())
      {
         if (send.getErrorsCount() != 0 || rec1.getErrorsCount() != 0 || rec2.getErrorsCount() != 0)
         {
            System.out.println("There are sequence errors in some of the clients, please look at the logs");
            break;
         }
         Thread.sleep(10000);
      }

      send.setRunning(false);
      rec1.setRunning(false);
      rec2.setRunning(false);

      send.join();
      rec1.join();
      rec2.join();

      assertEquals(0, send.getErrorsCount());
      assertEquals(0, rec1.getErrorsCount());
      assertEquals(0, rec2.getErrorsCount());

      locator.close();

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
