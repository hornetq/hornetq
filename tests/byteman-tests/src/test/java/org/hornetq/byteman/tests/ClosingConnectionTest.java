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
package org.hornetq.byteman.tests;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.api.core.management.HornetQServerControl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.integration.IntegrationTestLogger;
import org.hornetq.tests.integration.management.ManagementControlHelper;
import org.hornetq.tests.util.ServiceTestBase;
import org.jboss.byteman.contrib.bmunit.BMRule;
import org.jboss.byteman.contrib.bmunit.BMRules;
import org.jboss.byteman.contrib.bmunit.BMUnitRunner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(BMUnitRunner.class)
public class ClosingConnectionTest extends ServiceTestBase
{
   public static final SimpleString ADDRESS = new SimpleString("SimpleAddress");

   private ServerLocator locator;

   private HornetQServer server;

   private static MBeanServer mBeanServer;

   private static boolean readyToKill = false;

   protected boolean isNetty()
   {
      return true;
   }


   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();
      mBeanServer = MBeanServerFactory.createMBeanServer();
      server = newHornetQServer();
      server.getConfiguration().setJournalType(JournalType.NIO);
      server.getConfiguration().setJMXManagementEnabled(true);
      server.start();
      waitForServer(server);
      locator = createFactory(isNetty());
      readyToKill = false;
   }

   public static void killConnection() throws InterruptedException
   {
      if (readyToKill)
      {
         // We have to kill the connection in a new thread otherwise Netty won't interrupt the current thread
         Thread closeConnectionThread = new Thread(new Runnable()
         {
            @Override
            public void run()
            {
               try
               {
                  HornetQServerControl serverControl = ManagementControlHelper.createHornetQServerControl(mBeanServer);
                  serverControl.closeConnectionsForUser("guest");
                  readyToKill = false;
               }
               catch (Exception e)
               {
                  e.printStackTrace();
               }
            }
         });

         closeConnectionThread.start();

         try
         {
            /* We want to simulate a long-running remoting thread here. If closing the connection in the closeConnectionThread
             * interrupts this thread then it will cause sleep() to throw and InterruptedException. Therefore we catch
             * the InterruptedException and re-interrupt the current thread so the interrupt will be passed properly
             * back to the caller. It's a bit of a hack, but I couldn't find any other way to simulate it.
             */
            Thread.sleep(1500);
         }
         catch (InterruptedException e)
         {
            Thread.currentThread().interrupt();
         }
      }
   }

   /*
   * Test for https://bugzilla.redhat.com/show_bug.cgi?id=1193085
   * */
   @Test
   @BMRules
      (
         rules =
            {
               @BMRule
                  (
                     name = "rule to kill connection",
                     targetClass = "org.hornetq.core.journal.impl.NIOSequentialFile",
                     targetMethod = "open(int, boolean)",
                     targetLocation = "AT INVOKE java.nio.channels.FileChannel.size()",
                     action = "org.hornetq.byteman.tests.ClosingConnectionTest.killConnection();"

                  )
            }
      )
   public void testKillConnection() throws Exception
   {
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setBlockOnAcknowledge(true);

      ClientSessionFactory sf = createSessionFactory(locator);
      ClientSession session = sf.createSession("guest", null, false, true, true, false, 0);

      session.createQueue(ADDRESS, ADDRESS, null, true);

      ClientProducer producer = session.createProducer(ADDRESS);

      ClientMessage message = session.createMessage(true);
      message.getBodyBuffer().writeBytes(new byte[1024]);

      for (int i = 0; i < 200; i++)
      {
         producer.send(message);
      }

      assertTrue(server.locateQueue(ADDRESS).getPageSubscription().getPagingStore().isPaging());

      readyToKill = true;
      try
      {
         for (int i = 0; i < 8; i++)
         {
            producer.send(message);
         }
         fail("Sending message here should result in failure.");
      }
      catch (Exception e)
      {
         IntegrationTestLogger.LOGGER.info("Caught exception: " + e.getMessage());
      }

      Thread.sleep(1000);

      assertTrue(server.isStarted());

      session.close();
   }

   private HornetQServer newHornetQServer() throws Exception
   {
      HornetQServer server = createServer(true, createDefaultConfig(isNetty()), mBeanServer);

      AddressSettings defaultSetting = new AddressSettings();
      defaultSetting.setPageSizeBytes(10 * 1024);
      defaultSetting.setMaxSizeBytes(20 * 1024);

      server.getAddressSettingsRepository().addMatch("#", defaultSetting);

      return server;
   }
}
