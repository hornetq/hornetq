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

package org.hornetq.tests.integration.paging;

import java.util.ArrayList;
import java.util.List;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.paging.cursor.impl.PagePositionImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;
public class PagingLeakTest extends ServiceTestBase
{

   public void testValidateLeak() throws Throwable
   {
      
      if (PagePositionImpl.getCount() != 0)
      {
         forceGC();
         if (PagePositionImpl.getCount() != 0)
         {
            System.out.println("There were some previous leaks (from previous tests) on PagePositionImpl::" + PagePositionImpl.getCount());
            PagePositionImpl.resetCount();
         }
      }

      List<PagePositionImpl> positions = new ArrayList<PagePositionImpl>();

      for (int i = 0; i < 300; i++)
      {
         positions.add(new PagePositionImpl(3, 3));
      }

      long timeout = System.currentTimeMillis() + 5000;
      while (PagePositionImpl.getCount() != 300 && timeout > System.currentTimeMillis())
      {
         forceGC();
      }

      // This is just to validate the rules are correctly applied on byteman
      assertEquals("You have changed something on PagePositionImpl in such way that these byteman rules are no longer working", 300, PagePositionImpl.getCount());

      positions.clear();

      timeout = System.currentTimeMillis() + 5000;
      while (PagePositionImpl.getCount() != 0 && timeout > System.currentTimeMillis())
      {
         forceGC();
      }

      // This is just to validate the rules are correctly applied on byteman
      assertEquals("You have changed something on PagePositionImpl in such way that these byteman rules are no longer working", 0, PagePositionImpl.getCount());


      forceGC();
      final ArrayList<Exception> errors = new ArrayList<Exception>();
      // A backup that will be waiting to be activated
      Configuration conf = createDefaultConfig(true);
      conf.setSecurityEnabled(false);
      conf.getConnectorConfigurations().put("invm", new TransportConfiguration(INVM_CONNECTOR_FACTORY));

      final HornetQServer server = HornetQServers.newHornetQServer(conf, true);


      server.start();
      
      
      try
      {


      AddressSettings settings = new AddressSettings();
      settings.setPageSizeBytes(20 * 1024);
      settings.setMaxSizeBytes(200 * 1024);
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);


      server.getAddressSettingsRepository().addMatch("#", settings);


      final SimpleString address = new SimpleString("pgdAddress");

      class Consumer extends Thread
      {
         final ServerLocator locator;
         final ClientSessionFactory sf;
         final ClientSession session;
         final ClientConsumer consumer;

         final int sleepTime;
         final int maxConsumed;

         Consumer(int sleepTime, String suffix, int maxConsumed) throws Exception
         {

            server.createQueue(address, address.concat(suffix), null, true, false);

            this.sleepTime = sleepTime;
            locator = createInVMLocator(0);
            sf = locator.createSessionFactory();
            session = sf.createSession(true, true);
            consumer = session.createConsumer(address.concat(suffix));

            this.maxConsumed = maxConsumed;
         }

         public void run()
         {
            try
            {
               session.start();

               long lastTime = System.currentTimeMillis();

               for (long i = 0; i < maxConsumed; i++)
               {
                  ClientMessage msg = consumer.receive(5000);

                  if (msg == null)
                  {
                     errors.add(new Exception("didn't receive a message"));
                     return;
                  }

                  msg.acknowledge();


                  if (sleepTime > 0)
                  {

                     Thread.sleep(sleepTime);
                  }

                  if (i % 1000 == 0)
                  {
                     System.out.println("Consumed " + i + " events in " + (System.currentTimeMillis() - lastTime));
                     lastTime = System.currentTimeMillis();
                  }
               }
            }
            catch (Exception e)
            {
               e.printStackTrace();
            }
         }
      }


      int numberOfMessages = 10000;

      Consumer consumer1 = new Consumer(100, "-1", 150);
      Consumer consumer2 = new Consumer(0, "-2", numberOfMessages);

      final ServerLocator locator = createInVMLocator(0);
      final ClientSessionFactory sf = locator.createSessionFactory();
      final ClientSession session = sf.createSession(true, true);
      final ClientProducer producer = session.createProducer(address);


      byte[] b = new byte[1024];


      for (long i = 0; i < numberOfMessages; i++)
      {
         ClientMessage msg = session.createMessage(true);
         msg.getBodyBuffer().writeBytes(b);
         producer.send(msg);

         if (i == 1000)
         {
            System.out.println("Starting consumers!!!");
            consumer1.start();
            consumer2.start();
         }

         if (i % 1000 == 0)
         {
            validateInstances();
         }

      }


      consumer1.join();
      consumer2.join();

      validateInstances();
      Throwable elast = null;

      for (Throwable e : errors)
      {
         e.printStackTrace();
         elast = e;
      }

      if (elast != null)
      {
         throw elast;
      }
      }
      finally
      {
         try
         {
            server.stop();
         }
         catch (Throwable ignored)
         {
         }
      }

   }

   private void validateInstances()
   {
      forceGC();
      long count2 = PagePositionImpl.getCount();
      assertTrue("There is a leak, you shouldn't have this many instances (" + count2 + ")", count2 < 5000);
   }


}
