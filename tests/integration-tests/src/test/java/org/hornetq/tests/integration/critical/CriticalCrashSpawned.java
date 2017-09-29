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

package org.hornetq.tests.integration.critical;

import javax.management.MBeanServer;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.spi.core.security.HornetQSecurityManager;
import org.hornetq.spi.core.security.HornetQSecurityManagerImpl;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * @author Clebert Suconi
 * c
 */

public class CriticalCrashSpawned extends ServiceTestBase
{
   public static void main(String[] arg)
   {
      try
      {
         CriticalCrashSpawned test = new CriticalCrashSpawned();
         test.runSimple();
      }
      catch (Exception e)
      {
         e.printStackTrace();
         System.exit(-1);
      }
   }

   public void runSimple() throws Exception
   {
      deleteDirectory(new File("./target/server"));
      HornetQServer server = createServer("./target/server");

      try
      {
         server.start();


         server.createQueue(SimpleString.toSimpleString("q1"),
                            SimpleString.toSimpleString("q1"),
                            null,
                            true,
                            false);
         ServerLocator locator = createNettyNonHALocator();
         ClientSession session = locator.createSessionFactory().createSession();
         ClientProducer producer = session.createProducer(SimpleString.toSimpleString("q1"));
         for (int i = 0; i < 500; i++)
         {
            producer.send(session.createMessage(true));
         }

         System.out.println("Sent messages");

      }
      finally
      {
         server.stop();

      }

   }


   static HornetQServer createServer(String folder)
   {
      final AtomicBoolean blocked = new AtomicBoolean(false);
      Configuration conf = createConfig(folder);
      MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
      HornetQSecurityManager securityManager = new HornetQSecurityManagerImpl();

      conf.setPersistenceEnabled(true);

      HornetQServer server = new HornetQServerImpl(conf, mbeanServer, securityManager)
      {
         @Override
         protected StorageManager createStorageManager()
         {
            JournalStorageManager storageManager = new JournalStorageManager(configuration, analyzer, executorFactory, shutdownOnCriticalIO)
            {
               @Override
               public void readLock()
               {
                  super.readLock();
                  if (blocked.get())
                  {
                     while (true)
                     {
                        try
                        {
                           System.out.println("Blocking forever");
                           Thread.sleep(1000);
                        }
                        catch (Throwable ignored)
                        {

                        }
                     }
                  }
               }

               @Override
               public void storeMessage(ServerMessage message) throws Exception
               {
                  super.storeMessage(message);
                  blocked.set(true);
               }
            };

            analyzer.add(storageManager);
            return storageManager;
         }


      };

      return server;
   }

   static Configuration createConfig(String folder)
   {
      Configuration conf = ServiceTestBase.createBasicConfig(folder, 0);
      conf.setSecurityEnabled(false);
      conf.setPersistenceEnabled(true);

      AddressSettings settings = new AddressSettings();
      settings.setMaxDeliveryAttempts(-1);
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      settings.setPageSizeBytes(10 * 1024);
      settings.setMaxSizeBytes(100 * 1024);
      conf.getAddressesSettings().put("#", settings);
      conf.getAcceptorConfigurations().add(new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory"));
      return conf;
   }

}
