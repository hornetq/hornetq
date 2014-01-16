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

package org.hornetq.tests.integration.paging;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.settings.impl.AddressFullMessagePolicy;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * This is a sub process of the test {@link org.hornetq.tests.integration.paging.PageCountSyncOnNonTXTest}
 *  The System.out calls here are meant to be here as they will appear on the process output and test output.
 *  It helps to identify what happened on the test in case of failures.
 * @author Clebert Suconic
 */

public class PageCountSyncServer
{

   public void perform(final String folder, final long timeToRun) throws Exception
   {

      try
      {
         HornetQServer server = createServer(folder);

         server.start();

         System.out.println("Server started!!!");

         System.out.println("Waiting " + timeToRun + " seconds");
         Thread.sleep(timeToRun);

         System.out.println("Going down now!!!");
         System.exit(1);
      }
      catch (Exception e)
      {
         e.printStackTrace();
         System.exit(-1);
      }
   }

   static HornetQServer createServer(String folder)
   {
      Configuration conf = createConfig(folder);
      return HornetQServers.newHornetQServer(conf, true);
   }

   static Configuration createConfig(String folder)
   {
      Configuration conf = ServiceTestBase.createBasicConfig(folder, 0);
      conf.setSecurityEnabled(false);
      conf.setPersistenceEnabled(true);
      AddressSettings settings = new AddressSettings();
      settings.setAddressFullMessagePolicy(AddressFullMessagePolicy.PAGE);
      settings.setPageSizeBytes(10 * 1024);
      settings.setMaxSizeBytes(100 * 1024);
      conf.getAddressesSettings().put("#", settings);
      conf.getAcceptorConfigurations().add(new TransportConfiguration("org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory"));
      return conf;
   }


   public static void main(String[] args) throws Exception
   {
      PageCountSyncServer ss = new PageCountSyncServer();

      System.out.println("Args.length = " + args.length);
      for (String arg: args)
      {
         System.out.println("Argument: " + arg);
      }

      if (args.length == 2)
      {
         ss.perform(args[0], Long.parseLong(args[1]));
      }
      else
      {
         System.err.println("you were expected to pass getTestDir as an argument on SpawnVMSupport");
      }
   }

}
