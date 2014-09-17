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
package org.hornetq.cli.commands;

import io.airlift.command.Arguments;
import io.airlift.command.Command;
import org.hornetq.cli.HornetQ;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.dto.BrokerDTO;
import org.hornetq.factory.BrokerFactory;
import org.hornetq.factory.CoreFactory;
import org.hornetq.factory.SecurityManagerFactory;
import org.hornetq.integration.bootstrap.HornetQBootstrapLogger;
import org.hornetq.jms.server.JMSServerManager;
import org.hornetq.jms.server.impl.JMSServerManagerImpl;
import org.hornetq.jms.server.impl.StandaloneNamingServer;
import org.hornetq.spi.core.security.HornetQSecurityManager;

import javax.management.MBeanServer;
import java.io.File;
import java.lang.management.ManagementFactory;
import java.util.Timer;
import java.util.TimerTask;

@Command(name = "run", description = "runs the broker instance")
public class Run implements Action
{

   @Arguments(description = "Broker Configuration URI, default 'xml:${HORNETQ_HOME}/config/hornetq.xml'")
   String configuration;
   private StandaloneNamingServer namingServer;
   private JMSServerManager jmsServerManager;

   @Override
   public Object execute(ActionContext context) throws Exception
   {

      HornetQ.printBanner();

      if (configuration == null)
      {
         configuration = "xml:" + System.getProperty("hornetq.home") + "/config/hornetq.xml";
      }

      System.out.println("Loading configuration file: " + configuration);

      BrokerDTO broker = BrokerFactory.createBroker(configuration);

      addShutdownHook(new File(broker.core.configuration).getParentFile());

      Configuration core = CoreFactory.create(broker.core);

      HornetQSecurityManager security = SecurityManagerFactory.create(broker.security);

      MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

      HornetQServerImpl server = new HornetQServerImpl(core, mBeanServer, security);

      namingServer = new StandaloneNamingServer(server);

      namingServer.start();

      jmsServerManager = new JMSServerManagerImpl(server);

      jmsServerManager.start();

      return null;
   }

   /**
    * Add a simple shutdown hook to stop the server.
    * @param configurationDir
    */
   private void addShutdownHook(File configurationDir)
   {
      final File file = new File(configurationDir,"STOP_ME");
      if (file.exists())
      {
         if (!file.delete())
         {
            HornetQBootstrapLogger.LOGGER.errorDeletingFile(file.getAbsolutePath());
         }
      }
      final Timer timer = new Timer("HornetQ Server Shutdown Timer", true);
      timer.scheduleAtFixedRate(new TimerTask()
      {
         @Override
         public void run()
         {
            if (file.exists())
            {
               try
               {
                  try
                  {
                     jmsServerManager.stop();
                  }
                  catch (Exception e)
                  {
                     e.printStackTrace();
                  }
                  timer.cancel();
               }
               finally
               {
                  Runtime.getRuntime().exit(0);
               }
            }
         }
      }, 500, 500);
   }
}
