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
package org.hornetq.rest.test;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.jboss.resteasy.plugins.server.tjws.TJWSEmbeddedJaxrsServer;
import org.hornetq.rest.MessageServiceConfiguration;
import org.hornetq.rest.MessageServiceManager;
import org.jboss.resteasy.test.TestPortProvider;

/**
 * @author <a href="mailto:bill@burkecentral.com">Bill Burke</a>
 * @version $Revision: 1 $
 */
public class Embedded
{
   protected MessageServiceManager manager = new MessageServiceManager(null);
   protected MessageServiceConfiguration config = new MessageServiceConfiguration();
   protected HornetQServer hornetqServer;
   protected TJWSEmbeddedJaxrsServer tjws = new TJWSEmbeddedJaxrsServer();

   public Embedded()
   {
      int port = TestPortProvider.getPort();
      System.out.println("default port is: " + port);
      tjws.setPort(port);
      tjws.setRootResourcePath("");
      tjws.setSecurityDomain(null);
   }

   public MessageServiceConfiguration getConfig()
   {
      return config;
   }

   public void setConfig(MessageServiceConfiguration config)
   {
      this.config = config;
   }

   public HornetQServer getHornetqServer()
   {
      return hornetqServer;
   }

   public void setHornetqServer(HornetQServer hornetqServer)
   {
      this.hornetqServer = hornetqServer;
   }

   public TJWSEmbeddedJaxrsServer getJaxrsServer()
   {
      return tjws;
   }

   public MessageServiceManager getManager()
   {
      return manager;
   }

   public void start() throws Exception
   {
      System.out.println("\nStarting Embedded");
      if (hornetqServer == null)
      {
         Configuration configuration = new ConfigurationImpl();
         configuration.setPersistenceEnabled(false);
         configuration.setSecurityEnabled(false);
         configuration.getAcceptorConfigurations().add(new TransportConfiguration(InVMAcceptorFactory.class.getName()));

         hornetqServer = HornetQServers.newHornetQServer(configuration);
         hornetqServer.start();
      }
      tjws.start();
      manager.setConfiguration(config);
      manager.start();
      tjws.getDeployment().getRegistry().addSingletonResource(manager.getQueueManager().getDestination());
      tjws.getDeployment().getRegistry().addSingletonResource(manager.getTopicManager().getDestination());

   }

   public void stop() throws Exception
   {
      System.out.println("\nStopping Embedded");
      manager.stop();
      tjws.stop();
      hornetqServer.stop();
   }
}
