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

package org.hornetq.tests.integration.cluster.failover;

import java.util.ArrayList;
import java.util.Map;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.ClusterConnectionConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.core.server.JournalType;
import org.hornetq.tests.integration.cluster.util.RemoteProcessHornetQServer;
import org.hornetq.tests.integration.cluster.util.RemoteServerConfiguration;
import org.hornetq.tests.integration.cluster.util.SameProcessHornetQServer;
import org.hornetq.tests.integration.cluster.util.TestableServer;

/**
 * A RemoteFailoverTest
 *
 * @author jmesnil
 *
 *
 */
public class RemoteFailoverTest extends FailoverTest
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   // Static --------------------------------------------------------

   public class SharedLiveServerConfiguration extends RemoteServerConfiguration
   {

      @Override
      public Configuration getConfiguration()
      {
         Configuration config = createDefaultConfig(generateParams(0, true), NettyAcceptorFactory.class.getName());
         config.setJournalType(JournalType.NIO);
         config.setSharedStore(true);
         config.setClustered(true);
         config.getConnectorConfigurations().put("self",
                                                 createTransportConfiguration(true, false, generateParams(0, true)));
         config.getClusterConfigurations().add(new ClusterConnectionConfiguration("cluster",
                                                                                  "foo",
                                                                                  "self",
                                                                                  -1,
                                                                                  false,
                                                                                  false,
                                                                                  1,
                                                                                  1,
                                                                                  new ArrayList<String>(), false));
         return config;
      }

   }

   @Override
   protected void tearDown() throws Exception
   {
      super.tearDown();
      //just to make sure
      if (liveServer != null)
      {
         try
         {
            liveServer.destroy();
         }
         catch (Exception e)
         {
            e.printStackTrace(); 
         }
      }
   }

   @Override
   protected TestableServer createLiveServer()
   {
      return new RemoteProcessHornetQServer(SharedLiveServerConfiguration.class.getName());
   }
   
   @Override
   protected TestableServer createBackupServer()
   {
      return new SameProcessHornetQServer(HornetQServers.newHornetQServer(backupConfig));
   }
   
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live) {
      Map<String, Object> params = null;
      if (live)
      {
         params = generateParams(0, true);
      } else
      {
         params = generateParams(1, true);
      }
      return createTransportConfiguration(true, false, params);
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(boolean live)
   {
      Map<String, Object> params = null;
      if (live)
      {
         params = generateParams(0, true);
      } else
      {
         params = generateParams(1, true);
      }
      return createTransportConfiguration(true, true, params);
   }

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------


   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
