/*
 * Copyright 2009 Red Hat, Inc.
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

package org.hornetq.tests.integration.cluster.bridge;

import java.util.ArrayList;
import java.util.Map;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.remoting.impl.invm.InVMConnector;
import org.hornetq.core.remoting.impl.netty.NettyAcceptorFactory;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQServers;
import org.hornetq.tests.util.UnitTestCase;

/**
 * A BridgeTestBase
 *
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * Created 21 Nov 2008 10:32:23
 *
 *
 */
public abstract class BridgeTestBase extends UnitTestCase
{

   private ArrayList<HornetQServer> servers;

   @Override
   public void setUp() throws Exception
   {
      super.setUp();
      servers = new ArrayList<HornetQServer>();
   }

   @Override
   public void tearDown() throws Exception
   {
      for (HornetQServer server : servers)
      {
         try
         {
            if (server.isStarted())
            {
               server.stop();
            }
         }
         catch (Throwable e)
         {
            // System.out -> junit report
            System.out.println("Error while stopping server:");
            e.printStackTrace(System.out);
         }
      }

      servers = null;

      InVMConnector.failOnCreateConnection = false;

      super.tearDown();
   }

   protected HornetQServer createHornetQServer(final int id, final boolean netty, final Map<String, Object> params)
   {
      return createHornetQServer(id, params, netty, false);
   }

   protected HornetQServer createHornetQServer(final int id,
                                               final Map<String, Object> params,
                                               final boolean netty,
                                               final boolean backup)
   {
      Configuration serviceConf = new ConfigurationImpl();
      serviceConf.setClustered(true);
      serviceConf.setSecurityEnabled(false);
      serviceConf.setBackup(backup);
      serviceConf.setSharedStore(true);
      serviceConf.setBindingsDirectory(getBindingsDir(id, false));
      serviceConf.setJournalMinFiles(2);
      serviceConf.setJournalDirectory(getJournalDir(id, false));
      serviceConf.setPagingDirectory(getPageDir(id, false));
      serviceConf.setLargeMessagesDirectory(getLargeMessagesDir(id, false));
      // these tests don't need any big storage so limiting the size of the journal files to speed up the test
      serviceConf.setJournalFileSize(100 * 1024);

      if (netty)
      {
         params.put(org.hornetq.core.remoting.impl.netty.TransportConstants.PORT_PROP_NAME,
                    org.hornetq.core.remoting.impl.netty.TransportConstants.DEFAULT_PORT + id);
         serviceConf.getAcceptorConfigurations().add(new TransportConfiguration(NettyAcceptorFactory.class.getName(),
                                                                                params));

      }
      else
      {
         params.put(org.hornetq.core.remoting.impl.invm.TransportConstants.SERVER_ID_PROP_NAME, id);
         serviceConf.getAcceptorConfigurations()
                    .add(new TransportConfiguration("org.hornetq.core.remoting.impl.invm.InVMAcceptorFactory", params));
      }
      HornetQServer service = HornetQServers.newHornetQServer(serviceConf, true);

      servers.add(service);

      return service;
   }

}
