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
package org.hornetq.tests.integration.cluster.failover;

import java.util.Map;

import org.hornetq.api.core.config.Configuration;
import org.hornetq.api.core.config.ConfigurationImpl;
import org.hornetq.api.core.config.TransportConfiguration;
import org.hornetq.api.core.server.HornetQ;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.JournalType;
import org.hornetq.tests.util.ServiceTestBase;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Oct 26, 2009
 */
public class GroupingFailoverSharedServerTest extends GroupingFailoverTestBase
{
   @Override
   protected void setupReplicatedServer(final int node,
                                        final boolean fileStorage,
                                        final boolean netty,
                                        final int backupNode)
   {
      if (servers[node] != null)
      {
         throw new IllegalArgumentException("Already a server at node " + node);
      }

      Configuration configuration = new ConfigurationImpl();

      configuration.setSecurityEnabled(false);
      configuration.setBindingsDirectory(getBindingsDir(backupNode, false));
      configuration.setJournalMinFiles(2);
      configuration.setJournalMaxIO_AIO(1000);
      configuration.setJournalDirectory(getJournalDir(backupNode, false));
      configuration.setJournalFileSize(100 * 1024);
      configuration.setJournalType(JournalType.ASYNCIO);
      configuration.setPagingDirectory(getPageDir(backupNode, false));
      configuration.setLargeMessagesDirectory(getLargeMessagesDir(backupNode, false));
      configuration.setClustered(true);
      configuration.setJournalCompactMinFiles(0);
      configuration.setBackup(true);
      configuration.setSharedStore(true);

      configuration.getAcceptorConfigurations().clear();

      Map<String, Object> params = generateParams(node, netty);

      TransportConfiguration invmtc = new TransportConfiguration(ServiceTestBase.INVM_ACCEPTOR_FACTORY, params);
      configuration.getAcceptorConfigurations().add(invmtc);

      if (netty)
      {
         TransportConfiguration nettytc = new TransportConfiguration(ServiceTestBase.NETTY_ACCEPTOR_FACTORY, params);
         configuration.getAcceptorConfigurations().add(nettytc);
      }

      HornetQServer server;

      if (fileStorage)
      {
         server = HornetQ.newHornetQServer(configuration);
      }
      else
      {
         server = HornetQ.newHornetQServer(configuration, false);
      }
      servers[node] = server;
   }

   @Override
   public void setupMasterServer(final int i, final boolean fileStorage, final boolean netty)
   {
      setupServer(i, fileStorage, netty);
   }

}
