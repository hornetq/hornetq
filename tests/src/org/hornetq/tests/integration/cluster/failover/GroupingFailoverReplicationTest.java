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

import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.TransportConfiguration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.HornetQ;

import java.util.Map;

/**
 * @author <a href="mailto:andy.taylor@jboss.org">Andy Taylor</a>
 *         Created Oct 26, 2009
 */
public class GroupingFailoverReplicationTest extends GroupingFailoverTestBase
{

   protected void setupReplicatedServer(int node, boolean fileStorage, boolean netty, int backupNode)
   {
      if (servers[node] != null)
      {
         throw new IllegalArgumentException("Already a server at node " + node);
      }

      Configuration configuration = new ConfigurationImpl();

      configuration.setSecurityEnabled(false);
      configuration.setBindingsDirectory(getBindingsDir(node, false));
      configuration.setJournalMinFiles(2);
      configuration.setJournalMaxAIO(1000);
      configuration.setJournalDirectory(getJournalDir(node, false));
      configuration.setJournalFileSize(100 * 1024);
      configuration.setJournalType(JournalType.ASYNCIO);
      configuration.setJournalMaxAIO(1000);
      configuration.setPagingDirectory(getPageDir(node, false));
      configuration.setLargeMessagesDirectory(getLargeMessagesDir(node, false));
      configuration.setClustered(true);
      configuration.setJournalCompactMinFiles(0);
      configuration.setBackup(true);
      configuration.setSharedStore(false);

      configuration.getAcceptorConfigurations().clear();

      Map<String, Object> params = generateParams(node, netty);

      TransportConfiguration invmtc = new TransportConfiguration(INVM_ACCEPTOR_FACTORY, params);
      configuration.getAcceptorConfigurations().add(invmtc);

      if (netty)
      {
         TransportConfiguration nettytc = new TransportConfiguration(NETTY_ACCEPTOR_FACTORY, params);
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

   void setupMasterServer(int i, boolean fileStorage, boolean netty)
   {
      setupServer(i, fileStorage, false, netty, false, 2);
   }
}
