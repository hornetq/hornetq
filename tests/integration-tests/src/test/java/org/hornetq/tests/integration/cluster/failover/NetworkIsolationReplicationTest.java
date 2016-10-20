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

import java.net.InetAddress;

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.hornetq.tests.util.TransportConfigurationUtils;
import org.junit.Assert;
import org.junit.Test;

public class NetworkIsolationReplicationTest extends FailoverTestBase
{
   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(final boolean live)
   {
      return TransportConfigurationUtils.getInVMAcceptor(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(final boolean live)
   {
      return TransportConfigurationUtils.getInVMConnector(live);
   }

   protected ClientSession createSession(ClientSessionFactory sf1,
                                         boolean xa,
                                         boolean autoCommitSends,
                                         boolean autoCommitAcks) throws Exception
   {
      return addClientSession(sf1.createSession(xa, autoCommitSends, autoCommitAcks));
   }

   @Test
   /*
   * default maxSavedReplicatedJournalsSize is 2, this means the backup will fall back to replicated only twice, after this
   * it is stopped permanently
   *
   * */
   public void testDoNotActivateOnIsolation() throws Exception
   {
      ServerLocator locator = getServerLocator();
      backupServer.getServer().getConfiguration().setFailbackDelay(2000);
      backupServer.getServer().getConfiguration().setMaxSavedReplicatedJournalSize(2);

      backupServer.getServer().getNetworkHealthCheck().addAddress(InetAddress.getByName("203.0.113.1"));

      ClientSessionFactory sf = addSessionFactory(locator.createSessionFactory());

      ClientSession session = createSession(sf, false, true, true);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

      Assert.assertFalse(backupServer.getServer().getNetworkHealthCheck().check());

      crash(false, true, session);

      for (int i = 0; i < 1000 && !backupServer.isStarted(); i++)
      {
         Thread.sleep(10);
      }

      Assert.assertTrue(backupServer.isStarted());
      Assert.assertFalse(backupServer.isActive());

      liveServer.start();

      for (int i = 0; i < 1000 && backupServer.getServer().getReplicationEndpoint() != null && !backupServer.getServer().getReplicationEndpoint().isStarted(); i++)
      {
         Thread.sleep(10);
      }


      backupServer.getServer().getNetworkHealthCheck().clearAddresses();

      // This will make sure the backup got synchronized after the network was activated again
      Assert.assertTrue(backupServer.getServer().getReplicationEndpoint().isStarted());


   }
   @Override
   protected void createConfigs() throws Exception
   {
      createReplicatedConfigs();
   }

   @Override
   protected void crash(boolean failover, boolean waitFailure, ClientSession... sessions) throws Exception
   {
      if (sessions.length > 0)
      {
         for (ClientSession session : sessions)
         {
            waitForRemoteBackup(((ClientSessionInternal)session).getSessionFactory(), 5, true, backupServer.getServer());
         }
      }
      else
      {
         waitForRemoteBackup(null, 5, true, backupServer.getServer());
      }
      super.crash(failover, waitFailure, sessions);
   }
}
