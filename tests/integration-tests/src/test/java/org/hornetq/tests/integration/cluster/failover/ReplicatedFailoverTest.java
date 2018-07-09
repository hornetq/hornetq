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

import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ClientSessionInternal;
import org.junit.Assert;
import org.junit.Test;

public class ReplicatedFailoverTest extends FailoverTest
{

   @Test
   /*
   * default maxSavedReplicatedJournalsSize is 2, this means the backup will fall back to replicated only twice, after this
   * it is stopped permanently
   *
   * */
   public void testReplicatedFailback() throws Exception
   {
      try
      {
         backupServer.getServer().getConfiguration().setFailbackDelay(2000);
         backupServer.getServer().getConfiguration().setMaxSavedReplicatedJournalSize(2);
         createSessionFactory();

         ClientSession session = createSession(sf, true, true);

         session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);

         crash(session);

         liveServer.getServer().getConfiguration().setCheckForLiveServer(true);

         liveServer.start();

         waitForRemoteBackupSynchronization(liveServer.getServer());

         waitForRemoteBackupSynchronization(backupServer.getServer());

         waitForServer(liveServer.getServer());

         session = createSession(sf, true, true);

         crash(session);

         liveServer.getServer().getConfiguration().setCheckForLiveServer(true);

         liveServer.start();

         waitForRemoteBackupSynchronization(liveServer.getServer());

         waitForRemoteBackupSynchronization(backupServer.getServer());

         waitForServer(liveServer.getServer());

         session = createSession(sf, true, true);

         crash(session);

         liveServer.getServer().getConfiguration().setCheckForLiveServer(true);

         liveServer.start();

         waitForRemoteBackupSynchronization(liveServer.getServer());

         waitForServer(liveServer.getServer());

         //this will give the backup time to stop fully
         waitForServerToStop(backupServer.getServer());

         assertFalse(backupServer.getServer().isStarted());

         //the server wouldnt have reset to backup
         assertFalse(backupServer.getServer().getConfiguration().isBackup());
      }
      finally
      {
         sf.close();
      }
   }
   @Override
   protected void createConfigs() throws Exception
   {
      createReplicatedConfigs();
   }

   @Override
   protected void crash(boolean waitFailure, ClientSession... sessions) throws Exception
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
      super.crash(waitFailure, sessions);
   }

   @Override
   protected void crash(ClientSession... sessions) throws Exception
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
      super.crash(sessions);
   }

   @Override
   @Test(timeout = 10000)
   public void testFailLiveTooSoon() throws Exception
   {
      backupServer.getServer().getConfiguration().setFailbackDelay(2000);
      backupServer.getServer().getConfiguration().setMaxSavedReplicatedJournalSize(2);
      ServerLocator locator = getServerLocator();

      locator.setReconnectAttempts(-1);
      locator.setRetryInterval(10);

      sf = (ClientSessionFactoryInternal)locator.createSessionFactory();

      waitForBackupConfig(sf);

      TransportConfiguration initialLive = getFieldFromSF(sf, "currentConnectorConfig");
      TransportConfiguration initialBackup = getFieldFromSF(sf, "backupConfig");

      System.out.println("initlive: " + initialLive);
      System.out.println("initback: " + initialBackup);

      TransportConfiguration last = getFieldFromSF(sf, "connectorConfig");
      TransportConfiguration current = getFieldFromSF(sf, "currentConnectorConfig");

      System.out.println("now last: " + last);
      System.out.println("now current: " + current);
      assertTrue(current.equals(initialLive));

      ClientSession session = createSession(sf, true, true);

      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, true);

      //crash 1
      crash();

      //make sure failover is ok
      createSession(sf, true, true).close();

      last = getFieldFromSF(sf, "connectorConfig");
      current = getFieldFromSF(sf, "currentConnectorConfig");

      System.out.println("now after live crashed last: " + last);
      System.out.println("now current: " + current);

      assertTrue(current.equals(initialBackup));

      //fail back
      //beforeRestart(liveServer);
      liveServer.getServer().getConfiguration().setCheckForLiveServer(true);
      liveServer.getServer().start();

      waitForRemoteBackupSynchronization(liveServer.getServer());

      waitForRemoteBackupSynchronization(backupServer.getServer());

      waitForServer(liveServer.getServer());

      //make sure failover is ok
      createSession(sf, true, true).close();

      last = getFieldFromSF(sf, "connectorConfig");
      current = getFieldFromSF(sf, "currentConnectorConfig");

      System.out.println("now after live back again last: " + last);
      System.out.println("now current: " + current);

      assertTrue(current.equals(initialLive));

      //now manually corrupt the backup in sf
      setSFFieldValue(sf, "backupConfig", null);

      //crash 2
      crash();

      createSession(sf, true, true).close();

      sf.close();
      Assert.assertEquals(0, sf.numSessions());
      Assert.assertEquals(0, sf.numConnections());
   }

}
