package org.hornetq.tests.integration.cluster.failover;

import org.hornetq.api.core.client.ClientSession;
import org.hornetq.tests.integration.cluster.util.BackupSyncDelay;

public class ReplicatedWithDelayFailoverTest extends ReplicatedFailoverTest
{

   private BackupSyncDelay syncDelay;

   @Override
   protected void setUp() throws Exception
   {
      startBackupServer = false;
      super.setUp();
      syncDelay = new BackupSyncDelay(backupServer, liveServer);
      backupServer.start();
   }

   @Override
   protected void crash(ClientSession... sessions) throws Exception
   {
      syncDelay.deliverUpToDateMsg();
      super.crash(sessions);
   }
}
