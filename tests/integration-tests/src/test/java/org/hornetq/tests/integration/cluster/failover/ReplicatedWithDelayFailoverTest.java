package org.hornetq.tests.integration.cluster.failover;
import org.junit.Before;

import org.hornetq.api.core.client.ClientSession;
import org.hornetq.tests.integration.cluster.util.BackupSyncDelay;

/**
 * See {@link BackupSyncDelay} for the rationale about these 'WithDelay' tests.
 */
public class ReplicatedWithDelayFailoverTest extends ReplicatedFailoverTest
{

   private BackupSyncDelay syncDelay;

   @Override
   @Before
   public void setUp() throws Exception
   {
      startBackupServer = false;
      super.setUp();
      syncDelay = new BackupSyncDelay(backupServer, liveServer);
      backupServer.start();
      waitForServer(backupServer.getServer());
   }

   @Override
   protected void crash(ClientSession... sessions) throws Exception
   {
      syncDelay.deliverUpToDateMsg();
      super.crash(sessions);
   }

   @Override
   protected void crash(boolean waitFailure, ClientSession... sessions) throws Exception
   {
      syncDelay.deliverUpToDateMsg();
      waitForBackup(null, 5);
      super.crash(waitFailure, sessions);
   }
}
