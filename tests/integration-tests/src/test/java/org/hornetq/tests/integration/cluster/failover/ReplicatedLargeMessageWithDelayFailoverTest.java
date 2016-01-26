package org.hornetq.tests.integration.cluster.failover;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.tests.integration.cluster.util.BackupSyncDelay;
import org.junit.After;
import org.junit.Before;

/**
 * See {@link BackupSyncDelay} for the rationale about these 'WithDelay' tests.
 */
public class ReplicatedLargeMessageWithDelayFailoverTest extends ReplicatedLargeMessageFailoverTest
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
   }

   @Override
   protected void crash(ClientSession... sessions) throws Exception
   {
      crash(true, sessions);
   }

   @Override
   protected void crash(boolean waitFailure, ClientSession... sessions) throws Exception
   {
      syncDelay.deliverUpToDateMsg();
      waitForBackup(null, 30);
      super.crash(waitFailure, sessions);
   }

   @Override
   @After
   public void tearDown() throws Exception
   {
      syncDelay.deliverUpToDateMsg();
      super.tearDown();
   }
}
