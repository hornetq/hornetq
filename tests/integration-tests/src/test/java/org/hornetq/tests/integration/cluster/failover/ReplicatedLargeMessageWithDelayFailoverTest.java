package org.hornetq.tests.integration.cluster.failover;
import org.junit.Before;
import org.junit.After;

import org.hornetq.api.core.client.ClientSession;
import org.hornetq.tests.integration.cluster.util.BackupSyncDelay;

/**
 * See {@link BackupSyncDelay} for the rationale about these 'WithDelay' tests.
 */
public class ReplicatedLargeMessageWithDelayFailoverTest extends ReplicatedLargeMessageFailoverTest
{
   private BackupSyncDelay syncDelay;

   public ReplicatedLargeMessageWithDelayFailoverTest(String name)
   {
      super(name);
   }

   public ReplicatedLargeMessageWithDelayFailoverTest()
   {
      super();
   }


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
      waitForBackup(null, 5);
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
