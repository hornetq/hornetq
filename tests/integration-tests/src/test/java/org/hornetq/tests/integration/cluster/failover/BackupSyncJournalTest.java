package org.hornetq.tests.integration.cluster.failover;

import java.util.HashSet;
import java.util.Set;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.journal.impl.JournalFile;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.tests.integration.cluster.util.BackupSyncDelay;
import org.hornetq.tests.integration.cluster.util.TestableServer;
import org.hornetq.tests.util.TransportConfigurationUtils;

public class BackupSyncJournalTest extends FailoverTestBase
{

   private static final int BACKUP_WAIT_TIME = 20;
   private ServerLocatorInternal locator;
   private ClientSessionFactoryInternal sessionFactory;
   private ClientSession session;
   private ClientProducer producer;
   private BackupSyncDelay syncDelay;
   protected int n_msgs = 20;

   @Override
   protected void setUp() throws Exception
   {
      startBackupServer = false;
      super.setUp();
      locator = getServerLocator();
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setReconnectAttempts(-1);
      sessionFactory = createSessionFactoryAndWaitForTopology(locator, 1);
      syncDelay = new BackupSyncDelay(backupServer, liveServer);
   }

   public void testNodeID() throws Exception
   {
      startBackupFinishSyncing();
      assertTrue("must be running", backupServer.isStarted());
      assertEquals("backup and live should have the same nodeID", liveServer.getServer().getNodeID(),
                   backupServer.getServer().getNodeID());
   }

   public void testReserveFileIdValuesOnBackup() throws Exception
   {
      final int totalRounds = 5;
      createProducerSendSomeMessages();
      JournalImpl messageJournal = getMessageJournalFromServer(liveServer);
      for (int i = 0; i < totalRounds; i++)
      {
         messageJournal.forceMoveNextFile();
         sendMessages(session, producer, n_msgs);
      }

      backupServer.start();

      // Deliver messages with Backup in-sync
      waitForBackup(sessionFactory, BACKUP_WAIT_TIME, false);
      sendMessages(session, producer, n_msgs);

      // Deliver messages with Backup up-to-date
      syncDelay.deliverUpToDateMsg();
      waitForBackup(sessionFactory, BACKUP_WAIT_TIME, true);
      // SEND more messages, now with the backup replicating
      sendMessages(session, producer, n_msgs);

      Set<Long> liveIds = getFileIds(messageJournal);
      int size = messageJournal.getFileSize();
      PagingStore ps = liveServer.getServer().getPagingManager().getPageStore(ADDRESS);
      if (ps.getPageSizeBytes() == PAGE_SIZE)
      {
         assertTrue("isStarted", ps.isStarted());
         assertFalse("start paging should return false, because we expect paging to be running", ps.startPaging());
      }
      finishSyncAndFailover();

      JournalImpl backupMsgJournal = getMessageJournalFromServer(backupServer);
      System.out.println("backup journal " + backupMsgJournal);
      System.out.println("live journal " + messageJournal);
      assertEquals("file sizes must be the same", size, backupMsgJournal.getFileSize());
      Set<Long> backupIds = getFileIds(backupMsgJournal);
      assertEquals("File IDs must match!", liveIds, backupIds);

      // "+ 2": there two other calls that send N_MSGS.
      for (int i = 0; i < totalRounds + 3; i++)
      {
         receiveMsgsInRange(0, n_msgs);
      }
      assertNoMoreMessages();
   }

   private void assertNoMoreMessages() throws HornetQException
   {
      session.start();
      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);
      ClientMessage msg = consumer.receive(200);
      assertNull("there should be no more messages to receive! " + msg, msg);
      consumer.close();
      session.commit();

   }

   protected void startBackupFinishSyncing() throws Exception
   {
      syncDelay.deliverUpToDateMsg();
      backupServer.start();
      waitForBackup(sessionFactory, BACKUP_WAIT_TIME, true);
   }

   public void testReplicationDuringSync() throws Exception
   {
      createProducerSendSomeMessages();
      backupServer.start();
      waitForBackup(sessionFactory, BACKUP_WAIT_TIME, false);

      sendMessages(session, producer, n_msgs);
      session.commit();
      receiveMsgsInRange(0, n_msgs);

      finishSyncAndFailover();

      receiveMsgsInRange(0, n_msgs);
      assertNoMoreMessages();
   }

   private void finishSyncAndFailover() throws Exception
   {
      syncDelay.deliverUpToDateMsg();
      waitForBackup(sessionFactory, BACKUP_WAIT_TIME, true);
      assertFalse("should not be initialized", backupServer.getServer().isInitialised());
      crash(session);
      waitForServerInitialization(backupServer, 5);
   }

   public void testMessageSyncSimple() throws Exception
   {
      createProducerSendSomeMessages();
      startBackupCrashLive();
      receiveMsgsInRange(0, n_msgs);
      assertNoMoreMessages();
   }

   public void testMessageSync() throws Exception
   {
      createProducerSendSomeMessages();
      receiveMsgsInRange(0, n_msgs / 2);
      startBackupCrashLive();
      receiveMsgsInRange(n_msgs / 2, n_msgs);
      assertNoMoreMessages();
   }

   private void startBackupCrashLive() throws Exception
   {
      assertFalse("backup is started?", backupServer.isStarted());
      liveServer.removeInterceptor(syncDelay);
      backupServer.start();
      waitForBackup(sessionFactory, BACKUP_WAIT_TIME);
      crash(session);
      waitForServerInitialization(backupServer, 5);
   }

   protected void createProducerSendSomeMessages() throws HornetQException, Exception
   {
      session = sessionFactory.createSession(true, true);
      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);
      producer = session.createProducer(FailoverTestBase.ADDRESS);
      sendMessages(session, producer, n_msgs);
      session.commit();
   }

   protected void receiveMsgsInRange(int start, int end) throws HornetQException
   {
      session.start();
      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);
      receiveMessagesAndAck(consumer, start, end);
      consumer.close();
      session.commit();
   }

   private static void waitForServerInitialization(TestableServer server, int seconds)
   {
      long time = System.currentTimeMillis();
      long toWait = seconds * 1000;
      while (!server.isInitialised())
      {
         try
         {
            Thread.sleep(50);
         }
         catch (InterruptedException e)
         {
            // ignore
         }
         if (System.currentTimeMillis() > (time + toWait))
         {
            fail("component did not start within timeout of " + seconds);
         }
      }
   }

   private Set<Long> getFileIds(JournalImpl journal)
   {
      Set<Long> results = new HashSet<Long>();
      for (JournalFile jf : journal.getDataFiles())
      {
         results.add(Long.valueOf(jf.getFileID()));
      }
      return results;
   }

   static JournalImpl getMessageJournalFromServer(TestableServer server)
   {
      JournalStorageManager sm = (JournalStorageManager)server.getServer().getStorageManager();
      return (JournalImpl)sm.getMessageJournal();
   }

   @Override
   protected void tearDown() throws Exception
   {
      if (sessionFactory != null)
         sessionFactory.close();
      if (session != null)
         session.close();
      closeServerLocator(locator);

      super.tearDown();
   }

   @Override
   protected void createConfigs() throws Exception
   {
      createReplicatedConfigs();
   }

   @Override
   protected TransportConfiguration getAcceptorTransportConfiguration(boolean live)
   {
      return TransportConfigurationUtils.getInVMAcceptor(live);
   }

   @Override
   protected TransportConfiguration getConnectorTransportConfiguration(boolean live)
   {
      return TransportConfigurationUtils.getInVMConnector(live);
   }


}
