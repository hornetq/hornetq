package org.hornetq.tests.integration.cluster.failover;
import org.junit.Before;

import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.journal.impl.JournalFile;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.persistence.impl.journal.DescribeJournal;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.tests.integration.cluster.util.BackupSyncDelay;
import org.hornetq.tests.integration.cluster.util.TestableServer;
import org.hornetq.tests.util.TransportConfigurationUtils;
import org.hornetq.utils.UUID;

public class BackupSyncJournalTest extends FailoverTestBase
{
   protected static final int BACKUP_WAIT_TIME = 20;
   private ServerLocatorInternal locator;
   protected ClientSessionFactoryInternal sessionFactory;
   protected ClientSession session;
   protected ClientProducer producer;
   private BackupSyncDelay syncDelay;
   private final int defaultNMsgs = 20;
   private int n_msgs = defaultNMsgs;

   protected void setNumberOfMessages(int nmsg)
   {
      this.n_msgs = nmsg;
   }

   protected int getNumberOfMessages()
   {
      return n_msgs;
   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      startBackupServer = false;
      super.setUp();
      setNumberOfMessages(defaultNMsgs);
      locator = getServerLocator();
      locator.setBlockOnNonDurableSend(true);
      locator.setBlockOnDurableSend(true);
      locator.setReconnectAttempts(-1);
      sessionFactory = createSessionFactoryAndWaitForTopology(locator, 1);
      syncDelay = new BackupSyncDelay(backupServer, liveServer);

   }

   @Test
   public void testNodeID() throws Exception
   {
      startBackupFinishSyncing();
      assertTrue("must be running", backupServer.isStarted());
      assertEquals("backup and live should have the same nodeID", liveServer.getServer().getNodeID(),
                   backupServer.getServer().getNodeID());
   }

   @Test
   public void testReserveFileIdValuesOnBackup() throws Exception
   {
      final int totalRounds = 50;
      createProducerSendSomeMessages();
      JournalImpl messageJournal = getMessageJournalFromServer(liveServer);
      for (int i = 0; i < totalRounds; i++)
      {
         messageJournal.forceMoveNextFile();
         sendMessages(session, producer, n_msgs);
      }
      backupServer.start();

      // Deliver messages with Backup in-sync
      waitForRemoteBackup(sessionFactory, BACKUP_WAIT_TIME, false, backupServer.getServer());

      final JournalImpl backupMsgJournal = getMessageJournalFromServer(backupServer);
      sendMessages(session, producer, n_msgs);

      // Deliver messages with Backup up-to-date
      syncDelay.deliverUpToDateMsg();
      waitForRemoteBackup(sessionFactory, BACKUP_WAIT_TIME, true, backupServer.getServer());
      // SEND more messages, now with the backup replicating
      sendMessages(session, producer, n_msgs);

      Set<Pair<Long, Integer>> liveIds = getFileIds(messageJournal);
      int size = messageJournal.getFileSize();
      PagingStore ps = liveServer.getServer().getPagingManager().getPageStore(ADDRESS);
      if (ps.getPageSizeBytes() == PAGE_SIZE)
      {
         assertTrue("isStarted", ps.isStarted());
         assertFalse("start paging should return false, because we expect paging to be running", ps.startPaging());
      }
      finishSyncAndFailover();

      assertEquals("file sizes must be the same", size, backupMsgJournal.getFileSize());
      Set<Pair<Long, Integer>> backupIds = getFileIds(backupMsgJournal);

      int total = 0;
      for (Pair<Long, Integer> pair : liveIds)
      {
         total += pair.getB();
      }
      int totalBackup = 0;
      for (Pair<Long, Integer> pair : backupIds)
      {
         totalBackup += pair.getB();
      }
      assertEquals("number of records must match ", total, totalBackup);

      // "+ 2": there two other calls that send N_MSGS.
      for (int i = 0; i < totalRounds + 3; i++)
      {
         receiveMsgsInRange(0, n_msgs);
      }
      assertNoMoreMessages();
   }

   protected void assertNoMoreMessages() throws HornetQException
   {
      session.start();
      ClientConsumer consumer = session.createConsumer(FailoverTestBase.ADDRESS);
      ClientMessage msg = consumer.receiveImmediate();
      assertNull("there should be no more messages to receive! " + msg, msg);
      consumer.close();
      session.commit();

   }

   protected void startBackupFinishSyncing() throws Exception
   {
      syncDelay.deliverUpToDateMsg();
      backupServer.start();
      waitForRemoteBackup(sessionFactory, BACKUP_WAIT_TIME, true, backupServer.getServer());
   }

   @Test
   public void testReplicationDuringSync() throws Exception
   {
      try
      {
         createProducerSendSomeMessages();
         backupServer.start();
         waitForRemoteBackup(sessionFactory, BACKUP_WAIT_TIME, false, backupServer.getServer());

         sendMessages(session, producer, n_msgs);
         session.commit();
         receiveMsgsInRange(0, n_msgs);

         finishSyncAndFailover();

         receiveMsgsInRange(0, n_msgs);
         assertNoMoreMessages();
      }
      catch (AssertionError error)
      {
         printJournal(liveServer);
         printJournal(backupServer);
         // test failed
         throw error;
      }
   }

   void printJournal(TestableServer server)
   {
      try
      {
         System.out.println("\n\n BINDINGS JOURNAL\n\n");
         Configuration config = server.getServer().getConfiguration();
         DescribeJournal.describeBindingsJournal(config.getBindingsDirectory());
         System.out.println("\n\n MESSAGES JOURNAL\n\n");
         DescribeJournal.describeMessagesJournal(config.getJournalDirectory());
      }
      catch (Exception ignored)
      {
         ignored.printStackTrace();
      }
   }

   protected void finishSyncAndFailover() throws Exception
   {
      syncDelay.deliverUpToDateMsg();
      waitForRemoteBackup(sessionFactory, BACKUP_WAIT_TIME, true, backupServer.getServer());
      assertFalse("should not be initialized", backupServer.getServer().isActive());

      crash(session);
      assertTrue("backup initialized", backupServer.getServer().waitForActivation(5, TimeUnit.SECONDS));

      assertNodeIdWasSaved();
   }

   /**
    * @throws FileNotFoundException
    * @throws IOException
    * @throws InterruptedException
    */
   private void assertNodeIdWasSaved() throws Exception
   {
      assertTrue("backup initialized", backupServer.getServer().waitForActivation(5, TimeUnit.SECONDS));

      // assert that nodeID was saved (to the right file!)

      String journalDirectory = backupConfig.getJournalDirectory();

      File serverLockFile = new File(journalDirectory, "server.lock");
      assertTrue("server.lock must exist!\n " + serverLockFile, serverLockFile.exists());
      RandomAccessFile raFile = new RandomAccessFile(serverLockFile, "r");
      try
      {
         // verify the nodeID was written correctly
         FileChannel channel = raFile.getChannel();
         final int size = 16;
         ByteBuffer id = ByteBuffer.allocateDirect(size);
         int read = channel.read(id, 3);
         assertEquals("tried to read " + size + " bytes", size, read);
         byte[] bytes = new byte[16];
         id.position(0);
         id.get(bytes);
         UUID uuid = new UUID(UUID.TYPE_TIME_BASED, bytes);
         SimpleString storedNodeId = new SimpleString(uuid.toString());
         assertEquals("nodeId must match", backupServer.getServer().getNodeID(), storedNodeId);
      }
      finally
      {
         raFile.close();
      }
   }

   @Test
   public void testMessageSyncSimple() throws Exception
   {
      createProducerSendSomeMessages();
      startBackupCrashLive();
      receiveMsgsInRange(0, n_msgs);
      assertNoMoreMessages();
   }

   /**
    * Basic fail-back test.
    * @throws Exception
    */
   @Test
   public void testFailBack() throws Exception
   {
      createProducerSendSomeMessages();
      startBackupCrashLive();
      receiveMsgsInRange(0, n_msgs);
      assertNoMoreMessages();

      sendMessages(session, producer, n_msgs);
      receiveMsgsInRange(0, n_msgs);
      assertNoMoreMessages();

      sendMessages(session, producer, 2 * n_msgs);
      assertFalse("must NOT be a backup", liveServer.getServer().getConfiguration().isBackup());
      adaptLiveConfigForReplicatedFailBack(liveServer.getServer().getConfiguration());
      liveServer.start();
      waitForServer(liveServer.getServer());
      assertTrue("must have become a backup", liveServer.getServer().getConfiguration().isBackup());

      assertTrue("Fail-back must initialize live!", liveServer.getServer().waitForActivation(15, TimeUnit.SECONDS));
      assertFalse("must be LIVE!", liveServer.getServer().getConfiguration().isBackup());
      int i = 0;
      while (backupServer.isStarted() && i++ < 100)
      {
         Thread.sleep(100);
      }
      assertFalse("Backup should stop!", backupServer.getServer().isStarted());
      assertTrue(liveServer.getServer().isStarted());
      receiveMsgsInRange(0, 2 * n_msgs);
      assertNoMoreMessages();
   }

   @Test
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
      backupServer.getServer().waitForActivation(5, TimeUnit.SECONDS);
   }

   protected void createProducerSendSomeMessages() throws HornetQException, Exception
   {
      session = addClientSession(sessionFactory.createSession(true, true));
      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);
      if (producer != null)
         producer.close();
      producer = addClientProducer(session.createProducer(FailoverTestBase.ADDRESS));
      sendMessages(session, producer, n_msgs);
      session.commit();
   }

   protected void receiveMsgsInRange(int start, int end) throws HornetQException
   {
      session.start();
      ClientConsumer consumer = addClientConsumer(session.createConsumer(FailoverTestBase.ADDRESS));
      receiveMessages(consumer, start, end, true);
      consumer.close();
      session.commit();
   }

   private Set<Pair<Long, Integer>> getFileIds(JournalImpl journal)
   {
      Set<Pair<Long, Integer>> results = new HashSet<Pair<Long, Integer>>();
      for (JournalFile jf : journal.getDataFiles())
      {
         results.add(getPair(jf));
      }
      results.add(getPair(journal.getCurrentFile()));
      return results;
   }

   /**
    * @param jf
    * @return
    */
   private Pair<Long, Integer> getPair(JournalFile jf)
   {
      return new Pair<Long, Integer>(jf.getFileID(), jf.getPosCount());
   }

   static JournalImpl getMessageJournalFromServer(TestableServer server)
   {
      JournalStorageManager sm = (JournalStorageManager)server.getServer().getStorageManager();
      return (JournalImpl)sm.getMessageJournal();
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
