package org.hornetq.tests.integration.cluster.failover;

import java.util.HashSet;
import java.util.Set;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.core.client.impl.ClientSessionFactoryInternal;
import org.hornetq.core.client.impl.ServerLocatorInternal;
import org.hornetq.core.journal.impl.JournalFile;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.tests.integration.cluster.util.BackupSyncDelay;
import org.hornetq.tests.integration.cluster.util.TestableServer;
import org.hornetq.tests.util.TransportConfigurationUtils;

public class BackupSyncJournalTest extends FailoverTestBase
{

   private ServerLocatorInternal locator;
   private ClientSessionFactoryInternal sessionFactory;
   private ClientSession session;
   private ClientProducer producer;
   private BackupSyncDelay syncDelay;
   private static final int N_MSGS = 10;

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
      backupServer.start();
      waitForComponent(backupServer, 5);
      assertTrue("must be running", backupServer.isStarted());
      assertEquals("backup and live should have the same nodeID", liveServer.getServer().getNodeID(),
                   backupServer.getServer().getNodeID());
   }

   public void testReserveFileIdValuesOnBackup() throws Exception
   {
      createProducerSendSomeMessages();
      JournalImpl messageJournal = getMessageJournalFromServer(liveServer);
      for (int i = 0; i < 5; i++)
      {
         messageJournal.forceMoveNextFile();
         sendMessages(session, producer, N_MSGS);
      }

      backupServer.start();

      waitForBackup(sessionFactory, 10, false);

      // SEND more messages, now with the backup replicating
      sendMessages(session, producer, N_MSGS);
      Set<Long> liveIds = getFileIds(messageJournal);

      finishSyncAndFailover();

      JournalImpl backupMsgJournal = getMessageJournalFromServer(backupServer);
      Set<Long> backupIds = getFileIds(backupMsgJournal);
      assertEquals("File IDs must match!", liveIds, backupIds);
   }

   public void testReplicationDuringSync() throws Exception
   {
      createProducerSendSomeMessages();
      backupServer.start();
      waitForBackup(sessionFactory, 10, false);

      sendMessages(session, producer, N_MSGS);
      session.commit();
      receiveMsgs(0, N_MSGS);
      finishSyncAndFailover();
   }

   private void finishSyncAndFailover() throws Exception
   {
      syncDelay.deliverUpToDateMsg();
      waitForBackup(sessionFactory, 10, true);
      assertFalse("should not be initialized", backupServer.getServer().isInitialised());
      crash(session);
      waitForServerInitialization(backupServer, 5);
   }

   public void testMessageSyncSimple() throws Exception
   {
      createProducerSendSomeMessages();
      startBackupCrashLive();
      receiveMsgs(0, N_MSGS);
   }

   public void testMessageSync() throws Exception
   {
      createProducerSendSomeMessages();
      receiveMsgs(0, N_MSGS / 2);
      startBackupCrashLive();
      receiveMsgs(N_MSGS / 2, N_MSGS);
   }

   private void startBackupCrashLive() throws Exception
   {
      assertFalse("backup is started?", backupServer.isStarted());
      liveServer.removeInterceptor(syncDelay);
      backupServer.start();
      waitForBackup(sessionFactory, 20);
      crash(session);
      waitForServerInitialization(backupServer, 5);
   }

   private void createProducerSendSomeMessages() throws HornetQException, Exception
   {
      session = sessionFactory.createSession(true, true);
      session.createQueue(FailoverTestBase.ADDRESS, FailoverTestBase.ADDRESS, null, true);
      producer = session.createProducer(FailoverTestBase.ADDRESS);
      sendMessages(session, producer, N_MSGS);
      session.commit();
   }

   private void receiveMsgs(int start, int end) throws HornetQException
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
