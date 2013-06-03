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

package org.hornetq.tests.integration.replication;
import org.junit.Before;
import org.junit.After;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQExceptionType;
import org.hornetq.api.core.HornetQNotConnectedException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientConsumer;
import org.hornetq.api.core.client.ClientMessage;
import org.hornetq.api.core.client.ClientProducer;
import org.hornetq.api.core.client.ClientSession;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.ServerLocator;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.journal.IOCompletion;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.journal.LoaderCallback;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.TransactionFailureCallback;
import org.hornetq.core.journal.impl.JournalFile;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.impl.PagedMessageImpl;
import org.hornetq.core.paging.impl.PagingManagerImpl;
import org.hornetq.core.paging.impl.PagingStoreFactoryNIO;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.persistence.impl.journal.OperationContextImpl;
import org.hornetq.core.protocol.core.Packet;
import org.hornetq.core.replication.ReplicatedJournal;
import org.hornetq.core.replication.ReplicationManager;
import org.hornetq.core.server.HornetQComponent;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.util.ReplicatedBackupUtils;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.tests.util.TransportConfigurationUtils;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.HornetQThreadFactory;
import org.hornetq.utils.OrderedExecutorFactory;

/**
 * A ReplicationTest
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 */
public final class ReplicationTest extends ServiceTestBase
{

   private ThreadFactory tFactory;
   private ExecutorService executor;
   private ExecutorFactory factory;
   private ScheduledExecutorService scheduledExecutor;

   private HornetQServer backupServer;
   /** This field is not always used. */
   private HornetQServer liveServer;

   private ServerLocator locator;

   private ReplicationManager manager;
   private static final SimpleString ADDRESS = new SimpleString("foobar123");


   private void setupServer(boolean backup, String... interceptors) throws Exception
   {

      final TransportConfiguration liveConnector = TransportConfigurationUtils.getInVMConnector(true);
      final TransportConfiguration backupConnector = TransportConfigurationUtils.getInVMConnector(false);
      final TransportConfiguration backupAcceptor = TransportConfigurationUtils.getInVMAcceptor(false);

      Configuration backupConfig = createDefaultConfig();
      Configuration liveConfig = createDefaultConfig();

      backupConfig.setBackup(backup);

      final String suffix = "_backup";
      backupConfig.setBindingsDirectory(backupConfig.getBindingsDirectory() + suffix);
      backupConfig.setJournalDirectory(backupConfig.getJournalDirectory() + suffix);
      backupConfig.setPagingDirectory(backupConfig.getPagingDirectory() + suffix);
      backupConfig.setLargeMessagesDirectory(backupConfig.getLargeMessagesDirectory() + suffix);

      if (interceptors.length > 0)
      {
         List<String> interceptorsList = Arrays.asList(interceptors);
         backupConfig.setIncomingInterceptorClassNames(interceptorsList);
      }

      ReplicatedBackupUtils.configureReplicationPair(backupConfig, backupConnector, backupAcceptor, liveConfig,
                                                     liveConnector);
      if (backup)
      {
         liveServer = createServer(liveConfig);
         liveServer.start();
         waitForComponent(liveServer);
      }

      backupServer = createServer(backupConfig);
      locator = createInVMNonHALocator();
      backupServer.start();
      if (backup)
      {
         ServiceTestBase.waitForRemoteBackup(null, 5, true, backupServer);
      }
      int count = 0;
      waitForReplication(count);
   }

   private void waitForReplication(int count) throws InterruptedException
   {
      if (liveServer == null)
         return;

      while (liveServer.getReplicationManager() == null && count < 10)
      {
         Thread.sleep(50);
         count++;
      }
   }

   private static void waitForComponent(HornetQComponent component) throws Exception
   {
      waitForComponent(component, 3);
   }

   @Test
   public void testBasicConnection() throws Exception
   {
      setupServer(true);
      waitForComponent(liveServer.getReplicationManager());
   }

   @Test
   public void testConnectIntoNonBackup() throws Exception
   {
      setupServer(false);
      try
      {
         ClientSessionFactory sf = createSessionFactory(locator);
         manager = new ReplicationManager(sf.getConnection(), factory);
         addHornetQComponent(manager);
         manager.start();
         Assert.fail("Exception was expected");
      }
      catch(HornetQNotConnectedException nce)
      {
         // ok
      }
      catch (HornetQException expected)
      {
         fail("Invalid Exception type:" + expected.getType());
      }
   }

   @Test
   public void testSendPackets() throws Exception
   {
      setupServer(true);

      StorageManager storage = getStorage();

      manager = liveServer.getReplicationManager();
      waitForComponent(manager);

      Journal replicatedJournal = new ReplicatedJournal((byte)1, new FakeJournal(), manager);

      replicatedJournal.appendPrepareRecord(1, new FakeData(), false);

      replicatedJournal.appendAddRecord(1, (byte)1, new FakeData(), false);
      replicatedJournal.appendUpdateRecord(1, (byte)2, new FakeData(), false);
      replicatedJournal.appendDeleteRecord(1, false);
      replicatedJournal.appendAddRecordTransactional(2, 2, (byte)1, new FakeData());
      replicatedJournal.appendUpdateRecordTransactional(2, 2, (byte)2, new FakeData());
      replicatedJournal.appendCommitRecord(2, false);

      replicatedJournal.appendDeleteRecordTransactional(3, 4, new FakeData());
      replicatedJournal.appendPrepareRecord(3, new FakeData(), false);
      replicatedJournal.appendRollbackRecord(3, false);

      blockOnReplication(storage, manager);

      Assert.assertTrue("Expecting no active tokens:" + manager.getActiveTokens(), manager.getActiveTokens().isEmpty());

      ServerMessage msg = new ServerMessageImpl(1, 1024);

      SimpleString dummy = new SimpleString("dummy");
      msg.setAddress(dummy);

      replicatedJournal.appendAddRecordTransactional(23, 24, (byte)1, new FakeData());

      PagedMessage pgmsg = new PagedMessageImpl(msg, new long[0]);
      manager.pageWrite(pgmsg, 1);
      manager.pageWrite(pgmsg, 2);
      manager.pageWrite(pgmsg, 3);
      manager.pageWrite(pgmsg, 4);

      blockOnReplication(storage, manager);

      PagingManager pagingManager =
               createPageManager(backupServer.getStorageManager(), backupServer.getConfiguration(),
                                 backupServer.getExecutorFactory(), backupServer.getAddressSettingsRepository());

      PagingStore store = pagingManager.getPageStore(dummy);
      store.start();
      Assert.assertEquals(4, store.getNumberOfPages());
      store.stop();

      manager.pageDeleted(dummy, 1);
      manager.pageDeleted(dummy, 2);
      manager.pageDeleted(dummy, 3);
      manager.pageDeleted(dummy, 4);
      manager.pageDeleted(dummy, 5);
      manager.pageDeleted(dummy, 6);

      blockOnReplication(storage, manager);

      ServerMessageImpl serverMsg = new ServerMessageImpl();
      serverMsg.setMessageID(500);
      serverMsg.setAddress(new SimpleString("tttt"));

      HornetQBuffer buffer = HornetQBuffers.dynamicBuffer(100);
      serverMsg.encodeHeadersAndProperties(buffer);

      manager.largeMessageBegin(500);

      manager.largeMessageWrite(500, new byte[1024]);

      manager.largeMessageDelete(Long.valueOf(500));

      blockOnReplication(storage, manager);

      store.start();

      Assert.assertEquals(0, store.getNumberOfPages());
   }

   @Test
   public void testSendPacketsWithFailure() throws Exception
   {
      final int nMsg = 100;
      final int stop = 37;
      setupServer(true, TestInterceptor.class.getName());

      manager = liveServer.getReplicationManager();
      waitForComponent(manager);
      ClientSessionFactory sf = createSessionFactory(locator);
      final ClientSession session = sf.createSession();
      final ClientSession session2 = sf.createSession();
      session.createQueue(ADDRESS, ADDRESS, null, true);

      final ClientProducer producer = session.createProducer(ADDRESS);

      session.start();
      session2.start();
      try
      {
         final ClientConsumer consumer = session2.createConsumer(ADDRESS);
         for (int i = 0; i < nMsg; i++)
         {

            ClientMessage message = session.createMessage(true);
            setBody(i, message);
            message.putIntProperty("counter", i);
            producer.send(message);
            if (i == stop)
            {
               // Now we start intercepting the communication with the backup
               TestInterceptor.value.set(false);
            }
            ClientMessage msgRcvd = consumer.receive(1000);
            Assert.assertNotNull("Message should exist!", msgRcvd);
            assertMessageBody(i, msgRcvd);
            Assert.assertEquals(i, msgRcvd.getIntProperty("counter").intValue());
            msgRcvd.acknowledge();
         }
      }
      finally
      {
         TestInterceptor.value.set(false);
         if (!session.isClosed())
            session.close();
         if (!session2.isClosed())
            session2.close();
      }
   }

   @Test
   public void testExceptionSettingActionBefore() throws Exception
   {
      OperationContext ctx = OperationContextImpl.getContext(factory);

      ctx.storeLineUp();

      String msg = "I'm an exception";

      ctx.onError(HornetQExceptionType.UNBLOCKED.getCode(), msg);

      final AtomicInteger lastError = new AtomicInteger(0);

      final List<String> msgsResult = new ArrayList<String>();

      final CountDownLatch latch = new CountDownLatch(1);

      ctx.executeOnCompletion(new IOAsyncTask()
      {
         public void onError(final int errorCode, final String errorMessage)
         {
            lastError.set(errorCode);
            msgsResult.add(errorMessage);
            latch.countDown();
         }

         public void done()
         {
         }
      });

      Assert.assertTrue(latch.await(5, TimeUnit.SECONDS));

      Assert.assertEquals(5, lastError.get());

      Assert.assertEquals(1, msgsResult.size());

      Assert.assertEquals(msg, msgsResult.get(0));

      final CountDownLatch latch2 = new CountDownLatch(1);

      // Adding the Task after the exception should still throw an exception
      ctx.executeOnCompletion(new IOAsyncTask()
      {
         public void onError(final int errorCode, final String errorMessage)
         {
            lastError.set(errorCode);
            msgsResult.add(errorMessage);
            latch2.countDown();
         }

         public void done()
         {
         }
      });

      Assert.assertTrue(latch2.await(5, TimeUnit.SECONDS));

      Assert.assertEquals(2, msgsResult.size());

      Assert.assertEquals(msg, msgsResult.get(0));

      Assert.assertEquals(msg, msgsResult.get(1));

      final CountDownLatch latch3 = new CountDownLatch(1);

      ctx.executeOnCompletion(new IOAsyncTask()
      {
         public void onError(final int errorCode, final String errorMessage)
         {
         }

         public void done()
         {
            latch3.countDown();
         }
      });

      Assert.assertTrue(latch2.await(5, TimeUnit.SECONDS));

   }

   /**
    * @return
    * @throws Exception
    */
   private JournalStorageManager getStorage() throws Exception
   {
      return new JournalStorageManager(createDefaultConfig(), factory, null);
   }

   /**
    * @param manager1
    */
   private void blockOnReplication(final StorageManager storage, final ReplicationManager manager1) throws Exception
   {
      final CountDownLatch latch = new CountDownLatch(1);
      storage.afterCompleteOperations(new IOAsyncTask()
      {

         public void onError(final int errorCode, final String errorMessage)
         {
         }

         public void done()
         {
            latch.countDown();
         }
      });

      Assert.assertTrue(latch.await(30, TimeUnit.SECONDS));
   }

   @Test
   public void testNoActions() throws Exception
   {

      setupServer(true);
      StorageManager storage = getStorage();
      manager = liveServer.getReplicationManager();
      waitForComponent(manager);

      Journal replicatedJournal = new ReplicatedJournal((byte)1, new FakeJournal(), manager);

      replicatedJournal.appendPrepareRecord(1, new FakeData(), false);

      final CountDownLatch latch = new CountDownLatch(1);
      storage.afterCompleteOperations(new IOAsyncTask()
      {

         public void onError(final int errorCode, final String errorMessage)
         {
         }

         public void done()
         {
            latch.countDown();
         }
      });

      Assert.assertTrue(latch.await(1, TimeUnit.SECONDS));

      Assert.assertEquals("should be empty " + manager.getActiveTokens(), 0, manager.getActiveTokens().size());
   }

   @Test
   public void testOrderOnNonPersistency() throws Exception
   {

      setupServer(true);

      final ArrayList<Integer> executions = new ArrayList<Integer>();

      StorageManager storage = getStorage();
      manager = liveServer.getReplicationManager();
      Journal replicatedJournal = new ReplicatedJournal((byte)1, new FakeJournal(), manager);

      int numberOfAdds = 200;

      final CountDownLatch latch = new CountDownLatch(numberOfAdds);

      OperationContext ctx = storage.getContext();

      for (int i = 0; i < numberOfAdds; i++)
      {
         final int nAdd = i;

         if (i % 2 == 0)
         {
            replicatedJournal.appendPrepareRecord(i, new FakeData(), false);
         }

         ctx.executeOnCompletion(new IOAsyncTask()
         {

            public void onError(final int errorCode, final String errorMessage)
            {
            }

            public void done()
            {
               executions.add(nAdd);
               latch.countDown();
            }
         });
      }

      Assert.assertTrue(latch.await(10, TimeUnit.SECONDS));

      for (int i = 0; i < numberOfAdds; i++)
      {
         Assert.assertEquals(i, executions.get(i).intValue());
      }

      Assert.assertEquals(0, manager.getActiveTokens().size());
   }

   class FakeData implements EncodingSupport
   {

      public void decode(final HornetQBuffer buffer)
      {
      }

      public void encode(final HornetQBuffer buffer)
      {
         buffer.writeBytes(new byte[5]);
      }

      public int getEncodeSize()
      {
         return 5;
      }

   }

   @Override
   @Before
   public void setUp() throws Exception
   {
      super.setUp();

      tFactory = new HornetQThreadFactory("HornetQ-ReplicationTest", false, this.getClass().getClassLoader());

      executor = Executors.newCachedThreadPool(tFactory);

      scheduledExecutor = new ScheduledThreadPoolExecutor(10, tFactory);

      factory = new OrderedExecutorFactory(executor);
   }

   /**
    * We need to shutdown the executors before calling {@link super#tearDown()} (which will check
    * for leaking threads). Due to that, we need to close/stop all components here.
    */
   @Override
   @After
   public void tearDown() throws Exception
   {
      stopComponent(manager);
      manager = null;
      closeServerLocator(locator);
      stopComponent(backupServer);
      backupServer = null;
      stopComponent(liveServer);
      liveServer = null;

      executor.shutdownNow();
      scheduledExecutor.shutdownNow();

      tFactory = null;
      scheduledExecutor = null;

      super.tearDown();
   }

   protected
            PagingManager
            createPageManager(final StorageManager storageManager,
                              final Configuration configuration,
                              final ExecutorFactory executorFactory,
                              final HierarchicalRepository<AddressSettings> addressSettingsRepository) throws Exception
   {

      PagingManager paging =
               new PagingManagerImpl(new PagingStoreFactoryNIO(storageManager, configuration.getPagingDirectory(),
                                                               1000, null,
 executorFactory, false, null),
                                     addressSettingsRepository);

      paging.start();
      return paging;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   public static final class TestInterceptor implements Interceptor
   {
      static AtomicBoolean value = new AtomicBoolean(true);

      public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
      {
         return TestInterceptor.value.get();
      }

   };

   static final class FakeJournal implements Journal
   {

      public
               void
               appendAddRecord(final long id, final byte recordType, final byte[] record, final boolean sync) throws Exception
      {

      }

      public
               void appendAddRecord(final long id,
                                    final byte recordType,
                                    final EncodingSupport record,
                                    final boolean sync) throws Exception
      {

      }

      public void appendAddRecordTransactional(final long txID,
                                               final long id,
                                               final byte recordType,
                                               final byte[] record) throws Exception
      {

      }

      public void appendAddRecordTransactional(final long txID,
                                               final long id,
                                               final byte recordType,
                                               final EncodingSupport record) throws Exception
      {

      }

      public void appendCommitRecord(final long txID, final boolean sync) throws Exception
      {

      }

      public void appendDeleteRecord(final long id, final boolean sync) throws Exception
      {

      }

      public void appendDeleteRecordTransactional(final long txID, final long id, final byte[] record) throws Exception
      {

      }

      public
               void
               appendDeleteRecordTransactional(final long txID, final long id, final EncodingSupport record) throws Exception
      {

      }

      public void appendDeleteRecordTransactional(final long txID, final long id) throws Exception
      {

      }

      public
               void
               appendPrepareRecord(final long txID, final EncodingSupport transactionData, final boolean sync) throws Exception
      {

      }

      public
               void
               appendPrepareRecord(final long txID, final byte[] transactionData, final boolean sync) throws Exception
      {

      }

      public void appendRollbackRecord(final long txID, final boolean sync) throws Exception
      {

      }

      public
               void
               appendUpdateRecord(final long id, final byte recordType, final byte[] record, final boolean sync) throws Exception
      {

      }

      public void appendUpdateRecord(final long id,
                                     final byte recordType,
                                     final EncodingSupport record,
                                     final boolean sync) throws Exception
      {

      }

      public void appendUpdateRecordTransactional(final long txID,
                                                  final long id,
                                                  final byte recordType,
                                                  final byte[] record) throws Exception
      {

      }

      public void appendUpdateRecordTransactional(final long txID,
                                                  final long id,
                                                  final byte recordType,
                                                  final EncodingSupport record) throws Exception
      {

      }

      public int getAlignment() throws Exception
      {

         return 0;
      }

      public JournalLoadInformation load(final LoaderCallback reloadManager) throws Exception
      {

         return new JournalLoadInformation();
      }

      public JournalLoadInformation load(final List<RecordInfo> committedRecords,
                                         final List<PreparedTransactionInfo> preparedTransactions,
                                         final TransactionFailureCallback transactionFailure) throws Exception
      {

         return new JournalLoadInformation();
      }

      public void perfBlast(final int pages)
      {

      }

      public boolean isStarted()
      {

         return false;
      }

      public void start() throws Exception
      {

      }

      public void stop() throws Exception
      {

      }

      public JournalLoadInformation loadInternalOnly() throws Exception
      {
         return new JournalLoadInformation();
      }

      public int getNumberOfRecords()
      {
         return 0;
      }

      public void appendAddRecord(final long id,
                                  final byte recordType,
                                  final EncodingSupport record,
                                  final boolean sync,
                                  final IOCompletion completionCallback) throws Exception
      {
      }

      public void appendCommitRecord(final long txID, final boolean sync, final IOCompletion callback) throws Exception
      {
      }

      public
               void
               appendDeleteRecord(final long id, final boolean sync, final IOCompletion completionCallback) throws Exception
      {
      }

      public void appendPrepareRecord(final long txID,
                                      final EncodingSupport transactionData,
                                      final boolean sync,
                                      final IOCompletion callback) throws Exception
      {
      }

      public
               void
               appendRollbackRecord(final long txID, final boolean sync, final IOCompletion callback) throws Exception
      {
      }

      public void appendUpdateRecord(final long id,
                                     final byte recordType,
                                     final EncodingSupport record,
                                     final boolean sync,
                                     final IOCompletion completionCallback) throws Exception
      {
      }

      public void sync(final IOCompletion callback)
      {
      }

      public void runDirectJournalBlast() throws Exception
      {
      }

      public int getUserVersion()
      {
         return 0;
      }

      @Override
      public
               void
               appendCommitRecord(long txID, boolean sync, IOCompletion callback, boolean lineUpContext) throws Exception
      {

      }

      @Override
      public void lineUpContext(IOCompletion callback)
      {

      }

      @Override
      public JournalLoadInformation loadSyncOnly(JournalState s) throws Exception
      {
         return null;
      }

      @Override
      public Map<Long, JournalFile> createFilesForBackupSync(long[] fileIds) throws Exception
      {
         return null;
      }

      @Override
      public void synchronizationLock()
      {

      }

      @Override
      public void synchronizationUnlock()
      {

      }

      @Override
      public void forceMoveNextFile() throws Exception
      {

      }

      @Override
      public JournalFile[] getDataFiles()
      {
         return null;
      }

      @Override
      public SequentialFileFactory getFileFactory()
      {
         return null;
      }

      @Override
      public int getFileSize()
      {
         return 0;
      }

      @Override
      public void scheduleCompactAndBlock(int timeout) throws Exception
      {
      }

      @Override
      public void replicationSyncPreserveOldFiles()
      {
         // no-op
      }

      @Override
      public void replicationSyncFinished()
      {
         // no-op
      }
   }
}
