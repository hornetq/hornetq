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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import junit.framework.Assert;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Interceptor;
import org.hornetq.api.core.SimpleString;
import org.hornetq.api.core.TransportConfiguration;
import org.hornetq.api.core.client.ClientSessionFactory;
import org.hornetq.api.core.client.HornetQClient;
import org.hornetq.core.client.impl.FailoverManager;
import org.hornetq.core.client.impl.FailoverManagerImpl;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.journal.IOCompletion;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.journal.LoaderCallback;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.TransactionFailureCallback;
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
import org.hornetq.core.remoting.impl.invm.InVMConnectorFactory;
import org.hornetq.core.replication.impl.ReplicatedJournal;
import org.hornetq.core.replication.impl.ReplicationManagerImpl;
import org.hornetq.core.server.HornetQServer;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.impl.HornetQServerImpl;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.settings.HierarchicalRepository;
import org.hornetq.core.settings.impl.AddressSettings;
import org.hornetq.spi.core.protocol.RemotingConnection;
import org.hornetq.tests.util.RandomUtil;
import org.hornetq.tests.util.ServiceTestBase;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.HornetQThreadFactory;
import org.hornetq.utils.OrderedExecutorFactory;

/**
 * A ReplicationTest
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 *
 */
public class ReplicationTest extends ServiceTestBase
{

   // Constants -----------------------------------------------------

   // Attributes ----------------------------------------------------

   private ThreadFactory tFactory;

   private ExecutorService executor;

   private ExecutorFactory factory;

   private ScheduledExecutorService scheduledExecutor;

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   public void testBasicConnection() throws Exception
   {

      Configuration config = createDefaultConfig(false);

      config.setBackup(true);

      HornetQServer server = new HornetQServerImpl(config);

      FailoverManager failoverManager = createFailoverManager();

      server.start();

      try
      {
         ReplicationManagerImpl manager = new ReplicationManagerImpl(failoverManager, factory);
         manager.start();
         manager.stop();
      }
      finally
      {
         server.stop();
      }
   }

   public void testInvalidJournal() throws Exception
   {

      Configuration config = createDefaultConfig(false);

      config.setBackup(true);

      HornetQServer server = new HornetQServerImpl(config);

      FailoverManager failoverManager = createFailoverManager();

      server.start();

      try
      {
         ReplicationManagerImpl manager = new ReplicationManagerImpl(failoverManager, factory);
         manager.start();
         try
         {
            manager.compareJournals(new JournalLoadInformation[] { new JournalLoadInformation(2, 2),
                                                                  new JournalLoadInformation(2, 2) });
            Assert.fail("Exception was expected");
         }
         catch (HornetQException e)
         {
            e.printStackTrace();
            Assert.assertEquals(HornetQException.ILLEGAL_STATE, e.getCode());
         }

         manager.compareJournals(new JournalLoadInformation[] { new JournalLoadInformation(),
                                                               new JournalLoadInformation() });

         manager.stop();
      }
      finally
      {
         server.stop();
      }
   }

   // should throw an exception if a second server connects to the same backup
   public void testInvalidConnection() throws Exception
   {

      Configuration config = createDefaultConfig(false);

      config.setBackup(true);

      HornetQServer server = new HornetQServerImpl(config);

      FailoverManager failoverManager = createFailoverManager();

      server.start();

      try
      {
         ReplicationManagerImpl manager = new ReplicationManagerImpl(failoverManager, factory);

         manager.start();

         try
         {
            ReplicationManagerImpl manager2 = new ReplicationManagerImpl(failoverManager, factory);

            manager2.start();
            Assert.fail("Exception was expected");
         }
         catch (Exception e)
         {
         }

         manager.stop();

      }
      finally
      {
         server.stop();
      }
   }

   public void testConnectIntoNonBackup() throws Exception
   {

      Configuration config = createDefaultConfig(false);

      config.setBackup(false);

      HornetQServer server = new HornetQServerImpl(config);

      server.start();

      FailoverManager failoverManager = createFailoverManager();

      try
      {
         ReplicationManagerImpl manager = new ReplicationManagerImpl(failoverManager, factory);

         try
         {
            manager.start();
            Assert.fail("Exception was expected");
         }
         catch (HornetQException expected)
         {
         }

         manager.stop();
      }
      finally
      {
         server.stop();
      }
   }

   public void testSendPackets() throws Exception
   {

      Configuration config = createDefaultConfig(false);

      config.setBackup(true);

      HornetQServer server = new HornetQServerImpl(config);

      server.start();

      FailoverManager failoverManager = createFailoverManager();

      try
      {
         StorageManager storage = getStorage();

         ReplicationManagerImpl manager = new ReplicationManagerImpl(failoverManager, factory);
         manager.start();

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

         Assert.assertEquals(0, manager.getActiveTokens().size());

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

         PagingManager pagingManager = createPageManager(server.getStorageManager(),
                                                         server.getConfiguration(),
                                                         server.getExecutorFactory(),
                                                         server.getAddressSettingsRepository());

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

         manager.largeMessageDelete(500);

         blockOnReplication(storage, manager);

         store.start();

         Assert.assertEquals(0, store.getNumberOfPages());

         manager.stop();
      }
      finally
      {
         server.stop();
      }
   }

   public void testSendPacketsWithFailure() throws Exception
   {

      Configuration config = createDefaultConfig(false);

      config.setBackup(true);

      ArrayList<String> intercepts = new ArrayList<String>();

      intercepts.add(TestInterceptor.class.getName());

      config.setInterceptorClassNames(intercepts);

      HornetQServer server = new HornetQServerImpl(config);

      server.start();

      FailoverManager failoverManager = createFailoverManager();

      try
      {
         StorageManager storage = getStorage();
         ReplicationManagerImpl manager = new ReplicationManagerImpl(failoverManager, factory);
         manager.start();

         Journal replicatedJournal = new ReplicatedJournal((byte)1, new FakeJournal(), manager);

         TestInterceptor.value.set(false);

         for (int i = 0; i < 500; i++)
         {
            replicatedJournal.appendAddRecord(i, (byte)1, new FakeData(), false);
         }

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

         server.stop();

         Assert.assertTrue(latch.await(50, TimeUnit.SECONDS));
      }
      finally
      {
         server.stop();
      }
   }

   public void testExceptionSettingActionBefore() throws Exception
   {
      OperationContext ctx = OperationContextImpl.getContext(factory);

      ctx.storeLineUp();

      String msg = "I'm an exception";

      ctx.onError(5, msg);

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
    */
   private JournalStorageManager getStorage()
   {
      return new JournalStorageManager(createDefaultConfig(), factory);
   }

   /**
    * @param manager
    * @return
    */
   private void blockOnReplication(final StorageManager storage, final ReplicationManagerImpl manager) throws Exception
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

   public void testNoServer() throws Exception
   {
      FailoverManager failoverManager = createFailoverManager();

      try
      {
         ReplicationManagerImpl manager = new ReplicationManagerImpl(failoverManager, factory);
         manager.start();
         Assert.fail("Exception expected");
      }
      catch (HornetQException expected)
      {
         Assert.assertEquals(HornetQException.ILLEGAL_STATE, expected.getCode());
      }
   }

   public void testNoActions() throws Exception
   {

      Configuration config = createDefaultConfig(false);

      config.setBackup(true);

      HornetQServer server = new HornetQServerImpl(config);

      server.start();

      FailoverManager failoverManager = createFailoverManager();

      try
      {
         StorageManager storage = getStorage();
         ReplicationManagerImpl manager = new ReplicationManagerImpl(failoverManager, factory);
         manager.start();

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

         Assert.assertEquals(0, manager.getActiveTokens().size());
         manager.stop();
      }
      finally
      {
         server.stop();
      }
   }

   public void testOrderOnNonPersistency() throws Exception
   {

      Configuration config = createDefaultConfig(false);

      config.setBackup(true);

      HornetQServer server = new HornetQServerImpl(config);

      server.start();

      FailoverManager failoverManager = createFailoverManager();

      final ArrayList<Integer> executions = new ArrayList<Integer>();

      try
      {
         StorageManager storage = getStorage();
         ReplicationManagerImpl manager = new ReplicationManagerImpl(failoverManager, factory);
         manager.start();

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
                  System.out.println("Add " + nAdd);
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
         manager.stop();
      }
      finally
      {
         server.stop();
      }
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

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#getEncodeSize()
       */
      public int getEncodeSize()
      {
         return 5;
      }

   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   @Override
   protected void setUp() throws Exception
   {
      super.setUp();

      tFactory = new HornetQThreadFactory("HornetQ-ReplicationTest", false, this.getClass().getClassLoader());

      executor = Executors.newCachedThreadPool(tFactory);

      scheduledExecutor = new ScheduledThreadPoolExecutor(10, tFactory);

      factory = new OrderedExecutorFactory(executor);
   }

   @Override
   protected void tearDown() throws Exception
   {

      executor.shutdown();

      scheduledExecutor.shutdown();

      tFactory = null;

      scheduledExecutor = null;

      super.tearDown();

   }

   private FailoverManagerImpl createFailoverManager()
   {
      return createFailoverManager(null);
   }

   private FailoverManagerImpl createFailoverManager(final List<Interceptor> interceptors)
   {
      TransportConfiguration connectorConfig = new TransportConfiguration(InVMConnectorFactory.class.getName(),
                                                                          new HashMap<String, Object>(),
                                                                          RandomUtil.randomString());

      return new FailoverManagerImpl((ClientSessionFactory)null,
                                     connectorConfig,
                                     null,
                                     false,
                                     HornetQClient.DEFAULT_CALL_TIMEOUT,
                                     HornetQClient.DEFAULT_CLIENT_FAILURE_CHECK_PERIOD,
                                     HornetQClient.DEFAULT_CONNECTION_TTL,
                                     0,
                                     1.0d,
                                     0,
                                     1,
                                     HornetQClient.DEFAULT_FAILOVER_ON_INITIAL_CONNECTION,
                                     executor,
                                     scheduledExecutor,
                                     interceptors);
   }

   protected PagingManager createPageManager(final StorageManager storageManager,
                                             final Configuration configuration,
                                             final ExecutorFactory executorFactory,
                                             final HierarchicalRepository<AddressSettings> addressSettingsRepository) throws Exception
   {

      PagingManager paging = new PagingManagerImpl(new PagingStoreFactoryNIO(configuration.getPagingDirectory(),
                                                                             executorFactory,
                                                                             false),
                                                   storageManager,
                                                   addressSettingsRepository);

      paging.start();
      return paging;
   }

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------
   public static class TestInterceptor implements Interceptor
   {
      static AtomicBoolean value = new AtomicBoolean(true);

      public boolean intercept(final Packet packet, final RemotingConnection connection) throws HornetQException
      {
         return TestInterceptor.value.get();
      }

   };

   static class FakeJournal implements Journal
   {

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendAddRecord(long, byte, byte[], boolean)
       */
      public void appendAddRecord(final long id, final byte recordType, final byte[] record, final boolean sync) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendAddRecord(long, byte, org.hornetq.core.journal.EncodingSupport, boolean)
       */
      public void appendAddRecord(final long id, final byte recordType, final EncodingSupport record, final boolean sync) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendAddRecordTransactional(long, long, byte, byte[])
       */
      public void appendAddRecordTransactional(final long txID,
                                               final long id,
                                               final byte recordType,
                                               final byte[] record) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendAddRecordTransactional(long, long, byte, org.hornetq.core.journal.EncodingSupport)
       */
      public void appendAddRecordTransactional(final long txID,
                                               final long id,
                                               final byte recordType,
                                               final EncodingSupport record) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendCommitRecord(long, boolean)
       */
      public void appendCommitRecord(final long txID, final boolean sync) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendDeleteRecord(long, boolean)
       */
      public void appendDeleteRecord(final long id, final boolean sync) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendDeleteRecordTransactional(long, long, byte[])
       */
      public void appendDeleteRecordTransactional(final long txID, final long id, final byte[] record) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendDeleteRecordTransactional(long, long, org.hornetq.core.journal.EncodingSupport)
       */
      public void appendDeleteRecordTransactional(final long txID, final long id, final EncodingSupport record) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendDeleteRecordTransactional(long, long)
       */
      public void appendDeleteRecordTransactional(final long txID, final long id) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendPrepareRecord(long, org.hornetq.core.journal.EncodingSupport, boolean)
       */
      public void appendPrepareRecord(final long txID, final EncodingSupport transactionData, final boolean sync) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendPrepareRecord(long, byte[], boolean)
       */
      public void appendPrepareRecord(final long txID, final byte[] transactionData, final boolean sync) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendRollbackRecord(long, boolean)
       */
      public void appendRollbackRecord(final long txID, final boolean sync) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendUpdateRecord(long, byte, byte[], boolean)
       */
      public void appendUpdateRecord(final long id, final byte recordType, final byte[] record, final boolean sync) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendUpdateRecord(long, byte, org.hornetq.core.journal.EncodingSupport, boolean)
       */
      public void appendUpdateRecord(final long id,
                                     final byte recordType,
                                     final EncodingSupport record,
                                     final boolean sync) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendUpdateRecordTransactional(long, long, byte, byte[])
       */
      public void appendUpdateRecordTransactional(final long txID,
                                                  final long id,
                                                  final byte recordType,
                                                  final byte[] record) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendUpdateRecordTransactional(long, long, byte, org.hornetq.core.journal.EncodingSupport)
       */
      public void appendUpdateRecordTransactional(final long txID,
                                                  final long id,
                                                  final byte recordType,
                                                  final EncodingSupport record) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#getAlignment()
       */
      public int getAlignment() throws Exception
      {

         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#load(org.hornetq.core.journal.LoaderCallback)
       */
      public JournalLoadInformation load(final LoaderCallback reloadManager) throws Exception
      {

         return new JournalLoadInformation();
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#load(java.util.List, java.util.List, org.hornetq.core.journal.TransactionFailureCallback)
       */
      public JournalLoadInformation load(final List<RecordInfo> committedRecords,
                                         final List<PreparedTransactionInfo> preparedTransactions,
                                         final TransactionFailureCallback transactionFailure) throws Exception
      {

         return new JournalLoadInformation();
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#perfBlast(int)
       */
      public void perfBlast(final int pages) throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQComponent#isStarted()
       */
      public boolean isStarted()
      {

         return false;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQComponent#start()
       */
      public void start() throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.server.HornetQComponent#stop()
       */
      public void stop() throws Exception
      {

      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#loadInternalOnly()
       */
      public JournalLoadInformation loadInternalOnly() throws Exception
      {
         return new JournalLoadInformation();
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#getNumberOfRecords()
       */
      public int getNumberOfRecords()
      {
         return 0;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendAddRecord(long, byte, byte[], boolean, org.hornetq.core.journal.IOCompletion)
       */
      public void appendAddRecord(final long id,
                                  final byte recordType,
                                  final byte[] record,
                                  final boolean sync,
                                  final IOCompletion completionCallback) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendAddRecord(long, byte, org.hornetq.core.journal.EncodingSupport, boolean, org.hornetq.core.journal.IOCompletion)
       */
      public void appendAddRecord(final long id,
                                  final byte recordType,
                                  final EncodingSupport record,
                                  final boolean sync,
                                  final IOCompletion completionCallback) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendCommitRecord(long, boolean, org.hornetq.core.journal.IOCompletion)
       */
      public void appendCommitRecord(final long txID, final boolean sync, final IOCompletion callback) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendDeleteRecord(long, boolean, org.hornetq.core.journal.IOCompletion)
       */
      public void appendDeleteRecord(final long id, final boolean sync, final IOCompletion completionCallback) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendPrepareRecord(long, org.hornetq.core.journal.EncodingSupport, boolean, org.hornetq.core.journal.IOCompletion)
       */
      public void appendPrepareRecord(final long txID,
                                      final EncodingSupport transactionData,
                                      final boolean sync,
                                      final IOCompletion callback) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendPrepareRecord(long, byte[], boolean, org.hornetq.core.journal.IOCompletion)
       */
      public void appendPrepareRecord(final long txID,
                                      final byte[] transactionData,
                                      final boolean sync,
                                      final IOCompletion callback) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendRollbackRecord(long, boolean, org.hornetq.core.journal.IOCompletion)
       */
      public void appendRollbackRecord(final long txID, final boolean sync, final IOCompletion callback) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendUpdateRecord(long, byte, byte[], boolean, org.hornetq.core.journal.IOCompletion)
       */
      public void appendUpdateRecord(final long id,
                                     final byte recordType,
                                     final byte[] record,
                                     final boolean sync,
                                     final IOCompletion completionCallback) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#appendUpdateRecord(long, byte, org.hornetq.core.journal.EncodingSupport, boolean, org.hornetq.core.journal.IOCompletion)
       */
      public void appendUpdateRecord(final long id,
                                     final byte recordType,
                                     final EncodingSupport record,
                                     final boolean sync,
                                     final IOCompletion completionCallback) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#sync(org.hornetq.core.journal.IOCompletion)
       */
      public void sync(final IOCompletion callback)
      {
      }

      public void runDirectJournalBlast() throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.Journal#getUserVersion()
       */
      public int getUserVersion()
      {
         return 0;
      }

   }
}
