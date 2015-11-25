/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.hornetq.core.transaction.impl;

import javax.transaction.xa.Xid;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.message.impl.MessageInternal;
import org.hornetq.core.paging.PageTransactionInfo;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.cursor.PagePosition;
import org.hornetq.core.persistence.GroupingInfo;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.core.persistence.QueueBindingInfo;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.config.PersistedAddressSetting;
import org.hornetq.core.persistence.config.PersistedRoles;
import org.hornetq.core.persistence.impl.PageCountPending;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.replication.ReplicationManager;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.RouteContextList;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.group.impl.GroupBinding;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.TransactionOperation;
import org.hornetq.tests.util.ServiceTestBase;
import org.junit.Assert;
import org.junit.Test;

public class TransactionImplTest extends ServiceTestBase
{

   @Test
   public void testTimeoutAndThenCommitWithARollback() throws Exception
   {
      TransactionImpl tx = new TransactionImpl(newXID(), new FakeSM(), 10);
      Assert.assertTrue(tx.hasTimedOut(System.currentTimeMillis() + 60000, 10));

      final AtomicInteger commit = new AtomicInteger(0);
      final AtomicInteger rollback = new AtomicInteger(0);

      tx.addOperation(new TransactionOperation()
      {
         @Override
         public void beforePrepare(Transaction tx) throws Exception
         {

         }

         @Override
         public void afterPrepare(Transaction tx)
         {

         }

         @Override
         public void beforeCommit(Transaction tx) throws Exception
         {

         }

         @Override
         public void afterCommit(Transaction tx)
         {
            System.out.println("commit...");
            commit.incrementAndGet();
         }

         @Override
         public void beforeRollback(Transaction tx) throws Exception
         {

         }

         @Override
         public void afterRollback(Transaction tx)
         {
            System.out.println("rollback...");
            rollback.incrementAndGet();
         }

         @Override
         public List<MessageReference> getRelatedMessageReferences()
         {
            return null;
         }

         @Override
         public List<MessageReference> getListOnConsumer(long consumerID)
         {
            return null;
         }
      });

      for (int i = 0; i < 2; i++)
      {
         try
         {
            tx.commit();
            Assert.fail("Exception expected!");
         }
         catch (HornetQException expected)
         {
         }
      }

      // it should just be ignored!
      tx.rollback();



      Assert.assertEquals(0, commit.get());
      Assert.assertEquals(1, rollback.get());

   }


   @Test
   public void testTimeoutThenRollbackWithRollback() throws Exception
   {
      TransactionImpl tx = new TransactionImpl(newXID(), new FakeSM(), 10);
      Assert.assertTrue(tx.hasTimedOut(System.currentTimeMillis() + 60000, 10));

      final AtomicInteger commit = new AtomicInteger(0);
      final AtomicInteger rollback = new AtomicInteger(0);

      tx.addOperation(new TransactionOperation()
      {
         @Override
         public void beforePrepare(Transaction tx) throws Exception
         {

         }

         @Override
         public void afterPrepare(Transaction tx)
         {

         }

         @Override
         public void beforeCommit(Transaction tx) throws Exception
         {

         }

         @Override
         public void afterCommit(Transaction tx)
         {
            System.out.println("commit...");
            commit.incrementAndGet();
         }

         @Override
         public void beforeRollback(Transaction tx) throws Exception
         {

         }

         @Override
         public void afterRollback(Transaction tx)
         {
            System.out.println("rollback...");
            rollback.incrementAndGet();
         }

         @Override
         public List<MessageReference> getRelatedMessageReferences()
         {
            return null;
         }

         @Override
         public List<MessageReference> getListOnConsumer(long consumerID)
         {
            return null;
         }
      });

      tx.rollback();

      // This is a case where another failure was detected (In parallel with the TX timeout for instance)
      tx.markAsRollbackOnly(new HornetQException("rollback only again"));
      tx.rollback();


      Assert.assertEquals(0, commit.get());
      Assert.assertEquals(1, rollback.get());

   }


   class FakeSM implements StorageManager
   {
      @Override
      public OperationContext getContext()
      {
         return null;
      }

      @Override
      public void lineUpContext()
      {

      }

      @Override
      public OperationContext newContext(Executor executor)
      {
         return null;
      }

      @Override
      public OperationContext newSingleThreadContext()
      {
         return null;
      }

      @Override
      public void setContext(OperationContext context)
      {

      }

      @Override
      public void stop(boolean ioCriticalError) throws Exception
      {

      }

      @Override
      public void pageClosed(SimpleString storeName, int pageNumber)
      {

      }

      @Override
      public void pageDeleted(SimpleString storeName, int pageNumber)
      {

      }

      @Override
      public void pageWrite(PagedMessage message, int pageNumber)
      {

      }

      @Override
      public void afterCompleteOperations(IOAsyncTask run)
      {
         run.done();
      }

      @Override
      public boolean waitOnOperations(long timeout) throws Exception
      {
         return false;
      }

      @Override
      public void waitOnOperations() throws Exception
      {

      }

      @Override
      public void beforePageRead() throws Exception
      {

      }

      @Override
      public void afterPageRead() throws Exception
      {

      }

      @Override
      public ByteBuffer allocateDirectBuffer(int size)
      {
         return null;
      }

      @Override
      public void freeDirectBuffer(ByteBuffer buffer)
      {

      }

      @Override
      public void clearContext()
      {

      }

      @Override
      public long generateUniqueID()
      {
         return 0;
      }

      @Override
      public long getCurrentUniqueID()
      {
         return 0;
      }

      @Override
      public void confirmPendingLargeMessageTX(Transaction transaction, long messageID, long recordID) throws Exception
      {

      }

      @Override
      public void confirmPendingLargeMessage(long recordID) throws Exception
      {

      }

      @Override
      public void storeMessage(ServerMessage message) throws Exception
      {

      }

      @Override
      public void storeReference(long queueID, long messageID, boolean last) throws Exception
      {

      }

      @Override
      public void deleteMessage(long messageID) throws Exception
      {

      }

      @Override
      public void storeAcknowledge(long queueID, long messageID) throws Exception
      {

      }

      @Override
      public void storeCursorAcknowledge(long queueID, PagePosition position) throws Exception
      {

      }

      @Override
      public void updateDeliveryCount(MessageReference ref) throws Exception
      {

      }

      @Override
      public void updateScheduledDeliveryTime(MessageReference ref) throws Exception
      {

      }

      @Override
      public void storeDuplicateID(SimpleString address, byte[] duplID, long recordID) throws Exception
      {

      }

      @Override
      public void deleteDuplicateID(long recordID) throws Exception
      {

      }

      @Override
      public void storeMessageTransactional(long txID, ServerMessage message) throws Exception
      {

      }

      @Override
      public void storeReferenceTransactional(long txID, long queueID, long messageID) throws Exception
      {

      }

      @Override
      public void storeAcknowledgeTransactional(long txID, long queueID, long messageID) throws Exception
      {

      }

      @Override
      public void storeCursorAcknowledgeTransactional(long txID, long queueID, PagePosition position) throws Exception
      {

      }

      @Override
      public void deleteCursorAcknowledgeTransactional(long txID, long ackID) throws Exception
      {

      }

      @Override
      public void deleteCursorAcknowledge(long ackID) throws Exception
      {

      }

      @Override
      public void storePageCompleteTransactional(long txID, long queueID, PagePosition position) throws Exception
      {

      }

      @Override
      public void deletePageComplete(long ackID) throws Exception
      {

      }

      @Override
      public void updateScheduledDeliveryTimeTransactional(long txID, MessageReference ref) throws Exception
      {

      }

      @Override
      public void storeDuplicateIDTransactional(long txID, SimpleString address, byte[] duplID, long recordID) throws Exception
      {

      }

      @Override
      public void updateDuplicateIDTransactional(long txID, SimpleString address, byte[] duplID, long recordID) throws Exception
      {

      }

      @Override
      public void deleteDuplicateIDTransactional(long txID, long recordID) throws Exception
      {

      }

      @Override
      public LargeServerMessage createLargeMessage()
      {
         return null;
      }

      @Override
      public LargeServerMessage createLargeMessage(long id, MessageInternal message) throws Exception
      {
         return null;
      }

      @Override
      public SequentialFile createFileForLargeMessage(long messageID, LargeMessageExtension extension)
      {
         return null;
      }

      @Override
      public void prepare(long txID, Xid xid) throws Exception
      {

      }

      @Override
      public void commit(long txID) throws Exception
      {

      }

      @Override
      public void commit(long txID, boolean lineUpContext) throws Exception
      {

      }

      @Override
      public void rollback(long txID) throws Exception
      {

      }

      @Override
      public void rollbackBindings(long txID) throws Exception
      {

      }

      @Override
      public void commitBindings(long txID) throws Exception
      {

      }

      @Override
      public void storePageTransaction(long txID, PageTransactionInfo pageTransaction) throws Exception
      {

      }

      @Override
      public void updatePageTransaction(long txID, PageTransactionInfo pageTransaction, int depage) throws Exception
      {

      }

      @Override
      public void updatePageTransaction(PageTransactionInfo pageTransaction, int depage) throws Exception
      {

      }

      @Override
      public void deletePageTransactional(long recordID) throws Exception
      {

      }

      @Override
      public JournalLoadInformation loadMessageJournal(PostOffice postOffice, PagingManager pagingManager, ResourceManager resourceManager, Map<Long, Queue> queues, Map<Long, QueueBindingInfo> queueInfos, Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap, Set<Pair<Long, Long>> pendingLargeMessages, List<PageCountPending> pendingNonTXPageCounter) throws Exception
      {
         return null;
      }

      @Override
      public long storeHeuristicCompletion(Xid xid, boolean isCommit) throws Exception
      {
         return 0;
      }

      @Override
      public void deleteHeuristicCompletion(long id) throws Exception
      {

      }

      @Override
      public void addQueueBinding(long tx, Binding binding) throws Exception
      {

      }

      @Override
      public void deleteQueueBinding(long tx, long queueBindingID) throws Exception
      {

      }

      @Override
      public JournalLoadInformation loadBindingJournal(List<QueueBindingInfo> queueBindingInfos, List<GroupingInfo> groupingInfos) throws Exception
      {
         return null;
      }

      @Override
      public void addGrouping(GroupBinding groupBinding) throws Exception
      {

      }

      @Override
      public void deleteGrouping(long tx, GroupBinding groupBinding) throws Exception
      {

      }

      @Override
      public void storeAddressSetting(PersistedAddressSetting addressSetting) throws Exception
      {

      }

      @Override
      public void deleteAddressSetting(SimpleString addressMatch) throws Exception
      {

      }

      @Override
      public List<PersistedAddressSetting> recoverAddressSettings() throws Exception
      {
         return null;
      }

      @Override
      public void storeSecurityRoles(PersistedRoles persistedRoles) throws Exception
      {

      }

      @Override
      public void deleteSecurityRoles(SimpleString addressMatch) throws Exception
      {

      }

      @Override
      public List<PersistedRoles> recoverPersistedRoles() throws Exception
      {
         return null;
      }

      @Override
      public long storePageCounter(long txID, long queueID, long value) throws Exception
      {
         return 0;
      }

      @Override
      public long storePendingCounter(long queueID, long pageID, int inc) throws Exception
      {
         return 0;
      }

      @Override
      public void deleteIncrementRecord(long txID, long recordID) throws Exception
      {

      }

      @Override
      public void deletePageCounter(long txID, long recordID) throws Exception
      {

      }

      @Override
      public void deletePendingPageCounter(long txID, long recordID) throws Exception
      {

      }

      @Override
      public long storePageCounterInc(long txID, long queueID, int add) throws Exception
      {
         return 0;
      }

      @Override
      public long storePageCounterInc(long queueID, int add) throws Exception
      {
         return 0;
      }

      @Override
      public Journal getBindingsJournal()
      {
         return null;
      }

      @Override
      public Journal getMessageJournal()
      {
         return null;
      }

      @Override
      public void startReplication(ReplicationManager replicationManager, PagingManager pagingManager, String nodeID, boolean autoFailBack) throws Exception
      {

      }

      @Override
      public boolean addToPage(PagingStore store, ServerMessage msg, Transaction tx, RouteContextList listCtx) throws Exception
      {
         return false;
      }

      @Override
      public void stopReplication()
      {

      }

      @Override
      public void addBytesToLargeMessage(SequentialFile appendFile, long messageID, byte[] bytes) throws Exception
      {

      }

      @Override
      public void storeID(long journalID, long id) throws Exception
      {

      }

      @Override
      public void readLock()
      {

      }

      @Override
      public void readUnLock()
      {

      }

      @Override
      public void persistIdGenerator()
      {

      }

      @Override
      public void start() throws Exception
      {

      }

      @Override
      public void stop() throws Exception
      {

      }

      @Override
      public boolean isStarted()
      {
         return false;
      }
   }
}
