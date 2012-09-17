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

package org.hornetq.core.persistence.impl.nullpm;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicLong;

import javax.transaction.xa.Xid;

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

/**
 * A NullStorageManager
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 */
public class NullStorageManager implements StorageManager
{
   private final AtomicLong idSequence = new AtomicLong(0);

   private volatile boolean started;

   private static final OperationContext dummyContext = new OperationContext()
   {

      @Override
      public void onError(final int errorCode, final String errorMessage)
      {
      }

      @Override
      public void done()
      {
      }

      @Override
      public void storeLineUp()
      {
      }

      @Override
      public boolean waitCompletion(final long timeout) throws Exception
      {
         return true;
      }

      @Override
      public void waitCompletion() throws Exception
      {
      }

      @Override
      public void replicationLineUp()
      {
      }

      @Override
      public void replicationDone()
      {
      }

      @Override
      public void pageSyncLineUp()
      {
      }

      @Override
      public void pageSyncDone()
      {
      }

      @Override
      public void executeOnCompletion(final IOAsyncTask runnable)
      {
         runnable.done();
      }
   };

   @Override
   public void deleteQueueBinding(final long queueBindingID) throws Exception
   {
   }

   @Override
   public void commit(final long txID) throws Exception
   {
   }

   @Override
   public JournalLoadInformation loadBindingJournal(final List<QueueBindingInfo> queueBindingInfos,
                                                    final List<GroupingInfo> groupingInfos) throws Exception
   {
      return new JournalLoadInformation();
   }

   @Override
   public void prepare(final long txID, final Xid xid) throws Exception
   {
   }

   @Override
   public void rollback(final long txID) throws Exception
   {
   }

   @Override
   public void rollbackBindings(final long txID) throws Exception
   {
   }

   @Override
   public void commitBindings(final long txID) throws Exception
   {
   }

   @Override
   public void storeReference(final long queueID, final long messageID, final boolean last) throws Exception
   {
   }

   @Override
   public void storeReferenceTransactional(final long txID, final long queueID, final long messageID) throws Exception
   {
   }

   @Override
   public void storeAcknowledge(final long queueID, final long messageID) throws Exception
   {
   }

   @Override
   public void storeAcknowledgeTransactional(final long txID, final long queueID, final long messageiD)
                                                                                                       throws Exception
   {
   }

   @Override
   public void deleteMessage(final long messageID) throws Exception
   {
   }
   @Override
   public void storeMessage(final ServerMessage message) throws Exception
   {
   }

   @Override
   public void storeMessageTransactional(final long txID, final ServerMessage message) throws Exception
   {
   }

   @Override
   public void updateScheduledDeliveryTime(final MessageReference ref) throws Exception
   {
   }

   @Override
   public void updateScheduledDeliveryTimeTransactional(final long txID, final MessageReference ref) throws Exception
   {
   }

   @Override
   public void storePageTransaction(final long txID, final PageTransactionInfo pageTransaction) throws Exception
   {
   }

   @Override
   public void updateDeliveryCount(final MessageReference ref) throws Exception
   {
   }

   @Override
   public void storeDuplicateID(final SimpleString address, final byte[] duplID, final long recordID) throws Exception
   {
   }

   @Override
   public void storeDuplicateIDTransactional(final long txID, final SimpleString address, final byte[] duplID,
                                             final long recordID) throws Exception
   {
   }

   @Override
   public void updateDuplicateIDTransactional(final long txID, final SimpleString address, final byte[] duplID,
                                              final long recordID) throws Exception
   {
   }

   @Override
   public long storeHeuristicCompletion(final Xid xid, final boolean isCommit) throws Exception
   {
      return generateUniqueID();
   }

   @Override
   public void deleteHeuristicCompletion(final long txID) throws Exception
   {
   }

   @Override
   public void addQueueBinding(final long tx, final Binding binding) throws Exception
   {
   }

   @Override
   public LargeServerMessage createLargeMessage()
   {
      return new NullStorageLargeServerMessage();
   }

   @Override
   public LargeServerMessage createLargeMessage(final long id, final MessageInternal message)
   {
      NullStorageLargeServerMessage largeMessage = new NullStorageLargeServerMessage();

      largeMessage.copyHeadersAndProperties(message);

      largeMessage.setMessageID(id);

      return largeMessage;
   }

   @Override
   public long generateUniqueID()
   {
      long id = idSequence.getAndIncrement();

      return id;
   }

   @Override
   public long getCurrentUniqueID()
   {
      return idSequence.get();
   }

   @Override
   public synchronized void start() throws Exception
   {
      if (started)
      {
         throw new IllegalStateException("Already started");
      }

      started = true;
   }

   @Override
   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         throw new IllegalStateException("Not started");
      }

      idSequence.set(0);

      started = false;
   }

   @Override
   public synchronized boolean isStarted()
   {
      return started;
   }

   @Override
   public JournalLoadInformation loadMessageJournal(final PostOffice postOffice, final PagingManager pagingManager,
                                                    final ResourceManager resourceManager,
                                                    final Map<Long, Queue> queues,
                                                    final Map<Long, QueueBindingInfo> queueInfos,
                                                    final Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap,
                                                    final Set<Pair<Long, Long>> pendingLargeMessages) throws Exception
   {
      return new JournalLoadInformation();
   }

   @Override
   public void deleteDuplicateIDTransactional(final long txID, final long recordID) throws Exception
   {
   }

   @Override
   public void deleteDuplicateID(final long recordID) throws Exception
   {
   }

   @Override
   public void pageClosed(final SimpleString storeName, final int pageNumber)
   {
   }

   @Override
   public void pageDeleted(final SimpleString storeName, final int pageNumber)
   {
   }

   @Override
   public void pageWrite(final PagedMessage message, final int pageNumber)
   {
   }

   @Override
   public void addGrouping(final GroupBinding groupBinding) throws Exception
   {
   }

   @Override
   public void deleteGrouping(final GroupBinding groupBinding) throws Exception
   {
   }

   @Override
   public boolean waitOnOperations(final long timeout) throws Exception
   {
      return true;
   }

   @Override
   public void afterCompleteOperations(final IOAsyncTask run)
   {
      run.done();
   }

   @Override
   public void waitOnOperations() throws Exception
   {
   }

   @Override
   public OperationContext getContext()
   {
      return NullStorageManager.dummyContext;
   }

   @Override
   public OperationContext newContext(final Executor executor)
   {
      return NullStorageManager.dummyContext;
   }

   @Override
   public OperationContext newSingleThreadContext()
   {
      return NullStorageManager.dummyContext;
   }

   @Override
   public void setContext(final OperationContext context)
   {
   }

   @Override
   public void clearContext()
   {
   }

   @Override
   public List<PersistedAddressSetting> recoverAddressSettings() throws Exception
   {
      return Collections.emptyList();
   }

   @Override
   public void storeAddressSetting(final PersistedAddressSetting addressSetting) throws Exception
   {
   }

   @Override
   public List<PersistedRoles> recoverPersistedRoles() throws Exception
   {
      return Collections.emptyList();
   }

   @Override
   public void storeSecurityRoles(final PersistedRoles persistedRoles) throws Exception
   {
   }

   @Override
   public void deleteAddressSetting(final SimpleString addressMatch) throws Exception
   {
   }

   @Override
   public void deleteSecurityRoles(final SimpleString addressMatch) throws Exception
   {
   }

   @Override
   public void deletePageTransactional(final long recordID) throws Exception
   {
   }

   @Override
   public
            void
            updatePageTransaction(final long txID, final PageTransactionInfo pageTransaction, final int depage)
                                                                                                               throws Exception
   {
   }

   @Override
   public void storeCursorAcknowledge(final long queueID, final PagePosition position)
   {
   }

   @Override
   public void storeCursorAcknowledgeTransactional(final long txID, final long queueID, final PagePosition position)
   {
   }

   @Override
   public void deleteCursorAcknowledgeTransactional(final long txID, final long ackID) throws Exception
   {
   }

   public void storePageCompleteTransactional(long txID, long queueID, PagePosition position) throws Exception
   {
   }

   public void deletePageComplete(long ackID) throws Exception
   {
   }

   @Override
   public void updatePageTransaction(final PageTransactionInfo pageTransaction, final int depage) throws Exception
   {
   }

   @Override
   public long storePageCounter(final long txID, final long queueID, final long value) throws Exception
   {
      return 0;
   }

   @Override
   public void deleteIncrementRecord(final long txID, final long recordID) throws Exception
   {
   }

   @Override
   public void deletePageCounter(final long txID, final long recordID) throws Exception
   {
   }

   @Override
   public long storePageCounterInc(final long txID, final long queueID, final int add) throws Exception
   {
      return 0;
   }

   @Override
   public long storePageCounterInc(final long queueID, final int add) throws Exception
   {
      return 0;
   }

   @Override
   public void commit(final long txID, final boolean lineUpContext) throws Exception
   {
   }

   @Override
   public void lineUpContext()
   {
   }

   @Override
   public
            void
            confirmPendingLargeMessageTX(final Transaction transaction, final long messageID, final long recordID)
                                                                                                                  throws Exception
   {
   }

   @Override
   public void confirmPendingLargeMessage(final long recordID) throws Exception
   {
   }

   @Override
   public void stop(final boolean ioCriticalError) throws Exception
   {
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
   public void startReplication(final ReplicationManager replicationManager, final PagingManager pagingManager,
                                final String nodeID, final boolean autoFailBack) throws Exception
   {
      // no-op
   }

   @Override
   public boolean addToPage(PagingStore s, ServerMessage msg, Transaction tx, RouteContextList listCtx)
                                                                                                       throws Exception
   {
      return false;
   }

   @Override
   public void stopReplication()
   {
      // no-op
   }

   @Override
   public SequentialFile createFileForLargeMessage(final long messageID, final String extension)
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void addBytesToLargeMessage(SequentialFile appendFile, long messageID, byte[] bytes) throws Exception
   {
      // no-op
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
   public ByteBuffer allocateDirectBuffer(final int size)
   {
      return ByteBuffer.allocateDirect(size);
   }

   @Override
   public void freeDirectBuffer(final ByteBuffer buffer)
   {
      // We can just have hope on GC here :-)
   }

   @Override
   public void storeID(final long journalID, final long id) throws Exception
   {

   }
}
