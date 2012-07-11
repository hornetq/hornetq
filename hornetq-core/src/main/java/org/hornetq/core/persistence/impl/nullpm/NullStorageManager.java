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
import org.hornetq.core.server.RoutingContext;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.group.impl.GroupBinding;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.Transaction;

/**
 *
 * A NullStorageManager
 *
 * @author <a href="mailto:ataylor@redhat.com">Andy Taylor</a>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 *
 */
public class NullStorageManager implements StorageManager
{
   private final AtomicLong idSequence = new AtomicLong(0);

   private volatile boolean started;

   private static final OperationContext dummyContext = new OperationContext()
   {

      public void onError(int errorCode, String errorMessage)
      {
      }

      public void done()
      {
      }

      public void storeLineUp()
      {
      }

      public boolean waitCompletion(long timeout) throws Exception
      {
         return true;
      }

      public void waitCompletion() throws Exception
      {
      }

      public void replicationLineUp()
      {
      }

      public void replicationDone()
      {
      }

      public void pageSyncLineUp()
      {
      }

      public void pageSyncDone()
      {
      }

      public void executeOnCompletion(IOAsyncTask runnable)
      {
         runnable.done();
      }
   };

   public void sync()
   {
      // NO OP
   }

   public void addQueueBinding(final Binding queueBinding) throws Exception
   {
   }

   public void deleteQueueBinding(final long queueBindingID) throws Exception
   {
   }

   public void commit(final long txID) throws Exception
   {
   }

   public JournalLoadInformation loadBindingJournal(final List<QueueBindingInfo> queueBindingInfos,
                                                    final List<GroupingInfo> groupingInfos) throws Exception
   {
      return new JournalLoadInformation();
   }

   public void prepare(final long txID, final Xid xid) throws Exception
   {
   }

   public void rollback(final long txID) throws Exception
   {
   }

   public void rollbackBindings(long txID) throws Exception
   {
   }

   public void commitBindings(long txID) throws Exception
   {
   }

    public void storeReference(final long queueID, final long messageID, final boolean last) throws Exception
   {
   }

   public void storeReferenceTransactional(final long txID, final long queueID, final long messageID) throws Exception
   {
   }

   public void storeAcknowledge(final long queueID, final long messageID) throws Exception
   {
   }

   public void storeMessageReferenceScheduled(final long queueID, final long messageID, final long scheduledDeliveryTime) throws Exception
   {
   }

   public void storeAcknowledgeTransactional(final long txID, final long queueID, final long messageiD) throws Exception
   {
   }

   public void deleteMessage(final long messageID) throws Exception
   {
   }

   public void deletePageTransactional(final long txID, final long messageID) throws Exception
   {
   }

   public void storeMessage(final ServerMessage message) throws Exception
   {
   }

   public void storeMessageTransactional(final long txID, final ServerMessage message) throws Exception
   {
   }

   public void updateScheduledDeliveryTime(final MessageReference ref) throws Exception
   {
   }

   public void updateScheduledDeliveryTimeTransactional(final long txID, final MessageReference ref) throws Exception
   {
   }

   public void storePageTransaction(final long txID, final PageTransactionInfo pageTransaction) throws Exception
   {
   }

   public void updatePageTransaction(final long txID, final PageTransactionInfo pageTransaction) throws Exception
   {
   }

   public void updateDeliveryCount(final MessageReference ref) throws Exception
   {
   }

   public void storeDuplicateID(final SimpleString address, final byte[] duplID, final long recordID) throws Exception
   {
   }

   public void storeDuplicateIDTransactional(final long txID,
                                             final SimpleString address,
                                             final byte[] duplID,
                                             final long recordID) throws Exception
   {
   }

   public void updateDuplicateID(final SimpleString address, final byte[] duplID, final long recordID) throws Exception
   {
   }

   public void updateDuplicateIDTransactional(final long txID,
                                              final SimpleString address,
                                              final byte[] duplID,
                                              final long recordID) throws Exception
   {
   }

   public long storeHeuristicCompletion(final Xid xid, final boolean isCommit) throws Exception
   {
      return generateUniqueID();
   }

   public void deleteHeuristicCompletion(final long txID) throws Exception
   {
   }

   public void addQueueBinding(long tx, Binding binding) throws Exception
   {
   }

    /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#createLargeMessageStorage(long, int, int)
    */
   public LargeServerMessage createLargeMessage()
   {
      return new NullStorageLargeServerMessage();
   }

   public LargeServerMessage createLargeMessage(final long id, final MessageInternal message)
   {
      NullStorageLargeServerMessage largeMessage = new NullStorageLargeServerMessage();

      largeMessage.copyHeadersAndProperties(message);

      largeMessage.setMessageID(id);

      return largeMessage;
   }

   public long generateUniqueID()
   {
      long id = idSequence.getAndIncrement();

      return id;
   }

   public long getCurrentUniqueID()
   {
      return idSequence.get();
   }

   public void setUniqueIDSequence(final long id)
   {
      idSequence.set(id);
   }

   public synchronized void start() throws Exception
   {
      if (started)
      {
         throw new IllegalStateException("Already started");
      }

      started = true;
   }

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         throw new IllegalStateException("Not started");
      }

      idSequence.set(0);

      started = false;
   }

   public synchronized boolean isStarted()
   {
      return started;
   }

   public void deleteMessageTransactional(final long txID, final long messageID, final long queueID) throws Exception
   {
   }

   @Override
   public JournalLoadInformation loadMessageJournal(final PostOffice postOffice,
                                                    final PagingManager pagingManager,
                                                    final ResourceManager resourceManager,
 final Map<Long, Queue> queues,
            Map<Long, QueueBindingInfo> queueInfos, final Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap,
            final Set<Pair<Long, Long>> pendingLargeMessages) throws Exception
   {
      return new JournalLoadInformation();
   }

   public void deleteDuplicateIDTransactional(final long txID, final long recordID) throws Exception
   {
   }

   public void deleteDuplicateID(final long recordID) throws Exception
   {
   }

   public JournalLoadInformation[] loadInternalOnly() throws Exception
   {
      return null;
   }

   public void completeOperations()
   {
   }

   public void pageClosed(final SimpleString storeName, final int pageNumber)
   {
   }

   public void pageDeleted(final SimpleString storeName, final int pageNumber)
   {
   }

   public void pageWrite(final PagedMessage message, final int pageNumber)
   {
   }

   public void addGrouping(final GroupBinding groupBinding) throws Exception
   {
   }

   public void deleteGrouping(final GroupBinding groupBinding) throws Exception
   {
   }

   public boolean waitOnOperations(final long timeout) throws Exception
   {
      return true;
   }

   public void setReplicator(final ReplicationManager replicator)
   {
      throw new IllegalStateException("Null Persistence should never be used as replicated");
   }

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
      return dummyContext;
   }

   @Override
   public OperationContext newContext(final Executor executor)
   {
      return dummyContext;
   }

   @Override
   public OperationContext newSingleThreadContext()
   {
      return dummyContext;
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
   public void storeAddressSetting(PersistedAddressSetting addressSetting) throws Exception
   {
   }

   @Override
   public List<PersistedRoles> recoverPersistedRoles() throws Exception
   {
      return Collections.emptyList();
   }

   @Override
   public void storeSecurityRoles(PersistedRoles persistedRoles) throws Exception
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#deleteAddressSetting(org.hornetq.api.core.SimpleString)
    */
   public void deleteAddressSetting(SimpleString addressMatch) throws Exception
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#deleteSecurityRoles(org.hornetq.api.core.SimpleString)
    */
   public void deleteSecurityRoles(SimpleString addressMatch) throws Exception
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#deletePageTransactional(long)
    */
   public void deletePageTransactional(long recordID) throws Exception
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#updatePageTransaction(long, org.hornetq.core.paging.PageTransactionInfo, int)
    */
   public void updatePageTransaction(long txID, PageTransactionInfo pageTransaction, int depage) throws Exception
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#storeCursorAcknowledge(long, org.hornetq.core.paging.cursor.PagePosition)
    */
   public void storeCursorAcknowledge(long queueID, PagePosition position)
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#storeCursorAcknowledgeTransactional(long, long, org.hornetq.core.paging.cursor.PagePosition)
    */
   public void storeCursorAcknowledgeTransactional(long txID, long queueID, PagePosition position)
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#deleteCursorAcknowledgeTransactional(long, long)
    */
   public void deleteCursorAcknowledgeTransactional(long txID, long ackID) throws Exception
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#updatePageTransaction(org.hornetq.core.paging.PageTransactionInfo, int)
    */
   public void updatePageTransaction(PageTransactionInfo pageTransaction, int depage) throws Exception
   {
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#storePageCounter(long, long, long)
    */
   public long storePageCounter(long txID, long queueID, long value) throws Exception
   {
      return 0;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#deleteIncrementRecord(long, long)
    */
   public void deleteIncrementRecord(long txID, long recordID) throws Exception
   {
   }

   @Override
   public void deletePageCounter(long txID, long recordID) throws Exception
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
   public void commit(long txID, boolean lineUpContext) throws Exception
   {
   }

   @Override
   public void lineUpContext()
   {
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
   public void stop(boolean ioCriticalError) throws Exception
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
   public void startReplication(ReplicationManager replicationManager, PagingManager pagingManager, String nodeID,
                                boolean autoFailBack) throws Exception
   {
      // no-op
   }

   @Override
   public boolean addToPage(PagingManager manager,
      SimpleString address,
      ServerMessage message,
      RoutingContext ctx,
      RouteContextList listCtx) throws Exception
   {
      return false;
   }

   @Override
   public void stopReplication()
   {
      // no-op
   }

   @Override
   public SequentialFile createFileForLargeMessage(long messageID, String extension)
   {
      throw new UnsupportedOperationException();
   }

   @Override
   public void addBytesToLargeMessage(SequentialFile appendFile, long messageID, byte[] bytes) throws Exception
   {
      // no-op
   }

   public void beforePageRead() throws Exception
   {
   }

   public void afterPageRead() throws Exception
   {
   }

   public ByteBuffer allocateDirectBuffer(int size)
   {
      return ByteBuffer.allocateDirect(size);
   }

   public void freeDirectBuffer(ByteBuffer buffer)
   {
      // We can just have hope on GC here :-)
   }

   @Override
   public void storeID(long journalID, long id) throws Exception
   {

   }
 }
