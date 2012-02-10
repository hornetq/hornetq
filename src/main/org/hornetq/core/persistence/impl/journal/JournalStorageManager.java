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

package org.hornetq.core.persistence.impl.journal;

import java.io.File;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import javax.transaction.xa.Xid;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.Message;
import org.hornetq.api.core.Pair;
import org.hornetq.api.core.SimpleString;
import org.hornetq.core.config.Configuration;
import org.hornetq.core.config.impl.ConfigurationImpl;
import org.hornetq.core.filter.Filter;
import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.IOAsyncTask;
import org.hornetq.core.journal.IOCriticalErrorListener;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.SequentialFile;
import org.hornetq.core.journal.SequentialFileFactory;
import org.hornetq.core.journal.TransactionFailureCallback;
import org.hornetq.core.journal.impl.AIOSequentialFileFactory;
import org.hornetq.core.journal.impl.JournalFile;
import org.hornetq.core.journal.impl.JournalImpl;
import org.hornetq.core.journal.impl.JournalReaderCallback;
import org.hornetq.core.journal.impl.NIOSequentialFileFactory;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.message.impl.MessageInternal;
import org.hornetq.core.paging.PageTransactionInfo;
import org.hornetq.core.paging.PagedMessage;
import org.hornetq.core.paging.PagingManager;
import org.hornetq.core.paging.PagingStore;
import org.hornetq.core.paging.cursor.PagePosition;
import org.hornetq.core.paging.cursor.PageSubscription;
import org.hornetq.core.paging.cursor.PagedReferenceImpl;
import org.hornetq.core.paging.cursor.impl.PagePositionImpl;
import org.hornetq.core.paging.impl.PageTransactionInfoImpl;
import org.hornetq.core.persistence.GroupingInfo;
import org.hornetq.core.persistence.OperationContext;
import org.hornetq.core.persistence.QueueBindingInfo;
import org.hornetq.core.persistence.StorageManager;
import org.hornetq.core.persistence.config.PersistedAddressSetting;
import org.hornetq.core.persistence.config.PersistedRoles;
import org.hornetq.core.persistence.impl.journal.BatchingIDGenerator.IDCounterEncoding;
import org.hornetq.core.postoffice.Binding;
import org.hornetq.core.postoffice.DuplicateIDCache;
import org.hornetq.core.postoffice.PostOffice;
import org.hornetq.core.replication.ReplicationManager;
import org.hornetq.core.replication.impl.ReplicatedJournal;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.ServerMessage;
import org.hornetq.core.server.group.impl.GroupBinding;
import org.hornetq.core.server.impl.ServerMessageImpl;
import org.hornetq.core.transaction.ResourceManager;
import org.hornetq.core.transaction.Transaction;
import org.hornetq.core.transaction.Transaction.State;
import org.hornetq.core.transaction.TransactionOperation;
import org.hornetq.core.transaction.TransactionPropertyIndexes;
import org.hornetq.core.transaction.impl.TransactionImpl;
import org.hornetq.utils.Base64;
import org.hornetq.utils.DataConstants;
import org.hornetq.utils.ExecutorFactory;
import org.hornetq.utils.HornetQThreadFactory;
import org.hornetq.utils.XidCodecSupport;

/**
 * 
 * A JournalStorageManager
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 *
 */
public class JournalStorageManager implements StorageManager
{
   private static final Logger log = Logger.getLogger(JournalStorageManager.class);

   private static final long CHECKPOINT_BATCH_SIZE = Integer.MAX_VALUE;

   // grouping journal record type
   public static final byte GROUP_RECORD = 20;

   // Bindings journal record type

   public static final byte QUEUE_BINDING_RECORD = 21;

   public static final byte ID_COUNTER_RECORD = 24;

   public static final byte ADDRESS_SETTING_RECORD = 25;

   public static final byte SECURITY_RECORD = 26;

   // Message journal record types

   // This is used when a large message is created but not yet stored on the system.
   // We use this to avoid temporary files missing
   public static final byte ADD_LARGE_MESSAGE_PENDING = 29;

   public static final byte ADD_LARGE_MESSAGE = 30;

   public static final byte ADD_MESSAGE = 31;

   public static final byte ADD_REF = 32;

   public static final byte ACKNOWLEDGE_REF = 33;

   public static final byte UPDATE_DELIVERY_COUNT = 34;

   public static final byte PAGE_TRANSACTION = 35;

   public static final byte SET_SCHEDULED_DELIVERY_TIME = 36;

   public static final byte DUPLICATE_ID = 37;

   public static final byte HEURISTIC_COMPLETION = 38;

   public static final byte ACKNOWLEDGE_CURSOR = 39;

   public static final byte PAGE_CURSOR_COUNTER_VALUE = 40;

   public static final byte PAGE_CURSOR_COUNTER_INC = 41;
   
   private final Semaphore pageMaxConcurrentIO;

   private final BatchingIDGenerator idGenerator;

   private final ReplicationManager replicator;

   private final Journal messageJournal;

   private final Journal bindingsJournal;

   private final SequentialFileFactory largeMessagesFactory;
   
   private SequentialFileFactory journalFF = null;

   private volatile boolean started;

   /** Used to create Operation Contexts */
   private final ExecutorFactory executorFactory;

   private final Executor executor;

   private ExecutorService singleThreadExecutor;

   private final boolean syncTransactional;

   private final boolean syncNonTransactional;

   private final int perfBlastPages;

   private final boolean createBindingsDir;

   private final String bindingsDir;

   private final boolean createJournalDir;

   private final String journalDir;

   private final String largeMessagesDirectory;

   private boolean journalLoaded = false;

   // Persisted core configuration
   private final Map<SimpleString, PersistedRoles> mapPersistedRoles = new ConcurrentHashMap<SimpleString, PersistedRoles>();

   private final Map<SimpleString, PersistedAddressSetting> mapPersistedAddressSettings = new ConcurrentHashMap<SimpleString, PersistedAddressSetting>();

   public JournalStorageManager(final Configuration config, final ExecutorFactory executorFactory)
   {
      this(config, executorFactory, null);
   }

   public JournalStorageManager(final Configuration config,
                                final ExecutorFactory executorFactory,
                                final IOCriticalErrorListener criticalErrorListener)
   {
      this(config, executorFactory, null, criticalErrorListener);
   }

   public JournalStorageManager(final Configuration config,
                                final ExecutorFactory executorFactory,
                                final ReplicationManager replicator,
                                final IOCriticalErrorListener criticalErrorListener)
   {
      this.executorFactory = executorFactory;

      executor = executorFactory.getExecutor();

      this.replicator = replicator;

      if (config.getJournalType() != JournalType.NIO && config.getJournalType() != JournalType.ASYNCIO)
      {
         throw new IllegalArgumentException("Only NIO and AsyncIO are supported journals");
      }

      bindingsDir = config.getBindingsDirectory();

      if (bindingsDir == null)
      {
         throw new NullPointerException("bindings-dir is null");
      }

      createBindingsDir = config.isCreateBindingsDir();

      journalDir = config.getJournalDirectory();

      SequentialFileFactory bindingsFF = new NIOSequentialFileFactory(bindingsDir, criticalErrorListener);

      Journal localBindings = new JournalImpl(1024 * 1024,
                                              2,
                                              config.getJournalCompactMinFiles(),
                                              config.getJournalCompactPercentage(),
                                              bindingsFF,
                                              "hornetq-bindings",
                                              "bindings",
                                              1);

      if (replicator != null)
      {
         bindingsJournal = new ReplicatedJournal((byte)0, localBindings, replicator);
      }
      else
      {
         bindingsJournal = localBindings;
      }

      if (journalDir == null)
      {
         throw new NullPointerException("journal-dir is null");
      }

      createJournalDir = config.isCreateJournalDir();

      syncNonTransactional = config.isJournalSyncNonTransactional();

      syncTransactional = config.isJournalSyncTransactional();

      if (config.getJournalType() == JournalType.ASYNCIO)
      {
         JournalStorageManager.log.info("Using AIO Journal");

         journalFF = new AIOSequentialFileFactory(journalDir,
                                                  config.getJournalBufferSize_AIO(),
                                                  config.getJournalBufferTimeout_AIO(),
                                                  config.isLogJournalWriteRate(),
                                                  criticalErrorListener);
      }
      else if (config.getJournalType() == JournalType.NIO)
      {
         JournalStorageManager.log.info("Using NIO Journal");
         journalFF = new NIOSequentialFileFactory(journalDir,
                                                  true,
                                                  config.getJournalBufferSize_NIO(),
                                                  config.getJournalBufferTimeout_NIO(),
                                                  config.isLogJournalWriteRate(),
                                                  criticalErrorListener);
      }
      else
      {
         throw new IllegalArgumentException("Unsupported journal type " + config.getJournalType());
      }

      if (config.isBackup() && !config.isSharedStore())
      {
         idGenerator = null;
      }
      else
      {
         idGenerator = new BatchingIDGenerator(0, JournalStorageManager.CHECKPOINT_BATCH_SIZE, bindingsJournal);
      }
      Journal localMessage = new JournalImpl(config.getJournalFileSize(),
                                             config.getJournalMinFiles(),
                                             config.getJournalCompactMinFiles(),
                                             config.getJournalCompactPercentage(),
                                             journalFF,
                                             "hornetq-data",
                                             "hq",
                                             config.getJournalType() == JournalType.ASYNCIO ? config.getJournalMaxIO_AIO()
                                                                                           : config.getJournalMaxIO_NIO());

      if (replicator != null)
      {
         messageJournal = new ReplicatedJournal((byte)1, localMessage, replicator);
      }
      else
      {
         messageJournal = localMessage;
      }

      largeMessagesDirectory = config.getLargeMessagesDirectory();

      largeMessagesFactory = new NIOSequentialFileFactory(largeMessagesDirectory, false, criticalErrorListener);

      perfBlastPages = config.getJournalPerfBlastPages();
      
      if (config.getPageMaxConcurrentIO() != 1)
      {
         pageMaxConcurrentIO = new Semaphore(config.getPageMaxConcurrentIO());
      }
      else
      {
         pageMaxConcurrentIO = null;
      }
   }

   public void clearContext()
   {
      OperationContextImpl.clearContext();
   }

   public boolean isReplicated()
   {
      return replicator != null;
   }

   public void waitOnOperations() throws Exception
   {
      if (!started)
      {
         JournalStorageManager.log.warn("Server is stopped");
         throw new IllegalStateException("Server is stopped");
      }
      waitOnOperations(0);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#blockOnReplication()
    */
   public boolean waitOnOperations(final long timeout) throws Exception
   {
      if (!started)
      {
         JournalStorageManager.log.warn("Server is stopped");
         throw new IllegalStateException("Server is stopped");
      }
      return getContext().waitCompletion(timeout);
   }

   /*
    *
    * (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#pageClosed(org.hornetq.utils.SimpleString, int)
    */
   public void pageClosed(final SimpleString storeName, final int pageNumber)
   {
      if (isReplicated())
      {
         replicator.pageClosed(storeName, pageNumber);
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#pageDeleted(org.hornetq.utils.SimpleString, int)
    */
   public void pageDeleted(final SimpleString storeName, final int pageNumber)
   {
      if (isReplicated())
      {
         replicator.pageDeleted(storeName, pageNumber);
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#pageWrite(org.hornetq.utils.SimpleString, int, org.hornetq.api.core.buffers.ChannelBuffer)
    */
   public void pageWrite(final PagedMessage message, final int pageNumber)
   {
      if (isReplicated())
      {
         replicator.pageWrite(message, pageNumber);
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#getContext()
    */
   public OperationContext getContext()
   {
      return OperationContextImpl.getContext(executorFactory);
   }

   public void setContext(final OperationContext context)
   {
      OperationContextImpl.setContext(context);
   }

   public Executor getSingleThreadExecutor()
   {
      return singleThreadExecutor;
   }

   public OperationContext newSingleThreadContext()
   {
      return newContext(singleThreadExecutor);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#newContext()
    */
   public OperationContext newContext(final Executor executor)
   {
      return new OperationContextImpl(executor);
   }

   public void afterCompleteOperations(final IOAsyncTask run)
   {
      getContext().executeOnCompletion(run);
   }

   public long generateUniqueID()
   {
      long id = idGenerator.generateID();

      return id;
   }

   public long getCurrentUniqueID()
   {
      return idGenerator.getCurrentID();
   }

   public LargeServerMessage createLargeMessage()
   {
      return new LargeServerMessageImpl(this);
   }

   public void addBytesToLargeMessage(final SequentialFile file, final long messageId, final byte[] bytes) throws Exception
   {
      file.position(file.size());

      file.writeDirect(ByteBuffer.wrap(bytes), false);

      if (isReplicated())
      {
         replicator.largeMessageWrite(messageId, bytes);
      }
   }

   public LargeServerMessage createLargeMessage(final long id, final MessageInternal message) throws Exception
   {
      if (isReplicated())
      {
         replicator.largeMessageBegin(id);
      }

      LargeServerMessageImpl largeMessage = (LargeServerMessageImpl)createLargeMessage();

      largeMessage.copyHeadersAndProperties(message);

      largeMessage.setMessageID(id);

      if (largeMessage.isDurable())
      {
         // We store a marker on the journal that the large file is pending
         long pendingRecordID = storePendingLargeMessage(id);

         largeMessage.setPendingRecordID(pendingRecordID);
      }

      return largeMessage;
   }

   // Non transactional operations

   public long storePendingLargeMessage(final long messageID) throws Exception
   {
      long recordID = generateUniqueID();

      messageJournal.appendAddRecord(recordID,
                                     ADD_LARGE_MESSAGE_PENDING,
                                     new PendingLargeMessageEncoding(messageID),
                                     true,
                                     getContext(true));

      return recordID;
   }

   public void confirmPendingLargeMessageTX(final Transaction tx, long messageID, long recordID) throws Exception
   {
      installLargeMessageConfirmationOnTX(tx, recordID);
      messageJournal.appendDeleteRecordTransactional(tx.getID(),
                                                     recordID,
                                                     new DeleteEncoding(ADD_LARGE_MESSAGE_PENDING, messageID));
   }

   /** We don't need messageID now but we are likely to need it we ever decide to support a database */
   public void confirmPendingLargeMessage(long recordID) throws Exception
   {
      messageJournal.appendDeleteRecord(recordID, true, getContext());
   }

   public void storeMessage(final ServerMessage message) throws Exception
   {
      if (message.getMessageID() <= 0)
      {
         // Sanity check only... this shouldn't happen unless there is a bug
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "MessageId was not assigned to Message");
      }

      // Note that we don't sync, the add reference that comes immediately after will sync if appropriate

      if (message.isLargeMessage())
      {
         messageJournal.appendAddRecord(message.getMessageID(),
                                        JournalStorageManager.ADD_LARGE_MESSAGE,
                                        new LargeMessageEncoding((LargeServerMessage)message),
                                        false,
                                        getContext(false));
      }
      else
      {
         messageJournal.appendAddRecord(message.getMessageID(),
                                        JournalStorageManager.ADD_MESSAGE,
                                        message,
                                        false,
                                        getContext(false));
      }
   }

   public void storeReference(final long queueID, final long messageID, final boolean last) throws Exception

   {
      messageJournal.appendUpdateRecord(messageID,
                                        JournalStorageManager.ADD_REF,
                                        new RefEncoding(queueID),
                                        last && syncNonTransactional,
                                        getContext(last && syncNonTransactional));
   }

   public void storeAcknowledge(final long queueID, final long messageID) throws Exception
   {
      messageJournal.appendUpdateRecord(messageID,
                                        JournalStorageManager.ACKNOWLEDGE_REF,
                                        new RefEncoding(queueID),
                                        syncNonTransactional,
                                        getContext(syncNonTransactional));
   }

   public void storeCursorAcknowledge(long queueID, PagePosition position) throws Exception
   {
      long ackID = idGenerator.generateID();
      position.setRecordID(ackID);
      messageJournal.appendAddRecord(ackID,
                                     ACKNOWLEDGE_CURSOR,
                                     new CursorAckRecordEncoding(queueID, position),
                                     syncNonTransactional,
                                     getContext(syncNonTransactional));
   }

   public void deleteMessage(final long messageID) throws Exception
   {
      // Messages are deleted on postACK, one after another.
      // If these deletes are synchronized, we would build up messages on the Executor
      // increasing chances of losing deletes.
      // The StorageManager should verify messages without references
      messageJournal.appendDeleteRecord(messageID, false, getContext(false));
   }

   public void updateScheduledDeliveryTime(final MessageReference ref) throws Exception
   {
      ScheduledDeliveryEncoding encoding = new ScheduledDeliveryEncoding(ref.getScheduledDeliveryTime(), ref.getQueue()
                                                                                                            .getID());

      messageJournal.appendUpdateRecord(ref.getMessage().getMessageID(),
                                        JournalStorageManager.SET_SCHEDULED_DELIVERY_TIME,
                                        encoding,
                                        syncNonTransactional,
                                        getContext(syncNonTransactional));
   }

   public void storeDuplicateID(final SimpleString address, final byte[] duplID, final long recordID) throws Exception
   {
      DuplicateIDEncoding encoding = new DuplicateIDEncoding(address, duplID);

      messageJournal.appendAddRecord(recordID,
                                     JournalStorageManager.DUPLICATE_ID,
                                     encoding,
                                     syncNonTransactional,
                                     getContext(syncNonTransactional));
   }

   public void deleteDuplicateID(final long recordID) throws Exception
   {
      messageJournal.appendDeleteRecord(recordID, syncNonTransactional, getContext(syncNonTransactional));
   }

   // Transactional operations

   public void storeMessageTransactional(final long txID, final ServerMessage message) throws Exception
   {
      if (message.getMessageID() <= 0)
      {
         throw new HornetQException(HornetQException.ILLEGAL_STATE, "MessageId was not assigned to Message");
      }

      if (message.isLargeMessage())
      {
         messageJournal.appendAddRecordTransactional(txID,
                                                     message.getMessageID(),
                                                     JournalStorageManager.ADD_LARGE_MESSAGE,
                                                     new LargeMessageEncoding(((LargeServerMessage)message)));
      }
      else
      {
         messageJournal.appendAddRecordTransactional(txID,
                                                     message.getMessageID(),
                                                     JournalStorageManager.ADD_MESSAGE,
                                                     message);
      }

   }

   public void storePageTransaction(final long txID, final PageTransactionInfo pageTransaction) throws Exception
   {
      pageTransaction.setRecordID(generateUniqueID());

      messageJournal.appendAddRecordTransactional(txID,
                                                  pageTransaction.getRecordID(),
                                                  JournalStorageManager.PAGE_TRANSACTION,
                                                  pageTransaction);
   }

   public void updatePageTransaction(final long txID, final PageTransactionInfo pageTransaction, final int depages) throws Exception
   {
      messageJournal.appendUpdateRecordTransactional(txID,
                                                     pageTransaction.getRecordID(),
                                                     JournalStorageManager.PAGE_TRANSACTION,
                                                     new PageUpdateTXEncoding(pageTransaction.getTransactionID(),
                                                                              depages));
   }

   public void updatePageTransaction(final PageTransactionInfo pageTransaction, final int depages) throws Exception
   {
      messageJournal.appendUpdateRecord(pageTransaction.getRecordID(),
                                        JournalStorageManager.PAGE_TRANSACTION,
                                        new PageUpdateTXEncoding(pageTransaction.getTransactionID(), depages),
                                        syncNonTransactional,
                                        getContext(syncNonTransactional));
   }

   public void storeReferenceTransactional(final long txID, final long queueID, final long messageID) throws Exception
   {
      messageJournal.appendUpdateRecordTransactional(txID,
                                                     messageID,
                                                     JournalStorageManager.ADD_REF,
                                                     new RefEncoding(queueID));
   }

   public void storeAcknowledgeTransactional(final long txID, final long queueID, final long messageID) throws Exception
   {
      messageJournal.appendUpdateRecordTransactional(txID,
                                                     messageID,
                                                     JournalStorageManager.ACKNOWLEDGE_REF,
                                                     new RefEncoding(queueID));
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#storeCursorAcknowledgeTransactional(long, long, org.hornetq.core.paging.cursor.PagePosition)
    */
   public void storeCursorAcknowledgeTransactional(long txID, long queueID, PagePosition position) throws Exception
   {
      long ackID = idGenerator.generateID();
      position.setRecordID(ackID);
      messageJournal.appendAddRecordTransactional(txID,
                                                  ackID,
                                                  ACKNOWLEDGE_CURSOR,
                                                  new CursorAckRecordEncoding(queueID, position));
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#deleteCursorAcknowledgeTransactional(long, long)
    */
   public void deleteCursorAcknowledgeTransactional(long txID, long ackID) throws Exception
   {
      messageJournal.appendDeleteRecordTransactional(txID, ackID);
   }

   public long storeHeuristicCompletion(final Xid xid, final boolean isCommit) throws Exception
   {
      long id = generateUniqueID();

      messageJournal.appendAddRecord(id,
                                     JournalStorageManager.HEURISTIC_COMPLETION,
                                     new HeuristicCompletionEncoding(xid, isCommit),
                                     true,
                                     getContext(true));
      return id;
   }

   public void deleteHeuristicCompletion(final long id) throws Exception
   {
      messageJournal.appendDeleteRecord(id, true, getContext(true));
   }

   public void deletePageTransactional(final long recordID) throws Exception
   {
      messageJournal.appendDeleteRecord(recordID, false);
   }

   public void updateScheduledDeliveryTimeTransactional(final long txID, final MessageReference ref) throws Exception
   {
      ScheduledDeliveryEncoding encoding = new ScheduledDeliveryEncoding(ref.getScheduledDeliveryTime(), ref.getQueue()
                                                                                                            .getID());

      messageJournal.appendUpdateRecordTransactional(txID,
                                                     ref.getMessage().getMessageID(),
                                                     JournalStorageManager.SET_SCHEDULED_DELIVERY_TIME,
                                                     encoding);
   }

   public void prepare(final long txID, final Xid xid) throws Exception
   {
      messageJournal.appendPrepareRecord(txID, new XidEncoding(xid), syncTransactional, getContext(syncTransactional));
   }

   public void commit(final long txID) throws Exception
   {
      commit(txID, true);
   }

   public void commitBindings(final long txID) throws Exception
   {
      bindingsJournal.appendCommitRecord(txID, true);
   }
   
   public void rollbackBindings(final long txID) throws Exception
   {
      // no need to sync, it's going away anyways
      bindingsJournal.appendRollbackRecord(txID, false);
   }

   public void commit(final long txID, final boolean lineUpContext) throws Exception
   {
      messageJournal.appendCommitRecord(txID, syncTransactional, getContext(syncTransactional), lineUpContext);
      if (!lineUpContext && !syncTransactional)
      {
         // if lineUpContext == false, we have previously lined up a context, hence we need to mark it as done even if
         // syncTransactional = false
         getContext(true).done();
      }
   }

   public void rollback(final long txID) throws Exception
   {
      messageJournal.appendRollbackRecord(txID, syncTransactional, getContext(syncTransactional));
   }

   public void storeDuplicateIDTransactional(final long txID,
                                             final SimpleString address,
                                             final byte[] duplID,
                                             final long recordID) throws Exception
   {
      DuplicateIDEncoding encoding = new DuplicateIDEncoding(address, duplID);

      messageJournal.appendAddRecordTransactional(txID, recordID, JournalStorageManager.DUPLICATE_ID, encoding);
   }

   public void updateDuplicateIDTransactional(final long txID,
                                              final SimpleString address,
                                              final byte[] duplID,
                                              final long recordID) throws Exception
   {
      DuplicateIDEncoding encoding = new DuplicateIDEncoding(address, duplID);

      messageJournal.appendUpdateRecordTransactional(txID, recordID, JournalStorageManager.DUPLICATE_ID, encoding);
   }

   public void deleteDuplicateIDTransactional(final long txID, final long recordID) throws Exception
   {
      messageJournal.appendDeleteRecordTransactional(txID, recordID);
   }

   // Other operations

   public void updateDeliveryCount(final MessageReference ref) throws Exception
   {
      // no need to store if it's the same value
      // otherwise the journal will get OME in case of lots of redeliveries
      if (ref.getDeliveryCount() != ref.getPersistedCount())
      {
         ref.setPersistedCount(ref.getDeliveryCount());
         DeliveryCountUpdateEncoding updateInfo = new DeliveryCountUpdateEncoding(ref.getQueue().getID(),
                                                                                  ref.getDeliveryCount());

         messageJournal.appendUpdateRecord(ref.getMessage().getMessageID(),
                                           JournalStorageManager.UPDATE_DELIVERY_COUNT,
                                           updateInfo,

                                           syncNonTransactional,
                                           getContext(syncNonTransactional));
      }

   }

   public void storeAddressSetting(PersistedAddressSetting addressSetting) throws Exception
   {
      deleteAddressSetting(addressSetting.getAddressMatch());
      long id = idGenerator.generateID();
      addressSetting.setStoreId(id);
      bindingsJournal.appendAddRecord(id, ADDRESS_SETTING_RECORD, addressSetting, true);
      mapPersistedAddressSettings.put(addressSetting.getAddressMatch(), addressSetting);
   }

   public List<PersistedAddressSetting> recoverAddressSettings() throws Exception
   {
      ArrayList<PersistedAddressSetting> list = new ArrayList<PersistedAddressSetting>(mapPersistedAddressSettings.size());
      list.addAll(mapPersistedAddressSettings.values());
      return list;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#recoverPersistedRoles()
    */
   public List<PersistedRoles> recoverPersistedRoles() throws Exception
   {
      ArrayList<PersistedRoles> list = new ArrayList<PersistedRoles>(mapPersistedRoles.size());
      list.addAll(mapPersistedRoles.values());
      return list;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#storeSecurityRoles(org.hornetq.core.persistconfig.PersistedRoles)
    */
   public void storeSecurityRoles(PersistedRoles persistedRoles) throws Exception
   {

      deleteSecurityRoles(persistedRoles.getAddressMatch());
      long id = idGenerator.generateID();
      persistedRoles.setStoreId(id);
      bindingsJournal.appendAddRecord(id, SECURITY_RECORD, persistedRoles, true);
      mapPersistedRoles.put(persistedRoles.getAddressMatch(), persistedRoles);
   }

   public void deleteAddressSetting(SimpleString addressMatch) throws Exception
   {
      PersistedAddressSetting oldSetting = mapPersistedAddressSettings.remove(addressMatch);
      if (oldSetting != null)
      {
         bindingsJournal.appendDeleteRecord(oldSetting.getStoreId(), false);
      }

   }

   public void deleteSecurityRoles(SimpleString addressMatch) throws Exception
   {
      PersistedRoles oldRoles = mapPersistedRoles.remove(addressMatch);
      if (oldRoles != null)
      {
         bindingsJournal.appendDeleteRecord(oldRoles.getStoreId(), false);
      }
   }

   public JournalLoadInformation loadMessageJournal(final PostOffice postOffice,
                                                    final PagingManager pagingManager,
                                                    final ResourceManager resourceManager,
                                                    final Map<Long, Queue> queues,
                                                    Map<Long, QueueBindingInfo> queueInfos,
                                                    final Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap,
                                                    final Set<Pair<Long, Long>> pendingLargeMessages) throws Exception
   {
      List<RecordInfo> records = new ArrayList<RecordInfo>();

      List<PreparedTransactionInfo> preparedTransactions = new ArrayList<PreparedTransactionInfo>();

      Map<Long, ServerMessage> messages = new HashMap<Long, ServerMessage>();

      JournalLoadInformation info = messageJournal.load(records,
                                                        preparedTransactions,
                                                        new LargeMessageTXFailureCallback(messages));

      ArrayList<LargeServerMessage> largeMessages = new ArrayList<LargeServerMessage>();

      Map<Long, Map<Long, AddMessageRecord>> queueMap = new HashMap<Long, Map<Long, AddMessageRecord>>();

      Map<Long, PageSubscription> pageSubscriptions = new HashMap<Long, PageSubscription>();

      final int totalSize = records.size();

      for (int reccount = 0; reccount < totalSize; reccount++)
      {
         // It will show log.info only with large journals (more than 1 million records)
         if (reccount > 0 && reccount % 1000000 == 0)
         {
            long percent = (long)((((double)reccount) / ((double)totalSize)) * 100f);

            log.info(percent + "% loaded");
         }

         RecordInfo record = records.get(reccount);
         byte[] data = record.data;

         HornetQBuffer buff = HornetQBuffers.wrappedBuffer(data);

         byte recordType = record.getUserRecordType();

         switch (recordType)
         {
            case ADD_LARGE_MESSAGE_PENDING:
            {
               PendingLargeMessageEncoding pending = new PendingLargeMessageEncoding();

               pending.decode(buff);

               if (pendingLargeMessages != null)
               {
                  // it could be null on tests, and we don't need anything on that case
                  pendingLargeMessages.add(new Pair<Long, Long>(record.id, pending.largeMessageID));
               }
               break;
            }
            case ADD_LARGE_MESSAGE:
            {
               LargeServerMessage largeMessage = parseLargeMessage(messages, buff);

               messages.put(record.id, largeMessage);

               largeMessages.add(largeMessage);

               break;
            }
            case ADD_MESSAGE:
            {
               ServerMessage message = new ServerMessageImpl(record.id, 50);

               message.decode(buff);

               messages.put(record.id, message);

               break;
            }
            case ADD_REF:
            {
               long messageID = record.id;

               RefEncoding encoding = new RefEncoding();

               encoding.decode(buff);

               Map<Long, AddMessageRecord> queueMessages = queueMap.get(encoding.queueID);

               if (queueMessages == null)
               {
                  queueMessages = new LinkedHashMap<Long, AddMessageRecord>();

                  queueMap.put(encoding.queueID, queueMessages);
               }

               ServerMessage message = messages.get(messageID);

               if (message == null)
               {
                  log.error("Cannot find message " + record.id);
               }
               else
               {
                  queueMessages.put(messageID, new AddMessageRecord(message));
               }

               break;
            }
            case ACKNOWLEDGE_REF:
            {
               long messageID = record.id;

               RefEncoding encoding = new RefEncoding();

               encoding.decode(buff);

               Map<Long, AddMessageRecord> queueMessages = queueMap.get(encoding.queueID);

               if (queueMessages == null)
               {
                  log.error("Cannot find queue messages for queueID=" + encoding.queueID +
                            " on ack for messageID=" +
                            messageID);
               }
               else
               {
                  AddMessageRecord rec = queueMessages.remove(messageID);

                  if (rec == null)
                  {
                     log.error("Cannot find message " + messageID);
                  }
               }

               break;
            }
            case UPDATE_DELIVERY_COUNT:
            {
               long messageID = record.id;

               DeliveryCountUpdateEncoding encoding = new DeliveryCountUpdateEncoding();

               encoding.decode(buff);

               Map<Long, AddMessageRecord> queueMessages = queueMap.get(encoding.queueID);

               if (queueMessages == null)
               {
                  log.warn("Cannot find queue " + encoding.queueID + " to update delivery count");
               }
               else
               {
                  AddMessageRecord rec = queueMessages.get(messageID);

                  if (rec == null)
                  {
                     log.warn("Cannot find message " + messageID + " to update delivery count");
                  }
                  else
                  {
                     rec.deliveryCount = encoding.count;
                  }
               }

               break;
            }
            case PAGE_TRANSACTION:
            {
               if (record.isUpdate)
               {
                  PageUpdateTXEncoding pageUpdate = new PageUpdateTXEncoding();

                  pageUpdate.decode(buff);

                  PageTransactionInfo pageTX = pagingManager.getTransaction(pageUpdate.pageTX);

                  pageTX.onUpdate(pageUpdate.recods, null, null);
               }
               else
               {
                  PageTransactionInfoImpl pageTransactionInfo = new PageTransactionInfoImpl();

                  pageTransactionInfo.decode(buff);

                  pageTransactionInfo.setRecordID(record.id);

                  pagingManager.addTransaction(pageTransactionInfo);
               }

               break;
            }
            case SET_SCHEDULED_DELIVERY_TIME:
            {
               long messageID = record.id;

               ScheduledDeliveryEncoding encoding = new ScheduledDeliveryEncoding();

               encoding.decode(buff);

               Map<Long, AddMessageRecord> queueMessages = queueMap.get(encoding.queueID);

               if (queueMessages == null)
               {
                  log.error("Cannot find queue messages " + encoding.queueID +
                            " for message " +
                            messageID +
                            " while processing scheduled messages");
               }
               else
               {

                  AddMessageRecord rec = queueMessages.get(messageID);

                  if (rec == null)
                  {
                     log.error("Cannot find message " + messageID);
                  }
                  else
                  {
                     rec.scheduledDeliveryTime = encoding.scheduledDeliveryTime;
                  }
               }

               break;
            }
            case DUPLICATE_ID:
            {
               DuplicateIDEncoding encoding = new DuplicateIDEncoding();

               encoding.decode(buff);

               List<Pair<byte[], Long>> ids = duplicateIDMap.get(encoding.address);

               if (ids == null)
               {
                  ids = new ArrayList<Pair<byte[], Long>>();

                  duplicateIDMap.put(encoding.address, ids);
               }

               ids.add(new Pair<byte[], Long>(encoding.duplID, record.id));

               break;
            }
            case HEURISTIC_COMPLETION:
            {
               HeuristicCompletionEncoding encoding = new HeuristicCompletionEncoding();
               encoding.decode(buff);
               resourceManager.putHeuristicCompletion(record.id, encoding.xid, encoding.isCommit);
               break;
            }
            case ACKNOWLEDGE_CURSOR:
            {
               CursorAckRecordEncoding encoding = new CursorAckRecordEncoding();
               encoding.decode(buff);

               encoding.position.setRecordID(record.id);

               PageSubscription sub = locateSubscription(encoding.queueID, pageSubscriptions, queueInfos, pagingManager);

               if (sub != null)
               {
                  sub.reloadACK(encoding.position);
               }
               else
               {
                  log.info("Can't find queue " + encoding.queueID + " while reloading ACKNOWLEDGE_CURSOR, deleting record now");
                  messageJournal.appendDeleteRecord(record.id, false);
                  
               }

               break;
            }
            case PAGE_CURSOR_COUNTER_VALUE:
            {
               PageCountRecord encoding = new PageCountRecord();

               encoding.decode(buff);

               PageSubscription sub = locateSubscription(encoding.queueID, pageSubscriptions, queueInfos, pagingManager);

               if (sub != null)
               {
                  sub.getCounter().loadValue(record.id, encoding.value);
               }
               else
               {
                  log.info("Can't find queue " + encoding.queueID + " while reloading PAGE_CURSOR_COUNTER_VALUE, deleting record now");
                  messageJournal.appendDeleteRecord(record.id, false);
               }

               break;
            }

            case PAGE_CURSOR_COUNTER_INC:
            {
               PageCountRecordInc encoding = new PageCountRecordInc();

               encoding.decode(buff);

               PageSubscription sub = locateSubscription(encoding.queueID, pageSubscriptions, queueInfos, pagingManager);

               if (sub != null)
               {
                  sub.getCounter().loadInc(record.id, encoding.value);
               }
               else
               {
                  log.info("Can't find queue " + encoding.queueID + " while reloading PAGE_CURSOR_COUNTER_INC, deleting record now");
                  messageJournal.appendDeleteRecord(record.id, false);
               }

               break;
            }
            default:
            {
               throw new IllegalStateException("Invalid record type " + recordType);
            }
         }

         // This will free up memory sooner. The record is not needed any more
         // and its byte array would consume memory during the load process even though it's not necessary any longer
         // what would delay processing time during load
         records.set(reccount, null);
      }

      // Release the memory as soon as not needed any longer
      records.clear();
      records = null;

      for (Map.Entry<Long, Map<Long, AddMessageRecord>> entry : queueMap.entrySet())
      {
         long queueID = entry.getKey();

         Map<Long, AddMessageRecord> queueRecords = entry.getValue();

         Queue queue = queues.get(queueID);

         if (queue == null)
         {
            log.warn("Message for queue " + queueID + " which does not exist. This message will be ignored.");

            continue;
         }

         // Redistribution could install a Redistributor while we are still loading records, what will be an issue with
         // prepared ACKs
         // We make sure te Queue is paused before we reroute values.
         queue.pause();

         Collection<AddMessageRecord> valueRecords = queueRecords.values();

         long currentTime = System.currentTimeMillis();

         for (AddMessageRecord record : valueRecords)
         {
            long scheduledDeliveryTime = record.scheduledDeliveryTime;

            if (scheduledDeliveryTime != 0 && scheduledDeliveryTime <= currentTime)
            {
               scheduledDeliveryTime = 0;
               record.message.removeProperty(Message.HDR_SCHEDULED_DELIVERY_TIME);
            }

            if (scheduledDeliveryTime != 0)
            {
               record.message.putLongProperty(Message.HDR_SCHEDULED_DELIVERY_TIME, scheduledDeliveryTime);
            }

            MessageReference ref = postOffice.reroute(record.message, queue, null);

            ref.setDeliveryCount(record.deliveryCount);

            if (scheduledDeliveryTime != 0)
            {
               record.message.removeProperty(Message.HDR_SCHEDULED_DELIVERY_TIME);
            }
         }
      }

      loadPreparedTransactions(postOffice,
                               pagingManager,
                               resourceManager,
                               queues,
                               queueInfos,
                               preparedTransactions,
                               duplicateIDMap,
                               pageSubscriptions,
                               pendingLargeMessages);

      for (PageSubscription sub : pageSubscriptions.values())
      {
         sub.getCounter().processReload();
      }

      for (LargeServerMessage msg : largeMessages)
      {
         if (msg.getRefCount() == 0)
         {
            JournalStorageManager.log.info("Large message: " + msg.getMessageID() +
                                            " didn't have any associated reference, file will be deleted");
            msg.decrementDelayDeletionCount();
         }
      }

      for (ServerMessage msg : messages.values())
      {
         if (msg.getRefCount() == 0)
         {
            log.info("Deleting unreferenced message id=" + msg.getMessageID() + " from the journal");
            try
            {
                deleteMessage(msg.getMessageID());
            }
            catch (Exception ignored)
            {
               log.warn("It wasn't possible to delete message " + msg.getMessageID(), ignored);
            }
         }
      }

      // To recover positions on Iterators
      if (pagingManager != null)
      {
         // it could be null on certain tests that are not dealing with paging
         // This could also be the case in certain embedded conditions
         pagingManager.processReload();
      }

      if (perfBlastPages != -1)
      {
         messageJournal.perfBlast(perfBlastPages);
      }

      for (Queue queue : queues.values())
      {
         queue.resume();
      }

      if (System.getProperty("org.hornetq.opt.directblast") != null)
      {
         messageJournal.runDirectJournalBlast();
      }
      journalLoaded = true;
      return info;
   }

   /**
    * @param queueID
    * @param pageSubscriptions
    * @param queueInfos
    * @return
    */
   private PageSubscription locateSubscription(final long queueID,
                                               final Map<Long, PageSubscription> pageSubscriptions,
                                               final Map<Long, QueueBindingInfo> queueInfos,
                                               final PagingManager pagingManager) throws Exception
   {

      PageSubscription subs = pageSubscriptions.get(queueID);
      if (subs == null)
      {
         QueueBindingInfo queueInfo = queueInfos.get(queueID);

         if (queueInfo != null)
         {
            SimpleString address = queueInfo.getAddress();
            PagingStore store = pagingManager.getPageStore(address);
            subs = store.getCursorProvier().getSubscription(queueID);
            pageSubscriptions.put(queueID, subs);
         }
      }

      return subs;
   }

   // grouping handler operations
   public void addGrouping(final GroupBinding groupBinding) throws Exception
   {
      GroupingEncoding groupingEncoding = new GroupingEncoding(groupBinding.getId(),
                                                               groupBinding.getGroupId(),
                                                               groupBinding.getClusterName());
      bindingsJournal.appendAddRecord(groupBinding.getId(), JournalStorageManager.GROUP_RECORD, groupingEncoding, true);
   }

   public void deleteGrouping(final GroupBinding groupBinding) throws Exception
   {
      bindingsJournal.appendDeleteRecord(groupBinding.getId(), true);
   }

   // Bindings operations

   public void addQueueBinding(final long tx, final Binding binding) throws Exception
   {
      Queue queue = (Queue)binding.getBindable();

      Filter filter = queue.getFilter();

      SimpleString filterString = filter == null ? null : filter.getFilterString();

      PersistentQueueBindingEncoding bindingEncoding = new PersistentQueueBindingEncoding(queue.getName(),
                                                                                          binding.getAddress(),
                                                                                          filterString);

      bindingsJournal.appendAddRecordTransactional(tx, binding.getID(),
                                      JournalStorageManager.QUEUE_BINDING_RECORD,
                                      bindingEncoding);
   }

   public void deleteQueueBinding(final long queueBindingID) throws Exception
   {
      bindingsJournal.appendDeleteRecord(queueBindingID, true);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#storePageCounterAdd(long, long, int)
    */
   public long storePageCounterInc(long txID, long queueID, int value) throws Exception
   {
      long recordID = idGenerator.generateID();
      messageJournal.appendAddRecordTransactional(txID,
                                                  recordID,
                                                  JournalStorageManager.PAGE_CURSOR_COUNTER_INC,
                                                  new PageCountRecordInc(queueID, value));
      return recordID;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#storePageCounterAdd(long, long, int)
    */
   public long storePageCounterInc(long queueID, int value) throws Exception
   {
      long recordID = idGenerator.generateID();
      messageJournal.appendAddRecord(recordID,
                                     JournalStorageManager.PAGE_CURSOR_COUNTER_INC,
                                     new PageCountRecordInc(queueID, value),
                                     true,
                                     getContext());
      return recordID;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#storePageCounter(long, long, long)
    */
   public long storePageCounter(long txID, long queueID, long value) throws Exception
   {
      long recordID = idGenerator.generateID();
      messageJournal.appendAddRecordTransactional(txID,
                                                  recordID,
                                                  JournalStorageManager.PAGE_CURSOR_COUNTER_VALUE,
                                                  new PageCountRecord(queueID, value));
      return recordID;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#deleteIncrementRecord(long, long)
    */
   public void deleteIncrementRecord(long txID, long recordID) throws Exception
   {
      messageJournal.appendDeleteRecordTransactional(txID, recordID);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#deletePageCounter(long, long)
    */
   public void deletePageCounter(long txID, long recordID) throws Exception
   {
      messageJournal.appendDeleteRecordTransactional(txID, recordID);
   }

   public static void describeBindingJournal(final String bindingsDir) throws Exception
   {

      SequentialFileFactory bindingsFF = new NIOSequentialFileFactory(bindingsDir, null);

      JournalImpl bindings = new JournalImpl(1024 * 1024, 2, -1, 0, bindingsFF, "hornetq-bindings", "bindings", 1);

      describeJournal(bindingsFF, bindings);
   }

   public static void describeMessagesJournal(final String messagesDir) throws Exception
   {

      SequentialFileFactory messagesFF = new NIOSequentialFileFactory(messagesDir, null);

      // Will use only default values. The load function should adapt to anything different
      ConfigurationImpl defaultValues = new ConfigurationImpl();

      JournalImpl messagesJournal = new JournalImpl(defaultValues.getJournalFileSize(),
                                                    defaultValues.getJournalMinFiles(),
                                                    0,
                                                    0,
                                                    messagesFF,
                                                    "hornetq-data",
                                                    "hq",
                                                    1);

      describeJournal(messagesFF, messagesJournal);
   }

   public JournalLoadInformation loadBindingJournal(final List<QueueBindingInfo> queueBindingInfos,
                                                    final List<GroupingInfo> groupingInfos) throws Exception
   {
      List<RecordInfo> records = new ArrayList<RecordInfo>();

      List<PreparedTransactionInfo> preparedTransactions = new ArrayList<PreparedTransactionInfo>();

      JournalLoadInformation bindingsInfo = bindingsJournal.load(records, preparedTransactions, null);

      for (RecordInfo record : records)
      {
         long id = record.id;

         HornetQBuffer buffer = HornetQBuffers.wrappedBuffer(record.data);

         byte rec = record.getUserRecordType();

         if (rec == JournalStorageManager.QUEUE_BINDING_RECORD)
         {
            PersistentQueueBindingEncoding bindingEncoding = newBindingEncoding(id, buffer);

            queueBindingInfos.add(bindingEncoding);
         }
         else if (rec == JournalStorageManager.ID_COUNTER_RECORD)
         {
            idGenerator.loadState(record.id, buffer);
         }
         else if (rec == JournalStorageManager.GROUP_RECORD)
         {
            GroupingEncoding encoding = newGroupEncoding(id, buffer);
            groupingInfos.add(encoding);
         }
         else if (rec == JournalStorageManager.ADDRESS_SETTING_RECORD)
         {
            PersistedAddressSetting setting = newAddressEncoding(id, buffer);
            mapPersistedAddressSettings.put(setting.getAddressMatch(), setting);
         }
         else if (rec == JournalStorageManager.SECURITY_RECORD)
         {
            PersistedRoles roles = newSecurityRecord(id, buffer);
            mapPersistedRoles.put(roles.getAddressMatch(), roles);
         }
         else
         {
            throw new IllegalStateException("Invalid record type " + rec);
         }
      }

      return bindingsInfo;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#lineUpContext()
    */
   public void lineUpContext()
   {
      messageJournal.lineUpContex(getContext());
   }

   // HornetQComponent implementation
   // ------------------------------------------------------

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      checkAndCreateDir(bindingsDir, createBindingsDir);

      checkAndCreateDir(journalDir, createJournalDir);

      checkAndCreateDir(largeMessagesDirectory, createJournalDir);

      cleanupIncompleteFiles();

      singleThreadExecutor = Executors.newSingleThreadExecutor(new HornetQThreadFactory("HornetQ-IO-SingleThread",
                                                                                        true,
                                                                                        getThisClassLoader()));

      bindingsJournal.start();

      messageJournal.start();

      started = true;
   }

   public  void stop() throws Exception
   {
      stop(false);
   }

   public synchronized void stop(boolean ioCriticalError) throws Exception
   {
      if (!started)
      {
         return;
      }

      if (!ioCriticalError && journalLoaded && idGenerator != null)
      {
         // Must call close to make sure last id is persisted
         idGenerator.close();
      }

      bindingsJournal.stop();

      messageJournal.stop();

      singleThreadExecutor.shutdown();

      journalLoaded = false;

      started = false;
   }

   public synchronized boolean isStarted()
   {
      return started;
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#loadInternalOnly()
    */
   public JournalLoadInformation[] loadInternalOnly() throws Exception
   {
      JournalLoadInformation[] info = new JournalLoadInformation[2];
      info[0] = bindingsJournal.loadInternalOnly();
      info[1] = messageJournal.loadInternalOnly();

      return info;
   }


   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#startPageRead()
    */
   public void beforePageRead() throws Exception
   {
      if (pageMaxConcurrentIO != null)
      {
         pageMaxConcurrentIO.acquire();
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#finishPageRead()
    */
   public void afterPageRead() throws Exception
   {
      if (pageMaxConcurrentIO != null)
      {
         pageMaxConcurrentIO.release();
      }
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#allocateDirectBuffer(long)
    */
   public ByteBuffer allocateDirectBuffer(int size)
   {
      return journalFF.allocateDirectBuffer(size);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.persistence.StorageManager#freeDirectuffer(java.nio.ByteBuffer)
    */
   public void freeDirectBuffer(ByteBuffer buffer)
   {
      journalFF.releaseBuffer(buffer);
   }

   // Public -----------------------------------------------------------------------------------

   public Journal getMessageJournal()
   {
      return messageJournal;
   }

   public Journal getBindingsJournal()
   {
      return bindingsJournal;
   }

   // Package protected ---------------------------------------------

   // This should be accessed from this package only
   void deleteLargeMessage(final SequentialFile file)
   {
      Runnable deleteAction = new Runnable()
      {
         public void run()
         {
            try
            {
               file.delete();
            }
            catch (Exception e)
            {
               JournalStorageManager.log.warn(e.getMessage(), e);
            }
         }

      };

      if (executor == null)
      {
         deleteAction.run();
      }
      else
      {
         executor.execute(deleteAction);
      }
   }

   /**
    * @param messageID
    * @return
    */
   SequentialFile createFileForLargeMessage(final long messageID, final boolean durable)
   {
      if (durable)
      {
         return largeMessagesFactory.createSequentialFile(messageID + ".msg", -1);
      }
      else
      {
         return largeMessagesFactory.createSequentialFile(messageID + ".tmp", -1);
      }
   }

   // Private ----------------------------------------------------------------------------------

   private void checkAndCreateDir(final String dir, final boolean create)
   {
      File f = new File(dir);

      if (!f.exists())
      {
         if (create)
         {
            if (!f.mkdirs())
            {
               throw new IllegalStateException("Failed to create directory " + dir);
            }
         }
         else
         {
            throw new IllegalArgumentException("Directory " + dir + " does not exist and will not create it");
         }
      }
   }

   /**
    * @param messages
    * @param buff
    * @return
    * @throws Exception
    */
   private LargeServerMessage parseLargeMessage(final Map<Long, ServerMessage> messages, final HornetQBuffer buff) throws Exception
   {
      LargeServerMessage largeMessage = createLargeMessage();

      LargeMessageEncoding messageEncoding = new LargeMessageEncoding(largeMessage);

      messageEncoding.decode(buff);

      if (largeMessage.containsProperty(Message.HDR_ORIG_MESSAGE_ID))
      {
         // for compatibility: couple with old behaviour, copying the old file to avoid message loss
         long originalMessageID = largeMessage.getLongProperty(Message.HDR_ORIG_MESSAGE_ID);
         
         SequentialFile currentFile = createFileForLargeMessage(largeMessage.getMessageID(), true);
         
         if (!currentFile.exists())
         {
            SequentialFile linkedFile = createFileForLargeMessage(originalMessageID, true);
            if (linkedFile.exists())
            {
               linkedFile.copyTo(currentFile);
               linkedFile.close();
            }
         }
         
         currentFile.close();
      }

      return largeMessage;
   }

   private void loadPreparedTransactions(final PostOffice postOffice,
                                         final PagingManager pagingManager,
                                         final ResourceManager resourceManager,
                                         final Map<Long, Queue> queues,
                                         final Map<Long, QueueBindingInfo> queueInfos,
                                         final List<PreparedTransactionInfo> preparedTransactions,
                                         final Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap,
                                         final Map<Long, PageSubscription> pageSubscriptions,
                                         final Set<Pair<Long, Long>> pendingLargeMessages) throws Exception
   {
      // recover prepared transactions
      for (PreparedTransactionInfo preparedTransaction : preparedTransactions)
      {
         XidEncoding encodingXid = new XidEncoding(preparedTransaction.extraData);

         Xid xid = encodingXid.xid;

         Transaction tx = new TransactionImpl(preparedTransaction.id, xid, this);

         List<MessageReference> referencesToAck = new ArrayList<MessageReference>();

         Map<Long, ServerMessage> messages = new HashMap<Long, ServerMessage>();

         // Use same method as load message journal to prune out acks, so they don't get added.
         // Then have reacknowledge(tx) methods on queue, which needs to add the page size

         // first get any sent messages for this tx and recreate
         for (RecordInfo record : preparedTransaction.records)
         {
            byte[] data = record.data;

            HornetQBuffer buff = HornetQBuffers.wrappedBuffer(data);

            byte recordType = record.getUserRecordType();

            switch (recordType)
            {
               case ADD_LARGE_MESSAGE:
               {
                  messages.put(record.id, parseLargeMessage(messages, buff));

                  break;
               }
               case ADD_MESSAGE:
               {
                  ServerMessage message = new ServerMessageImpl(record.id, 50);

                  message.decode(buff);

                  messages.put(record.id, message);

                  break;
               }
               case ADD_REF:
               {
                  long messageID = record.id;

                  RefEncoding encoding = new RefEncoding();

                  encoding.decode(buff);

                  Queue queue = queues.get(encoding.queueID);

                  if (queue == null)
                  {
                     log.warn("Message in prepared tx for queue " + encoding.queueID +
                              " which does not exist. This message will be ignored.");

                  }
                  else
                  {
                     ServerMessage message = messages.get(messageID);

                     if (message == null)
                     {
                        throw new IllegalStateException("Cannot find message with id " + messageID);
                     }

                     postOffice.reroute(message, queue, tx);
                  }

                  break;
               }
               case ACKNOWLEDGE_REF:
               {
                  long messageID = record.id;

                  RefEncoding encoding = new RefEncoding();

                  encoding.decode(buff);

                  Queue queue = queues.get(encoding.queueID);

                  if (queue == null)
                  {
                     throw new IllegalStateException("Cannot find queue with id " + encoding.queueID);
                  }

                  MessageReference removed = queue.removeReferenceWithID(messageID);

                  if (removed == null)
                  {
                     log.warn("Failed to remove reference for " + messageID);
                  }
                  else
                  {
                     referencesToAck.add(removed);
                  }

                  break;
               }
               case PAGE_TRANSACTION:
               {

                  PageTransactionInfo pageTransactionInfo = new PageTransactionInfoImpl();

                  pageTransactionInfo.decode(buff);

                  if (record.isUpdate)
                  {
                     PageTransactionInfo pgTX = pagingManager.getTransaction(pageTransactionInfo.getTransactionID());
                     pgTX.reloadUpdate(this, pagingManager, tx, pageTransactionInfo.getNumberOfMessages());
                  }
                  else
                  {
                     pageTransactionInfo.setCommitted(false);

                     tx.putProperty(TransactionPropertyIndexes.PAGE_TRANSACTION, pageTransactionInfo);

                     pagingManager.addTransaction(pageTransactionInfo);

                     tx.addOperation(new FinishPageMessageOperation());
                  }

                  break;
               }
               case SET_SCHEDULED_DELIVERY_TIME:
               {
                  // Do nothing - for prepared txs, the set scheduled delivery time will only occur in a send in which
                  // case the message will already have the header for the scheduled delivery time, so no need to do
                  // anything.

                  break;
               }
               case DUPLICATE_ID:
               {
                  // We need load the duplicate ids at prepare time too
                  DuplicateIDEncoding encoding = new DuplicateIDEncoding();

                  encoding.decode(buff);

                  DuplicateIDCache cache = postOffice.getDuplicateIDCache(encoding.address);

                  cache.load(tx, encoding.duplID);

                  break;
               }
               case ACKNOWLEDGE_CURSOR:
               {
                  CursorAckRecordEncoding encoding = new CursorAckRecordEncoding();
                  encoding.decode(buff);

                  encoding.position.setRecordID(record.id);

                  PageSubscription sub = locateSubscription(encoding.queueID,
                                                            pageSubscriptions,
                                                            queueInfos,
                                                            pagingManager);

                  if (sub != null)
                  {
                     sub.reloadPreparedACK(tx, encoding.position);
                     referencesToAck.add(new PagedReferenceImpl(encoding.position, null, sub));
                  }
                  else
                  {
                     log.warn("Can't find queue " + encoding.queueID + " while reloading ACKNOWLEDGE_CURSOR");
                  }
                  break;
               }
               case PAGE_CURSOR_COUNTER_VALUE:
               {
                  log.warn("PAGE_CURSOR_COUNTER_VALUE record used on a prepared statement, what shouldn't happen");

                  break;
               }

               case PAGE_CURSOR_COUNTER_INC:
               {
                  PageCountRecordInc encoding = new PageCountRecordInc();

                  encoding.decode(buff);

                  PageSubscription sub = locateSubscription(encoding.queueID,
                                                            pageSubscriptions,
                                                            queueInfos,
                                                            pagingManager);

                  if (sub != null)
                  {
                     sub.getCounter().applyIncrement(tx, record.id, encoding.value);
                  }
                  else
                  {
                     log.warn("Can't find queue " + encoding.queueID + " while reloading ACKNOWLEDGE_CURSOR");
                  }

                  break;
               }

               default:
               {
                  JournalStorageManager.log.warn("InternalError: Record type " + recordType +
                                                 " not recognized. Maybe you're using journal files created on a different version");
               }
            }
         }

         for (RecordInfo recordDeleted : preparedTransaction.recordsToDelete)
         {
            byte[] data = recordDeleted.data;

            if (data.length > 0)
            {
               HornetQBuffer buff = HornetQBuffers.wrappedBuffer(data);
               byte b = buff.readByte();

               switch (b)
               {
                  case ADD_LARGE_MESSAGE_PENDING:
                  {
                     long messageID = buff.readLong();
                     pendingLargeMessages.remove(new Pair<Long, Long>(recordDeleted.id, messageID));
                     installLargeMessageConfirmationOnTX(tx, recordDeleted.id);
                     break;
                  }
                  default:
                     log.warn("can't locate recordType=" + b + " on loadPreparedTransaction//deleteRecords");
               }
            }

         }

         for (MessageReference ack : referencesToAck)
         {
            ack.getQueue().reacknowledge(tx, ack);
         }

         tx.setState(Transaction.State.PREPARED);

         resourceManager.putTransaction(xid, tx);
      }
   }

   /**
    * @throws Exception
    */
   private void cleanupIncompleteFiles() throws Exception
   {
      if (largeMessagesFactory != null)
      {
         List<String> tmpFiles = largeMessagesFactory.listFiles("tmp");
         for (String tmpFile : tmpFiles)
         {
            SequentialFile file = largeMessagesFactory.createSequentialFile(tmpFile, -1);
            file.delete();
         }
      }
   }

   protected OperationContext getContext(final boolean sync)
   {
      if (sync)
      {
         return getContext();
      }
      else
      {
         return DummyOperationContext.getInstance();
      }
   }

   private static ClassLoader getThisClassLoader()
   {
      return AccessController.doPrivileged(new PrivilegedAction<ClassLoader>()
      {
         public ClassLoader run()
         {
            return JournalStorageManager.class.getClassLoader();
         }
      });
   }

   // Inner Classes
   // ----------------------------------------------------------------------------

   static class DummyOperationContext implements OperationContext
   {

      private static DummyOperationContext instance = new DummyOperationContext();

      public static OperationContext getInstance()
      {
         return DummyOperationContext.instance;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.OperationContext#complete()
       */
      public void complete()
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.OperationContext#executeOnCompletion(org.hornetq.core.journal.IOAsyncTask)
       */
      public void executeOnCompletion(final IOAsyncTask runnable)
      {
         // There are no executeOnCompletion calls while using the DummyOperationContext
         // However we keep the code here for correctness
         runnable.done();
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.OperationContext#replicationDone()
       */
      public void replicationDone()
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.OperationContext#replicationLineUp()
       */
      public void replicationLineUp()
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.IOCompletion#lineUp()
       */
      public void storeLineUp()
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.asyncio.AIOCallback#done()
       */
      public void done()
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.asyncio.AIOCallback#onError(int, java.lang.String)
       */
      public void onError(final int errorCode, final String errorMessage)
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.OperationContext#waitCompletion()
       */
      public void waitCompletion()
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.OperationContext#waitCompletion(long)
       */
      public boolean waitCompletion(final long timeout)
      {
         return true;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.OperationContext#pageLineUp()
       */
      public void pageSyncLineUp()
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.persistence.OperationContext#pageDone()
       */
      public void pageSyncDone()
      {
      }

   }

   /** It's public as other classes may want to unparse data on tools*/
   public static class XidEncoding implements EncodingSupport
   {
      public final Xid xid;

      XidEncoding(final Xid xid)
      {
         this.xid = xid;
      }

      XidEncoding(final byte[] data)
      {
         xid = XidCodecSupport.decodeXid(HornetQBuffers.wrappedBuffer(data));
      }

      public void decode(final HornetQBuffer buffer)
      {
         throw new IllegalStateException("Non Supported Operation");
      }

      public void encode(final HornetQBuffer buffer)
      {
         XidCodecSupport.encodeXid(xid, buffer);
      }

      public int getEncodeSize()
      {
         return XidCodecSupport.getXidEncodeLength(xid);
      }
   }

   public static class HeuristicCompletionEncoding implements EncodingSupport
   {
      public Xid xid;

      public boolean isCommit;

      /* (non-Javadoc)
       * @see java.lang.Object#toString()
       */
      @Override
      public String toString()
      {
         return "HeuristicCompletionEncoding [xid=" + xid + ", isCommit=" + isCommit + "]";
      }

      HeuristicCompletionEncoding(final Xid xid, final boolean isCommit)
      {
         this.xid = xid;
         this.isCommit = isCommit;
      }

      HeuristicCompletionEncoding()
      {
      }

      public void decode(final HornetQBuffer buffer)
      {
         xid = XidCodecSupport.decodeXid(buffer);
         isCommit = buffer.readBoolean();
      }

      public void encode(final HornetQBuffer buffer)
      {
         XidCodecSupport.encodeXid(xid, buffer);
         buffer.writeBoolean(isCommit);
      }

      public int getEncodeSize()
      {
         return XidCodecSupport.getXidEncodeLength(xid) + DataConstants.SIZE_BOOLEAN;
      }
   }

   public static class GroupingEncoding implements EncodingSupport, GroupingInfo
   {
      public long id;

      public SimpleString groupId;

      public SimpleString clusterName;

      public GroupingEncoding(final long id, final SimpleString groupId, final SimpleString clusterName)
      {
         this.id = id;
         this.groupId = groupId;
         this.clusterName = clusterName;
      }

      public GroupingEncoding()
      {
      }

      public int getEncodeSize()
      {
         return SimpleString.sizeofString(groupId) + SimpleString.sizeofString(clusterName);
      }

      public void encode(final HornetQBuffer buffer)
      {
         buffer.writeSimpleString(groupId);
         buffer.writeSimpleString(clusterName);
      }

      public void decode(final HornetQBuffer buffer)
      {
         groupId = buffer.readSimpleString();
         clusterName = buffer.readSimpleString();
      }

      public long getId()
      {
         return id;
      }

      public void setId(final long id)
      {
         this.id = id;
      }

      public SimpleString getGroupId()
      {
         return groupId;
      }

      public SimpleString getClusterName()
      {
         return clusterName;
      }

      /* (non-Javadoc)
       * @see java.lang.Object#toString()
       */
      @Override
      public String toString()
      {
         return "GroupingEncoding [id=" + id + ", groupId=" + groupId + ", clusterName=" + clusterName + "]";
      }
   }

   public static class PersistentQueueBindingEncoding implements EncodingSupport, QueueBindingInfo
   {
      public long id;

      public SimpleString name;

      public SimpleString address;

      public SimpleString filterString;

      public PersistentQueueBindingEncoding()
      {
      }

      /* (non-Javadoc)
       * @see java.lang.Object#toString()
       */
      @Override
      public String toString()
      {
         return "PersistentQueueBindingEncoding [id=" + id +
                ", name=" +
                name +
                ", address=" +
                address +
                ", filterString=" +
                filterString +
                "]";
      }

      public PersistentQueueBindingEncoding(final SimpleString name,
                                            final SimpleString address,
                                            final SimpleString filterString)
      {
         this.name = name;
         this.address = address;
         this.filterString = filterString;
      }

      public long getId()
      {
         return id;
      }

      public void setId(final long id)
      {
         this.id = id;
      }

      public SimpleString getAddress()
      {
         return address;
      }

      public SimpleString getFilterString()
      {
         return filterString;
      }

      public SimpleString getQueueName()
      {
         return name;
      }

      public void decode(final HornetQBuffer buffer)
      {
         name = buffer.readSimpleString();
         address = buffer.readSimpleString();
         filterString = buffer.readNullableSimpleString();
      }

      public void encode(final HornetQBuffer buffer)
      {
         buffer.writeSimpleString(name);
         buffer.writeSimpleString(address);
         buffer.writeNullableSimpleString(filterString);
      }

      public int getEncodeSize()
      {
         return SimpleString.sizeofString(name) + SimpleString.sizeofString(address) +
                SimpleString.sizeofNullableString(filterString);
      }
   }

   public static class LargeMessageEncoding implements EncodingSupport
   {
      public final LargeServerMessage message;

      public LargeMessageEncoding(final LargeServerMessage message)
      {
         this.message = message;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#decode(org.hornetq.spi.core.remoting.HornetQBuffer)
       */
      public void decode(final HornetQBuffer buffer)
      {
         message.decodeHeadersAndProperties(buffer);
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#encode(org.hornetq.spi.core.remoting.HornetQBuffer)
       */
      public void encode(final HornetQBuffer buffer)
      {
         message.encode(buffer);
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#getEncodeSize()
       */
      public int getEncodeSize()
      {
         return message.getEncodeSize();
      }

   }

   public static class PendingLargeMessageEncoding implements EncodingSupport
   {
      public long largeMessageID;

      public PendingLargeMessageEncoding(final long pendingLargeMessageID)
      {
         this.largeMessageID = pendingLargeMessageID;
      }

      public PendingLargeMessageEncoding()
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#decode(org.hornetq.spi.core.remoting.HornetQBuffer)
       */
      public void decode(final HornetQBuffer buffer)
      {
         largeMessageID = buffer.readLong();
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#encode(org.hornetq.spi.core.remoting.HornetQBuffer)
       */
      public void encode(final HornetQBuffer buffer)
      {
         buffer.writeLong(largeMessageID);
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#getEncodeSize()
       */
      public int getEncodeSize()
      {
         return DataConstants.SIZE_LONG;
      }

      public String toString()
      {
         return "PendingLargeMessageEncoding::MessageID=" + largeMessageID;
      }

   }

   public static class DeliveryCountUpdateEncoding implements EncodingSupport
   {
      public long queueID;

      public int count;

      public DeliveryCountUpdateEncoding()
      {
         super();
      }

      public DeliveryCountUpdateEncoding(final long queueID, final int count)
      {
         super();
         this.queueID = queueID;
         this.count = count;
      }

      public void decode(final HornetQBuffer buffer)
      {
         queueID = buffer.readLong();
         count = buffer.readInt();
      }

      public void encode(final HornetQBuffer buffer)
      {
         buffer.writeLong(queueID);
         buffer.writeInt(count);
      }

      public int getEncodeSize()
      {
         return 8 + 4;
      }

      /* (non-Javadoc)
       * @see java.lang.Object#toString()
       */
      @Override
      public String toString()
      {
         return "DeliveryCountUpdateEncoding [queueID=" + queueID + ", count=" + count + "]";
      }

   }

   public static class QueueEncoding implements EncodingSupport
   {
      public long queueID;

      public QueueEncoding(final long queueID)
      {
         super();
         this.queueID = queueID;
      }

      public QueueEncoding()
      {
         super();
      }

      public void decode(final HornetQBuffer buffer)
      {
         queueID = buffer.readLong();
      }

      public void encode(final HornetQBuffer buffer)
      {
         buffer.writeLong(queueID);
      }

      public int getEncodeSize()
      {
         return 8;
      }

      /* (non-Javadoc)
       * @see java.lang.Object#toString()
       */
      @Override
      public String toString()
      {
         return "QueueEncoding [queueID=" + queueID + "]";
      }

   }

   public static class DeleteEncoding implements EncodingSupport
   {
      public byte recordType;

      public long id;

      public DeleteEncoding()
      {
         super();
      }

      public DeleteEncoding(final byte recordType, final long id)
      {
         this.recordType = recordType;
         this.id = id;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#getEncodeSize()
       */
      public int getEncodeSize()
      {
         return DataConstants.SIZE_BYTE + DataConstants.SIZE_LONG;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#encode(org.hornetq.api.core.HornetQBuffer)
       */
      public void encode(HornetQBuffer buffer)
      {
         buffer.writeByte(recordType);
         buffer.writeLong(id);
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#decode(org.hornetq.api.core.HornetQBuffer)
       */
      public void decode(HornetQBuffer buffer)
      {
         recordType = buffer.readByte();
         id = buffer.readLong();
      }
   }

   public static class RefEncoding extends QueueEncoding
   {
      public RefEncoding()
      {
         super();
      }

      public RefEncoding(final long queueID)
      {
         super(queueID);
      }
   }

   public static class PageUpdateTXEncoding implements EncodingSupport
   {

      public long pageTX;

      public int recods;

      /* (non-Javadoc)
       * @see java.lang.Object#toString()
       */
      @Override
      public String toString()
      {
         return "PageUpdateTXEncoding [pageTX=" + pageTX + ", recods=" + recods + "]";
      }

      public PageUpdateTXEncoding()
      {
      }

      public PageUpdateTXEncoding(final long pageTX, final int records)
      {
         this.pageTX = pageTX;
         this.recods = records;
      }

      public void decode(HornetQBuffer buffer)
      {
         this.pageTX = buffer.readLong();
         this.recods = buffer.readInt();
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#encode(org.hornetq.api.core.HornetQBuffer)
       */
      public void encode(HornetQBuffer buffer)
      {
         buffer.writeLong(pageTX);
         buffer.writeInt(recods);
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#getEncodeSize()
       */
      public int getEncodeSize()
      {
         return DataConstants.SIZE_LONG + DataConstants.SIZE_INT;
      }

      public List<MessageReference> getRelatedMessageReferences()
      {
         return null;
      }
   }

   public static class ScheduledDeliveryEncoding extends QueueEncoding
   {
      long scheduledDeliveryTime;

      /* (non-Javadoc)
       * @see java.lang.Object#toString()
       */
      @Override
      public String toString()
      {
         return "ScheduledDeliveryEncoding [scheduledDeliveryTime=" + scheduledDeliveryTime + "]";
      }

      private ScheduledDeliveryEncoding(final long scheduledDeliveryTime, final long queueID)
      {
         super(queueID);
         this.scheduledDeliveryTime = scheduledDeliveryTime;
      }

      public ScheduledDeliveryEncoding()
      {
      }

      @Override
      public int getEncodeSize()
      {
         return super.getEncodeSize() + 8;
      }

      @Override
      public void encode(final HornetQBuffer buffer)
      {
         super.encode(buffer);
         buffer.writeLong(scheduledDeliveryTime);
      }

      @Override
      public void decode(final HornetQBuffer buffer)
      {
         super.decode(buffer);
         scheduledDeliveryTime = buffer.readLong();
      }
   }

   public static class DuplicateIDEncoding implements EncodingSupport
   {
      SimpleString address;

      byte[] duplID;

      public DuplicateIDEncoding(final SimpleString address, final byte[] duplID)
      {
         this.address = address;

         this.duplID = duplID;
      }

      public DuplicateIDEncoding()
      {
      }

      public void decode(final HornetQBuffer buffer)
      {
         address = buffer.readSimpleString();

         int size = buffer.readInt();

         duplID = new byte[size];

         buffer.readBytes(duplID);
      }

      public void encode(final HornetQBuffer buffer)
      {
         buffer.writeSimpleString(address);

         buffer.writeInt(duplID.length);

         buffer.writeBytes(duplID);
      }

      public int getEncodeSize()
      {
         return SimpleString.sizeofString(address) + DataConstants.SIZE_INT + duplID.length;
      }

      /* (non-Javadoc)
       * @see java.lang.Object#toString()
       */
      @Override
      public String toString()
      {
         // this would be useful when testing. Most tests on the testsuite will use a SimpleString on the duplicate ID
         // and this may be useful to validate the journal on those tests
         // You may uncomment these two lines on that case and replcate the toString for the PrintData

         // SimpleString simpleStr = new SimpleString(duplID);
         // return "DuplicateIDEncoding [address=" + address + ", duplID=" + simpleStr + "]";

         return "DuplicateIDEncoding [address=" + address + ", duplID=" + Arrays.toString(duplID) + "]";
      }

   }

   private class FinishPageMessageOperation implements TransactionOperation
   {

      public void afterCommit(final Transaction tx)
      {
         // If part of the transaction goes to the queue, and part goes to paging, we can't let depage start for the
         // transaction until all the messages were added to the queue
         // or else we could deliver the messages out of order

         PageTransactionInfo pageTransaction = (PageTransactionInfo)tx.getProperty(TransactionPropertyIndexes.PAGE_TRANSACTION);

         if (pageTransaction != null)
         {
            pageTransaction.commit();
         }
      }

      public void afterPrepare(final Transaction tx)
      {
      }

      public void afterRollback(final Transaction tx)
      {
         PageTransactionInfo pageTransaction = (PageTransactionInfo)tx.getProperty(TransactionPropertyIndexes.PAGE_TRANSACTION);

         if (tx.getState() == State.PREPARED && pageTransaction != null)
         {
            pageTransaction.rollback();
         }
      }

      public void beforeCommit(final Transaction tx) throws Exception
      {
      }

      public void beforePrepare(final Transaction tx) throws Exception
      {
      }

      public void beforeRollback(final Transaction tx) throws Exception
      {
      }

      public List<MessageReference> getRelatedMessageReferences()
      {
         return null;
      }

   }

   private static final class PageCountRecord implements EncodingSupport
   {

      /* (non-Javadoc)
       * @see java.lang.Object#toString()
       */
      @Override
      public String toString()
      {
         return "PageCountRecord [queueID=" + queueID + ", value=" + value + "]";
      }

      PageCountRecord()
      {

      }

      PageCountRecord(long queueID, long value)
      {
         this.queueID = queueID;
         this.value = value;
      }

      long queueID;

      long value;

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#getEncodeSize()
       */
      public int getEncodeSize()
      {
         return DataConstants.SIZE_LONG * 2;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#encode(org.hornetq.api.core.HornetQBuffer)
       */
      public void encode(HornetQBuffer buffer)
      {
         buffer.writeLong(queueID);
         buffer.writeLong(value);
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#decode(org.hornetq.api.core.HornetQBuffer)
       */
      public void decode(HornetQBuffer buffer)
      {
         queueID = buffer.readLong();
         value = buffer.readLong();
      }

   }

   private static final class PageCountRecordInc implements EncodingSupport
   {

      /* (non-Javadoc)
       * @see java.lang.Object#toString()
       */
      @Override
      public String toString()
      {
         return "PageCountRecordInc [queueID=" + queueID + ", value=" + value + "]";
      }

      PageCountRecordInc()
      {

      }

      PageCountRecordInc(long queueID, int value)
      {
         this.queueID = queueID;
         this.value = value;
      }

      long queueID;

      int value;

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#getEncodeSize()
       */
      public int getEncodeSize()
      {
         return DataConstants.SIZE_LONG + DataConstants.SIZE_INT;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#encode(org.hornetq.api.core.HornetQBuffer)
       */
      public void encode(HornetQBuffer buffer)
      {
         buffer.writeLong(queueID);
         buffer.writeInt(value);
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#decode(org.hornetq.api.core.HornetQBuffer)
       */
      public void decode(HornetQBuffer buffer)
      {
         queueID = buffer.readLong();
         value = buffer.readInt();
      }

   }

   private static final class AddMessageRecord
   {
      public AddMessageRecord(final ServerMessage message)
      {
         this.message = message;
      }

      final ServerMessage message;

      long scheduledDeliveryTime;

      int deliveryCount;
   }

   public static final class CursorAckRecordEncoding implements EncodingSupport
   {
      public CursorAckRecordEncoding(final long queueID, final PagePosition position)
      {
         this.queueID = queueID;
         this.position = position;
      }

      public CursorAckRecordEncoding()
      {
         this.position = new PagePositionImpl();
      }

      /* (non-Javadoc)
       * @see java.lang.Object#toString()
       */
      @Override
      public String toString()
      {
         return "CursorAckRecordEncoding [queueID=" + queueID + ", position=" + position + "]";
      }

      public long queueID;

      public PagePosition position;

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#getEncodeSize()
       */
      public int getEncodeSize()
      {
         return DataConstants.SIZE_LONG + DataConstants.SIZE_LONG + DataConstants.SIZE_INT;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#encode(org.hornetq.api.core.HornetQBuffer)
       */
      public void encode(HornetQBuffer buffer)
      {
         buffer.writeLong(queueID);
         buffer.writeLong(position.getPageNr());
         buffer.writeInt(position.getMessageNr());
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#decode(org.hornetq.api.core.HornetQBuffer)
       */
      public void decode(HornetQBuffer buffer)
      {
         queueID = buffer.readLong();
         long pageNR = buffer.readLong();
         int messageNR = buffer.readInt();
         this.position = new PagePositionImpl(pageNR, messageNR);
      }
   }

   private class LargeMessageTXFailureCallback implements TransactionFailureCallback
   {
      private final Map<Long, ServerMessage> messages;

      public LargeMessageTXFailureCallback(final Map<Long, ServerMessage> messages)
      {
         super();
         this.messages = messages;
      }

      public void failedTransaction(final long transactionID,
                                    final List<RecordInfo> records,
                                    final List<RecordInfo> recordsToDelete)
      {
         for (RecordInfo record : records)
         {
            if (record.userRecordType == JournalStorageManager.ADD_LARGE_MESSAGE)
            {
               byte[] data = record.data;

               HornetQBuffer buff = HornetQBuffers.wrappedBuffer(data);

               try
               {
                  LargeServerMessage serverMessage = parseLargeMessage(messages, buff);
                  serverMessage.decrementDelayDeletionCount();
               }
               catch (Exception e)
               {
                  JournalStorageManager.log.warn(e.getMessage(), e);
               }
            }
         }
      }

   }

   private static String describeRecord(RecordInfo info)
   {
      return "recordID=" + info.id +
             ";userRecordType=" +
             info.userRecordType +
             ";isUpdate=" +
             info.isUpdate +
             ";" +
             newObjectEncoding(info);
   }

   private static String describeRecord(RecordInfo info, Object o)
   {
      return "recordID=" + info.id + ";userRecordType=" + info.userRecordType + ";isUpdate=" + info.isUpdate + ";" + o;
   }

   // Encoding functions for binding Journal

   public static Object newObjectEncoding(RecordInfo info)
   {
      HornetQBuffer buffer = HornetQBuffers.wrappedBuffer(info.data);
      long id = info.id;
      int rec = info.getUserRecordType();

      switch (rec)
      {
         case ADD_LARGE_MESSAGE_PENDING:
         {
            PendingLargeMessageEncoding lmEncoding = new PendingLargeMessageEncoding();
            lmEncoding.decode(buffer);

            return lmEncoding;
         }
         case ADD_LARGE_MESSAGE:
         {

            LargeServerMessage largeMessage = new LargeServerMessageImpl(null);

            LargeMessageEncoding messageEncoding = new LargeMessageEncoding(largeMessage);

            messageEncoding.decode(buffer);

            return new MessageDescribe(largeMessage);
         }
         case ADD_MESSAGE:
         {
            ServerMessage message = new ServerMessageImpl(rec, 50);

            message.decode(buffer);

            return new MessageDescribe(message);
         }
         case ADD_REF:
         {
            final RefEncoding encoding = new RefEncoding();
            encoding.decode(buffer);
            return new ReferenceDescribe(encoding);
         }

         case ACKNOWLEDGE_REF:
         {
            final RefEncoding encoding = new RefEncoding();
            encoding.decode(buffer);
            return new AckDescribe(encoding);
         }

         case UPDATE_DELIVERY_COUNT:
         {
            DeliveryCountUpdateEncoding updateDeliveryCount = new DeliveryCountUpdateEncoding();
            updateDeliveryCount.decode(buffer);
            return updateDeliveryCount;
         }

         case PAGE_TRANSACTION:
         {
            if (info.isUpdate)
            {
               PageUpdateTXEncoding pageUpdate = new PageUpdateTXEncoding();

               pageUpdate.decode(buffer);

               return pageUpdate;
            }
            else
            {
               PageTransactionInfoImpl pageTransactionInfo = new PageTransactionInfoImpl();

               pageTransactionInfo.decode(buffer);

               pageTransactionInfo.setRecordID(info.id);

               return pageTransactionInfo;
            }
         }

         case SET_SCHEDULED_DELIVERY_TIME:
         {
            ScheduledDeliveryEncoding encoding = new ScheduledDeliveryEncoding();

            encoding.decode(buffer);

            return encoding;
         }
         case DUPLICATE_ID:
         {
            DuplicateIDEncoding encoding = new DuplicateIDEncoding();

            encoding.decode(buffer);

            return encoding;
         }
         case HEURISTIC_COMPLETION:
         {
            HeuristicCompletionEncoding encoding = new HeuristicCompletionEncoding();

            encoding.decode(buffer);

            return encoding;
         }
         case ACKNOWLEDGE_CURSOR:
         {
            CursorAckRecordEncoding encoding = new CursorAckRecordEncoding();

            encoding.decode(buffer);

            return encoding;
         }
         case PAGE_CURSOR_COUNTER_VALUE:
         {
            PageCountRecord encoding = new PageCountRecord();

            encoding.decode(buffer);

            return encoding;
         }

         case PAGE_CURSOR_COUNTER_INC:
         {
            PageCountRecordInc encoding = new PageCountRecordInc();

            encoding.decode(buffer);

            return encoding;
         }

         case JournalStorageManager.QUEUE_BINDING_RECORD:
            return newBindingEncoding(id, buffer);

         case JournalStorageManager.ID_COUNTER_RECORD:
            IDCounterEncoding idReturn = new IDCounterEncoding();

            idReturn.decode(buffer);

            return idReturn;

         case JournalStorageManager.GROUP_RECORD:
            return newGroupEncoding(id, buffer);

         case JournalStorageManager.ADDRESS_SETTING_RECORD:
            return newAddressEncoding(id, buffer);

         case JournalStorageManager.SECURITY_RECORD:
            return newSecurityRecord(id, buffer);

         default:
            return null;
      }
   }

   public static class ReferenceDescribe
   {
      public RefEncoding refEncoding;

      public ReferenceDescribe(RefEncoding refEncoding)
      {
         this.refEncoding = refEncoding;
      }

      public String toString()
      {
         return "AddRef;" + refEncoding;
      }

   }

   public static class AckDescribe
   {
      RefEncoding refEncoding;

      public AckDescribe(RefEncoding refEncoding)
      {
         this.refEncoding = refEncoding;
      }

      public String toString()
      {
         return "ACK;" + refEncoding;
      }

   }

   public static class MessageDescribe
   {
      public MessageDescribe(Message msg)
      {
         this.msg = msg;
      }

      Message msg;

      public String toString()
      {
         StringBuffer buffer = new StringBuffer();
         buffer.append(msg.isLargeMessage() ? "LargeMessage(" : "Message(");
         buffer.append("messageID=" + msg.getMessageID());
         buffer.append(";properties=[");

         Set<SimpleString> properties = msg.getPropertyNames();

         for (SimpleString prop : properties)
         {
            Object value = msg.getObjectProperty(prop);
            if (value instanceof byte[])
            {
               buffer.append(prop + "=" + Arrays.toString((byte[])value) + ",");

            }
            else
            {
               buffer.append(prop + "=" + value + ",");
            }
         }

         buffer.append("#properties = " + properties.size());

         buffer.append("]");

         buffer.append(" - " + msg.toString());

         return buffer.toString();
      }

   }

   /**
    * @param id
    * @param buffer
    * @return
    */
   protected static PersistedRoles newSecurityRecord(long id, HornetQBuffer buffer)
   {
      PersistedRoles roles = new PersistedRoles();
      roles.decode(buffer);
      roles.setStoreId(id);
      return roles;
   }

   /**
    * @param id
    * @param buffer
    * @return
    */
   protected static PersistedAddressSetting newAddressEncoding(long id, HornetQBuffer buffer)
   {
      PersistedAddressSetting setting = new PersistedAddressSetting();
      setting.decode(buffer);
      setting.setStoreId(id);
      return setting;
   }

   /**
    * @param id
    * @param buffer
    * @return
    */
   protected static GroupingEncoding newGroupEncoding(long id, HornetQBuffer buffer)
   {
      GroupingEncoding encoding = new GroupingEncoding();
      encoding.decode(buffer);
      encoding.setId(id);
      return encoding;
   }

   /**
    * @param id
    * @param buffer
    * @return
    */
   protected static PersistentQueueBindingEncoding newBindingEncoding(long id, HornetQBuffer buffer)
   {
      PersistentQueueBindingEncoding bindingEncoding = new PersistentQueueBindingEncoding();

      bindingEncoding.decode(buffer);

      bindingEncoding.setId(id);
      return bindingEncoding;
   }

   private static String encode(final byte[] data)
   {
      return Base64.encodeBytes(data, 0, data.length, Base64.DONT_BREAK_LINES | Base64.URL_SAFE);
   }

   /**
    * @param fileFactory
    * @param journal
    * @throws Exception
    */
   protected static void describeJournal(SequentialFileFactory fileFactory, JournalImpl journal) throws Exception
   {
      List<JournalFile> files = journal.orderFiles();

      final PrintStream out = System.out;

      for (JournalFile file : files)
      {
         out.println("#" + file);

         JournalImpl.readJournalFile(fileFactory, file, new JournalReaderCallback()
         {

            public void onReadUpdateRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception
            {
               out.println("operation@UpdateTX;txID=" + transactionID + "," + describeRecord(recordInfo));
            }

            public void onReadUpdateRecord(final RecordInfo recordInfo) throws Exception
            {
               out.println("operation@Update;" + describeRecord(recordInfo));
            }

            public void onReadRollbackRecord(final long transactionID) throws Exception
            {
               out.println("operation@Rollback;txID=" + transactionID);
            }

            public void onReadPrepareRecord(final long transactionID, final byte[] extraData, final int numberOfRecords) throws Exception
            {
               out.println("operation@Prepare,txID=" + transactionID +
                           ",numberOfRecords=" +
                           numberOfRecords +
                           ",extraData=" +
                           encode(extraData));
            }

            public void onReadDeleteRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception
            {
               out.println("operation@DeleteRecordTX;txID=" + transactionID + "," + describeRecord(recordInfo));
            }

            public void onReadDeleteRecord(final long recordID) throws Exception
            {
               out.println("operation@DeleteRecord;recordID=" + recordID);
            }

            public void onReadCommitRecord(final long transactionID, final int numberOfRecords) throws Exception
            {
               out.println("operation@Commit;txID=" + transactionID + ",numberOfRecords=" + numberOfRecords);
            }

            public void onReadAddRecordTX(final long transactionID, final RecordInfo recordInfo) throws Exception
            {
               out.println("operation@AddRecordTX;txID=" + transactionID + "," + describeRecord(recordInfo));
            }

            public void onReadAddRecord(final RecordInfo recordInfo) throws Exception
            {
               out.println("operation@AddRecord;" + describeRecord(recordInfo));
            }

            public void markAsDataFile(final JournalFile file)
            {
            }
         });
      }

      out.println();

      out.println("### Surviving Records Summary ###");

      List<RecordInfo> records = new LinkedList<RecordInfo>();
      List<PreparedTransactionInfo> preparedTransactions = new LinkedList<PreparedTransactionInfo>();

      journal.start();

      final StringBuffer bufferFailingTransactions = new StringBuffer();

      int messageCount = 0;
      Map<Long, Integer> messageRefCounts = new HashMap<Long, Integer>();
      int preparedMessageCount = 0;
      Map<Long, Integer> preparedMessageRefCount = new HashMap<Long, Integer>();
      journal.load(records, preparedTransactions, new TransactionFailureCallback()
      {

         public void failedTransaction(long transactionID, List<RecordInfo> records, List<RecordInfo> recordsToDelete)
         {
            bufferFailingTransactions.append("Transaction " + transactionID + " failed with these records:\n");
            for (RecordInfo info : records)
            {
               bufferFailingTransactions.append("- " + describeRecord(info) + "\n");
            }

            for (RecordInfo info : recordsToDelete)
            {
               bufferFailingTransactions.append("- " + describeRecord(info) + " <marked to delete>\n");
            }

         }
      }, false);

      for (RecordInfo info : records)
      {
         Object o = newObjectEncoding(info);
         if (info.getUserRecordType() == JournalStorageManager.ADD_MESSAGE)
         {
            messageCount++;
         }
         else if (info.getUserRecordType() == JournalStorageManager.ADD_REF)
         {
            ReferenceDescribe ref = (ReferenceDescribe)o;
            Integer count = messageRefCounts.get(ref.refEncoding.queueID);
            if (count == null)
            {
               count = 1;
               messageRefCounts.put(ref.refEncoding.queueID, count);
            }
            else
            {
               messageRefCounts.put(ref.refEncoding.queueID, count + 1);
            }
         }
         else if (info.getUserRecordType() == JournalStorageManager.ACKNOWLEDGE_REF)
         {
            AckDescribe ref = (AckDescribe)o;
            Integer count = messageRefCounts.get(ref.refEncoding.queueID);
            if (count == null)
            {
               messageRefCounts.put(ref.refEncoding.queueID, 0);
            }
            else
            {
               messageRefCounts.put(ref.refEncoding.queueID, count - 1);
            }
         }
         out.println(describeRecord(info, o));
      }

      out.println();
      out.println("### Prepared TX ###");

      for (PreparedTransactionInfo tx : preparedTransactions)
      {
         System.out.println(tx.id);
         for (RecordInfo info : tx.records)
         {
            Object o = newObjectEncoding(info);
            out.println("- " + describeRecord(info, o));
            if (info.getUserRecordType() == 31)
            {
               preparedMessageCount++;
            }
            else if (info.getUserRecordType() == 32)
            {
               ReferenceDescribe ref = (ReferenceDescribe)o;
               Integer count = preparedMessageRefCount.get(ref.refEncoding.queueID);
               if (count == null)
               {
                  count = 1;
                  preparedMessageRefCount.put(ref.refEncoding.queueID, count);
               }
               else
               {
                  preparedMessageRefCount.put(ref.refEncoding.queueID, count + 1);
               }
            }
         }

         for (RecordInfo info : tx.recordsToDelete)
         {
            out.println("- " + describeRecord(info) + " <marked to delete>");
         }
      }

      String missingTX = bufferFailingTransactions.toString();

      if (missingTX.length() > 0)
      {
         out.println();
         out.println("### Failed Transactions (Missing commit/prepare/rollback record) ###");
      }

      out.println(bufferFailingTransactions.toString());

      out.println("### Message Counts ###");
      out.println("message count=" + messageCount);
      out.println("message reference count");
      for (Map.Entry<Long, Integer> longIntegerEntry : messageRefCounts.entrySet())
      {
         System.out.println("queue id " + longIntegerEntry.getKey() + ",count=" + longIntegerEntry.getValue());
      }

      out.println("prepared message count=" + preparedMessageCount);

      for (Map.Entry<Long, Integer> longIntegerEntry : preparedMessageRefCount.entrySet())
      {
         System.out.println("queue id " + longIntegerEntry.getKey() + ",count=" + longIntegerEntry.getValue());
      }

      journal.stop();
   }

   private void installLargeMessageConfirmationOnTX(Transaction tx, long recordID)
   {
      TXLargeMessageConfirmationOperation txoper = (TXLargeMessageConfirmationOperation)tx.getProperty(TransactionPropertyIndexes.LARGE_MESSAGE_CONFIRMATIONS);
      if (txoper == null)
      {
         txoper = new TXLargeMessageConfirmationOperation();
         tx.putProperty(TransactionPropertyIndexes.LARGE_MESSAGE_CONFIRMATIONS, txoper);
      }
      txoper.confirmedMessages.add(recordID);
   }

   class TXLargeMessageConfirmationOperation implements TransactionOperation
   {

      public List<Long> confirmedMessages = new LinkedList<Long>();

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.TransactionOperation#beforePrepare(org.hornetq.core.transaction.Transaction)
       */
      public void beforePrepare(Transaction tx) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.TransactionOperation#afterPrepare(org.hornetq.core.transaction.Transaction)
       */
      public void afterPrepare(Transaction tx)
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.TransactionOperation#beforeCommit(org.hornetq.core.transaction.Transaction)
       */
      public void beforeCommit(Transaction tx) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.TransactionOperation#afterCommit(org.hornetq.core.transaction.Transaction)
       */
      public void afterCommit(Transaction tx)
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.TransactionOperation#beforeRollback(org.hornetq.core.transaction.Transaction)
       */
      public void beforeRollback(Transaction tx) throws Exception
      {
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.TransactionOperation#afterRollback(org.hornetq.core.transaction.Transaction)
       */
      public void afterRollback(Transaction tx)
      {
         for (Long msg : confirmedMessages)
         {
            try
            {
               JournalStorageManager.this.confirmPendingLargeMessage(msg);
            }
            catch (Throwable e)
            {
               log.warn("Error while confirming large message completion on rollback for recordID=" + msg +
                                 "->" +
                                 e.getMessage(),
                        e);
            }
         }
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.transaction.TransactionOperation#getRelatedMessageReferences()
       */
      public List<MessageReference> getRelatedMessageReferences()
      {
         return null;
      }

   }

}
