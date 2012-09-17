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
import java.security.InvalidParameterException;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import javax.transaction.xa.Xid;

import org.hornetq.api.core.HornetQBuffer;
import org.hornetq.api.core.HornetQBuffers;
import org.hornetq.api.core.HornetQException;
import org.hornetq.api.core.HornetQIllegalStateException;
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
import org.hornetq.core.paging.impl.PagingStoreImpl;
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
import org.hornetq.core.replication.ReplicatedJournal;
import org.hornetq.core.replication.ReplicationManager;
import org.hornetq.core.server.HornetQLogger;
import org.hornetq.core.server.HornetQMessageBundle;
import org.hornetq.core.server.JournalType;
import org.hornetq.core.server.LargeServerMessage;
import org.hornetq.core.server.MessageReference;
import org.hornetq.core.server.Queue;
import org.hornetq.core.server.RouteContextList;
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
 * Controls access to the journals and other storage files such as the ones used to store pages and
 * large messages. This class must control writing of any non-transient data, as it is the key point
 * for synchronizing a replicating backup server.
 * <p>
 * Using this class also ensures that locks are acquired in the right order, avoiding dead-locks.
 * <p>
 * Notice that, turning on and off replication (on the live server side) is _mostly_ a matter of
 * using {@link ReplicatedJournal}s instead of regular {@link JournalImpl}, and sync the existing
 * data.
 * <p>
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 * @author <a href="jmesnil@redhat.com">Jeff Mesnil</a>
 */
public class JournalStorageManager implements StorageManager
{
   private static final long CHECKPOINT_BATCH_SIZE = Integer.MAX_VALUE;

   // grouping journal record type
   private static final byte GROUP_RECORD = 20;

   // Bindings journal record type

   public static final byte QUEUE_BINDING_RECORD = 21;

   public static final byte ID_COUNTER_RECORD = 24;

   private static final byte ADDRESS_SETTING_RECORD = 25;

   private static final byte SECURITY_RECORD = 26;

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

   private static final byte SET_SCHEDULED_DELIVERY_TIME = 36;

   private static final byte DUPLICATE_ID = 37;

   private static final byte HEURISTIC_COMPLETION = 38;

   public static final byte ACKNOWLEDGE_CURSOR = 39;

   private static final byte PAGE_CURSOR_COUNTER_VALUE = 40;

   private static final byte PAGE_CURSOR_COUNTER_INC = 41;

   public static final byte PAGE_CURSOR_COMPLETE = 42;

   private final Semaphore pageMaxConcurrentIO;

   private final BatchingIDGenerator idGenerator;

   private final ReentrantReadWriteLock storageManagerLock = new ReentrantReadWriteLock(true);

   private ReplicationManager replicator;

   public enum JournalContent
   {
      BINDINGS((byte)0), MESSAGES((byte)1);

      public final byte typeByte;

      JournalContent(byte b){
         typeByte = b;
      }

      public static JournalContent getType(byte type)
      {
         if (MESSAGES.typeByte == type)
            return MESSAGES;
         if (BINDINGS.typeByte == type)
            return BINDINGS;
         throw new InvalidParameterException("invalid byte: " + type);
      }
   }

   private final SequentialFileFactory journalFF;

   private Journal messageJournal;

   private Journal bindingsJournal;

   private final Journal originalMessageJournal;

   private final Journal originalBindingsJournal;

   private final SequentialFileFactory largeMessagesFactory;

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
   private final Map<SimpleString, PersistedRoles> mapPersistedRoles =
      new ConcurrentHashMap<SimpleString, PersistedRoles>();

   private final Map<SimpleString, PersistedAddressSetting> mapPersistedAddressSettings =
      new ConcurrentHashMap<SimpleString, PersistedAddressSetting>();

   public JournalStorageManager(final Configuration config, final ExecutorFactory executorFactory,
                                final IOCriticalErrorListener criticalErrorListener)
   {
      this.executorFactory = executorFactory;

      executor = executorFactory.getExecutor();

      if (config.getJournalType() != JournalType.NIO && config.getJournalType() != JournalType.ASYNCIO)
      {
         throw HornetQMessageBundle.BUNDLE.invalidJournal();
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

      bindingsJournal = localBindings;
      originalBindingsJournal = localBindings;

      if (journalDir == null)
      {
         throw new NullPointerException("journal-dir is null");
      }

      createJournalDir = config.isCreateJournalDir();

      syncNonTransactional = config.isJournalSyncNonTransactional();

      syncTransactional = config.isJournalSyncTransactional();

      if (config.getJournalType() == JournalType.ASYNCIO)
      {
         HornetQLogger.LOGGER.journalUseAIO();

         journalFF = new AIOSequentialFileFactory(journalDir,
            config.getJournalBufferSize_AIO(),
            config.getJournalBufferTimeout_AIO(),
            config.isLogJournalWriteRate(),
            criticalErrorListener);
      }
      else if (config.getJournalType() == JournalType.NIO)
      {
         HornetQLogger.LOGGER.journalUseNIO();
         journalFF = new NIOSequentialFileFactory(journalDir,
            true,
            config.getJournalBufferSize_NIO(),
            config.getJournalBufferTimeout_NIO(),
            config.isLogJournalWriteRate(),
            criticalErrorListener);
      }
      else
      {
         throw HornetQMessageBundle.BUNDLE.invalidJournalType2(config.getJournalType());
      }

      idGenerator = new BatchingIDGenerator(0, JournalStorageManager.CHECKPOINT_BATCH_SIZE, this);

      Journal localMessage = new JournalImpl(config.getJournalFileSize(),
         config.getJournalMinFiles(),
         config.getJournalCompactMinFiles(),
         config.getJournalCompactPercentage(),
         journalFF,
         "hornetq-data",
         "hq",
         config.getJournalType() == JournalType.ASYNCIO ? config.getJournalMaxIO_AIO()
            : config.getJournalMaxIO_NIO());

      messageJournal = localMessage;
      originalMessageJournal = localMessage;

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

   /**
    * Starts replication. In practice that means 2 things:<br/>
    * (1) every persistent piece of data is also replicated (sent) to the backup.<br/>
    * (2) all currently existing data must be sent to the backup.
    * <p>
    * To achieve this we lock the entire journal while collecting the list of files to send to the
    * backup. The journal does not remain locked during actual synchronization.
    * @param replicationManager
    * @param pagingManager
    * @throws HornetQException
    */
   @Override
   public void startReplication(ReplicationManager replicationManager, PagingManager pagingManager, String nodeID,
                                final boolean autoFailBack) throws Exception
   {
      if (!started)
      {
         throw new IllegalStateException("JournalStorageManager must be started...");
      }
      assert replicationManager != null;

      if (!(messageJournal instanceof JournalImpl) || !(bindingsJournal instanceof JournalImpl))
      {
         throw HornetQMessageBundle.BUNDLE.notJournalImpl();
      }
      JournalFile[] messageFiles = null;
      JournalFile[] bindingsFiles = null;

      try
      {

         Map<String, Long> largeMessageFilesToSync;
         Map<SimpleString, Collection<Integer>> pageFilesToSync;
         storageManagerLock.writeLock().lock();
         try
         {
            if (isReplicated())
               throw new HornetQIllegalStateException("already replicating");
            replicator = replicationManager;
            originalMessageJournal.synchronizationLock();
            originalBindingsJournal.synchronizationLock();
            try
            {
               pagingManager.lock();
               try
               {
                  messageFiles =
                     prepareJournalForCopy(originalMessageJournal, JournalContent.MESSAGES, nodeID, autoFailBack);
                  bindingsFiles =
                     prepareJournalForCopy(originalBindingsJournal, JournalContent.BINDINGS, nodeID, autoFailBack);
                  pageFilesToSync = getPageInformationForSync(pagingManager);
                  largeMessageFilesToSync = getLargeMessageInformation();
               }
               finally
               {
                  pagingManager.unlock();
               }
            }
            finally
            {
               originalMessageJournal.synchronizationUnlock();
               originalBindingsJournal.synchronizationUnlock();
            }
            bindingsJournal = new ReplicatedJournal(((byte)0), originalBindingsJournal, replicator);
            messageJournal = new ReplicatedJournal((byte)1, originalMessageJournal, replicator);
         }
         finally
         {
            storageManagerLock.writeLock().unlock();
         }

         sendJournalFile(messageFiles, JournalContent.MESSAGES);
         sendJournalFile(bindingsFiles, JournalContent.BINDINGS);
         sendLargeMessageFiles(largeMessageFilesToSync);
         sendPagesToBackup(pageFilesToSync, pagingManager);

         storageManagerLock.writeLock().lock();
         try
         {
            replicator.sendSynchronizationDone(nodeID);
            // XXX HORNETQ-720 SEND a compare journals message?
         }
         finally
         {
            storageManagerLock.writeLock().unlock();
         }
      }
      catch (Exception e)
      {
         stopReplication();
         throw e;
      }
   }

   /**
    * Stops replication by resetting replication-related fields to their 'unreplicated' state.
    */
   @Override
   public void stopReplication()
   {

      storageManagerLock.writeLock().lock();
      try
      {
         if (replicator == null)
            return;
         bindingsJournal = originalBindingsJournal;
         messageJournal = originalMessageJournal;
         try
         {
            replicator.stop();
         }
         catch (Exception e)
         {
            HornetQLogger.LOGGER.errorStoppingReplicationManager(e);
         }
         replicator = null;
      }
      finally
      {
         storageManagerLock.writeLock().unlock();
      }
   }

   /**
    * @param pageFilesToSync
    * @throws Exception
    */
   private void sendPagesToBackup(Map<SimpleString, Collection<Integer>> pageFilesToSync, PagingManager manager)
      throws Exception
   {
      for (Entry<SimpleString, Collection<Integer>> entry : pageFilesToSync.entrySet())
      {
         if (!started)
            return;
         PagingStore store = manager.getPageStore(entry.getKey());
         store.sendPages(replicator, entry.getValue());
      }
   }

   /**
    * @param pagingManager
    * @return
    * @throws Exception
    */
   private Map<SimpleString, Collection<Integer>> getPageInformationForSync(PagingManager pagingManager)
      throws Exception
   {
      Map<SimpleString, Collection<Integer>> info = new HashMap<SimpleString, Collection<Integer>>();
      for (SimpleString storeName : pagingManager.getStoreNames())
      {
         PagingStore store = pagingManager.getPageStore(storeName);
         info.put(storeName, store.getCurrentIds());
         store.forceAnotherPage();
      }
      return info;
   }

   private void sendLargeMessageFiles(Map<String, Long> largeMessageFilesToSync) throws Exception
   {
      for (Entry<String, Long> entry : largeMessageFilesToSync.entrySet())
      {
         String fileName = entry.getKey();
         long size = entry.getValue();
         SequentialFile seqFile = largeMessagesFactory.createSequentialFile(fileName, 1);
         if (!seqFile.exists())
            continue;
         if (!started)
            return;
         replicator.syncLargeMessageFile(seqFile, size, getLargeMessageIdFromFilename(fileName));
      }
   }

   private long getLargeMessageIdFromFilename(String filename)
   {
      return Long.parseLong(filename.split("\\.")[0]);
   }

   /**
    * Collects a list of existing large messages and their current size.
    * <p>
    * So we know how much of a given message to sync with the backup. Further data appends to the
    * messages will be replicated normally.
    * @return
    * @throws Exception
    */
   private Map<String, Long> getLargeMessageInformation() throws Exception
   {
      final String prefix = "msg";
      Map<String, Long> largeMessages = new HashMap<String, Long>();
      List<String> filenames = largeMessagesFactory.listFiles(prefix);

      List<Long> idList = new ArrayList<Long>();
      for (String filename : filenames)
      {
         idList.add(Long.valueOf(filename.substring(0, filename.length() - (prefix.length() + 1))));
         SequentialFile seqFile = largeMessagesFactory.createSequentialFile(filename, 1);
         long size = seqFile.size();
         largeMessages.put(filename, size);
      }
      replicator.sendLargeMessageIdListMessage(idList);
      return largeMessages;
   }

   /**
    * Send an entire journal file to a replicating backup server.
    */
   private void sendJournalFile(JournalFile[] journalFiles, JournalContent type) throws Exception
   {
      for (JournalFile jf : journalFiles)
      {
         if (!started)
            return;
         replicator.syncJournalFile(jf, type);
      }
   }

   private JournalFile[] prepareJournalForCopy(Journal journal, JournalContent contentType, String nodeID,
                                               boolean autoFailBack) throws Exception
   {
      journal.forceMoveNextFile();
      JournalFile[] datafiles = journal.getDataFiles();
      replicator.sendStartSyncMessage(datafiles, contentType, nodeID, autoFailBack);
      return datafiles;
   }


   @Override
   public final void waitOnOperations() throws Exception
   {
      if (!started)
      {
         HornetQLogger.LOGGER.serverIsStopped();
         throw new IllegalStateException("Server is stopped");
      }
      waitOnOperations(0);
   }

   @Override
   public final boolean waitOnOperations(final long timeout) throws Exception
   {
      if (!started)
      {
         HornetQLogger.LOGGER.serverIsStopped();
         throw new IllegalStateException("Server is stopped");
      }
      return getContext().waitCompletion(timeout);
   }

   @Override
   public void pageClosed(final SimpleString storeName, final int pageNumber)
   {
      if (isReplicated())
      {
         readLock();
         try
         {
            if (isReplicated())
               replicator.pageClosed(storeName, pageNumber);
         }
         finally
         {
            readUnLock();
         }
      }
   }

   @Override
   public void pageDeleted(final SimpleString storeName, final int pageNumber)
   {
      if (isReplicated())
      {
         readLock();
         try
         {
            if (isReplicated())
               replicator.pageDeleted(storeName, pageNumber);
         }
         finally
         {
            readUnLock();
         }
      }
   }

   @Override
   public void pageWrite(final PagedMessage message, final int pageNumber)
   {
      if (isReplicated())
      {
         if (!message.getMessage().isDurable())
            return;
         readLock();
         try
         {
            if (isReplicated())
               replicator.pageWrite(message, pageNumber);
         }
         finally
         {
            readUnLock();
         }
      }
   }

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
      return idGenerator.generateID();
   }

   public long getCurrentUniqueID()
   {
      return idGenerator.getCurrentID();
   }

   public LargeServerMessage createLargeMessage()
   {
      return new LargeServerMessageImpl(this);
   }

   public final void addBytesToLargeMessage(final SequentialFile file,
                                            final long messageId, final byte[] bytes) throws Exception
   {
      readLock();
      try
      {
         file.position(file.size());

         file.writeDirect(ByteBuffer.wrap(bytes), false);

         if (isReplicated())
         {
            replicator.largeMessageWrite(messageId, bytes);
         }
      }
      finally
      {
         readUnLock();
      }
   }

   public LargeServerMessage createLargeMessage(final long id, final MessageInternal message) throws Exception
   {
      readLock();
      try
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
      finally
      {
         readUnLock();
      }
   }

   // Non transactional operations

   public long storePendingLargeMessage(final long messageID) throws Exception
   {
      readLock();
      try
      {
         long recordID = generateUniqueID();

         messageJournal.appendAddRecord(recordID,
            ADD_LARGE_MESSAGE_PENDING,
            new PendingLargeMessageEncoding(messageID),
            true,
            getContext(true));

         return recordID;
      }
      finally
      {
         readUnLock();
      }
   }

   public void confirmPendingLargeMessageTX(final Transaction tx, long messageID, long recordID) throws Exception
   {
      readLock();
      try
      {
         installLargeMessageConfirmationOnTX(tx, recordID);
         messageJournal.appendDeleteRecordTransactional(tx.getID(), recordID,
            new DeleteEncoding(ADD_LARGE_MESSAGE_PENDING, messageID));
      }
      finally
      {
         readUnLock();
      }
   }

   /** We don't need messageID now but we are likely to need it we ever decide to support a database */
   public void confirmPendingLargeMessage(long recordID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendDeleteRecord(recordID, true, getContext());
      }
      finally
      {
         readUnLock();
      }
   }

   public void storeMessage(final ServerMessage message) throws Exception
   {
      if (message.getMessageID() <= 0)
      {
         // Sanity check only... this shouldn't happen unless there is a bug
         throw HornetQMessageBundle.BUNDLE.messageIdNotAssigned();
      }

      readLock();
      try
      {
         // Note that we don't sync, the add reference that comes immediately after will sync if
         // appropriate

         if (message.isLargeMessage())
         {
            messageJournal.appendAddRecord(message.getMessageID(), JournalStorageManager.ADD_LARGE_MESSAGE,
               new LargeMessageEncoding((LargeServerMessage)message), false,
               getContext(false));
         }
         else
         {
            messageJournal.appendAddRecord(message.getMessageID(), JournalStorageManager.ADD_MESSAGE, message, false,
               getContext(false));
         }
      }
      finally
      {
         readUnLock();
      }
   }

   public void storeReference(final long queueID, final long messageID, final boolean last) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendUpdateRecord(messageID, JournalStorageManager.ADD_REF, new RefEncoding(queueID), last &&
            syncNonTransactional, getContext(last && syncNonTransactional));
      }
      finally
      {
         readUnLock();
      }
   }

   private void readLock()
   {
      storageManagerLock.readLock().lock();
   }

   private void readUnLock()
   {
      storageManagerLock.readLock().unlock();
   }

   public void storeAcknowledge(final long queueID, final long messageID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendUpdateRecord(messageID, JournalStorageManager.ACKNOWLEDGE_REF, new RefEncoding(queueID),
            syncNonTransactional, getContext(syncNonTransactional));
      }
      finally
      {
         readUnLock();
      }
   }

   public void storeCursorAcknowledge(long queueID, PagePosition position) throws Exception
   {
      readLock();
      try
      {
         long ackID = idGenerator.generateID();
         position.setRecordID(ackID);
         messageJournal.appendAddRecord(ackID,
            ACKNOWLEDGE_CURSOR,
            new CursorAckRecordEncoding(queueID, position),
            syncNonTransactional,
            getContext(syncNonTransactional));
      }
      finally
      {
         readUnLock();
      }
   }

   public void deleteMessage(final long messageID) throws Exception
   {
      readLock();
      try
      {
         // Messages are deleted on postACK, one after another.
         // If these deletes are synchronized, we would build up messages on the Executor
         // increasing chances of losing deletes.
         // The StorageManager should verify messages without references
         messageJournal.appendDeleteRecord(messageID, false, getContext(false));
      }
      finally
      {
         readUnLock();
      }
   }

   public void updateScheduledDeliveryTime(final MessageReference ref) throws Exception
   {
      ScheduledDeliveryEncoding encoding = new ScheduledDeliveryEncoding(ref.getScheduledDeliveryTime(), ref.getQueue()
         .getID());
      readLock();
      try
      {
         messageJournal.appendUpdateRecord(ref.getMessage().getMessageID(),
            JournalStorageManager.SET_SCHEDULED_DELIVERY_TIME,
            encoding,
            syncNonTransactional,
            getContext(syncNonTransactional));
      }
      finally
      {
         readUnLock();
      }
   }

   public void storeDuplicateID(final SimpleString address, final byte[] duplID, final long recordID) throws Exception
   {
      readLock();
      try
      {
         DuplicateIDEncoding encoding = new DuplicateIDEncoding(address, duplID);

         messageJournal.appendAddRecord(recordID,
            JournalStorageManager.DUPLICATE_ID,
            encoding,
            syncNonTransactional,
            getContext(syncNonTransactional));
      }
      finally
      {
         readUnLock();
      }
   }

   public void deleteDuplicateID(final long recordID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendDeleteRecord(recordID, syncNonTransactional, getContext(syncNonTransactional));
      }
      finally
      {
         readUnLock();
      }
   }

   // Transactional operations

   public void storeMessageTransactional(final long txID, final ServerMessage message) throws Exception
   {
      if (message.getMessageID() <= 0)
      {
         throw HornetQMessageBundle.BUNDLE.messageIdNotAssigned();
      }

      readLock();
      try
      {
         if (message.isLargeMessage())
         {
            messageJournal.appendAddRecordTransactional(txID, message.getMessageID(),
               JournalStorageManager.ADD_LARGE_MESSAGE,
               new LargeMessageEncoding(((LargeServerMessage)message)));
         }
         else
         {
            messageJournal.appendAddRecordTransactional(txID, message.getMessageID(),
               JournalStorageManager.ADD_MESSAGE, message);
         }

      }
      finally
      {
         readUnLock();
      }
   }

   public void storePageTransaction(final long txID, final PageTransactionInfo pageTransaction) throws Exception
   {
      readLock();
      try
      {
         pageTransaction.setRecordID(generateUniqueID());
         messageJournal.appendAddRecordTransactional(txID, pageTransaction.getRecordID(),
            JournalStorageManager.PAGE_TRANSACTION, pageTransaction);
      }
      finally
      {
         readUnLock();
      }
   }

   public void updatePageTransaction(final long txID, final PageTransactionInfo pageTransaction, final int depages)
      throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendUpdateRecordTransactional(txID, pageTransaction.getRecordID(),
            JournalStorageManager.PAGE_TRANSACTION,
            new PageUpdateTXEncoding(pageTransaction.getTransactionID(),
               depages));
      }
      finally
      {
         readUnLock();
      }
   }


   public void updatePageTransaction(final PageTransactionInfo pageTransaction, final int depages) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendUpdateRecord(pageTransaction.getRecordID(), JournalStorageManager.PAGE_TRANSACTION,
            new PageUpdateTXEncoding(pageTransaction.getTransactionID(), depages),
            syncNonTransactional, getContext(syncNonTransactional));
      }
      finally
      {
         readUnLock();
      }
   }

   public void storeReferenceTransactional(final long txID, final long queueID, final long messageID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendUpdateRecordTransactional(txID, messageID, JournalStorageManager.ADD_REF,
            new RefEncoding(queueID));
      }
      finally
      {
         readUnLock();
      }
   }

   public void storeAcknowledgeTransactional(final long txID, final long queueID, final long messageID)
      throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendUpdateRecordTransactional(txID, messageID, JournalStorageManager.ACKNOWLEDGE_REF,
            new RefEncoding(queueID));
      }
      finally
      {
         readUnLock();
      }
   }

   public void storeCursorAcknowledgeTransactional(long txID, long queueID, PagePosition position) throws Exception
   {
      readLock();
      try
      {
         long ackID = idGenerator.generateID();
         position.setRecordID(ackID);
         messageJournal.appendAddRecordTransactional(txID,
            ackID,
            ACKNOWLEDGE_CURSOR,
            new CursorAckRecordEncoding(queueID, position));
      }
      finally
      {
         readUnLock();
      }
   }

   public void storePageCompleteTransactional(long txID, long queueID, PagePosition position) throws Exception
   {
      long recordID = idGenerator.generateID();
      position.setRecordID(recordID);
      messageJournal.appendAddRecordTransactional(txID,
         recordID,
         PAGE_CURSOR_COMPLETE,
         new CursorAckRecordEncoding(queueID, position));
   }

   public void deletePageComplete(long ackID) throws Exception
   {
      messageJournal.appendDeleteRecord(ackID, false);
   }

   public void deleteCursorAcknowledgeTransactional(long txID, long ackID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendDeleteRecordTransactional(txID, ackID);
      }
      finally
      {
         readUnLock();
      }
   }

   public long storeHeuristicCompletion(final Xid xid, final boolean isCommit) throws Exception
   {
      readLock();
      try
      {
         long id = generateUniqueID();

         messageJournal.appendAddRecord(id,
            JournalStorageManager.HEURISTIC_COMPLETION,
            new HeuristicCompletionEncoding(xid, isCommit),
            true,
            getContext(true));
         return id;
      }
      finally
      {
         readUnLock();
      }
   }

   public void deleteHeuristicCompletion(final long id) throws Exception
   {
      readLock();
      try
      {

         messageJournal.appendDeleteRecord(id, true, getContext(true));
      }
      finally
      {
         readUnLock();
      }
   }

   public void deletePageTransactional(final long recordID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendDeleteRecord(recordID, false);
      }
      finally
      {
         readUnLock();
      }
   }

   public void updateScheduledDeliveryTimeTransactional(final long txID, final MessageReference ref) throws Exception
   {
      ScheduledDeliveryEncoding encoding = new ScheduledDeliveryEncoding(ref.getScheduledDeliveryTime(), ref.getQueue()
         .getID());
      readLock();
      try
      {

         messageJournal.appendUpdateRecordTransactional(txID,
            ref.getMessage().getMessageID(),
            JournalStorageManager.SET_SCHEDULED_DELIVERY_TIME,
            encoding);
      }
      finally
      {
         readUnLock();
      }
   }

   public void prepare(final long txID, final Xid xid) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendPrepareRecord(txID, new XidEncoding(xid), syncTransactional, getContext(syncTransactional));
      }
      finally
      {
         readUnLock();
      }
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
      readLock();
      try
      {
         messageJournal.appendCommitRecord(txID, syncTransactional, getContext(syncTransactional), lineUpContext);
         if (!lineUpContext && !syncTransactional)
         {
            // if lineUpContext == false, we have previously lined up a context, hence we need to mark it as done even if
            // syncTransactional = false
            getContext(true).done();
         }
      }
      finally
      {
         readUnLock();
      }
   }

   public void rollback(final long txID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendRollbackRecord(txID, syncTransactional, getContext(syncTransactional));
      }
      finally
      {
         readUnLock();
      }
   }


   public void storeDuplicateIDTransactional(final long txID,
                                             final SimpleString address,
                                             final byte[] duplID,
                                             final long recordID) throws Exception
   {
      DuplicateIDEncoding encoding = new DuplicateIDEncoding(address, duplID);

      readLock();
      try
      {
         messageJournal.appendAddRecordTransactional(txID, recordID, JournalStorageManager.DUPLICATE_ID, encoding);
      }
      finally
      {
         readUnLock();
      }
   }

   public void updateDuplicateIDTransactional(final long txID,
                                              final SimpleString address,
                                              final byte[] duplID,
                                              final long recordID) throws Exception
   {
      DuplicateIDEncoding encoding = new DuplicateIDEncoding(address, duplID);

      readLock();
      try
      {
         messageJournal.appendUpdateRecordTransactional(txID, recordID, JournalStorageManager.DUPLICATE_ID, encoding);
      }
      finally
      {
         readUnLock();
      }
   }

   public void deleteDuplicateIDTransactional(final long txID, final long recordID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendDeleteRecordTransactional(txID, recordID);
      }
      finally
      {
         readUnLock();
      }
   }

   // Other operations

   public void updateDeliveryCount(final MessageReference ref) throws Exception
   {
      // no need to store if it's the same value
      // otherwise the journal will get OME in case of lots of redeliveries
      if (ref.getDeliveryCount() == ref.getPersistedCount())
      {
         return;
      }

      ref.setPersistedCount(ref.getDeliveryCount());
      DeliveryCountUpdateEncoding updateInfo =
         new DeliveryCountUpdateEncoding(ref.getQueue().getID(), ref.getDeliveryCount());

      readLock();
      try
      {
         messageJournal.appendUpdateRecord(ref.getMessage().getMessageID(),
            JournalStorageManager.UPDATE_DELIVERY_COUNT, updateInfo,
            syncNonTransactional, getContext(syncNonTransactional));
      }
      finally
      {
         readUnLock();
      }
   }

   public void storeAddressSetting(PersistedAddressSetting addressSetting) throws Exception
   {
      deleteAddressSetting(addressSetting.getAddressMatch());
      readLock();
      try
      {
         long id = idGenerator.generateID();
         addressSetting.setStoreId(id);
         bindingsJournal.appendAddRecord(id, ADDRESS_SETTING_RECORD, addressSetting, true);
         mapPersistedAddressSettings.put(addressSetting.getAddressMatch(), addressSetting);
      }
      finally
      {
         readUnLock();
      }
   }

   public List<PersistedAddressSetting> recoverAddressSettings() throws Exception
   {
      ArrayList<PersistedAddressSetting> list = new ArrayList<PersistedAddressSetting>(mapPersistedAddressSettings.size());
      list.addAll(mapPersistedAddressSettings.values());
      return list;
   }

   public List<PersistedRoles> recoverPersistedRoles() throws Exception
   {
      ArrayList<PersistedRoles> list = new ArrayList<PersistedRoles>(mapPersistedRoles.size());
      list.addAll(mapPersistedRoles.values());
      return list;
   }


   public void storeSecurityRoles(PersistedRoles persistedRoles) throws Exception
   {

      deleteSecurityRoles(persistedRoles.getAddressMatch());
      readLock();
      try
      {
         final long id = idGenerator.generateID();
         persistedRoles.setStoreId(id);
         bindingsJournal.appendAddRecord(id, SECURITY_RECORD, persistedRoles, true);
         mapPersistedRoles.put(persistedRoles.getAddressMatch(), persistedRoles);
      }
      finally
      {
         readUnLock();
      }
   }

   @Override
   public final void storeID(final long journalID, final long id) throws Exception
   {
      readLock();
      try
      {
         bindingsJournal.appendAddRecord(journalID, JournalStorageManager.ID_COUNTER_RECORD,
            BatchingIDGenerator.createIDEncodingSupport(id), true);
      }
      finally
      {
         readUnLock();
      }
   }

   public void deleteAddressSetting(SimpleString addressMatch) throws Exception
   {
      PersistedAddressSetting oldSetting = mapPersistedAddressSettings.remove(addressMatch);
      if (oldSetting != null)
      {
         readLock();
         try
         {
            bindingsJournal.appendDeleteRecord(oldSetting.getStoreId(), false);
         }
         finally
         {
            readUnLock();
         }
      }
   }

   public void deleteSecurityRoles(SimpleString addressMatch) throws Exception
   {
      PersistedRoles oldRoles = mapPersistedRoles.remove(addressMatch);
      if (oldRoles != null)
      {
         readLock();
         try
         {
            bindingsJournal.appendDeleteRecord(oldRoles.getStoreId(), false);
         }
         finally
         {
            readUnLock();
         }
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
      readLock();
      try
      {

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

               HornetQLogger.LOGGER.percentLoaded(percent);
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
                     HornetQLogger.LOGGER.cannotFindMessage(record.id);
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
                     HornetQLogger.LOGGER.journalCannotFindQueue(encoding.queueID, messageID);
                  }
                  else
                  {
                     AddMessageRecord rec = queueMessages.remove(messageID);

                     if (rec == null)
                     {
                        HornetQLogger.LOGGER.cannotFindMessage(messageID);
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
                     HornetQLogger.LOGGER.journalCannotFindQueueDelCount(encoding.queueID);
                  }
                  else
                  {
                     AddMessageRecord rec = queueMessages.get(messageID);

                     if (rec == null)
                     {
                        HornetQLogger.LOGGER.journalCannotFindMessageDelCount(messageID);
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
                     HornetQLogger.LOGGER.journalCannotFindQueueScheduled(encoding.queueID, messageID);
                  }
                  else
                  {

                     AddMessageRecord rec = queueMessages.get(messageID);

                     if (rec == null)
                     {
                        HornetQLogger.LOGGER.cannotFindMessage(messageID);
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
                     HornetQLogger.LOGGER.journalCannotFindQueueReloading(encoding.queueID);
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
                     HornetQLogger.LOGGER.journalCannotFindQueueReloadingPage(encoding.queueID);
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
                     HornetQLogger.LOGGER.journalCannotFindQueueReloadingPageCursor(encoding.queueID);
                     messageJournal.appendDeleteRecord(record.id, false);
                  }

                  break;
               }

               case PAGE_CURSOR_COMPLETE:
               {
                  CursorAckRecordEncoding encoding = new CursorAckRecordEncoding();
                  encoding.decode(buff);

                  encoding.position.setRecordID(record.id);

                  PageSubscription sub = locateSubscription(encoding.queueID, pageSubscriptions, queueInfos, pagingManager);

                  if (sub != null)
                  {
                     sub.reloadPageCompletion(encoding.position);
                  }
                  else
                  {
                     HornetQLogger.LOGGER.cantFindQueueOnPageComplete(encoding.queueID);
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
               if (queueRecords.values().size() != 0)
               {
                  HornetQLogger.LOGGER.journalCannotFindQueueForMessage(queueID);
               }

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
               HornetQLogger.LOGGER.largeMessageWithNoRef(msg.getMessageID());
               msg.decrementDelayDeletionCount();
            }
         }

         for (ServerMessage msg : messages.values())
         {
            if (msg.getRefCount() == 0)
            {
               HornetQLogger.LOGGER.journalUnreferencedMessage(msg.getMessageID());
               try
               {
                  deleteMessage(msg.getMessageID());
               }
               catch (Exception ignored)
               {
                  HornetQLogger.LOGGER.journalErrorDeletingMessage(ignored, msg.getMessageID());
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
      finally
      {
         readUnLock();
      }
   }

   /**
    * @param queueID
    * @param pageSubscriptions
    * @param queueInfos
    * @return
    */
   private static PageSubscription locateSubscription(final long queueID,
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
            subs = store.getCursorProvider().getSubscription(queueID);
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
      readLock();
      try
      {
         bindingsJournal.appendAddRecord(groupBinding.getId(), JournalStorageManager.GROUP_RECORD, groupingEncoding,
            true);
      }
      finally
      {
         readUnLock();
      }
   }

   public void deleteGrouping(final GroupBinding groupBinding) throws Exception
   {
      readLock();
      try
      {
         bindingsJournal.appendDeleteRecord(groupBinding.getId(), true);
      }
      finally
      {
         readUnLock();
      }
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

      readLock();
      try
      {
         bindingsJournal.appendAddRecordTransactional(tx, binding.getID(),
            JournalStorageManager.QUEUE_BINDING_RECORD,
            bindingEncoding);
      }
      finally
      {
         readUnLock();
      }
   }

   public void deleteQueueBinding(final long queueBindingID) throws Exception
   {
      readLock();
      try
      {
         bindingsJournal.appendDeleteRecord(queueBindingID, true);
      }
      finally
      {
         readUnLock();
      }
   }

   public long storePageCounterInc(long txID, long queueID, int value) throws Exception
   {
      readLock();
      try
      {
         long recordID = idGenerator.generateID();
         messageJournal.appendAddRecordTransactional(txID,
            recordID,
            JournalStorageManager.PAGE_CURSOR_COUNTER_INC,
            new PageCountRecordInc(queueID, value));
         return recordID;
      }
      finally
      {
         readUnLock();
      }
   }

   public long storePageCounterInc(long queueID, int value) throws Exception
   {
      readLock();
      try
      {
         final long recordID = idGenerator.generateID();
         messageJournal.appendAddRecord(recordID,
            JournalStorageManager.PAGE_CURSOR_COUNTER_INC,
            new PageCountRecordInc(queueID, value),
            true,
            getContext());
         return recordID;
      }
      finally
      {
         readUnLock();
      }
   }

   @Override
   public long storePageCounter(long txID, long queueID, long value) throws Exception
   {
      readLock();
      try
      {
         final long recordID = idGenerator.generateID();
         messageJournal.appendAddRecordTransactional(txID, recordID, JournalStorageManager.PAGE_CURSOR_COUNTER_VALUE,
            new PageCountRecord(queueID, value));
         return recordID;
      }
      finally
      {
         readUnLock();
      }
   }

   public void deleteIncrementRecord(long txID, long recordID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendDeleteRecordTransactional(txID, recordID);
      }
      finally
      {
         readUnLock();
      }
   }

   public void deletePageCounter(long txID, long recordID) throws Exception
   {
      readLock();
      try
      {
         messageJournal.appendDeleteRecordTransactional(txID, recordID);
      }
      finally
      {
         readUnLock();
      }
   }

   static final void describeBindingJournal(final String bindingsDir) throws Exception
   {

      SequentialFileFactory bindingsFF = new NIOSequentialFileFactory(bindingsDir, null);

      JournalImpl bindings = new JournalImpl(1024 * 1024, 2, -1, 0, bindingsFF, "hornetq-bindings", "bindings", 1);
      describeJournal(bindingsFF, bindings, bindingsDir);
   }

   static final void describeMessagesJournal(final String messagesDir) throws Exception
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

      describeJournal(messagesFF, messagesJournal, messagesDir);
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
      readLock();
      try
      {
         messageJournal.lineUpContex(getContext());
      }
      finally
      {
         readUnLock();
      }
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

      if (replicator != null)
         replicator.stop();

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

   /** TODO: Is this still being used ? */
   public JournalLoadInformation[] loadInternalOnly() throws Exception
   {
      readLock();
      try
      {
         JournalLoadInformation[] info = new JournalLoadInformation[2];
         info[0] = bindingsJournal.loadInternalOnly();
         info[1] = messageJournal.loadInternalOnly();

         return info;
      }
      finally
      {
         readUnLock();
      }
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
   void deleteLargeMessageFile(final LargeServerMessage largeServerMessageImpl) throws HornetQException
   {
      final SequentialFile file = largeServerMessageImpl.getFile();
      if (file == null)
      {
         return;
      }
      Runnable deleteAction = new Runnable()
      {
         public void run()
         {
            try
            {
               readLock();
               try
               {
                  if (replicator != null)
                  {
                     replicator.largeMessageDelete(largeServerMessageImpl.getMessageID());
                  }
                  file.delete();
               }
               finally
               {
                  readUnLock();
               }
            }
            catch (Exception e)
            {
               HornetQLogger.LOGGER.journalErrorDeletingMessage(e, largeServerMessageImpl.getMessageID());
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

   SequentialFile createFileForLargeMessage(final long messageID, final boolean durable)
   {
      if (durable)
      {
         return createFileForLargeMessage(messageID, ".msg");
      }
      else
      {
         return createFileForLargeMessage(messageID, ".tmp");
      }
   }


   public SequentialFile createFileForLargeMessage(final long messageID, String extension)
   {
      return largeMessagesFactory.createSequentialFile(messageID + extension, -1);
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
            throw HornetQMessageBundle.BUNDLE.cannotCreateDir(dir);
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
                     HornetQLogger.LOGGER.journalMessageInPreparedTX(encoding.queueID);
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
                     HornetQLogger.LOGGER.journalErrorRemovingRef(messageID);
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
                     HornetQLogger.LOGGER.journalCannotFindQueueReloadingACK(encoding.queueID);
                  }
                  break;
               }
               case PAGE_CURSOR_COUNTER_VALUE:
               {
                  HornetQLogger.LOGGER.journalPAGEOnPrepared();

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
                     HornetQLogger.LOGGER.journalCannotFindQueueReloadingACK(encoding.queueID);
                  }

                  break;
               }

               default:
               {
                  HornetQLogger.LOGGER.journalInvalidRecordType(recordType);
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
                     if (!pendingLargeMessages.remove(new Pair<Long, Long>(recordDeleted.id, messageID)))
                     {
                        // TODO: Logging
                        HornetQLogger.LOGGER.warn("Large message " + recordDeleted.id + " wasn't found when dealing with add pending large message");
                     }
                     installLargeMessageConfirmationOnTX(tx, recordDeleted.id);
                     break;
                  }
                  default:
                     HornetQLogger.LOGGER.journalInvalidRecordTypeOnPreparedTX(b);
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

   private OperationContext getContext(final boolean sync)
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

   private final static class DummyOperationContext implements OperationContext
   {

      private static DummyOperationContext instance = new DummyOperationContext();

      public static OperationContext getInstance()
      {
         return DummyOperationContext.instance;
      }

      public void executeOnCompletion(final IOAsyncTask runnable)
      {
         // There are no executeOnCompletion calls while using the DummyOperationContext
         // However we keep the code here for correctness
         runnable.done();
      }

      public void replicationDone()
      {
      }

      public void replicationLineUp()
      {
      }

      public void storeLineUp()
      {
      }

      public void done()
      {
      }

      public void onError(final int errorCode, final String errorMessage)
      {
      }

      public void waitCompletion()
      {
      }

      public boolean waitCompletion(final long timeout)
      {
         return true;
      }

      public void pageSyncLineUp()
      {
      }

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

   private static class HeuristicCompletionEncoding implements EncodingSupport
   {
      public Xid xid;

      public boolean isCommit;

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

   private static class GroupingEncoding implements EncodingSupport, GroupingInfo
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

      @Override
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

      @Override
      public String toString()
      {
         return "QueueEncoding [queueID=" + queueID + "]";
      }

   }

   private static class DeleteEncoding implements EncodingSupport
   {
      public byte recordType;

      public long id;

      public DeleteEncoding(final byte recordType, final long id)
      {
         this.recordType = recordType;
         this.id = id;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#getEncodeSize()
       */
      @Override
      public int getEncodeSize()
      {
         return DataConstants.SIZE_BYTE + DataConstants.SIZE_LONG;
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#encode(org.hornetq.api.core.HornetQBuffer)
       */
      @Override
      public void encode(HornetQBuffer buffer)
      {
         buffer.writeByte(recordType);
         buffer.writeLong(id);
      }

      /* (non-Javadoc)
       * @see org.hornetq.core.journal.EncodingSupport#decode(org.hornetq.api.core.HornetQBuffer)
       */
      @Override
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

   private static class ScheduledDeliveryEncoding extends QueueEncoding
   {
      long scheduledDeliveryTime;

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

   /** This is only used when loading a transaction
    it might be possible to merge the functionality of this class with {@link PagingStoreImpl.FinishPageMessageOperation}

    */
   // TODO: merge this class with the one on the PagingStoreImpl
   private static class FinishPageMessageOperation implements TransactionOperation
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

      @Override
      public int getEncodeSize()
      {
         return DataConstants.SIZE_LONG * 2;
      }

      @Override
      public void encode(HornetQBuffer buffer)
      {
         buffer.writeLong(queueID);
         buffer.writeLong(value);
      }

      @Override
      public void decode(HornetQBuffer buffer)
      {
         queueID = buffer.readLong();
         value = buffer.readLong();
      }

   }

   private static final class PageCountRecordInc implements EncodingSupport
   {

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

      public int getEncodeSize()
      {
         return DataConstants.SIZE_LONG + DataConstants.SIZE_INT;
      }

      public void encode(HornetQBuffer buffer)
      {
         buffer.writeLong(queueID);
         buffer.writeInt(value);
      }

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
                  HornetQLogger.LOGGER.journalError(e);
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

   public static Object newObjectEncoding(RecordInfo info)
   {
      return newObjectEncoding(info, null);
   }

   public static Object newObjectEncoding(RecordInfo info, JournalStorageManager storageManager)
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

            LargeServerMessage largeMessage = new LargeServerMessageImpl(storageManager);

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

         case PAGE_CURSOR_COMPLETE:
         {
            CursorAckRecordEncoding encoding = new CursorAckRecordEncoding();

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
            EncodingSupport idReturn = new IDCounterEncoding();
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

      @Override
      public String toString()
      {
         return "AddRef;" + refEncoding;
      }

   }

   public static class AckDescribe
   {
      public RefEncoding refEncoding;

      public AckDescribe(RefEncoding refEncoding)
      {
         this.refEncoding = refEncoding;
      }

      @Override
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

      @Override
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
   private static PersistedRoles newSecurityRecord(long id, HornetQBuffer buffer)
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
   private static PersistedAddressSetting newAddressEncoding(long id, HornetQBuffer buffer)
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
   private static GroupingEncoding newGroupEncoding(long id, HornetQBuffer buffer)
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
   private static PersistentQueueBindingEncoding newBindingEncoding(long id, HornetQBuffer buffer)
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

   private static Xid toXid(final byte[] data)
   {
      try
      {
         return XidCodecSupport.decodeXid(HornetQBuffers.wrappedBuffer(data));
      }
      catch (Exception e)
      {
         return null;
      }
   }


   /**
    * @param fileFactory
    * @param journal
    * @throws Exception
    */
   private static void
   describeJournal(SequentialFileFactory fileFactory, JournalImpl journal, final String path) throws Exception
   {
      List<JournalFile> files = journal.orderFiles();

      final PrintStream out = System.out;

      out.println("Journal path: " + path);

      for (JournalFile file : files)
      {
         out.println("#" + file + " (size=" + file.getFile().size() + ")");

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
                  encode(extraData) +
                  ", xid=" + toXid(extraData));
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

   @Override
   public
   boolean
   addToPage(PagingStore store, ServerMessage msg, Transaction tx, RouteContextList listCtx)
      throws Exception
   {
      /**
       * Exposing the read-lock here is an encapsulation violation done in order to keep the code
       * simpler. The alternative would be to add a second method, say 'verifyPaging', to
       * PagingStore.
       * <p>
       * Adding this second method would also be more surprise prone as it would require a certain
       * calling order.
       * <p>
       * The reasoning is that exposing the lock is more explicit and therefore `less bad`.
       */
      return store.page(msg, tx, listCtx, storageManagerLock.readLock());
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

   final class TXLargeMessageConfirmationOperation implements TransactionOperation
   {
      public List<Long> confirmedMessages = new LinkedList<Long>();

      public void beforePrepare(Transaction tx) throws Exception
      {
      }

      public void afterPrepare(Transaction tx)
      {
      }

      public void beforeCommit(Transaction tx) throws Exception
      {
      }

      public void afterCommit(Transaction tx)
      {
      }

      public void beforeRollback(Transaction tx) throws Exception
      {
      }

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
               HornetQLogger.LOGGER.journalErrorConfirmingLargeMessage(e, msg);
            }
         }
      }

      @Override
      public List<MessageReference> getRelatedMessageReferences()
      {
         return null;
      }
   }
}
