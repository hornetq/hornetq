/*
 * JBoss, Home of Professional Open Source
 * Copyright 2005-2008, Red Hat Middleware LLC, and individual contributors
 * by the @authors tag. See the copyright.txt in the distribution for a
 * full listing of individual contributors.
 *
 * This is free software; you can redistribute it and/or modify it
 * under the terms of the GNU Lesser General Public License as
 * published by the Free Software Foundation; either version 2.1 of
 * the License, or (at your option) any later version.
 *
 * This software is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this software; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA
 * 02110-1301 USA, or see the FSF site: http://www.fsf.org.
 */

package org.jboss.messaging.core.persistence.impl.journal;

import static org.jboss.messaging.util.DataConstants.SIZE_BYTE;
import static org.jboss.messaging.util.DataConstants.SIZE_INT;
import static org.jboss.messaging.util.DataConstants.SIZE_LONG;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.transaction.xa.Xid;

import org.jboss.messaging.core.config.Configuration;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.filter.Filter;
import org.jboss.messaging.core.journal.EncodingSupport;
import org.jboss.messaging.core.journal.Journal;
import org.jboss.messaging.core.journal.PreparedTransactionInfo;
import org.jboss.messaging.core.journal.RecordInfo;
import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.journal.impl.AIOSequentialFileFactory;
import org.jboss.messaging.core.journal.impl.JournalImpl;
import org.jboss.messaging.core.journal.impl.NIOSequentialFileFactory;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.message.impl.MessageImpl;
import org.jboss.messaging.core.paging.PageTransactionInfo;
import org.jboss.messaging.core.paging.PagingManager;
import org.jboss.messaging.core.paging.impl.PageTransactionInfoImpl;
import org.jboss.messaging.core.persistence.QueueBindingInfo;
import org.jboss.messaging.core.persistence.StorageManager;
import org.jboss.messaging.core.postoffice.Binding;
import org.jboss.messaging.core.postoffice.PostOffice;
import org.jboss.messaging.core.remoting.impl.ByteBufferWrapper;
import org.jboss.messaging.core.remoting.impl.wireformat.XidCodecSupport;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.core.server.JournalType;
import org.jboss.messaging.core.server.LargeServerMessage;
import org.jboss.messaging.core.server.MessageReference;
import org.jboss.messaging.core.server.Queue;
import org.jboss.messaging.core.server.ServerMessage;
import org.jboss.messaging.core.server.impl.ServerMessageImpl;
import org.jboss.messaging.core.settings.HierarchicalRepository;
import org.jboss.messaging.core.settings.impl.QueueSettings;
import org.jboss.messaging.core.transaction.ResourceManager;
import org.jboss.messaging.core.transaction.Transaction;
import org.jboss.messaging.core.transaction.TransactionOperation;
import org.jboss.messaging.core.transaction.TransactionPropertyIndexes;
import org.jboss.messaging.core.transaction.Transaction.State;
import org.jboss.messaging.core.transaction.impl.TransactionImpl;
import org.jboss.messaging.util.DataConstants;
import org.jboss.messaging.util.IDGenerator;
import org.jboss.messaging.util.JBMThreadFactory;
import org.jboss.messaging.util.Pair;
import org.jboss.messaging.util.SimpleString;
import org.jboss.messaging.util.TimeAndCounterIDGenerator;

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

   // Bindings journal record type

   public static final byte QUEUE_BINDING_RECORD = 21;

   public static final byte DESTINATION_RECORD = 22;

   // type + expiration + timestamp + priority
   public static final int SIZE_FIELDS = SIZE_INT + SIZE_LONG + SIZE_LONG + SIZE_BYTE;

   // Message journal record types

   public static final byte ADD_LARGE_MESSAGE = 30;

   public static final byte ADD_MESSAGE = 31;

   public static final byte ADD_REF = 32;

   public static final byte ACKNOWLEDGE_REF = 33;

   public static final byte UPDATE_DELIVERY_COUNT = 34;

   public static final byte PAGE_TRANSACTION = 35;

   public static final byte SET_SCHEDULED_DELIVERY_TIME = 36;

   public static final byte DUPLICATE_ID = 37;

   // This will produce a unique id **for this node only**
   private final IDGenerator idGenerator = new TimeAndCounterIDGenerator();

   private final Journal messageJournal;

   private final Journal bindingsJournal;

   private final SequentialFileFactory largeMessagesFactory;

   private final ConcurrentMap<SimpleString, Long> destinationIDMap = new ConcurrentHashMap<SimpleString, Long>();

   private volatile boolean started;

   private final ExecutorService executor;
    
   public JournalStorageManager(final Configuration config)
   {
      this.executor = Executors.newCachedThreadPool(new JBMThreadFactory("JBM-journal-storage-manager"));

      if (config.getJournalType() != JournalType.NIO && config.getJournalType() != JournalType.ASYNCIO)
      {
         throw new IllegalArgumentException("Only NIO and AsyncIO are supported journals");
      }

      String bindingsDir = config.getBindingsDirectory();

      if (bindingsDir == null)
      {
         throw new NullPointerException("bindings-dir is null");
      }

      checkAndCreateDir(bindingsDir, config.isCreateBindingsDir());

      SequentialFileFactory bindingsFF = new NIOSequentialFileFactory(bindingsDir);

      bindingsJournal = new JournalImpl(1024 * 1024, 2, true, true, bindingsFF, "jbm-bindings", "bindings", 1, -1);

      String journalDir = config.getJournalDirectory();

      if (journalDir == null)
      {
         throw new NullPointerException("journal-dir is null");
      }

      checkAndCreateDir(journalDir, config.isCreateJournalDir());

      SequentialFileFactory journalFF = null;

      if (config.getJournalType() == JournalType.ASYNCIO)
      {
         log.info("AIO journal selected");
         if (!AIOSequentialFileFactory.isSupported())
         {
            log.warn("AIO wasn't located on this platform, it will fall back to using pure Java NIO. " + "If your platform is Linux, install LibAIO to enable the AIO journal");
            journalFF = new NIOSequentialFileFactory(journalDir);
         }
         else
         {
            journalFF = new AIOSequentialFileFactory(journalDir);
            log.info("AIO loaded successfully");
         }
      }
      else if (config.getJournalType() == JournalType.NIO)
      {
         log.info("NIO Journal selected");
         journalFF = new NIOSequentialFileFactory(bindingsDir);
      }
      else
      {
         throw new IllegalArgumentException("Unsupported journal type " + config.getJournalType());
      }

      messageJournal = new JournalImpl(config.getJournalFileSize(),
                                       config.getJournalMinFiles(),
                                       config.isJournalSyncTransactional(),
                                       config.isJournalSyncNonTransactional(),
                                       journalFF,
                                       "jbm-data",
                                       "jbm",
                                       config.getJournalMaxAIO(),
                                       config.getJournalBufferReuseSize());

      String largeMessagesDirectory = config.getLargeMessagesDirectory();

      checkAndCreateDir(largeMessagesDirectory, config.isCreateJournalDir());

      largeMessagesFactory = new NIOSequentialFileFactory(config.getLargeMessagesDirectory());
   }

   /* This constructor is only used for testing */
   public JournalStorageManager(final Journal messageJournal,
                                final Journal bindingsJournal,
                                final SequentialFileFactory largeMessagesFactory)
   {
      this.executor = Executors.newCachedThreadPool(new JBMThreadFactory("JBM-journal-storage-manager"));
      this.messageJournal = messageJournal;
      this.bindingsJournal = bindingsJournal;
      this.largeMessagesFactory = largeMessagesFactory;
   }

   public long generateUniqueID()
   {
      return idGenerator.generateID();
   }
   
   public LargeServerMessage createLargeMessage()
   {
      return new JournalLargeServerMessage(this);
   }

   // Non transactional operations

   public void storeMessage(final ServerMessage message) throws Exception
   {
      if (message.getMessageID() <= 0)
      {
         throw new MessagingException(MessagingException.ILLEGAL_STATE, "MessageId was not assigned to Message");
      }

      if (message instanceof LargeServerMessage)
      {
         messageJournal.appendAddRecord(message.getMessageID(),
                                        ADD_LARGE_MESSAGE,
                                        new LargeMessageEncoding((LargeServerMessage)message));
      }
      else
      {
         messageJournal.appendAddRecord(message.getMessageID(), ADD_MESSAGE, message);
      }
   }

   public void storeReference(final long queueID, final long messageID) throws Exception
   {
      messageJournal.appendUpdateRecord(messageID, ADD_REF, new RefEncoding(queueID));
   }

   public void storeAcknowledge(final long queueID, final long messageID) throws Exception
   {
      messageJournal.appendUpdateRecord(messageID, ACKNOWLEDGE_REF, new RefEncoding(queueID));
   }

   public void deleteMessage(final long messageID) throws Exception
   {
      messageJournal.appendDeleteRecord(messageID);
   }

   public void updateScheduledDeliveryTime(final MessageReference ref) throws Exception
   {
      ScheduledDeliveryEncoding encoding = new ScheduledDeliveryEncoding(ref.getScheduledDeliveryTime(),
                                                                         ref.getQueue().getPersistenceID());

      messageJournal.appendUpdateRecord(ref.getMessage().getMessageID(), SET_SCHEDULED_DELIVERY_TIME, encoding);
   }

   public void storeDuplicateID(final SimpleString address, final byte[] duplID, final long recordID) throws Exception
   {
      DuplicateIDEncoding encoding = new DuplicateIDEncoding(address, duplID);

      messageJournal.appendAddRecord(recordID, DUPLICATE_ID, encoding);
   }

   public void updateDuplicateID(final SimpleString address, final byte[] duplID, final long recordID) throws Exception
   {
      DuplicateIDEncoding encoding = new DuplicateIDEncoding(address, duplID);

      messageJournal.appendUpdateRecord(recordID, DUPLICATE_ID, encoding);
   }

   public void deleteDuplicateID(long recordID) throws Exception
   {
      messageJournal.appendDeleteRecord(recordID);
   }

   // Transactional operations

   public void storeMessageTransactional(final long txID, final ServerMessage message) throws Exception
   {
      if (message.getMessageID() <= 0)
      {
         throw new MessagingException(MessagingException.ILLEGAL_STATE, "MessageId was not assigned to Message");
      }

      if (message instanceof LargeServerMessage)
      {
         messageJournal.appendAddRecordTransactional(txID,
                                                     message.getMessageID(),
                                                     ADD_LARGE_MESSAGE,
                                                     new LargeMessageEncoding(((LargeServerMessage)message)));
      }
      else
      {
         messageJournal.appendAddRecordTransactional(txID, message.getMessageID(), ADD_MESSAGE, message);
      }

   }

   public void storePageTransaction(final long txID, final PageTransactionInfo pageTransaction) throws Exception
   {
      if (pageTransaction.getRecordID() != 0)
      {
         // Instead of updating the record, we delete the old one as that is
         // better for reclaiming
         messageJournal.appendDeleteRecordTransactional(txID, pageTransaction.getRecordID());
      }

      pageTransaction.setRecordID(generateUniqueID());

      messageJournal.appendAddRecordTransactional(txID,
                                                  pageTransaction.getRecordID(),
                                                  PAGE_TRANSACTION,
                                                  pageTransaction);
   }

   public void storeReferenceTransactional(final long txID, final long queueID, final long messageID) throws Exception
   {
      messageJournal.appendUpdateRecordTransactional(txID, messageID, ADD_REF, new RefEncoding(queueID));
   }

   public void storeAcknowledgeTransactional(final long txID, final long queueID, final long messageID) throws Exception
   {
      messageJournal.appendUpdateRecordTransactional(txID, messageID, ACKNOWLEDGE_REF, new RefEncoding(queueID));
   }

   public void deletePageTransactional(final long txID, final long recordID) throws Exception
   {
      messageJournal.appendDeleteRecordTransactional(txID, recordID);
   }

   public void updateScheduledDeliveryTimeTransactional(final long txID, final MessageReference ref) throws Exception
   {
      ScheduledDeliveryEncoding encoding = new ScheduledDeliveryEncoding(ref.getScheduledDeliveryTime(),
                                                                         ref.getQueue().getPersistenceID());

      messageJournal.appendUpdateRecordTransactional(txID,
                                                     ref.getMessage().getMessageID(),
                                                     SET_SCHEDULED_DELIVERY_TIME,
                                                     encoding);
   }

   public void deleteMessageTransactional(final long txID, final long queueID, final long messageID) throws Exception
   {
      messageJournal.appendDeleteRecordTransactional(txID, messageID, new DeleteEncoding(queueID));
   }

   public void prepare(final long txID, final Xid xid) throws Exception
   {
      messageJournal.appendPrepareRecord(txID, new XidEncoding(xid));
   }

   public void commit(final long txID) throws Exception
   {
      messageJournal.appendCommitRecord(txID);
   }

   public void rollback(final long txID) throws Exception
   {
      messageJournal.appendRollbackRecord(txID);
   }

   public void storeDuplicateIDTransactional(final long txID,
                                             final SimpleString address,
                                             final byte[] duplID,
                                             final long recordID) throws Exception
   {
      DuplicateIDEncoding encoding = new DuplicateIDEncoding(address, duplID);

      messageJournal.appendAddRecordTransactional(txID, recordID, DUPLICATE_ID, encoding);
   }

   public void updateDuplicateIDTransactional(final long txID,
                                              final SimpleString address,
                                              final byte[] duplID,
                                              final long recordID) throws Exception
   {
      DuplicateIDEncoding encoding = new DuplicateIDEncoding(address, duplID);

      messageJournal.appendUpdateRecordTransactional(txID, recordID, DUPLICATE_ID, encoding);
   }

   public void deleteDuplicateIDTransactional(long txID, long recordID) throws Exception
   {
      messageJournal.appendDeleteRecordTransactional(txID, recordID);
   }

   // Other operations

   public void updateDeliveryCount(final MessageReference ref) throws Exception
   {
      DeliveryCountUpdateEncoding updateInfo = new DeliveryCountUpdateEncoding(ref.getQueue().getPersistenceID(),
                                                                               ref.getDeliveryCount());

      messageJournal.appendUpdateRecord(ref.getMessage().getMessageID(), UPDATE_DELIVERY_COUNT, updateInfo);
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

   public void loadMessageJournal(final PostOffice postOffice,
                                  final StorageManager storageManager,
                                  final HierarchicalRepository<QueueSettings> queueSettingsRepository,
                                  final Map<Long, Queue> queues,
                                  final ResourceManager resourceManager,
                                  final Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap) throws Exception
   {
      List<RecordInfo> records = new ArrayList<RecordInfo>();

      List<PreparedTransactionInfo> preparedTransactions = new ArrayList<PreparedTransactionInfo>();

      messageJournal.load(records, preparedTransactions);

      Map<Long, ServerMessage> messages = new HashMap<Long, ServerMessage>();

      Map<Long, Map<Long, AddMessageRecord>> queueMap = new HashMap<Long, Map<Long, AddMessageRecord>>();

      for (RecordInfo record : records)
      {
         byte[] data = record.data;

         ByteBuffer bb = ByteBuffer.wrap(data);

         MessagingBuffer buff = new ByteBufferWrapper(bb);

         byte recordType = record.getUserRecordType();

         switch (recordType)
         {
            case ADD_LARGE_MESSAGE:
            {
               LargeServerMessage largeMessage = createLargeMessage();

               LargeMessageEncoding messageEncoding = new LargeMessageEncoding(largeMessage);

               messageEncoding.decode(buff);

               messages.put(record.id, largeMessage);

               break;
            }
            case ADD_MESSAGE:
            {
               ServerMessage message = new ServerMessageImpl(record.id);

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
                  throw new IllegalStateException("Cannot find message " + record.id);
               }

               queueMessages.put(messageID, new AddMessageRecord(message));

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
                  throw new IllegalStateException("Cannot find queue messages " + encoding.queueID);
               }

               AddMessageRecord rec = queueMessages.remove(messageID);

               if (rec == null)
               {
                  throw new IllegalStateException("Cannot find message " + messageID);
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
                  throw new IllegalStateException("Cannot find queue messages " + encoding.queueID);
               }

               AddMessageRecord rec = queueMessages.get(messageID);

               if (rec == null)
               {
                  throw new IllegalStateException("Cannot find message " + messageID);
               }
               
               rec.deliveryCount = encoding.count;

               break;
            }
            case PAGE_TRANSACTION:
            {
               PageTransactionInfoImpl pageTransactionInfo = new PageTransactionInfoImpl();

               pageTransactionInfo.decode(buff);

               pageTransactionInfo.setRecordID(record.id);

               PagingManager pagingManager = postOffice.getPagingManager();
               
               pagingManager.addTransaction(pageTransactionInfo);

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
                  throw new IllegalStateException("Cannot find queue messages " + encoding.queueID);
               }

               AddMessageRecord rec = queueMessages.get(messageID);

               if (rec == null)
               {
                  throw new IllegalStateException("Cannot find message " + messageID);
               }
               
               rec.scheduledDeliveryTime = encoding.scheduledDeliveryTime;

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
            default:
            {
               throw new IllegalStateException("Invalid record type " + recordType);
            }
         }
      }
      
      for (Map.Entry<Long, Map<Long, AddMessageRecord>> entry: queueMap.entrySet())
      {
         long queueID = entry.getKey();
         
         Map<Long, AddMessageRecord> queueRecords = entry.getValue();
         
         Queue queue = queues.get(queueID);
         
         for (AddMessageRecord record: queueRecords.values())
         {
            long scheduledDeliveryTime = record.scheduledDeliveryTime;
            
            if (scheduledDeliveryTime != 0)
            {
               record.message.putLongProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME, scheduledDeliveryTime);
            }
            
            MessageReference ref = queue.reroute(record.message, null);
            
            ref.setDeliveryCount(record.deliveryCount);
            
            if (scheduledDeliveryTime != 0)
            {
               record.message.removeProperty(MessageImpl.HDR_SCHEDULED_DELIVERY_TIME);
            }
         }
      }

      loadPreparedTransactions(postOffice,
                               storageManager,
                               queueSettingsRepository,
                               queues,
                               resourceManager,
                               preparedTransactions,
                               duplicateIDMap);
   }
   
   private void loadPreparedTransactions(final PostOffice postOffice,
                                         final StorageManager storageManager,
                                         final HierarchicalRepository<QueueSettings> queueSettingsRepository,
                                         final Map<Long, Queue> queues,
                                         final ResourceManager resourceManager,
                                         final List<PreparedTransactionInfo> preparedTransactions,
                                         final Map<SimpleString, List<Pair<byte[], Long>>> duplicateIDMap) throws Exception
   {
      final PagingManager pagingManager = postOffice.getPagingManager();

      // recover prepared transactions
      for (PreparedTransactionInfo preparedTransaction : preparedTransactions)
      {
         XidEncoding encodingXid = new XidEncoding(preparedTransaction.extraData);

         Xid xid = encodingXid.xid;

         Transaction tx = new TransactionImpl(preparedTransaction.id, xid, this);

         List<MessageReference> referencesToAck = new ArrayList<MessageReference>();

         Map<Long, ServerMessage> messages = new HashMap<Long, ServerMessage>();
           
         //Use same method as load message journal to prune out acks, so they don't get added.
         //Then have reacknowledge(tx) methods on queue, which needs to add the page size

         // first get any sent messages for this tx and recreate
         for (RecordInfo record : preparedTransaction.records)
         {
            byte[] data = record.data;

            ByteBuffer bb = ByteBuffer.wrap(data);

            MessagingBuffer buff = new ByteBufferWrapper(bb);

            byte recordType = record.getUserRecordType();

            switch (recordType)
            {
               case ADD_MESSAGE:
               {
                  ServerMessage message = new ServerMessageImpl(record.id);

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
                     throw new IllegalStateException("Cannot find queue with id " + encoding.queueID);
                  }

                  ServerMessage message = messages.get(messageID);

                  if (message == null)
                  {
                     throw new IllegalStateException("Cannot find message with id " + messageID);
                  }

                  queue.reroute(message, tx);

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

                  //TODO - this involves a scan - we should find a quicker qay of doing it
                  MessageReference removed = queue.removeReferenceWithID(messageID);

                  referencesToAck.add(removed);

                  if (removed == null)
                  {
                     throw new IllegalStateException("Failed to remove reference for " + messageID);
                  }

                  break;
               }
               case PAGE_TRANSACTION:
               {
                  PageTransactionInfo pageTransactionInfo = new PageTransactionInfoImpl();

                  pageTransactionInfo.decode(buff);

                  pageTransactionInfo.markIncomplete();

                  tx.putProperty(TransactionPropertyIndexes.PAGE_TRANSACTION, pageTransactionInfo);

                  pagingManager.addTransaction(pageTransactionInfo);
                  
                  tx.addOperation(new FinishPageMessageOperation());

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

                  List<Pair<byte[], Long>> ids = duplicateIDMap.get(encoding.address);

                  if (ids == null)
                  {
                     ids = new ArrayList<Pair<byte[], Long>>();

                     duplicateIDMap.put(encoding.address, ids);
                  }

                  ids.add(new Pair<byte[], Long>(encoding.duplID, record.id));

                  break;
               }
               default:
               {
                  log.warn("InternalError: Record type " + recordType +
                           " not recognized. Maybe you're using journal files created on a different version");
               }
            }
         }

         for (RecordInfo record : preparedTransaction.recordsToDelete)
         {
            byte[] data = record.data;

            ByteBuffer bb = ByteBuffer.wrap(data);

            MessagingBuffer buff = new ByteBufferWrapper(bb);

            long messageID = record.id;

            DeleteEncoding encoding = new DeleteEncoding();

            encoding.decode(buff);

            Queue queue = queues.get(encoding.queueID);

            if (queue == null)
            {
               throw new IllegalStateException("Cannot find queue with id " + encoding.queueID);
            }

            MessageReference removed = queue.removeReferenceWithID(messageID);

            referencesToAck.add(removed);

            if (removed == null)
            {
               throw new IllegalStateException("Failed to remove reference for " + messageID);
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

   // Bindings operations

   public void addQueueBinding(final Binding binding) throws Exception
   {
      Queue queue = (Queue)binding.getBindable();
      Filter filter = queue.getFilter();

      SimpleString filterString = filter == null ? null : filter.getFilterString();

      PersistentQueueBindingEncoding bindingEncoding = new PersistentQueueBindingEncoding(queue.getName(),
                                                                                          binding.getAddress(),
                                                                                          filterString);

      long id = this.generateUniqueID();

      queue.setPersistenceID(id);

      bindingsJournal.appendAddRecord(id, QUEUE_BINDING_RECORD, bindingEncoding);
   }

   public void deleteQueueBinding(final long queueBindingID) throws Exception
   {
      bindingsJournal.appendDeleteRecord(queueBindingID);
   }

   public boolean addDestination(final SimpleString destination) throws Exception
   {
      long destinationID = idGenerator.generateID();

      if (destinationIDMap.putIfAbsent(destination, destinationID) != null)
      {
         // Already exists
         return false;
      }
      else
      {
         DestinationEncoding destinationEnc = new DestinationEncoding(destination);

         bindingsJournal.appendAddRecord(destinationID, DESTINATION_RECORD, destinationEnc);

         return true;
      }
   }

   public boolean deleteDestination(final SimpleString destination) throws Exception
   {
      Long destinationID = destinationIDMap.remove(destination);

      if (destinationID == null)
      {
         return false;
      }
      else
      {
         bindingsJournal.appendDeleteRecord(destinationID);

         return true;
      }
   }

   public void loadBindingJournal(final List<QueueBindingInfo> queueBindingInfos, final List<SimpleString> destinations) throws Exception
   {
      List<RecordInfo> records = new ArrayList<RecordInfo>();

      List<PreparedTransactionInfo> preparedTransactions = new ArrayList<PreparedTransactionInfo>();

      bindingsJournal.load(records, preparedTransactions);

      for (RecordInfo record : records)
      {
         long id = record.id;

         MessagingBuffer buffer = new ByteBufferWrapper(ByteBuffer.wrap(record.data));

         byte rec = record.getUserRecordType();

         if (rec == QUEUE_BINDING_RECORD)
         {
            PersistentQueueBindingEncoding bindingEncoding = new PersistentQueueBindingEncoding();

            bindingEncoding.decode(buffer);

            bindingEncoding.setPersistenceID(id);

            queueBindingInfos.add(bindingEncoding);
         }
         else if (rec == DESTINATION_RECORD)
         {
            DestinationEncoding destinationEncoding = new DestinationEncoding();

            destinationEncoding.decode(buffer);

            destinationIDMap.put(destinationEncoding.destination, id);

            destinations.add(destinationEncoding.destination);
         }
         else
         {
            throw new IllegalStateException("Invalid record type " + rec);
         }
      }
   }

   // MessagingComponent implementation
   // ------------------------------------------------------

   public synchronized void start() throws Exception
   {
      if (started)
      {
         return;
      }

      cleanupIncompleteFiles();

      bindingsJournal.start();

      messageJournal.start();

      started = true;
   }

   public synchronized void stop() throws Exception
   {
      if (!started)
      {
         return;
      }

      executor.shutdown();

      bindingsJournal.stop();

      messageJournal.stop();

      executor.awaitTermination(60, TimeUnit.SECONDS);

      started = false;
   }

   public synchronized boolean isStarted()
   {
      return started;
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
   void deleteFile(final SequentialFile file)
   {
      this.executor.execute(new Runnable()
      {

         public void run()
         {
            try
            {
               file.delete();
            }
            catch (Exception e)
            {
               log.warn(e.getMessage(), e);
            }
         }

      });
   }

   /**
    * @param messageID
    * @return
    */
   SequentialFile createFileForLargeMessage(final long messageID, final boolean completeFile)
   {
      if (completeFile)
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
         log.info("Directory " + dir + " does not already exists");

         if (create)
         {
            log.info("Creating it");

            if (!f.mkdirs())
            {
               throw new IllegalStateException("Failed to create directory " + dir);
            }
         }
         else
         {
            log.info("Not creating it");

            throw new IllegalArgumentException("Directory " + dir + " does not exist and will not create it");
         }
      }
      else
      {
         log.info("Directory " + dir + " already exists");
      }
   }

   /**
    * @throws Exception
    */
   private void cleanupIncompleteFiles() throws Exception
   {
      if (largeMessagesFactory != null)
      {
         List<String> tmpFiles = this.largeMessagesFactory.listFiles("tmp");
         for (String tmpFile : tmpFiles)
         {
            SequentialFile file = largeMessagesFactory.createSequentialFile(tmpFile, -1);
            log.info("Deleting file " + file);
            file.delete();
         }
      }
   }

   // Inner Classes
   // ----------------------------------------------------------------------------

   private static class XidEncoding implements EncodingSupport
   {
      final Xid xid;

      XidEncoding(final Xid xid)
      {
         this.xid = xid;
      }

      XidEncoding(final byte[] data)
      {
         xid = XidCodecSupport.decodeXid(new ByteBufferWrapper(ByteBuffer.wrap(data)));
      }

      public void decode(final MessagingBuffer buffer)
      {
         throw new IllegalStateException("Non Supported Operation");
      }

      public void encode(final MessagingBuffer buffer)
      {
         XidCodecSupport.encodeXid(xid, buffer);
      }

      public int getEncodeSize()
      {
         return XidCodecSupport.getXidEncodeLength(xid);
      }
   }

   private static class PersistentQueueBindingEncoding implements EncodingSupport, QueueBindingInfo
   {
      long persistenceID;

      SimpleString name;

      SimpleString address;

      SimpleString filterString;

      public PersistentQueueBindingEncoding()
      {
      }

      public PersistentQueueBindingEncoding(final SimpleString name,
                                            final SimpleString address,
                                            final SimpleString filterString)
      {
         this.name = name;
         this.address = address;
         this.filterString = filterString;
      }

      public long getPersistenceID()
      {
         return persistenceID;
      }

      public void setPersistenceID(final long id)
      {
         this.persistenceID = id;
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

      public void decode(final MessagingBuffer buffer)
      {
         name = buffer.getSimpleString();
         address = buffer.getSimpleString();
         filterString = buffer.getNullableSimpleString();
      }

      public void encode(final MessagingBuffer buffer)
      {
         buffer.putSimpleString(name);
         buffer.putSimpleString(address);
         buffer.putNullableSimpleString(filterString);
      }

      public int getEncodeSize()
      {
         return SimpleString.sizeofString(name) + SimpleString.sizeofString(address) +
                SimpleString.sizeofNullableString(filterString);
      }
   }

   private static class DestinationEncoding implements EncodingSupport
   {
      SimpleString destination;

      DestinationEncoding(final SimpleString destination)
      {
         this.destination = destination;
      }

      DestinationEncoding()
      {
      }

      public void decode(final MessagingBuffer buffer)
      {
         destination = buffer.getSimpleString();
      }

      public void encode(final MessagingBuffer buffer)
      {
         buffer.putSimpleString(destination);
      }

      public int getEncodeSize()
      {
         return SimpleString.sizeofString(destination);
      }

   }
   
   private static class LargeMessageEncoding implements EncodingSupport
   {
      private final LargeServerMessage message;

      public LargeMessageEncoding(LargeServerMessage message)
      {
         this.message = message;
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.journal.EncodingSupport#decode(org.jboss.messaging.core.remoting.spi.MessagingBuffer)
       */
      public void decode(final MessagingBuffer buffer)
      {
         message.decode(buffer);
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.journal.EncodingSupport#encode(org.jboss.messaging.core.remoting.spi.MessagingBuffer)
       */
      public void encode(final MessagingBuffer buffer)
      {
         message.encode(buffer);
      }

      /* (non-Javadoc)
       * @see org.jboss.messaging.core.journal.EncodingSupport#getEncodeSize()
       */
      public int getEncodeSize()
      {
         return message.getEncodeSize();
      }

   }

   private static class DeliveryCountUpdateEncoding implements EncodingSupport
   {
      long queueID;

      int count;

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

      public void decode(final MessagingBuffer buffer)
      {
         queueID = buffer.getLong();
         count = buffer.getInt();
      }

      public void encode(final MessagingBuffer buffer)
      {
         buffer.putLong(queueID);
         buffer.putInt(count);
      }

      public int getEncodeSize()
      {
         return 8 + 4;
      }
   }

   private static class QueueEncoding implements EncodingSupport
   {
      long queueID;

      public QueueEncoding(final long queueID)
      {
         super();
         this.queueID = queueID;
      }

      public QueueEncoding()
      {
         super();
      }

      public void decode(final MessagingBuffer buffer)
      {
         queueID = buffer.getLong();
      }

      public void encode(final MessagingBuffer buffer)
      {
         buffer.putLong(queueID);
      }

      public int getEncodeSize()
      {
         return 8;
      }
   }

   private static class DeleteEncoding extends QueueEncoding
   {
      public DeleteEncoding()
      {
         super();
      }

      public DeleteEncoding(final long queueID)
      {
         super(queueID);
      }
   }

   private static class RefEncoding extends QueueEncoding
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

   private static class ScheduledDeliveryEncoding extends QueueEncoding
   {

      long scheduledDeliveryTime;

      private ScheduledDeliveryEncoding(long scheduledDeliveryTime, long queueID)
      {
         super(queueID);
         this.scheduledDeliveryTime = scheduledDeliveryTime;
      }

      public ScheduledDeliveryEncoding()
      {
      }

      public int getEncodeSize()
      {
         return super.getEncodeSize() + 8;
      }

      public void encode(MessagingBuffer buffer)
      {
         super.encode(buffer);
         buffer.putLong(scheduledDeliveryTime);
      }

      public void decode(MessagingBuffer buffer)
      {
         super.decode(buffer);
         scheduledDeliveryTime = buffer.getLong();
      }
   }

   private static class DuplicateIDEncoding implements EncodingSupport
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

      public void decode(final MessagingBuffer buffer)
      {
         address = buffer.getSimpleString();

         int size = buffer.getInt();
         
         duplID = new byte[size];
         
         buffer.getBytes(duplID);
      }

      public void encode(final MessagingBuffer buffer)
      {
         buffer.putSimpleString(address);

         buffer.putInt(duplID.length);
         
         buffer.putBytes(duplID);
      }

      public int getEncodeSize()
      {
         return SimpleString.sizeofString(address) + DataConstants.SIZE_INT + duplID.length;
      }
   }
   
   private class FinishPageMessageOperation implements TransactionOperation
   {

      public void afterCommit(final Transaction tx) throws Exception
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

      public void afterPrepare(final Transaction tx) throws Exception
      {
      }

      public void afterRollback(final Transaction tx) throws Exception
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

      private void pageMessages(final Transaction tx) throws Exception
      {
      }

   }
   

}
