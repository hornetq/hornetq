/*
 * JBoss, Home of Professional Open Source Copyright 2005-2008, Red Hat
 * Middleware LLC, and individual contributors by the @authors tag. See the
 * copyright.txt in the distribution for a full listing of individual
 * contributors.
 * 
 * This is free software; you can redistribute it and/or modify it under the
 * terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 * 
 * This software is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
 * FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public License for more
 * details.
 * 
 * You should have received a copy of the GNU Lesser General Public License
 * along with this software; if not, write to the Free Software Foundation,
 * Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301 USA, or see the FSF
 * site: http://www.fsf.org.
 */

package org.jboss.messaging.core.journal.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.jboss.messaging.core.buffers.ChannelBuffer;
import org.jboss.messaging.core.buffers.ChannelBuffers;
import org.jboss.messaging.core.exception.MessagingException;
import org.jboss.messaging.core.journal.BufferCallback;
import org.jboss.messaging.core.journal.EncodingSupport;
import org.jboss.messaging.core.journal.IOCallback;
import org.jboss.messaging.core.journal.LoadManager;
import org.jboss.messaging.core.journal.PreparedTransactionInfo;
import org.jboss.messaging.core.journal.RecordInfo;
import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.journal.TestableJournal;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.core.remoting.spi.MessagingBuffer;
import org.jboss.messaging.utils.Pair;
import org.jboss.messaging.utils.VariableLatch;

/**
 * 
 * <p>A JournalImpl</p
 * 
 * <p>WIKI Page: <a href="http://wiki.jboss.org/wiki/JBossMessaging2Journal"> http://wiki.jboss.org/wiki/JBossMessaging2Journal</a></p>
 * 
 * 
 * <p>Look at {@link JournalImpl#load(LoadManager)} for the file layout
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * 
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class JournalImpl implements TestableJournal
{

   // Constants -----------------------------------------------------
   private static final int STATE_STOPPED = 0;

   private static final int STATE_STARTED = 1;

   private static final int STATE_LOADED = 2;

   // Static --------------------------------------------------------

   private static final Logger log = Logger.getLogger(JournalImpl.class);

   private static final boolean trace = log.isTraceEnabled();

   // This method exists just to make debug easier.
   // I could replace log.trace by log.info temporarily while I was debugging
   // Journal
   private static final void trace(final String message)
   {
      log.trace(message);
   }

   // The sizes of primitive types

   private static final int SIZE_LONG = 8;

   private static final int SIZE_INT = 4;

   private static final int SIZE_BYTE = 1;

   public static final int MIN_FILE_SIZE = 1024;

   public static final int SIZE_HEADER = 4;

   public static final int BASIC_SIZE = SIZE_BYTE + SIZE_INT + SIZE_INT;

   public static final int SIZE_ADD_RECORD = BASIC_SIZE + SIZE_LONG + SIZE_BYTE + SIZE_INT /* + record.length */;

   // Record markers - they must be all unique

   public static final byte ADD_RECORD = 11;

   public static final byte SIZE_UPDATE_RECORD = BASIC_SIZE + SIZE_LONG + SIZE_BYTE + SIZE_INT /* + record.length */;

   public static final byte UPDATE_RECORD = 12;

   public static final int SIZE_ADD_RECORD_TX = BASIC_SIZE + SIZE_LONG + SIZE_BYTE + SIZE_LONG + SIZE_INT /* + record.length */;

   public static final byte ADD_RECORD_TX = 13;

   public static final int SIZE_UPDATE_RECORD_TX = BASIC_SIZE + SIZE_LONG + SIZE_BYTE + SIZE_LONG + SIZE_INT /* + record.length */;

   public static final byte UPDATE_RECORD_TX = 14;

   public static final int SIZE_DELETE_RECORD_TX = BASIC_SIZE + SIZE_LONG + SIZE_LONG + SIZE_INT /* + record.length */;

   public static final byte DELETE_RECORD_TX = 15;

   public static final int SIZE_DELETE_RECORD = BASIC_SIZE + SIZE_LONG;

   public static final byte DELETE_RECORD = 16;

   public static final int SIZE_COMPLETE_TRANSACTION_RECORD = BASIC_SIZE + SIZE_INT + SIZE_LONG /* + NumerOfElements*SIZE_INT*2 */;

   public static final int SIZE_PREPARE_RECORD = SIZE_COMPLETE_TRANSACTION_RECORD + SIZE_INT;

   public static final byte PREPARE_RECORD = 17;

   public static final int SIZE_COMMIT_RECORD = SIZE_COMPLETE_TRANSACTION_RECORD;

   public static final byte COMMIT_RECORD = 18;

   public static final int SIZE_ROLLBACK_RECORD = BASIC_SIZE + SIZE_LONG;

   public static final byte ROLLBACK_RECORD = 19;

   public static final byte FILL_CHARACTER = (byte)'J';

   // Attributes ----------------------------------------------------

   private boolean autoReclaim = true;

   private final AtomicInteger nextOrderingId = new AtomicInteger(0);

   // used for Asynchronous IO only (ignored on NIO).
   private final int maxAIO;

   private final int fileSize;

   private final int minFiles;

   private final boolean syncTransactional;

   private final boolean syncNonTransactional;

   private final SequentialFileFactory fileFactory;

   public final String filePrefix;

   public final String fileExtension;

   private final Queue<JournalFile> dataFiles = new ConcurrentLinkedQueue<JournalFile>();

   private final Queue<JournalFile> freeFiles = new ConcurrentLinkedQueue<JournalFile>();

   private final BlockingQueue<JournalFile> openedFiles = new LinkedBlockingQueue<JournalFile>();

   private final ConcurrentMap<Long, PosFiles> posFilesMap = new ConcurrentHashMap<Long, PosFiles>();

   private final ConcurrentMap<Long, JournalTransaction> transactionInfos = new ConcurrentHashMap<Long, JournalTransaction>();

   private final ConcurrentMap<Long, TransactionCallback> transactionCallbacks = new ConcurrentHashMap<Long, TransactionCallback>();

   private ExecutorService filesExecutor = null;

   private final int reuseBufferSize;

   /** Object that will control buffer's callback and getting buffers from the queue */
   private final ReuseBuffersController buffersControl = new ReuseBuffersController();

   /**
    * Used to lock access while calculating the positioning of currentFile.
    * That has to be done in single-thread, and it needs to be a very-fast operation
    */
   private final Semaphore positionLock = new Semaphore(1, true);

   /**
    * a WriteLock means, currentFile is being changed. When we get a writeLock we wait all the write operations to finish on that file before we can move to the next file
    * a ReadLock means, currentFile is being used, do not change it until I'm done with it
    */
   private final ReadWriteLock rwlock = new ReentrantReadWriteLock();

   private volatile JournalFile currentFile;

   private volatile int state;

   private final Reclaimer reclaimer = new Reclaimer();

   // Constructors --------------------------------------------------

   public JournalImpl(final int fileSize,
                      final int minFiles,
                      final boolean syncTransactional,
                      final boolean syncNonTransactional,
                      final SequentialFileFactory fileFactory,
                      final String filePrefix,
                      final String fileExtension,
                      final int maxAIO,
                      final int reuseBufferSize)
   {
      if (fileSize < MIN_FILE_SIZE)
      {
         throw new IllegalArgumentException("File size cannot be less than " + MIN_FILE_SIZE + " bytes");
      }
      if (fileSize % fileFactory.getAlignment() != 0)
      {
         throw new IllegalArgumentException("Invalid journal-file-size " + fileSize +
                                            ", It should be multiple of " +
                                            fileFactory.getAlignment());
      }
      if (minFiles < 2)
      {
         throw new IllegalArgumentException("minFiles cannot be less than 2");
      }
      if (fileFactory == null)
      {
         throw new NullPointerException("fileFactory is null");
      }
      if (filePrefix == null)
      {
         throw new NullPointerException("filePrefix is null");
      }
      if (fileExtension == null)
      {
         throw new NullPointerException("fileExtension is null");
      }
      if (maxAIO <= 0)
      {
         throw new IllegalStateException("maxAIO should aways be a positive number");
      }

      this.reuseBufferSize = fileFactory.calculateBlockSize(reuseBufferSize);

      this.fileSize = fileSize;

      this.minFiles = minFiles;

      this.syncTransactional = syncTransactional;

      this.syncNonTransactional = syncNonTransactional;

      this.fileFactory = fileFactory;
      
      this.fileFactory.setBufferCallback(this.buffersControl.callback);

      this.filePrefix = filePrefix;

      this.fileExtension = fileExtension;

      this.maxAIO = maxAIO;
   }

   // Journal implementation
   // ----------------------------------------------------------------

   public void appendAddRecord(final long id, final byte recordType, final byte[] record) throws Exception
   {
      appendAddRecord(id, recordType, new ByteArrayEncoding(record));
   }
   
   public void appendAddRecord(final long id, final byte recordType, final EncodingSupport record) throws Exception
   {
      appendAddRecord(id, recordType, record, syncNonTransactional);
   }

   public void appendAddRecord(final long id, final byte recordType, final EncodingSupport record, final boolean sync) throws Exception
   {
      if (state != STATE_LOADED)
      {
         throw new IllegalStateException("Journal must be loaded first");
      }

      int recordLength = record.getEncodeSize();

      int size = SIZE_ADD_RECORD + recordLength;

      ChannelBuffer bb = ChannelBuffers.wrappedBuffer(newBuffer(size)); 

      bb.writeByte(ADD_RECORD);
      bb.writeInt(-1); // skip ID part
      bb.writeLong(id);
      bb.writeInt(recordLength);
      bb.writeByte(recordType);
      record.encode(bb);
      bb.writeInt(size);

      try
      {
         JournalFile usedFile = appendRecord(bb.toByteBuffer(), sync, null);

         posFilesMap.put(id, new PosFiles(usedFile));
      }
      finally
      {
         try
         {
            rwlock.readLock().unlock();
         }
         catch (Exception ignored)
         {
            // This could happen if the thread was interrupted
         }
      }
   }

   public void appendUpdateRecord(final long id, final byte recordType, final byte[] record) throws Exception
   {
      appendUpdateRecord(id, recordType, new ByteArrayEncoding(record));
   }

   public void appendUpdateRecord(final long id, final byte recordType, final EncodingSupport record) throws Exception
   {
      if (state != STATE_LOADED)
      {
         throw new IllegalStateException("Journal must be loaded first");
      }

      PosFiles posFiles = posFilesMap.get(id);

      if (posFiles == null)
      {
         throw new IllegalStateException("Cannot find add info " + id);
      }

      int size = SIZE_UPDATE_RECORD + record.getEncodeSize();

      ChannelBuffer bb = ChannelBuffers.wrappedBuffer(newBuffer(size)); 

      bb.writeByte(UPDATE_RECORD);
      bb.writeInt(-1); // skip ID part
      bb.writeLong(id);
      bb.writeInt(record.getEncodeSize());
      bb.writeByte(recordType);
      record.encode(bb);
      bb.writeInt(size);

      try
      {
         JournalFile usedFile = appendRecord(bb.toByteBuffer(), syncNonTransactional, null);

         posFiles.addUpdateFile(usedFile);
      }
      finally
      {
         try
         {
            rwlock.readLock().unlock();
         }
         catch (Exception ignored)
         {
            // This could happen if the thread was interrupted
         }
      }
   }

   public void appendDeleteRecord(final long id) throws Exception
   {
      if (state != STATE_LOADED)
      {
         throw new IllegalStateException("Journal must be loaded first");
      }

      PosFiles posFiles = posFilesMap.remove(id);

      if (posFiles == null)
      {
         throw new IllegalStateException("Cannot find add info " + id);
      }

      int size = SIZE_DELETE_RECORD;

      ByteBuffer bb = newBuffer(size);

      bb.put(DELETE_RECORD);
      bb.putInt(-1); // skip ID part
      bb.putLong(id);
      bb.putInt(size);

      try
      {
         JournalFile usedFile = appendRecord(bb, syncNonTransactional, null);

         posFiles.addDelete(usedFile);
      }
      finally
      {
         try
         {
            rwlock.readLock().unlock();
         }
         catch (Exception ignored)
         {
            // This could happen if the thread was interrupted
         }
      }
   }

   public void appendAddRecordTransactional(final long txID, final long id, final byte recordType, final byte[] record) throws Exception
   {
      appendAddRecordTransactional(txID, id, recordType, new ByteArrayEncoding(record));

   }

   public void appendAddRecordTransactional(final long txID,
                                            final long id,
                                            final byte recordType,
                                            final EncodingSupport record) throws Exception
   {
      if (state != STATE_LOADED)
      {
         throw new IllegalStateException("Journal must be loaded first");
      }
      
      int recordLength = record.getEncodeSize();

      int size = SIZE_ADD_RECORD_TX + recordLength;

      ChannelBuffer bb = ChannelBuffers.wrappedBuffer(newBuffer(size)); 

      bb.writeByte(ADD_RECORD_TX);
      bb.writeInt(-1); // skip ID part
      bb.writeLong(txID);
      bb.writeLong(id);
      bb.writeInt(recordLength);
      bb.writeByte(recordType);
      record.encode(bb);
      bb.writeInt(size);

      try
      {
         JournalFile usedFile = appendRecord(bb.toByteBuffer(), false, getTransactionCallback(txID));

         JournalTransaction tx = getTransactionInfo(txID);

         tx.addPositive(usedFile, id);
      }
      finally
      {
         try
         {
            rwlock.readLock().unlock();
         }
         catch (Exception ignored)
         {
            // This could happen if the thread was interrupted
         }
      }
   }

   public void appendUpdateRecordTransactional(final long txID,
                                               final long id,
                                               final byte recordType,
                                               final byte[] record) throws Exception
   {
      appendUpdateRecordTransactional(txID, id, recordType, new ByteArrayEncoding(record));
   }

   public void appendUpdateRecordTransactional(final long txID,
                                               final long id,
                                               final byte recordType,
                                               final EncodingSupport record) throws Exception
   {
      if (state != STATE_LOADED)
      {
         throw new IllegalStateException("Journal must be loaded first");
      }

      int size = SIZE_UPDATE_RECORD_TX + record.getEncodeSize();

      ChannelBuffer bb = ChannelBuffers.wrappedBuffer(newBuffer(size)); 

      bb.writeByte(UPDATE_RECORD_TX);
      bb.writeInt(-1); // skip ID part
      bb.writeLong(txID);
      bb.writeLong(id);
      bb.writeInt(record.getEncodeSize());
      bb.writeByte(recordType);
      record.encode(bb);
      bb.writeInt(size);

      try
      {
         JournalFile usedFile = appendRecord(bb.toByteBuffer(), false, getTransactionCallback(txID));

         JournalTransaction tx = getTransactionInfo(txID);

         tx.addPositive(usedFile, id);
      }
      finally
      {
         try
         {
            rwlock.readLock().unlock();
         }
         catch (Exception ignored)
         {
            // This could happen if the thread was interrupted
         }
      }
   }

   public void appendDeleteRecordTransactional(final long txID, final long id, final byte[] record) throws Exception
   {
      appendDeleteRecordTransactional(txID, id, new ByteArrayEncoding(record));
   }

   public void appendDeleteRecordTransactional(final long txID, final long id, final EncodingSupport record) throws Exception
   {
      if (state != STATE_LOADED)
      {
         throw new IllegalStateException("Journal must be loaded first");
      }

      int size = SIZE_DELETE_RECORD_TX + (record != null ? record.getEncodeSize() : 0);

      ChannelBuffer bb = ChannelBuffers.wrappedBuffer(newBuffer(size)); 

      bb.writeByte(DELETE_RECORD_TX);
      bb.writeInt(-1); // skip ID part
      bb.writeLong(txID);
      bb.writeLong(id);
      bb.writeInt(record != null ? record.getEncodeSize() : 0);
      if (record != null)
      {
         record.encode(bb);
      }
      bb.writeInt(size);

      try
      {
         JournalFile usedFile = appendRecord(bb.toByteBuffer(), false, getTransactionCallback(txID));

         JournalTransaction tx = getTransactionInfo(txID);

         tx.addNegative(usedFile, id);
      }
      finally
      {
         try
         {
            rwlock.readLock().unlock();
         }
         catch (Exception ignored)
         {
            // This could happen if the thread was interrupted
         }
      }
   }

   public void appendDeleteRecordTransactional(final long txID, final long id) throws Exception
   {
      if (state != STATE_LOADED)
      {
         throw new IllegalStateException("Journal must be loaded first");
      }

      int size = SIZE_DELETE_RECORD_TX;

      ChannelBuffer bb = ChannelBuffers.wrappedBuffer(newBuffer(size)); 

      bb.writeByte(DELETE_RECORD_TX);
      bb.writeInt(-1); // skip ID part
      bb.writeLong(txID);
      bb.writeLong(id);
      bb.writeInt(0);
      bb.writeInt(size);

      try
      {
         JournalFile usedFile = appendRecord(bb.toByteBuffer(), false, getTransactionCallback(txID));

         JournalTransaction tx = getTransactionInfo(txID);

         tx.addNegative(usedFile, id);
      }
      finally
      {
         try
         {
            rwlock.readLock().unlock();
         }
         catch (Exception ignored)
         {
            // This could happen if the thread was interrupted
         }
      }
   }

   /** 
    * 
    * <p>If the system crashed after a prepare was called, it should store information that is required to bring the transaction 
    *     back to a state it could be committed. </p>
    * 
    * <p> transactionData allows you to store any other supporting user-data related to the transaction</p>
    * 
    * <p> This method also uses the same logic applied on {@link JournalImpl#appendCommitRecord(long)}
    * 
    * @param txID
    * @param transactionData - extra user data for the prepare
    * @throws Exception
    */
   public void appendPrepareRecord(final long txID, final EncodingSupport transactionData) throws Exception
   {
      if (state != STATE_LOADED)
      {
         throw new IllegalStateException("Journal must be loaded first");
      }

      JournalTransaction tx = getTransactionInfo(txID);

      ByteBuffer bb = writeTransaction(PREPARE_RECORD, txID, tx, transactionData);

      TransactionCallback callback = getTransactionCallback(txID);

      try
      {
         JournalFile usedFile = appendRecord(bb, syncTransactional, callback);

         tx.prepare(usedFile);
      }
      finally
      {
         try
         {
            rwlock.readLock().unlock();
         }
         catch (Exception ignored)
         {
            // This could happen if the thread was interrupted
         }
      }

      // We should wait this outside of the lock, to increase throughput
      if (callback != null)
      {
         callback.waitCompletion();
      }
   }

   /**
    * <p>A transaction record (Commit or Prepare), will hold the number of elements the transaction has on each file.</p>
    * <p>For example, a transaction was spread along 3 journal files with 10 records on each file. 
    *    (What could happen if there are too many records, or if an user event delayed records to come in time to a single file).</p>
    * <p>The element-summary will then have</p>
    * <p>FileID1, 10</p>
    * <p>FileID2, 10</p>
    * <p>FileID3, 10</p>
    * 
    * <br>
    * <p> During the load, the transaction needs to have 30 records spread across the files as originally written.</p>
    * <p> If for any reason there are missing records, that means the transaction was not completed and we should ignore the whole transaction </p>
    * <p> We can't just use a global counter as reclaiming could delete files after the transaction was successfully committed. 
    *     That also means not having a whole file on journal-reload doesn't mean we have to invalidate the transaction </p>
    *
    * @see JournalImpl#writeTransaction(byte, long, org.jboss.messaging.core.journal.impl.JournalImpl.JournalTransaction, EncodingSupport)
    */
   public void appendCommitRecord(final long txID) throws Exception
   {
      if (state != STATE_LOADED)
      {
         throw new IllegalStateException("Journal must be loaded first");
      }

      JournalTransaction tx = transactionInfos.remove(txID);

      if (tx == null)
      {
         throw new IllegalStateException("Cannot find tx with id " + txID);
      }

      ByteBuffer bb = writeTransaction(COMMIT_RECORD, txID, tx, null);

      TransactionCallback callback = getTransactionCallback(txID);

      try
      {
         JournalFile usedFile = appendRecord(bb, syncTransactional, callback);

         transactionCallbacks.remove(txID);

         tx.commit(usedFile);
      }
      finally
      {
         try
         {
            rwlock.readLock().unlock();
         }
         catch (Exception ignored)
         {
            // This could happen if the thread was interrupted
         }
      }

      // We should wait this outside of the lock, to increase throuput
      if (callback != null)
      {
         callback.waitCompletion();
      }

   }

   public void appendRollbackRecord(final long txID) throws Exception
   {
      if (state != STATE_LOADED)
      {
         throw new IllegalStateException("Journal must be loaded first");
      }

      JournalTransaction tx = transactionInfos.remove(txID);

      if (tx == null)
      {
         throw new IllegalStateException("Cannot find tx with id " + txID);
      }

      int size = SIZE_ROLLBACK_RECORD;

      ByteBuffer bb = newBuffer(size);

      bb.put(ROLLBACK_RECORD);
      bb.putInt(-1); // skip ID part
      bb.putLong(txID);
      bb.putInt(size);

      TransactionCallback callback = getTransactionCallback(txID);

      try
      {
         JournalFile usedFile = appendRecord(bb, syncTransactional, callback);

         transactionCallbacks.remove(txID);

         tx.rollback(usedFile);
      }
      finally
      {
         try
         {
            rwlock.readLock().unlock();
         }
         catch (Exception ignored)
         {
            // This could happen if the thread was interrupted
         }
      }

      // We should wait this outside of the lock, to increase throuput
      if (callback != null)
      {
         callback.waitCompletion();
      }

   }

   /**
    * @see JournalImpl#load(LoadManager)
    */
   public synchronized long load(final List<RecordInfo> committedRecords,
                                 final List<PreparedTransactionInfo> preparedTransactions) throws Exception
   {
      final Set<Long> recordsToDelete = new HashSet<Long>();
      final List<RecordInfo> records = new ArrayList<RecordInfo>();

      long maxID = load(new LoadManager()
      {
         public void addPreparedTransaction(PreparedTransactionInfo preparedTransaction)
         {
            preparedTransactions.add(preparedTransaction);
         }

         public void addRecord(RecordInfo info)
         {
            records.add(info);
         }

         public void updateRecord(RecordInfo info)
         {
            records.add(info);
         }

         public void deleteRecord(long id)
         {
            recordsToDelete.add(id);
         }
      });

      for (RecordInfo record : records)
      {
         if (!recordsToDelete.contains(record.id))
         {
            committedRecords.add(record);
         }
      }

      return maxID;
   }

   /** 
    * <p>Load data accordingly to the record layouts</p>
    * 
    * <p>Basic record layout:</p>
    * <table border=1>
    *   <tr><td><b>Field Name</b></td><td><b>Size</b></td></tr>
    *   <tr><td>RecordType</td><td>Byte (1)</td></tr>
    *   <tr><td>FileID</td><td>Integer (4 bytes)</td></tr>
    *   <tr><td>TransactionID <i>(if record is transactional)</i></td><td>Long (8 bytes)</td></tr>
    *   <tr><td>RecordID</td><td>Long (8 bytes)</td></tr>
    *   <tr><td>BodySize(Add, update and delete)</td><td>Integer (4 bytes)</td></tr>
    *   <tr><td>UserDefinedRecordType (If add/update only)</td><td>Byte (1)</td</tr>
    *   <tr><td>RecordBody</td><td>Byte Array (size=BodySize)</td></tr>
    *   <tr><td>Check Size</td><td>Integer (4 bytes)</td></tr>
    * </table>
    * 
    * <p> The check-size is used to validate if the record is valid and complete </p>
    * 
    * <p>Commit/Prepare record layout:</p>
    * <table border=1>
    *   <tr><td><b>Field Name</b></td><td><b>Size</b></td></tr>
    *   <tr><td>RecordType</td><td>Byte (1)</td></tr>
    *   <tr><td>FileID</td><td>Integer (4 bytes)</td></tr>
    *   <tr><td>TransactionID <i>(if record is transactional)</i></td><td>Long (8 bytes)</td></tr>
    *   <tr><td>ExtraDataLength (Prepares only)</td><td>Integer (4 bytes)</td></tr>
    *   <tr><td>Number Of Files (N)</td><td>Integer (4 bytes)</td></tr>
    *   <tr><td>ExtraDataBytes</td><td>Bytes (sized by ExtraDataLength)</td></tr>
    *   <tr><td>* FileID(n)</td><td>Integer (4 bytes)</td></tr>
    *   <tr><td>* NumberOfElements(n)</td><td>Integer (4 bytes)</td></tr>
    *   <tr><td>CheckSize</td><td>Integer (4 bytes)</td</tr>
    * </table>
    * 
    * <p> * FileID and NumberOfElements are the transaction summary, and they will be repeated (N)umberOfFiles times </p> 
    * 
    * */
   public synchronized long load(final LoadManager loadManager) throws Exception
   {
      if (state != STATE_STARTED)
      {
         throw new IllegalStateException("Journal must be in started state");
      }

      // Disabling life cycle control on buffers, as we are reading the buffer 
      buffersControl.disable();


      Map<Long, TransactionHolder> transactions = new LinkedHashMap<Long, TransactionHolder>();

      List<JournalFile> orderedFiles = orderFiles();
   
      int lastDataPos = SIZE_HEADER;

      long maxID = -1;

      for (JournalFile file : orderedFiles)
      {
         ByteBuffer wholeFileBuffer = fileFactory.newBuffer(fileSize);

         file.getFile().open(1);

         int bytesRead = file.getFile().read(wholeFileBuffer);

         if (bytesRead != fileSize)
         {
            // FIXME - We should extract everything we can from this file
            // and then we shouldn't ever reuse this file on reclaiming (instead
            // reclaim on different size files would aways throw the file away)
            // rather than throw ISE!
            // We don't want to leave the user with an unusable system
            throw new IllegalStateException("File is wrong size " + bytesRead +
                                            " expected " +
                                            fileSize +
                                            " : " +
                                            file.getFile().getFileName());
         }

         
         wholeFileBuffer.position(0);
         
         // First long is the ordering timestamp, we just jump its position
         wholeFileBuffer.position(SIZE_HEADER);

         boolean hasData = false;

         while (wholeFileBuffer.hasRemaining())
         {
            final int pos = wholeFileBuffer.position();

            byte recordType = wholeFileBuffer.get();

            if (recordType < ADD_RECORD || recordType > ROLLBACK_RECORD)
            {
               // I - We scan for any valid record on the file. If a hole
               // happened on the middle of the file we keep looking until all
               // the possibilities are gone
               continue;
            }

            if (wholeFileBuffer.position() + SIZE_INT > fileSize)
            {
               // II - Ignore this record, lets keep looking
               continue;
            }

            // III - Every record has the file-id.
            // This is what supports us from not re-filling the whole file
            int readFileId = wholeFileBuffer.getInt();

            // IV - This record is from a previous file-usage. The file was
            // reused and we need to ignore this record
            if (readFileId != file.getOrderingID())
            {
               // If a file has damaged records, we make it a dataFile, and the
               // next reclaiming will fix it
               hasData = true;

               wholeFileBuffer.position(pos + 1);

               continue;
            }

            long transactionID = 0;

            if (isTransaction(recordType))
            {
               if (wholeFileBuffer.position() + SIZE_LONG > fileSize)
               {
                  continue;
               }

               transactionID = wholeFileBuffer.getLong();
            }

            long recordID = 0;

            if (!isCompleteTransaction(recordType))
            {
               if (wholeFileBuffer.position() + SIZE_LONG > fileSize)
               {
                  continue;
               }

               recordID = wholeFileBuffer.getLong();

               maxID = Math.max(maxID, recordID);
            }

            // We use the size of the record to validate the health of the
            // record.
            // (V) We verify the size of the record

            // The variable record portion used on Updates and Appends
            int variableSize = 0;

            // Used to hold extra data on transaction prepares
            int preparedTransactionExtraDataSize = 0;

            byte userRecordType = 0;

            byte record[] = null;

            if (isContainsBody(recordType))
            {
               if (wholeFileBuffer.position() + SIZE_INT > fileSize)
               {
                  continue;
               }

               variableSize = wholeFileBuffer.getInt();

               if (wholeFileBuffer.position() + variableSize > fileSize)
               {
                  log.warn("Record at position " + pos +
                           " file:" +
                           file.getFile().getFileName() +
                           " is corrupted and it is being ignored");
                  continue;
               }

               if (recordType != DELETE_RECORD_TX)
               {
                  userRecordType = wholeFileBuffer.get();
               }

               record = new byte[variableSize];

               wholeFileBuffer.get(record);
            }

            if (recordType == PREPARE_RECORD || recordType == COMMIT_RECORD)
            {
               if (recordType == PREPARE_RECORD)
               {
                  // Add the variable size required for preparedTransactions
                  preparedTransactionExtraDataSize = wholeFileBuffer.getInt();
               }
               // Both commit and record contain the recordSummary, and this is
               // used to calculate the record-size on both record-types
               variableSize += wholeFileBuffer.getInt() * SIZE_INT * 2;
            }

            int recordSize = getRecordSize(recordType);

            // VI - this is completing V, We will validate the size at the end
            // of the record,
            // But we avoid buffer overflows by damaged data
            if (pos + recordSize + variableSize + preparedTransactionExtraDataSize > fileSize)
            {
               // Avoid a buffer overflow caused by damaged data... continue
               // scanning for more records...
               log.warn("Record at position " + pos +
                        " file:" +
                        file.getFile().getFileName() +
                        " is corrupted and it is being ignored");
               // If a file has damaged records, we make it a dataFile, and the
               // next reclaiming will fix it
               hasData = true;

               continue;
            }

            int oldPos = wholeFileBuffer.position();

            wholeFileBuffer.position(pos + variableSize + recordSize + preparedTransactionExtraDataSize - SIZE_INT);

            int checkSize = wholeFileBuffer.getInt();

            // VII - The checkSize at the end has to match with the size
            // informed at the beggining.
            // This is like testing a hash for the record. (We could replace the
            // checkSize by some sort of calculated hash)
            if (checkSize != variableSize + recordSize + preparedTransactionExtraDataSize)
            {
               log.warn("Record at position " + pos +
                        " file:" +
                        file.getFile().getFileName() +
                        " is corrupted and it is being ignored");

               // If a file has damaged records, we make it a dataFile, and the
               // next reclaiming will fix it
               hasData = true;

               wholeFileBuffer.position(pos + SIZE_BYTE);

               continue;
            }

            wholeFileBuffer.position(oldPos);

            // At this point everything is checked. So we relax and just load
            // the data now.

            switch (recordType)
            {
               case ADD_RECORD:
               {
                  loadManager.addRecord(new RecordInfo(recordID, userRecordType, record, false));

                  posFilesMap.put(recordID, new PosFiles(file));

                  hasData = true;

                  break;
               }
               case UPDATE_RECORD:
               {
                  loadManager.updateRecord(new RecordInfo(recordID, userRecordType, record, true));

                  hasData = true;

                  PosFiles posFiles = posFilesMap.get(recordID);

                  if (posFiles != null)
                  {
                     // It's legal for this to be null. The file(s) with the may
                     // have been deleted
                     // just leaving some updates in this file

                     posFiles.addUpdateFile(file);
                  }

                  break;
               }
               case DELETE_RECORD:
               {
                  loadManager.deleteRecord(recordID);

                  hasData = true;

                  PosFiles posFiles = posFilesMap.remove(recordID);

                  if (posFiles != null)
                  {
                     posFiles.addDelete(file);
                  }

                  break;
               }
               case ADD_RECORD_TX:
               case UPDATE_RECORD_TX:
               {
                  TransactionHolder tx = transactions.get(transactionID);

                  if (tx == null)
                  {
                     tx = new TransactionHolder(transactionID);

                     transactions.put(transactionID, tx);
                  }

                  tx.recordInfos.add(new RecordInfo(recordID, userRecordType, record, recordType == UPDATE_RECORD_TX));

                  JournalTransaction tnp = transactionInfos.get(transactionID);

                  if (tnp == null)
                  {
                     tnp = new JournalTransaction();

                     transactionInfos.put(transactionID, tnp);
                  }

                  tnp.addPositive(file, recordID);

                  hasData = true;

                  break;
               }
               case DELETE_RECORD_TX:
               {
                  TransactionHolder tx = transactions.get(transactionID);

                  if (tx == null)
                  {
                     tx = new TransactionHolder(transactionID);

                     transactions.put(transactionID, tx);
                  }

                  tx.recordsToDelete.add(new RecordInfo(recordID, (byte)0, record, true));

                  JournalTransaction tnp = transactionInfos.get(transactionID);

                  if (tnp == null)
                  {
                     tnp = new JournalTransaction();

                     transactionInfos.put(transactionID, tnp);
                  }

                  tnp.addNegative(file, recordID);

                  hasData = true;

                  break;
               }
               case PREPARE_RECORD:
               {
                  TransactionHolder tx = transactions.get(transactionID);

                  if (tx == null)
                  {
                     // The user could choose to prepare empty transactions
                     tx = new TransactionHolder(transactionID);

                     transactions.put(transactionID, tx);
                  }

                  byte extraData[] = new byte[preparedTransactionExtraDataSize];

                  wholeFileBuffer.get(extraData);

                  // Pair <FileID, NumberOfElements>
                  Pair<Integer, Integer>[] recordedSummary = readTransactionalElementsSummary(variableSize, wholeFileBuffer);

                  tx.prepared = true;

                  tx.extraData = extraData;

                  JournalTransaction journalTransaction = transactionInfos.get(transactionID);

                  if (journalTransaction == null)
                  {
                     journalTransaction = new JournalTransaction();

                     transactionInfos.put(transactionID, journalTransaction);
                  }

                  boolean healthy = checkTransactionHealth(journalTransaction, orderedFiles, recordedSummary);

                  if (healthy)
                  {
                     journalTransaction.prepare(file);
                  }
                  else
                  {
                     log.warn("Prepared transaction " + transactionID + " wasn't considered completed, it will be ignored");
                     tx.invalid = true;
                  }

                  hasData = true;

                  break;
               }
               case COMMIT_RECORD:
               {
                  TransactionHolder tx = transactions.remove(transactionID);

                  // We need to read it even if transaction was not found, or
                  // the reading checks would fail
                  // Pair <OrderId, NumberOfElements>
                  Pair<Integer, Integer>[] recordedSummary = readTransactionalElementsSummary(variableSize, wholeFileBuffer);

                  // The commit could be alone on its own journal-file and the
                  // whole transaction body was reclaimed but not the
                  // commit-record
                  // So it is completely legal to not find a transaction at this
                  // point
                  // If we can't find it, we assume the TX was reclaimed and we
                  // ignore this
                  if (tx != null)
                  {
                     JournalTransaction journalTransaction = transactionInfos.remove(transactionID);

                     if (journalTransaction == null)
                     {
                        throw new IllegalStateException("Cannot find tx " + transactionID);
                     }

                     boolean healthy = checkTransactionHealth(journalTransaction, orderedFiles, recordedSummary);

                     if (healthy)
                     {
                        for (RecordInfo txRecord : tx.recordInfos)
                        {
                           if (txRecord.isUpdate)
                           {
                              loadManager.updateRecord(txRecord);
                           }
                           else
                           {
                              loadManager.addRecord(txRecord);
                           }
                        }

                        for (RecordInfo deleteValue : tx.recordsToDelete)
                        {
                           loadManager.deleteRecord(deleteValue.id);
                        }

                        journalTransaction.commit(file);
                     }
                     else
                     {
                        log.warn("Transaction " + transactionID +
                                 " is missing elements so the transaction is being ignored");

                        journalTransaction.forget();
                     }

                     hasData = true;
                  }

                  break;
               }
               case ROLLBACK_RECORD:
               {
                  TransactionHolder tx = transactions.remove(transactionID);

                  // The rollback could be alone on its own journal-file and the
                  // whole transaction body was reclaimed but the commit-record
                  // So it is completely legal to not find a transaction at this
                  // point
                  if (tx != null)
                  {
                     JournalTransaction tnp = transactionInfos.remove(transactionID);

                     if (tnp == null)
                     {
                        throw new IllegalStateException("Cannot find tx " + transactionID);
                     }

                     // There is no need to validate summaries/holes on
                     // Rollbacks.. We will ignore the data anyway.
                     tnp.rollback(file);

                     hasData = true;
                  }

                  break;
               }
               default:
               {
                  throw new IllegalStateException("Journal " + file.getFile().getFileName() +
                                                  " is corrupt, invalid record type " +
                                                  recordType);
               }
            }

            checkSize = wholeFileBuffer.getInt();

            // This is a sanity check about the loading code itself.
            // If this checkSize doesn't match, it means the reading method is
            // not doing what it was supposed to do
            if (checkSize != variableSize + recordSize + preparedTransactionExtraDataSize)
            {
               throw new IllegalStateException("Internal error on loading file. Position doesn't match with checkSize, file = " + file.getFile() + ", pos = " + pos);
            }

            lastDataPos = wholeFileBuffer.position();
         }
         
         fileFactory.releaseBuffer(wholeFileBuffer);

         file.getFile().close();

         if (hasData)
         {
            dataFiles.add(file);
         }
         else
         {
            // Empty dataFiles with no data
            freeFiles.add(file);
         }
      }

      buffersControl.enable();
      
      // Create any more files we need

      // FIXME - size() involves a scan
      int filesToCreate = minFiles - (dataFiles.size() + freeFiles.size());

      if (filesToCreate > 0)
      {
         for (int i = 0; i < filesToCreate; i++)
         {
            // Keeping all files opened can be very costly (mainly on AIO)
            freeFiles.add(createFile(false));
         }
      }

      // The current file is the last one

      Iterator<JournalFile> iter = dataFiles.iterator();

      while (iter.hasNext())
      {
         currentFile = iter.next();
         
         if (!iter.hasNext())
         {
            iter.remove();
         }
      }

      if (currentFile != null)
      {
         currentFile.getFile().open();

         currentFile.getFile().position(currentFile.getFile().calculateBlockStart(lastDataPos));

         currentFile.setOffset(currentFile.getFile().position());
      }
      else
      {
         currentFile = freeFiles.remove();

         openFile(currentFile);
      }

      pushOpenedFile();

      for (TransactionHolder transaction : transactions.values())
      {
         if (!transaction.prepared || transaction.invalid)
         {
            log.warn("Uncommitted transaction with id " + transaction.transactionID + " found and discarded");

            JournalTransaction transactionInfo = transactionInfos.get(transaction.transactionID);

            if (transactionInfo == null)
            {
               throw new IllegalStateException("Cannot find tx " + transaction.transactionID);
            }

            // Reverse the refs
            transactionInfo.forget();

            // Remove the transactionInfo
            transactionInfos.remove(transaction.transactionID);
         }
         else
         {
            PreparedTransactionInfo info = new PreparedTransactionInfo(transaction.transactionID, transaction.extraData);

            info.records.addAll(transaction.recordInfos);

            info.recordsToDelete.addAll(transaction.recordsToDelete);

            loadManager.addPreparedTransaction(info);
         }
      }

      state = STATE_LOADED;

      checkAndReclaimFiles();

      return maxID;
   }

   public int getAlignment() throws Exception
   {
      return fileFactory.getAlignment();
   }

   // TestableJournal implementation
   // --------------------------------------------------------------

   public void setAutoReclaim(final boolean autoReclaim)
   {
      this.autoReclaim = autoReclaim;
   }

   public boolean isAutoReclaim()
   {
      return autoReclaim;
   }

   public String debug() throws Exception
   {
      checkReclaimStatus();

      StringBuilder builder = new StringBuilder();

      for (JournalFile file : dataFiles)
      {
         builder.append("DataFile:" + file +
                        " posCounter = " +
                        file.getPosCount() +
                        " reclaimStatus = " +
                        file.isCanReclaim() +
                        "\n");
         if (file instanceof JournalFileImpl)
         {
            builder.append(((JournalFileImpl)file).debug());

         }
      }

      for (JournalFile file : freeFiles)
      {
         builder.append("FreeFile:" + file + "\n");
      }

      if (currentFile != null)
      {
         builder.append("CurrentFile:" + currentFile + " posCounter = " + currentFile.getPosCount() + "\n");

         if (currentFile instanceof JournalFileImpl)
         {
            builder.append(((JournalFileImpl)currentFile).debug());
         }
      }
      else
      {
         builder.append("CurrentFile: No current file at this point!");
      }

      builder.append("#Opened Files:" + openedFiles.size());

      return builder.toString();
   }

   /** Method for use on testcases.
    *  It will call waitComplete on every transaction, so any assertions on the file system will be correct after this */
   public void debugWait() throws Exception
   {
      for (TransactionCallback callback : transactionCallbacks.values())
      {
         callback.waitCompletion();
      }

      if (filesExecutor != null && !filesExecutor.isShutdown())
      {
         // Send something to the closingExecutor, just to make sure we went
         // until its end
         final CountDownLatch latch = new CountDownLatch(1);

         filesExecutor.execute(new Runnable()
         {
            public void run()
            {
               latch.countDown();
            }
         });

         latch.await();
      }

   }

   public void checkAndReclaimFiles() throws Exception
   {
      checkReclaimStatus();

      for (JournalFile file : dataFiles)
      {
         if (file.isCanReclaim())
         {
            // File can be reclaimed or deleted

            if (trace)
            {
               trace("Reclaiming file " + file);
            }

            dataFiles.remove(file);

            // FIXME - size() involves a scan!!!
            if (freeFiles.size() + dataFiles.size() + 1 + openedFiles.size() < minFiles)
            {
               // Re-initialise it

               JournalFile jf = reinitializeFile(file);

               freeFiles.add(jf);
            }
            else
            {
               file.getFile().open(1);

               file.getFile().delete();
            }
         }
      }
   }

   public int getDataFilesCount()
   {
      return dataFiles.size();
   }

   public int getFreeFilesCount()
   {
      return freeFiles.size();
   }

   public int getOpenedFilesCount()
   {
      return openedFiles.size();
   }

   public int getIDMapSize()
   {
      return posFilesMap.size();
   }

   public int getFileSize()
   {
      return fileSize;
   }

   public int getMinFiles()
   {
      return minFiles;
   }

   public boolean isSyncTransactional()
   {
      return syncTransactional;
   }

   public boolean isSyncNonTransactional()
   {
      return syncNonTransactional;
   }

   public String getFilePrefix()
   {
      return filePrefix;
   }

   public String getFileExtension()
   {
      return fileExtension;
   }

   public int getMaxAIO()
   {
      return maxAIO;
   }

   // In some tests we need to force the journal to move to a next file
   public void forceMoveNextFile() throws Exception
   {
      moveNextFile();
      debugWait();
   }

   // MessagingComponent implementation
   // ---------------------------------------------------

   public synchronized boolean isStarted()
   {
      return state != STATE_STOPPED;
   }

   public synchronized void start()
   {
      if (state != STATE_STOPPED)
      {
         throw new IllegalStateException("Journal is not stopped");
      }

      filesExecutor = Executors.newSingleThreadExecutor();

      state = STATE_STARTED;
   }

   public synchronized void stop() throws Exception
   {
      trace("Stopping the journal");
      
      if (state == STATE_STOPPED)
      {
         throw new IllegalStateException("Journal is already stopped");
      }

      positionLock.acquire();
      rwlock.writeLock().lock();

      try
      {
         filesExecutor.shutdown();

         if (!filesExecutor.awaitTermination(60, TimeUnit.SECONDS))
         {
            log.warn("Couldn't stop journal executor after 60 seconds");
         }

         if (currentFile != null)
         {
            currentFile.getFile().close();
         }

         for (JournalFile file : openedFiles)
         {
            file.getFile().close();
         }

         currentFile = null;

         dataFiles.clear();

         freeFiles.clear();

         openedFiles.clear();
         
         buffersControl.clearPoll();

         state = STATE_STOPPED;
      }
      finally
      {
         positionLock.release();
         rwlock.writeLock().unlock();
      }
   }

   // Public
   // -----------------------------------------------------------------------------

   // Private
   // -----------------------------------------------------------------------------

   private void checkReclaimStatus() throws Exception
   {
      JournalFile[] files = new JournalFile[dataFiles.size()];

      reclaimer.scan(dataFiles.toArray(files));
   }

   // Discard the old JournalFile and set it with a new ID
   private JournalFile reinitializeFile(final JournalFile file) throws Exception
   {
      int newOrderingID = generateOrderingID();

      SequentialFile sf = file.getFile();

      sf.open(1);

      ByteBuffer bb = fileFactory.newBuffer(SIZE_INT);

      bb.putInt(newOrderingID);

      int bytesWritten = sf.write(bb, true);

      JournalFile jf = new JournalFileImpl(sf, newOrderingID);

      sf.position(bytesWritten);

      jf.setOffset(bytesWritten);

      sf.close();

      return jf;
   }

   /** It will read the elements-summary back from the commit/prepare transaction 
    *  Pair<FileID, Counter> */
   @SuppressWarnings("unchecked")
   // See comment on the method body
   private Pair<Integer, Integer>[] readTransactionalElementsSummary(final int variableSize, final ByteBuffer bb)
   {
      int numberOfFiles = variableSize / (SIZE_INT * 2);

      // This line aways show an annoying compilation-warning, the
      // SupressWarning is to avoid a warning about this cast
      Pair<Integer, Integer> values[] = new Pair[numberOfFiles];

      for (int i = 0; i < numberOfFiles; i++)
      {
         values[i] = new Pair<Integer, Integer>(bb.getInt(), bb.getInt());
      }

      return values;
   }

   /**
    * <p> Check for holes on the transaction (a commit written but with an incomplete transaction) </p>
    * <p>This method will validate if the transaction (PREPARE/COMMIT) is complete as stated on the COMMIT-RECORD.</p>
    * <p> We record a summary about the records on the journal file on COMMIT and PREPARE. 
    *     When we load the records we build a new summary and we check the original summary to the current summary.
    *     This method is basically verifying if the entire transaction is being loaded </p> 
    *     
    * <p>Look at the javadoc on {@link JournalImpl#appendCommitRecord(long)} about how the transaction-summary is recorded</p> 
    *     
    * @param journalTransaction
    * @param orderedFiles
    * @param recordedSummary
    * @return
    */
   private boolean checkTransactionHealth(final JournalTransaction journalTransaction,
                                          final List<JournalFile> orderedFiles,
                                          final Pair<Integer, Integer>[] recordedSummary)
   {
      boolean healthy = true;

      // (I) First we get the summary of what we really have on the files now:

      // FileID, NumberOfElements
      Map<Integer, AtomicInteger> currentSummary = journalTransaction.getElementsSummary();

      // (II) We compare the recorded summary on the commit, against to the
      // reality on the files
      for (Pair<Integer, Integer> ref : recordedSummary)
      {
         AtomicInteger counter = currentSummary.get(ref.a);

         if (counter == null)
         {
            for (JournalFile lookupFile : orderedFiles)
            {
               if (lookupFile.getOrderingID() == ref.a)
               {
                  // (III) oops, we were expecting at least one record on this
                  // file.
                  // The file still exists and no records were found.
                  // That means the transaction crashed before complete,
                  // so this transaction is broken and needs to be ignored.
                  // This is probably a hole caused by a crash during commit.
                  healthy = false;
                  break;
               }
            }
         }
         else
         {
            // (IV) Missing a record... Transaction was not completed as stated.
            // we will ignore the whole transaction
            // This is probably a hole caused by a crash during commit/prepare.
            if (counter.get() != ref.b)
            {
               healthy = false;
               break;
            }
         }
      }
      return healthy;
   }

   /**
    * <p>A transaction record (Commit or Prepare), will hold the number of elements the transaction has on each file.</p>
    * <p>For example, a transaction was spread along 3 journal files with 10 records on each file. 
    *    (What could happen if there are too many records, or if an user event delayed records to come in time to a single file).</p>
    * <p>The element-summary will then have</p>
    * <p>FileID1, 10</p>
    * <p>FileID2, 10</p>
    * <p>FileID3, 10</p>
    * 
    * <br>
    * <p> During the load, the transaction needs to have 30 records spread across the files as originally written.</p>
    * <p> If for any reason there are missing records, that means the transaction was not completed and we should ignore the whole transaction </p>
    * <p> We can't just use a global counter as reclaiming could delete files after the transaction was successfully committed. 
    *     That also means not having a whole file on journal-reload doesn't mean we have to invalidate the transaction </p>
    * 
    * @param recordType
    * @param txID
    * @param tx
    * @param transactionData
    * @return
    * @throws Exception
    */
   private ByteBuffer writeTransaction(final byte recordType,
                                       final long txID,
                                       final JournalTransaction tx,
                                       final EncodingSupport transactionData) throws Exception
   {
      int size = SIZE_COMPLETE_TRANSACTION_RECORD + tx.getElementsSummary().size() *
                 SIZE_INT *
                 2 +
                 (transactionData != null ? transactionData.getEncodeSize() + SIZE_INT : 0);

      ChannelBuffer bb = ChannelBuffers.wrappedBuffer(newBuffer(size)); 
      
      bb.writeByte(recordType);
      bb.writeInt(-1); // skip ID part
      bb.writeLong(txID);

      if (transactionData != null)
      {
         bb.writeInt(transactionData.getEncodeSize());
      }

      bb.writeInt(tx.getElementsSummary().size());

      if (transactionData != null)
      {
         transactionData.encode(bb);
      }

      for (Map.Entry<Integer, AtomicInteger> entry : tx.getElementsSummary().entrySet())
      {
         bb.writeInt(entry.getKey());
         bb.writeInt(entry.getValue().get());
      }

      bb.writeInt(size);

      return bb.toByteBuffer();
   }

   private boolean isTransaction(final byte recordType)
   {
      return recordType == ADD_RECORD_TX || recordType == UPDATE_RECORD_TX ||
             recordType == DELETE_RECORD_TX ||
             isCompleteTransaction(recordType);
   }

   private boolean isCompleteTransaction(final byte recordType)
   {
      return recordType == COMMIT_RECORD || recordType == PREPARE_RECORD || recordType == ROLLBACK_RECORD;
   }

   private boolean isContainsBody(final byte recordType)
   {
      return recordType >= ADD_RECORD && recordType <= DELETE_RECORD_TX;
   }

   private int getRecordSize(final byte recordType)
   {
      // The record size (without the variable portion)
      int recordSize = 0;
      switch (recordType)
      {
         case ADD_RECORD:
            recordSize = SIZE_ADD_RECORD;
            break;
         case UPDATE_RECORD:
            recordSize = SIZE_UPDATE_RECORD;
            break;
         case ADD_RECORD_TX:
            recordSize = SIZE_ADD_RECORD_TX;
            break;
         case UPDATE_RECORD_TX:
            recordSize = SIZE_UPDATE_RECORD_TX;
            break;
         case DELETE_RECORD:
            recordSize = SIZE_DELETE_RECORD;
            break;
         case DELETE_RECORD_TX:
            recordSize = SIZE_DELETE_RECORD_TX;
            break;
         case PREPARE_RECORD:
            recordSize = SIZE_PREPARE_RECORD;
            break;
         case COMMIT_RECORD:
            recordSize = SIZE_COMMIT_RECORD;
            break;
         case ROLLBACK_RECORD:
            recordSize = SIZE_ROLLBACK_RECORD;
            break;
         default:
            // Sanity check, this was previously tested, nothing different
            // should be on this switch
            throw new IllegalStateException("Record other than expected");

      }
      return recordSize;
   }

   
   /** 
    * This method requires bufferControl disabled, or the reads are going to be invalid
    * */
   private List<JournalFile> orderFiles() throws Exception
   {
      
      if (buffersControl.enabled)
      {
         // Sanity check, this shouldn't happen unless someone made an invalid change on the code
         throw new IllegalStateException("Buffer life cycle control needs to be disabled at this point!!!");
      }
      
      List<String> fileNames = fileFactory.listFiles(fileExtension);

      List<JournalFile> orderedFiles = new ArrayList<JournalFile>(fileNames.size());
      
      for (String fileName : fileNames)
      {
         SequentialFile file = fileFactory.createSequentialFile(fileName, maxAIO);
 
         file.open(1);

         ByteBuffer bb = fileFactory.newBuffer(SIZE_INT);

         file.read(bb);

         int orderingID = bb.getInt();
         
         fileFactory.releaseBuffer(bb);
         
         bb = null;

         if (nextOrderingId.get() < orderingID)
         {
            nextOrderingId.set(orderingID);
         }

         orderedFiles.add(new JournalFileImpl(file, orderingID));

         file.close();
      }
      
      // Now order them by ordering id - we can't use the file name for ordering
      // since we can re-use dataFiles

      class JournalFileComparator implements Comparator<JournalFile>
      {
         public int compare(final JournalFile f1, final JournalFile f2)
         {
            int id1 = f1.getOrderingID();
            int id2 = f2.getOrderingID();

            return id1 < id2 ? -1 : id1 == id2 ? 0 : 1;
         }
      }

      Collections.sort(orderedFiles, new JournalFileComparator());

      return orderedFiles;
   }

   /** 
    * Note: This method will perform rwlock.readLock.lock(); 
    *       The method caller should aways unlock that readLock
    * */
   private JournalFile appendRecord(final ByteBuffer bb, final boolean sync, final TransactionCallback callback) throws Exception
   {
      positionLock.acquire();

      try
      {
         if (state != STATE_LOADED)
         {
            throw new IllegalStateException("The journal was stopped");
         }

         int size = bb.limit();

         if (size % currentFile.getFile().getAlignment() != 0)
         {
            throw new IllegalStateException("You can't write blocks in a size different than " + currentFile.getFile()
                                                                                                            .getAlignment());
         }

         // We take into account the fileID used on the Header
         if (size > fileSize - currentFile.getFile().calculateBlockStart(SIZE_HEADER))
         {
            throw new IllegalArgumentException("Record is too large to store " + size);
         }

         if (currentFile == null || fileSize - currentFile.getOffset() < size)
         {
            moveNextFile();
         }

         if (currentFile == null)
         {
            throw new IllegalStateException("Current file = null");
         }

         currentFile.extendOffset(size);

         // we must get the readLock before we release positionLock
         // We don't want a race condition where currentFile is changed by
         // another write as soon as we leave this block
         rwlock.readLock().lock();

      }
      finally
      {
         positionLock.release();
      }

      bb.position(SIZE_BYTE);

      bb.putInt(currentFile.getOrderingID());

      bb.rewind();

      if (callback != null)
      {
         // We are 100% sure currentFile won't change, since rwLock.readLock is
         // locked
         currentFile.getFile().write(bb, callback);
         // callback.waitCompletion() should be done on the caller of this
         // method, so we would have better performance
      }
      else
      {
         // We are 100% sure currentFile won't change, since rwLock.readLock is
         // locked
         currentFile.getFile().write(bb, sync);
      }

      return currentFile;
   }

   /**
    * This method will create a new file on the file system, pre-fill it with FILL_CHARACTER
    * @param keepOpened
    * @return
    * @throws Exception
    */
   private JournalFile createFile(final boolean keepOpened) throws Exception
   {
      int orderingID = generateOrderingID();

      String fileName = filePrefix + "-" + orderingID + "." + fileExtension;

      if (trace)
      {
         trace("Creating file " + fileName);
      }

      SequentialFile sequentialFile = fileFactory.createSequentialFile(fileName, maxAIO);

      sequentialFile.open();

      sequentialFile.fill(0, fileSize, FILL_CHARACTER);

      ByteBuffer bb = fileFactory.newBuffer(SIZE_INT);

      bb.putInt(orderingID);

      bb.rewind();

      int bytesWritten = sequentialFile.write(bb, true);

      JournalFile info = new JournalFileImpl(sequentialFile, orderingID);

      info.extendOffset(bytesWritten);

      if (!keepOpened)
      {
         sequentialFile.close();
      }

      return info;
   }

   private void openFile(final JournalFile file) throws Exception
   {
      file.getFile().open();

      file.getFile().position(file.getFile().calculateBlockStart(SIZE_HEADER));

      file.setOffset(file.getFile().calculateBlockStart(SIZE_HEADER));
   }

   private int generateOrderingID()
   {
      return nextOrderingId.incrementAndGet();
   }

   // You need to guarantee lock.acquire() before calling this method
   private void moveNextFile() throws InterruptedException
   {
      rwlock.writeLock().lock();
      try
      {
         closeFile(currentFile);

         currentFile = enqueueOpenFile();
      }
      finally
      {
         rwlock.writeLock().unlock();
      }
   }

   /** 
    * <p>This method will instantly return the opened file, and schedule opening and reclaiming.</p>
    * <p>In case there are no cached opened files, this method will block until the file was opened,
    * what would happen only if the system is under heavy load by another system (like a backup system, or a DB sharing the same box as JBM).</p> 
    * */
   private JournalFile enqueueOpenFile() throws InterruptedException
   {
      if (trace)
      {
         trace("enqueueOpenFile with openedFiles.size=" + openedFiles.size());
      }

      filesExecutor.execute(new Runnable()
      {
         public void run()
         {
            try
            {
               pushOpenedFile();
            }
            catch (Exception e)
            {
               log.error(e.getMessage(), e);
            }
         }
      });

      if (autoReclaim)
      {
         filesExecutor.execute(new Runnable()
         {
            public void run()
            {
               try
               {
                  checkAndReclaimFiles();
               }
               catch (Exception e)
               {
                  log.error(e.getMessage(), e);
               }
            }
         });
      }

      JournalFile nextFile = null;

      while (nextFile == null)
      {
         nextFile = openedFiles.poll(60, TimeUnit.SECONDS);
         if (nextFile == null)
         {
            log.warn("Couldn't open a file in 60 Seconds", new Exception("Warning: Couldn't open a file in 60 Seconds"));
         }
      }

      return nextFile;
   }

   /** 
    * 
    * Open a file and place it into the openedFiles queue
    * */
   private void pushOpenedFile() throws Exception
   {
      JournalFile nextOpenedFile = null;
      try
      {
         nextOpenedFile = freeFiles.remove();
      }
      catch (NoSuchElementException ignored)
      {
      }

      if (nextOpenedFile == null)
      {
         nextOpenedFile = createFile(true);
      }
      else
      {
         openFile(nextOpenedFile);
      }

      openedFiles.offer(nextOpenedFile);
   }

   private void closeFile(final JournalFile file)
   {
      filesExecutor.execute(new Runnable()
      {
         public void run()
         {
            try
            {
               file.getFile().close();
            }
            catch (Exception e)
            {
               log.warn(e.getMessage(), e);
            }
            dataFiles.add(file);
         }
      });
   }

   private JournalTransaction getTransactionInfo(final long txID)
   {
      JournalTransaction tx = transactionInfos.get(txID);

      if (tx == null)
      {
         tx = new JournalTransaction();

         JournalTransaction trans = transactionInfos.putIfAbsent(txID, tx);

         if (trans != null)
         {
            tx = trans;
         }
      }

      return tx;
   }

   private TransactionCallback getTransactionCallback(final long transactionId) throws MessagingException
   {
      if (fileFactory.isSupportsCallbacks() && syncTransactional)
      {
         TransactionCallback callback = transactionCallbacks.get(transactionId);

         if (callback == null)
         {
            callback = new TransactionCallback();

            TransactionCallback callbackCheck = transactionCallbacks.putIfAbsent(transactionId, callback);

            if (callbackCheck != null)
            {
               callback = callbackCheck;
            }
         }

         if (callback.errorMessage != null)
         {
            throw new MessagingException(callback.errorCode, callback.errorMessage);
         }

         callback.countUp();

         return callback;
      }
      else
      {
         return null;
      }
   }

   public ByteBuffer newBuffer(final int size)
   {
      return buffersControl.newBuffer(size);
   }

   // Inner classes
   // ---------------------------------------------------------------------------

   // Just encapsulates the VariableLatch waiting for transaction completions
   // Used if the SequentialFile supports Callbacks
   private static class TransactionCallback implements IOCallback
   {
      private final VariableLatch countLatch = new VariableLatch();

      private volatile String errorMessage = null;

      private volatile int errorCode = 0;

      public void countUp()
      {
         countLatch.up();
      }

      public void done()
      {
         countLatch.down();
      }

      public void waitCompletion() throws InterruptedException
      {
         countLatch.waitCompletion();

         if (errorMessage != null)
         {
            throw new IllegalStateException("Error on Transaction: " + errorCode + " - " + errorMessage);
         }
      }

      public void onError(final int errorCode, final String errorMessage)
      {
         this.errorMessage = errorMessage;

         this.errorCode = errorCode;

         countLatch.down();
      }

   }

   /** Used on the ref-count for reclaiming */
   private static class PosFiles
   {
      private final JournalFile addFile;

      private List<JournalFile> updateFiles;

      PosFiles(final JournalFile addFile)
      {
         this.addFile = addFile;

         addFile.incPosCount();
      }

      void addUpdateFile(final JournalFile updateFile)
      {
         if (updateFiles == null)
         {
            updateFiles = new ArrayList<JournalFile>();
         }

         updateFiles.add(updateFile);

         updateFile.incPosCount();
      }

      void addDelete(final JournalFile file)
      {
         file.incNegCount(addFile);

         if (updateFiles != null)
         {
            for (JournalFile jf : updateFiles)
            {
               file.incNegCount(jf);
            }
         }
      }
   }

   /** Class that will control buffer-reuse */
   private class ReuseBuffersController
   {
      private volatile long bufferReuseLastTime = System.currentTimeMillis();

      /** This queue is fed by {@link JournalImpl.ReuseBuffersController.LocalBufferCallback}} which is called directly by NIO or NIO.
       * On the case of the AIO this is almost called by the native layer as soon as the buffer is not being used any more
       * and ready to be reused or GCed */
      private final ConcurrentLinkedQueue<ByteBuffer> reuseBuffersQueue = new ConcurrentLinkedQueue<ByteBuffer>();
      
      /** During reload we may disable/enable buffer reuse */
      private boolean enabled = true;

      final BufferCallback callback = new LocalBufferCallback();
      
      public void enable()
      {
         this.enabled = true;
      }
      
      public void disable()
      {
         this.enabled = false;
      }

      public ByteBuffer newBuffer(final int size)
      {
         // if a new buffer wasn't requested in 10 seconds, we clear the queue
         // This is being done this way as we don't need another Timeout Thread
         // just to cleanup this
         if (reuseBufferSize > 0 && System.currentTimeMillis() - bufferReuseLastTime > 10000)
         {
            trace("Clearing reuse buffers queue with " + reuseBuffersQueue.size() + " elements");

            bufferReuseLastTime = System.currentTimeMillis();

            clearPoll();
         }

         // if a buffer is bigger than the configured-size, we just create a new
         // buffer.
         if (size > reuseBufferSize)
         {
            return fileFactory.newBuffer(size);
         }
         else
         {
            // We need to allocate buffers following the rules of the storage
            // being used (AIO/NIO)
            int alignedSize = fileFactory.calculateBlockSize(size);

            // Try getting a buffer from the queue...
            ByteBuffer buffer = reuseBuffersQueue.poll();

            if (buffer == null)
            {
               // if empty create a new one.
               buffer = fileFactory.newBuffer(reuseBufferSize);

               buffer.limit(alignedSize);
            }
            else
            {
               // set the limit of the buffer to the size being required
               buffer.limit(alignedSize);

               fileFactory.clearBuffer(buffer);
            }
            
            buffer.rewind();

            return buffer;
         }
      }

      public void clearPoll()
      {
         ByteBuffer reusedBuffer;
         
         while ((reusedBuffer = reuseBuffersQueue.poll()) != null)
         {
            fileFactory.releaseBuffer(reusedBuffer);
         }
      }

      private class LocalBufferCallback implements BufferCallback
      {
         public void bufferDone(final ByteBuffer buffer)
         {
            if (enabled)
            {
               bufferReuseLastTime = System.currentTimeMillis();
   
               // If a buffer has any other than the configured size, the buffer
               // will be just sent to GC
               if (buffer.capacity() == reuseBufferSize)
               {
                  reuseBuffersQueue.offer(buffer);
               }
               else
               {
                  fileFactory.releaseBuffer(buffer);
               }
            }
         }
      }
   }

   private class JournalTransaction
   {
      private List<Pair<JournalFile, Long>> pos;

      private List<Pair<JournalFile, Long>> neg;

      private Set<JournalFile> transactionPos;

      // Map of file id to number of elements participating on the transaction
      // in that file
      // Used to verify completion on reload
      private final Map<Integer, AtomicInteger> numberOfElementsPerFile = new HashMap<Integer, AtomicInteger>();

      public Map<Integer, AtomicInteger> getElementsSummary()
      {
         return numberOfElementsPerFile;
      }

      public void addPositive(final JournalFile file, final long id)
      {
         getCounter(file).incrementAndGet();

         addTXPosCount(file);

         if (pos == null)
         {
            pos = new ArrayList<Pair<JournalFile, Long>>();
         }

         pos.add(new Pair<JournalFile, Long>(file, id));
      }

      public void addNegative(final JournalFile file, final long id)
      {
         getCounter(file).incrementAndGet();

         addTXPosCount(file);

         if (neg == null)
         {
            neg = new ArrayList<Pair<JournalFile, Long>>();
         }

         neg.add(new Pair<JournalFile, Long>(file, id));
      }

      public void commit(final JournalFile file)
      {
         if (pos != null)
         {
            for (Pair<JournalFile, Long> p : pos)
            {
               PosFiles posFiles = posFilesMap.get(p.b);

               if (posFiles == null)
               {
                  posFiles = new PosFiles(p.a);

                  posFilesMap.put(p.b, posFiles);
               }
               else
               {
                  posFiles.addUpdateFile(p.a);
               }
            }
         }

         if (neg != null)
         {
            for (Pair<JournalFile, Long> n : neg)
            {
               PosFiles posFiles = posFilesMap.remove(n.b);

               if (posFiles != null)
               {
                  posFiles.addDelete(n.a);
               }
            }
         }

         // Now add negs for the pos we added in each file in which there were
         // transactional operations

         for (JournalFile jf : transactionPos)
         {
            file.incNegCount(jf);
         }
      }

      public void rollback(final JournalFile file)
      {
         // Now add negs for the pos we added in each file in which there were
         // transactional operations
         // Note that we do this on rollback as we do on commit, since we need
         // to ensure the file containing
         // the rollback record doesn't get deleted before the files with the
         // transactional operations are deleted
         // Otherwise we may run into problems especially with XA where we are
         // just left with a prepare when the tx
         // has actually been rolled back

         for (JournalFile jf : transactionPos)
         {
            file.incNegCount(jf);
         }
      }

      public void prepare(final JournalFile file)
      {
         // We don't want the prepare record getting deleted before time

         addTXPosCount(file);
      }

      public void forget()
      {
         // The transaction was not committed or rolled back in the file, so we
         // reverse any pos counts we added

         for (JournalFile jf : transactionPos)
         {
            jf.decPosCount();
         }
      }

      private void addTXPosCount(final JournalFile file)
      {
         if (transactionPos == null)
         {
            transactionPos = new HashSet<JournalFile>();
         }

         if (!transactionPos.contains(file))
         {
            transactionPos.add(file);

            // We add a pos for the transaction itself in the file - this
            // prevents any transactional operations
            // being deleted before a commit or rollback is written
            file.incPosCount();
         }
      }

      private AtomicInteger getCounter(final JournalFile file)
      {
         AtomicInteger value = numberOfElementsPerFile.get(file.getOrderingID());

         if (value == null)
         {
            value = new AtomicInteger();
            numberOfElementsPerFile.put(file.getOrderingID(), value);
         }

         return value;
      }

   }
   
   
   private class ByteArrayEncoding implements EncodingSupport
   {

      final byte[] data;

      public ByteArrayEncoding(final byte[] data)
      {
         this.data = data;
      }

      // Public --------------------------------------------------------

      public void decode(final MessagingBuffer buffer)
      {
         throw new IllegalStateException("operation not supported");
      }

      public void encode(final MessagingBuffer buffer)
      {
         buffer.writeBytes(data);
      }

      public int getEncodeSize()
      {
         return data.length;
      }
   }
   
   
   // Used on Load
   private static class TransactionHolder
   {
      public TransactionHolder(final long id)
      {
         transactionID = id;
      }

      public final long transactionID;

      public final List<RecordInfo> recordInfos = new ArrayList<RecordInfo>();

      public final List<RecordInfo> recordsToDelete = new ArrayList<RecordInfo>();

      public boolean prepared;

      public boolean invalid;

      public byte[] extraData;

   }



}
