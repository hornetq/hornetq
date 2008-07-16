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

package org.jboss.messaging.core.journal.impl;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.messaging.core.journal.EncodingSupport;
import org.jboss.messaging.core.journal.IOCallback;
import org.jboss.messaging.core.journal.PreparedTransactionInfo;
import org.jboss.messaging.core.journal.RecordInfo;
import org.jboss.messaging.core.journal.SequentialFile;
import org.jboss.messaging.core.journal.SequentialFileFactory;
import org.jboss.messaging.core.journal.TestableJournal;
import org.jboss.messaging.core.logging.Logger;
import org.jboss.messaging.util.ByteBufferWrapper;
import org.jboss.messaging.util.Pair;
import org.jboss.messaging.util.VariableLatch;

/**
 * 
 * A JournalImpl
 * 
 * @author <a href="mailto:tim.fox@jboss.com">Tim Fox</a>
 * @author <a href="mailto:clebert.suconic@jboss.com">Clebert Suconic</a>
 *
 */
public class JournalImpl implements TestableJournal
{
   
   // Constants -----------------------------------------------------
   private static final int STATE_STOPPED = 0;
   
   private static final int STATE_STARTED = 1;
   
   private static final int STATE_LOADED = 2;
   
   // The sizes of primitive types
   
   private static final int SIZE_LONG = 8;
   
   private static final int SIZE_INT = 4;

   private static final int SIZE_SHORT = 2;
   
   private static final int SIZE_BYTE = 1;
   
   public static final int MIN_FILE_SIZE = 1024;
   
   
   public static final int SIZE_HEADER = 4;

   //Record markers - they must be all unique
   
   public static final int BASIC_SIZE = SIZE_BYTE + SIZE_INT + SIZE_INT;
   
   public static final int SIZE_ADD_RECORD = BASIC_SIZE + SIZE_LONG + SIZE_BYTE + SIZE_INT; // + record.length
   
   public static final byte ADD_RECORD = 11;
   
   public static final byte SIZE_UPDATE_RECORD = BASIC_SIZE + SIZE_LONG + SIZE_BYTE + SIZE_INT; // + record.length;
   
   public static final byte UPDATE_RECORD = 12;
   
   public static final int SIZE_ADD_RECORD_TX = BASIC_SIZE + SIZE_LONG + SIZE_BYTE + SIZE_LONG + SIZE_INT; // + record.length

   public static final byte ADD_RECORD_TX = 13;
   
   public static final int  SIZE_UPDATE_RECORD_TX = BASIC_SIZE + SIZE_LONG + SIZE_BYTE + SIZE_LONG + SIZE_INT;  // + record.length
   
   public static final byte UPDATE_RECORD_TX = 14;
   
   public static final int SIZE_DELETE_RECORD = BASIC_SIZE + SIZE_LONG;
   
   public static final byte DELETE_RECORD = 15;
   
   public static final int  SIZE_DELETE_RECORD_TX = BASIC_SIZE + SIZE_LONG + SIZE_LONG;
   
   public static final byte DELETE_RECORD_TX = 16;
   
   public static final int SIZE_PREPARE_RECORD = BASIC_SIZE + SIZE_LONG;
   
   public static final byte PREPARE_RECORD = 17;
   
   
   public static final int SIZE_COMMIT_RECORD = BASIC_SIZE + SIZE_LONG;
   
   public static final byte COMMIT_RECORD = 18;
   
   public static final int SIZE_ROLLBACK_RECORD = BASIC_SIZE + SIZE_LONG;
   
   public static final byte ROLLBACK_RECORD = 19;
   
   public static final byte FILL_CHARACTER = 74; // Letter 'J' 
   
   
   // Attributes ----------------------------------------------------
   
   private boolean autoReclaim = true;
   
   private AtomicInteger nextOrderingId = new AtomicInteger(0);
   
   // used for Asynchronous IO only (ignored on NIO).
   private final int maxAIO;
   
   // used for Asynchronous IO only (ignored on NIO).
   private final long aioTimeout; // in ms
   
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
   
   private final Map<Long, PosFiles> posFilesMap = new ConcurrentHashMap<Long, PosFiles>();
   
   private final Map<Long, TransactionNegPos> transactionInfos = new ConcurrentHashMap<Long, TransactionNegPos>();
   
   private final ConcurrentMap<Long, TransactionCallback> transactionCallbacks = new ConcurrentHashMap<Long, TransactionCallback>();
   
   private ExecutorService closingExecutor = null;
   
   /** 
    * We have a separated executor for open, as if we used the same executor this would still represent
    * a point of wait between the closing and open.
    * */
   private ExecutorService openExecutor = null;
   
   /*
    * We use a semaphore rather than synchronized since it performs better when
    * contended
    */
   
   //TODO - improve concurrency by allowing concurrent accesses if doesn't change current file
   // this locks access to currentFile
   private final Semaphore lock = new Semaphore(1, true);
   
   private volatile JournalFile currentFile ;
   
   private volatile int state;
   
   private volatile long lastOrderingID;
   
   private final AtomicLong transactionIDSequence = new AtomicLong(0);
   
   private Reclaimer reclaimer = new Reclaimer();
   
   // Static --------------------------------------------------------
   
   private static final Logger log = Logger.getLogger(JournalImpl.class);
   
   private static final boolean trace = log.isTraceEnabled();
   
   // Constructors --------------------------------------------------
   
   public JournalImpl(final int fileSize, final int minFiles,
         final boolean syncTransactional, final boolean syncNonTransactional,
         final SequentialFileFactory fileFactory, 
         final String filePrefix, final String fileExtension, final int maxAIO, final long aioTimeout)
   {
      if (fileSize < MIN_FILE_SIZE)
      {
         throw new IllegalArgumentException("File size cannot be less than " + MIN_FILE_SIZE + " bytes");
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
      if (aioTimeout < 1)
      {
         throw new IllegalStateException("aio-timeout cannot be less than 1 second");
      }
      
      this.fileSize = fileSize;
      
      this.minFiles = minFiles;
      
      this.syncTransactional = syncTransactional;
      
      this.syncNonTransactional = syncNonTransactional;
      
      this.fileFactory = fileFactory;
      
      this.filePrefix = filePrefix;
      
      this.fileExtension = fileExtension;
      
      this.maxAIO = maxAIO;
      
      this.aioTimeout = aioTimeout;
   }
   
   // Journal implementation ----------------------------------------------------------------
   
   public void appendAddRecord(final long id, final byte recordType, final EncodingSupport record) throws Exception
   {
      if (state != STATE_LOADED)
      {
         throw new IllegalStateException("Journal must be loaded first");
      }
      
      int recordLength = record.getEncodeSize();
      
      int size = SIZE_ADD_RECORD + recordLength;
      
      ByteBufferWrapper bb = new ByteBufferWrapper(fileFactory.newBuffer(size));
      
      bb.putByte(ADD_RECORD);     
      bb.position(SIZE_BYTE + SIZE_INT); // skip ID part
      bb.putInt(recordLength);
      bb.putLong(id);
      bb.putByte(recordType);
      record.encode(bb);
      bb.putInt(size);        
      bb.rewind();
      
      JournalFile usedFile = appendRecord(bb.getBuffer(), syncNonTransactional, null);
      
      posFilesMap.put(id, new PosFiles(usedFile));
   }
   
   public void appendAddRecord(final long id, final byte recordType, final byte[] record) throws Exception
   {
      if (state != STATE_LOADED)
      {
         throw new IllegalStateException("Journal must be loaded first");
      }
      
      
      int size = SIZE_ADD_RECORD + record.length;
      
      ByteBuffer bb = fileFactory.newBuffer(size);
      
      bb.put(ADD_RECORD);
      bb.position(SIZE_BYTE + SIZE_INT); // skip ID part
      bb.putInt(record.length);     
      bb.putLong(id);
      bb.put(recordType);
      bb.put(record);		
      bb.putInt(size);			
      bb.rewind();
      
      JournalFile usedFile = appendRecord(bb, syncNonTransactional, null);
      
      posFilesMap.put(id, new PosFiles(usedFile));
   }
   
   public void appendUpdateRecord(final long id, final byte recordType, final byte[] record) throws Exception
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
      
      int size = SIZE_UPDATE_RECORD + record.length;
      
      ByteBuffer bb = fileFactory.newBuffer(size); 
      
      bb.put(UPDATE_RECORD);     
      bb.position(SIZE_BYTE + SIZE_INT); // skip ID part
      bb.putInt(record.length);     
      bb.putLong(id);      
      bb.put(recordType);
      bb.put(record);      
      bb.putInt(size);     
      bb.rewind();
      
      JournalFile usedFile = appendRecord(bb, syncNonTransactional, null);
      
      posFiles.addUpdateFile(usedFile);
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
      
      ByteBufferWrapper bb = new ByteBufferWrapper(fileFactory.newBuffer(size));
      
      bb.putByte(UPDATE_RECORD);     
      bb.position(SIZE_BYTE + SIZE_INT); // skip ID part
      bb.putInt(record.getEncodeSize());
      bb.putLong(id);      
      bb.putByte(recordType);
      record.encode(bb);
      bb.putInt(size);     
      bb.rewind();
      
      JournalFile usedFile = appendRecord(bb.getBuffer(), syncNonTransactional, null);
      
      posFiles.addUpdateFile(usedFile);
   }
   
   public void appendDeleteRecord(long id) throws Exception
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
      
      ByteBuffer bb = fileFactory.newBuffer(size); 
      
      bb.put(DELETE_RECORD);     
      bb.position(SIZE_BYTE + SIZE_INT); // skip ID part
      bb.putLong(id);      
      bb.putInt(size);     
      bb.rewind();
      
      JournalFile usedFile = appendRecord(bb, syncNonTransactional, null);
      posFiles.addDelete(usedFile);
   }     
   
   public long getTransactionID()
   {
      return transactionIDSequence.getAndIncrement();
   }
   
   public void appendAddRecordTransactional(final long txID, final long id, final byte recordType, 
                                            final EncodingSupport record) throws Exception
   {
      if (state != STATE_LOADED)
      {
         throw new IllegalStateException("Journal must be loaded first");
      }
      
      int recordLength = record.getEncodeSize();
      
      int size = SIZE_ADD_RECORD_TX + recordLength;
      
      ByteBufferWrapper bb = new ByteBufferWrapper(fileFactory.newBuffer(size)); 
      
      bb.putByte(ADD_RECORD_TX);
      bb.position(SIZE_BYTE + SIZE_INT); // skip ID part
      bb.putInt(recordLength);
      bb.putLong(txID);
      bb.putByte(recordType);
      bb.putLong(id);
      record.encode(bb);
      bb.putInt(size);     
      bb.rewind();
      
      JournalFile usedFile;
      
      usedFile = appendRecord(bb.getBuffer(), false, getTransactionCallback(txID));
      
      TransactionNegPos tx = getTransactionInfo(txID);
      
      tx.addPos(usedFile, id);
   }
   
   public void appendAddRecordTransactional(final long txID, final long id, final byte recordType, final byte[] record) throws Exception
   {
      if (state != STATE_LOADED)
      {
         throw new IllegalStateException("Journal must be loaded first");
      }
      
      int size = SIZE_ADD_RECORD_TX + record.length;
      
      ByteBuffer bb = fileFactory.newBuffer(size); 
      
      bb.put(ADD_RECORD_TX);
      bb.position(SIZE_BYTE + SIZE_INT); // skip ID part
      bb.putInt(record.length);
      bb.putLong(txID);
      bb.put(recordType);
      bb.putLong(id);
      bb.put(record);
      bb.putInt(size);
      bb.rewind();
      
      JournalFile usedFile;
      
      usedFile = appendRecord(bb, false, getTransactionCallback(txID));
      
      TransactionNegPos tx = getTransactionInfo(txID);
      
      tx.addPos(usedFile, id);
   }
   
   public void appendUpdateRecordTransactional(final long txID, final long id, byte recordType, final byte[] record) throws Exception
   {
      if (state != STATE_LOADED)
      {
         throw new IllegalStateException("Journal must be loaded first");
      }
      
      int size = SIZE_UPDATE_RECORD_TX + record.length; 
      
      ByteBuffer bb = fileFactory.newBuffer(size); 
      
      bb.put(UPDATE_RECORD_TX);     
      bb.position(SIZE_BYTE + SIZE_INT); // skip ID part
      bb.putInt(record.length);     
      bb.putLong(txID);
      bb.put(recordType);
      bb.putLong(id);      
      bb.put(record);
      bb.putInt(size);     
      bb.rewind();
      
      JournalFile usedFile;
      
      usedFile = appendRecord(bb, false, getTransactionCallback(txID));
      
      TransactionNegPos tx = getTransactionInfo(txID);
      
      tx.addPos(usedFile, id);
   }
   
   public void appendUpdateRecordTransactional(final long txID, final long id, byte recordType, EncodingSupport record) throws Exception
   {
      if (state != STATE_LOADED)
      {
         throw new IllegalStateException("Journal must be loaded first");
      }
      
      int size = SIZE_UPDATE_RECORD_TX + record.getEncodeSize(); 
      
      ByteBufferWrapper bb = new ByteBufferWrapper(fileFactory.newBuffer(size)); 
      
      
      bb.putByte(UPDATE_RECORD_TX);     
      bb.position(SIZE_BYTE + SIZE_INT); // skip ID part
      bb.putInt(record.getEncodeSize());
      bb.putLong(txID);
      bb.putByte(recordType);
      bb.putLong(id);      
      record.encode(bb);
      bb.putInt(size);     
      bb.rewind();
      
      JournalFile usedFile;
      
      usedFile = appendRecord(bb.getBuffer(), false, getTransactionCallback(txID));
      
      TransactionNegPos tx = getTransactionInfo(txID);
      
      tx.addPos(usedFile, id);
   }
   
   public void appendDeleteRecordTransactional(final long txID, final long id) throws Exception
   {
      if (state != STATE_LOADED)
      {
         throw new IllegalStateException("Journal must be loaded first");
      }
      
      int size = SIZE_DELETE_RECORD_TX;
      
      ByteBuffer bb = fileFactory.newBuffer(size); 
      
      bb.put(DELETE_RECORD_TX);     
      bb.position(SIZE_BYTE + SIZE_INT); // skip ID part
      bb.putLong(txID);    
      bb.putLong(id);      
      bb.putInt(size);     
      bb.rewind();
      
      JournalFile usedFile;
      
      usedFile = appendRecord(bb, false, getTransactionCallback(txID));
      
      TransactionNegPos tx = getTransactionInfo(txID);
      
      tx.addNeg(usedFile, id);      
   }  
   
   public void appendPrepareRecord(final long txID) throws Exception
   {
      if (state != STATE_LOADED)
      {
         throw new IllegalStateException("Journal must be loaded first");
      }
      
      TransactionNegPos tx = transactionInfos.get(txID);
      
      if (tx == null)
      {
         throw new IllegalStateException("Cannot find tx with id " + txID);
      }
      
      int size = SIZE_PREPARE_RECORD;
      
      ByteBuffer bb = fileFactory.newBuffer(size); 
      
      bb.put(PREPARE_RECORD);    
      bb.position(SIZE_BYTE + SIZE_INT); // skip ID part
      bb.putLong(txID);
      bb.putInt(size);           
      bb.rewind();
      
      JournalFile usedFile;
      
      usedFile = appendRecord(bb, syncTransactional, getTransactionCallback(txID));
      
      tx.prepare(usedFile);
   }
   
   public void appendCommitRecord(final long txID) throws Exception
   {
      if (state != STATE_LOADED)
      {
         throw new IllegalStateException("Journal must be loaded first");
      }		
      
      TransactionNegPos tx = transactionInfos.remove(txID);
      
      if (tx == null)
      {
         throw new IllegalStateException("Cannot find tx with id " + txID);
      }
      
      int size = SIZE_COMMIT_RECORD;
      
      ByteBuffer bb = fileFactory.newBuffer(size); 
      
      bb.put(COMMIT_RECORD);     
      bb.position(SIZE_BYTE + SIZE_INT); // skip ID part
      bb.putLong(txID);    
      bb.putInt(size);           
      bb.rewind();
      
      JournalFile usedFile;
      
      usedFile = appendRecord(bb, syncTransactional, getTransactionCallback(txID));
      
      transactionCallbacks.remove(txID);
      
      tx.commit(usedFile);
      
   }
   
   public void appendRollbackRecord(final long txID) throws Exception
   {
      if (state != STATE_LOADED)
      {
         throw new IllegalStateException("Journal must be loaded first");
      }
      
      TransactionNegPos tx = transactionInfos.remove(txID);
      
      if (tx == null)
      {
         throw new IllegalStateException("Cannot find tx with id " + txID);
      }
      
      int size = SIZE_ROLLBACK_RECORD;
      
      ByteBuffer bb = fileFactory.newBuffer(size); 
      
      bb.put(ROLLBACK_RECORD);      
      bb.position(SIZE_BYTE + SIZE_INT); // skip ID part
      bb.putLong(txID);
      bb.putInt(size);        
      bb.rewind();
      
      JournalFile usedFile;
      
      usedFile = appendRecord(bb, syncTransactional, getTransactionCallback(txID));      
      
      transactionCallbacks.remove(txID);
      
      tx.rollback(usedFile);
   }
   
   public synchronized long load(final List<RecordInfo> committedRecords,
         final List<PreparedTransactionInfo> preparedTransactions) throws Exception
         {
      if (state != STATE_STARTED)
      {
         throw new IllegalStateException("Journal must be in started state");
      }
      
      Set<Long> recordsToDelete = new HashSet<Long>();
      
      Map<Long, TransactionHolder> transactions = new LinkedHashMap<Long, TransactionHolder>();
      
      List<RecordInfo> records = new ArrayList<RecordInfo>();
      
      List<String> fileNames = fileFactory.listFiles(fileExtension);
      
      List<JournalFile> orderedFiles = new ArrayList<JournalFile>(fileNames.size());
      
      for (String fileName: fileNames)
      {
         SequentialFile file = fileFactory.createSequentialFile(fileName, maxAIO, aioTimeout);
         
         file.open();
         
         ByteBuffer bb = fileFactory.newBuffer(SIZE_INT);
         
         file.read(bb);
         
         int orderingID = bb.getInt();
         
         if (nextOrderingId.get() < orderingID)
         {
            nextOrderingId.set(orderingID);
         }
         
         orderedFiles.add(new JournalFileImpl(file, orderingID));
         
         file.close();
      }
      
      //Now order them by ordering id - we can't use the file name for ordering since we can re-use dataFiles
      
      class JournalFileComparator implements Comparator<JournalFile>
      {
         public int compare(JournalFile f1, JournalFile f2)
         {
            int id1 = f1.getOrderingID();
            int id2 = f2.getOrderingID();
            
            return (id1 < id2 ? -1 : (id1 == id2 ? 0 : 1));
         }
      }
      
      Collections.sort(orderedFiles, new JournalFileComparator());
      
      int lastDataPos = -1;
      
      long maxTransactionID = -1;
      
      long maxMessageID = -1;
      
      for (JournalFile file: orderedFiles)
      {  
         file.getFile().open();
         
         ByteBuffer bb = fileFactory.newBuffer(fileSize);
         
         int bytesRead = file.getFile().read(bb);
         
         if (bytesRead != fileSize)
         {
            //deal with this better
            
            throw new IllegalStateException("File is wrong size " + bytesRead +
                  " expected " + fileSize + " : " + file.getFile().getFileName());
         }
         
         //First long is the ordering timestamp, we just jump its position
         bb.position(file.getFile().calculateBlockStart(SIZE_HEADER));
         
         boolean hasData = false;
         
         while (bb.hasRemaining())
         {
            final int pos = bb.position();
            
            byte recordType = bb.get();
            
            if (recordType < ADD_RECORD || recordType > ROLLBACK_RECORD)
            {
               if (trace)
               {
                  log.trace("Invalid record type at " + bb.position() + " file:" + file);
               }
               continue;
            }

            if (bb.position() + SIZE_INT > fileSize)
            {
               continue;
            }

            int readFileId = bb.getInt();
            
            // The variable record portion used on Updates and Appends
            int variableSize = 0;
            // The record size (without the variable portion)
            int recordSize = 0;
            
            if (recordType >= ADD_RECORD && recordType <= UPDATE_RECORD_TX)
            {
               if (bb.position() + SIZE_INT > fileSize)
               {
                  continue;
               }
               
               variableSize = bb.getInt();
            }
            
            switch(recordType)
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
                  // Sanity check, this was previously tested, nothing different should be on this switch
                  throw new IllegalStateException("Record other than expected");
               
            }
            
            if (pos + recordSize + variableSize > fileSize)
            {
               continue;
            }
            
            int oldPos = bb.position();
            
            bb.position(pos + variableSize + recordSize - SIZE_INT);
            
            int checkSize = bb.getInt();
            
            if (checkSize != variableSize + recordSize)
            {
               log.warn("Record at position " + pos + " is corrupted and it is being ignored");
               bb.position(pos + SIZE_BYTE);
               continue;
            }
            
            if (readFileId != file.getOrderingID())
            {
               //log.info("Record read at position " + pos + " doesn't belong to this current journal file, ignoring it!");
               continue;
            }
            
            bb.position(oldPos);
            
            
            
            switch(recordType)
            {
               case ADD_RECORD:
               {                          
                  long id = bb.getLong();  
                  
                  maxMessageID = Math.max(maxMessageID, id);
                  
                  byte userRecordType = bb.get();
                  
                  byte[] record = new byte[variableSize];                 
                  
                  bb.get(record);

                  records.add(new RecordInfo(id, userRecordType, record, false));
                  hasData = true;                  
                  
                  posFilesMap.put(id, new PosFiles(file));
                  
                  break;
               }                             
               case UPDATE_RECORD:                 
               {
                  long id = bb.getLong();    
                  
                  maxMessageID = Math.max(maxMessageID, id);
                  
                  byte userRecordType = bb.get();
                  
                  byte[] record = new byte[variableSize];                 
                  bb.get(record);                  

                  records.add(new RecordInfo(id, userRecordType, record, true));                    
                  hasData = true;      
                  file.incPosCount();
                  
                  PosFiles posFiles = posFilesMap.get(id);
                  
                  if (posFiles != null)
                  {
                     //It's legal for this to be null. The file(s) with the  may have been deleted
                     //just leaving some updates in this file
                     
                     posFiles.addUpdateFile(file);
                  }
                  
                  break;
               }              
               case DELETE_RECORD:                 
               {
                  long id = bb.getLong(); 
                  
                  maxMessageID = Math.max(maxMessageID, id);
                  
                  recordsToDelete.add(id);                     
                  hasData = true;
                  
                  PosFiles posFiles = posFilesMap.remove(id);
                  
                  if (posFiles != null)
                  {
                     posFiles.addDelete(file);
                  }                    
                  
                  break;
               }              
               case ADD_RECORD_TX:
               {              
                  long txID = bb.getLong();                    
                  maxTransactionID = Math.max(maxTransactionID, txID); 
                  
                  byte userRecordType = bb.get();
                  
                  long id = bb.getLong();          
                  maxMessageID = Math.max(maxMessageID, id);
                  
                  byte[] record = new byte[variableSize];                 
                  bb.get(record);                  
                  
                  TransactionHolder tx = transactions.get(txID);
                  
                  if (tx == null)
                  {
                     tx = new TransactionHolder(txID);                        
                     transactions.put(txID, tx);
                  }
                  
                  tx.recordInfos.add(new RecordInfo(id, userRecordType, record, false));                     
                  
                  TransactionNegPos tnp = transactionInfos.get(txID);
                  
                  if (tnp == null)
                  {
                     tnp = new TransactionNegPos();
                     
                     transactionInfos.put(txID, tnp);
                  }
                  
                  tnp.addPos(file, id);
                  
                  hasData = true;                                          
                  
                  break;
               }     
               case UPDATE_RECORD_TX:
               {              
                  long txID = bb.getLong();  
                  maxTransactionID = Math.max(maxTransactionID, txID);
                  
                  byte userRecordType = bb.get();
                  
                  long id = bb.getLong();
                  maxMessageID = Math.max(maxMessageID, id);
                  
                  byte[] record = new byte[variableSize];                 
                  bb.get(record);                  
                  
                  TransactionHolder tx = transactions.get(txID);
                  
                  if (tx == null)
                  {
                     tx = new TransactionHolder(txID);                        
                     transactions.put(txID, tx);
                  }
                  
                  tx.recordInfos.add(new RecordInfo(id, userRecordType, record, true));
                  
                  TransactionNegPos tnp = transactionInfos.get(txID);
                  
                  if (tnp == null)
                  {
                     tnp = new TransactionNegPos();
                     
                     transactionInfos.put(txID, tnp);
                  }
                  
                  tnp.addPos(file, id);
                  
                  hasData = true;                     
                  
                  break;
               }  
               case DELETE_RECORD_TX:
               {              
                  long txID = bb.getLong();  
                  maxTransactionID = Math.max(maxTransactionID, txID);                 
                  long id = bb.getLong(); 
                  maxMessageID = Math.max(maxMessageID, id);
                  
                  TransactionHolder tx = transactions.get(txID);
                  
                  if (tx == null)
                  {
                     tx = new TransactionHolder(txID);                        
                     transactions.put(txID, tx);
                  }
                  
                  tx.recordsToDelete.add(id);                     
                  
                  TransactionNegPos tnp = transactionInfos.get(txID);
                  
                  if (tnp == null)
                  {
                     tnp = new TransactionNegPos();
                     
                     transactionInfos.put(txID, tnp);
                  }
                  
                  tnp.addNeg(file, id);
                  
                  hasData = true;                     
                  
                  break;
               }  
               case PREPARE_RECORD:
               {
                  long txID = bb.getLong();           
                  
                  maxTransactionID = Math.max(maxTransactionID, txID);                 

                  TransactionHolder tx = transactions.get(txID);
                  
                  if (tx == null)
                  {
                     throw new IllegalStateException("Cannot find tx with id " + txID);
                  }
                  
                  tx.prepared = true;
                  
                  TransactionNegPos tnp = transactionInfos.get(txID);
                  
                  if (tnp == null)
                  {
                     throw new IllegalStateException("Cannot find tx " + txID);
                  }
                  
                  tnp.prepare(file);   
                  
                  hasData = true;         
                  
                  break;
               }
               case COMMIT_RECORD:
               {
                  long txID = bb.getLong();  
                  
                  maxTransactionID = Math.max(maxTransactionID, txID);
                  TransactionHolder tx = transactions.remove(txID);
                  
                  if (tx != null)
                  {
                     records.addAll(tx.recordInfos);                    
                     recordsToDelete.addAll(tx.recordsToDelete);  
                     
                     TransactionNegPos tnp = transactionInfos.remove(txID);
                     
                     if (tnp == null)
                     {
                        throw new IllegalStateException("Cannot find tx " + txID);
                     }
                     
                     tnp.commit(file);       
                     
                     hasData = true;         
                  }
                  
                  break;
               }
               case ROLLBACK_RECORD:
               {
                  long txID = bb.getLong();     
                  
                  maxTransactionID = Math.max(maxTransactionID, txID);                 
                  
                  TransactionHolder tx = transactions.remove(txID);
                  
                  if (tx != null)
                  {                       
                     TransactionNegPos tnp = transactionInfos.remove(txID);
                     
                     if (tnp == null)
                     {
                        throw new IllegalStateException("Cannot find tx " + txID);
                     }
                     
                     tnp.rollback(file);  
                     
                     hasData = true;         
                  }
                  
                  break;
               }
               default:                
               {
                  throw new IllegalStateException("Journal " + file.getFile().getFileName() +
                        " is corrupt, invalid record type " + recordType);
               }
            }
            
            checkSize = bb.getInt();
            
            if (checkSize != variableSize + recordSize)
            {
               throw new IllegalStateException("Internal error on loading file. Position doesn't match with checkSize");
            }
            
            bb.position(file.getFile().calculateBlockStart(bb.position()));
            
            if (recordType != FILL_CHARACTER)
            {
               lastDataPos = bb.position();
            }
         }
         
         file.getFile().close();          
         
         if (hasData)
         {        
            dataFiles.add(file);
         }
         else
         {           
            //Empty dataFiles with no data
            freeFiles.add(file);
         }                       
      }        
      
      transactionIDSequence.set(maxTransactionID + 1);
      
      //Create any more files we need
      
      //FIXME - size() involves a scan
      int filesToCreate = minFiles - (dataFiles.size() + freeFiles.size());
      
      for (int i = 0; i < filesToCreate; i++)
      {
         // Keeping all files opened can be very costly (mainly on AIO)
         freeFiles.add(createFile(false));
      }
      
      //The current file is the last one
      
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
         
         currentFile.getFile().position(lastDataPos);
         
         currentFile.setOffset(lastDataPos);
      }
      else
      {
         currentFile = freeFiles.remove();
         openFile(currentFile);
      }
      
      pushOpenedFile();
      
      for (RecordInfo record: records)
      {
         if (!recordsToDelete.contains(record.id))
         {
            committedRecords.add(record);
         }
      }
      
      for (TransactionHolder transaction: transactions.values())
      {
         if (!transaction.prepared)
         {
            log.warn("Uncommitted transaction with id " + transaction.transactionID + " found and discarded");
            
            TransactionNegPos transactionInfo = this.transactionInfos.get(transaction.transactionID);
            
            if (transactionInfo == null)
            {
               throw new IllegalStateException("Cannot find tx " + transaction.transactionID);
            }
            
            //Reverse the refs
            transactionInfo.forget();
            
            // Remove the transactionInfo
            transactionInfos.remove(transaction.transactionID);
         }
         else
         {
            PreparedTransactionInfo info = new PreparedTransactionInfo(transaction.transactionID);
            
            info.records.addAll(transaction.recordInfos);
            
            info.recordsToDelete.addAll(transaction.recordsToDelete);
            
            preparedTransactions.add(info);
         }
      }
      
      state = STATE_LOADED;
      
      return maxMessageID;
         }
   
   public int getAlignment() throws Exception
   {
      return this.fileFactory.getAlignment();
   }
   
   public synchronized void checkReclaimStatus() throws Exception
   {
      JournalFile[] files = new JournalFile[dataFiles.size()];
      
      reclaimer.scan(dataFiles.toArray(files));		
   }
   
   public String debug() throws Exception
   {
      this.checkReclaimStatus();
      
      StringBuilder builder = new StringBuilder();
      
      for (JournalFile file: dataFiles)
      {
         builder.append("DataFile:" + file + " posCounter = " + file.getPosCount() + " reclaimStatus = " +  file.isCanReclaim() + "\n");
         if (file instanceof JournalFileImpl)
         {
            builder.append(((JournalFileImpl)file).debug());
            
         }
      }
      
      for (JournalFile file: freeFiles)
      {
         builder.append("FreeFile:" + file + "\n");
      }
      
      builder.append("CurrentFile:" + currentFile+ " posCounter = " + currentFile.getPosCount() + "\n");
      
      if (currentFile instanceof JournalFileImpl)
      {
         builder.append(((JournalFileImpl)currentFile).debug());
      }
      
      builder.append("#Opened Files:" + this.openedFiles.size());
      
      return builder.toString();
   }
   
   // TestableJournal implementation --------------------------------------------------------------
   
   /** Method for use on testcases.
    *  It will call waitComplete on every transaction, so any assertions on the file system will be correct after this */
   public void debugWait() throws Exception
   {
      for (TransactionCallback callback: transactionCallbacks.values())
      {
         callback.waitCompletion(aioTimeout);
      }
      
      if (closingExecutor != null && !closingExecutor.isShutdown())
      {
         // Send something to the closingExecutor, just to make sure we went until its end
         final CountDownLatch latch = new CountDownLatch(1);
         
         this.closingExecutor.execute(new Runnable()
         {
            public void run()
            {
               latch.countDown();
            }
         });
         
         latch.await();
      }
      
      if (openExecutor != null && !openExecutor.isShutdown())
      {
         // Send something to the closingExecutor, just to make sure we went until its end
         final CountDownLatch latch = new CountDownLatch(1);
         
         this.openExecutor.execute(new Runnable()
         {
            public void run()
            {
               latch.countDown();
            }
         });
         
         latch.await();
      }
      
   }
   
   public synchronized void checkAndReclaimFiles() throws Exception
   {
      checkReclaimStatus();
      
      for (JournalFile file: dataFiles)
      {           
         if (file.isCanReclaim())
         {
            //File can be reclaimed or deleted
            
            if (trace) log.trace("Reclaiming file " + file);
            
            log.info("Reclaiming file " + file);
            
            dataFiles.remove(file);
            
            //FIXME - size() involves a scan!!!
            if (freeFiles.size() + dataFiles.size() + 1 + openedFiles.size() < minFiles)
            {
               //Re-initialise it
               
               int newOrderingID = generateOrderingID();
               
               SequentialFile sf = file.getFile();
               
               sf.open();
               
               ByteBuffer bb = fileFactory.newBuffer(SIZE_LONG); 
               
               bb.putInt(newOrderingID);
               
               int bytesWritten = sf.write(bb, true);
               
               JournalFile jf = new JournalFileImpl(sf, newOrderingID);
               
               sf.position(bytesWritten);
               
               jf.setOffset(bytesWritten);
               
               sf.close();
               
               freeFiles.add(jf);  
            }
            else
            {
               file.getFile().open();
               
               file.getFile().delete();
            }
            
            log.info("Done reclaiming");
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
   
   public long getAIOTimeout()
   {
      return aioTimeout;
   }
   
   // In some tests we need to force the journal to move to a next file
   public void forceMoveNextFile() throws Exception
   {
      lock.acquire();
      
      try
      {
         moveNextFile();
      }
      finally
      {
         lock.release();
      }
      
      debugWait();
   }

   public void disableAutoReclaiming()
   {
      this.autoReclaim = false;
   }
   

   // MessagingComponent implementation ---------------------------------------------------
   
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
      
      this.openExecutor =  Executors.newSingleThreadExecutor();
      this.closingExecutor = Executors.newSingleThreadExecutor();
      
      state = STATE_STARTED;
   }
   
   public synchronized void stop() throws Exception
   {
      if (state == STATE_STOPPED)
      {
         throw new IllegalStateException("Journal is already stopped");
      }
      
      stopReclaimer();
      
      closingExecutor.shutdown();
      if (!closingExecutor.awaitTermination(aioTimeout, TimeUnit.MILLISECONDS))
      {
         throw new IllegalStateException("Time out waiting for closing executor to finish");
      }
      
      if (currentFile != null)
      {
         currentFile.getFile().close();
      }
      
      openExecutor.shutdown();
      if (!openExecutor.awaitTermination(aioTimeout, TimeUnit.MILLISECONDS))
      {
         throw new IllegalStateException("Time out waiting for open executor to finish");
      }
      
      for (JournalFile file: openedFiles)
      {
         file.getFile().close();
      }
      
      currentFile = null;
      
      dataFiles.clear();
      
      freeFiles.clear();
      
      openedFiles.clear();
      
      state = STATE_STOPPED;
   }
   
   public void startReclaimer()
   {
      if (state == STATE_STOPPED)
      {
         throw new IllegalStateException("Journal is stopped");
      }
   }
   
   public void stopReclaimer()
   {
      if (state == STATE_STOPPED)
      {
         throw new IllegalStateException("Journal is already stopped");
      }
   }
   
   // Public -----------------------------------------------------------------------------
   
   // Private -----------------------------------------------------------------------------
   
   private JournalFile appendRecord(final ByteBuffer bb, final boolean sync, final TransactionCallback callback) throws Exception
   {
      lock.acquire();
      
      int size = bb.capacity();
      
      try
      {                 
         checkFile(size);
         bb.position(SIZE_BYTE);
         bb.putInt(currentFile.getOrderingID());
         bb.rewind();
         if (callback != null)
         {
            currentFile.getFile().write(bb, callback);
            if (sync)
            {
               callback.waitCompletion(aioTimeout);
            }
         }
         else
         {
            currentFile.getFile().write(bb, sync);       
         }
         currentFile.extendOffset(size);
         return currentFile;
      }
      finally
      {
         lock.release();
      }
   }
   
   private void repairFrom(final int pos, final JournalFile file) throws Exception
   {
      log.warn("Corruption has been detected in file: " + file.getFile().getFileName() +
            " in the record that starts at position " + pos + ". " + 
      "The most likely cause is that a crash occurred in the previous run. The corrupt record will be discarded.");
      
      file.getFile().fill(pos, fileSize - pos, FILL_CHARACTER);
      
      file.getFile().position(pos);
   }
   
   private JournalFile createFile(boolean keepOpened) throws Exception
   {
      int orderingID = generateOrderingID();
      
      String fileName = filePrefix + "-" + orderingID + "." + fileExtension;
      
      if (trace) log.trace("Creating file " + fileName);
      
      SequentialFile sequentialFile = fileFactory.createSequentialFile(fileName, maxAIO, aioTimeout);
      
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
   
   private void openFile(JournalFile file) throws Exception
   {
      file.getFile().open();
      file.getFile().position(file.getFile().calculateBlockStart(SIZE_HEADER));
   }
   
   private int generateOrderingID()
   {
      return nextOrderingId.addAndGet(1);
   }
   
   // You need to guarantee lock.acquire() over currentFile before calling this method
   private void checkFile(final int size) throws Exception
   {		
      if (size % currentFile.getFile().getAlignment() != 0)
      {
         throw new IllegalStateException("You can't write blocks in a size different than " + currentFile.getFile().getAlignment());
      }
      
      //We take into account the first timestamp long
      if (size > fileSize - currentFile.getFile().calculateBlockStart(SIZE_HEADER))
      {
         throw new IllegalArgumentException("Record is too large to store " + size);
      }
      
      if (currentFile == null || fileSize - currentFile.getOffset() < size)
      {
         moveNextFile();
         
      }     
   }
   
   // You need to guarantee lock.acquire() before calling this method
   private void moveNextFile() throws InterruptedException
   {
      closeFile(currentFile);
      
      currentFile = enqueueOpenFile();
   }
   
   // You need to guarantee lock.acquire() before calling this method
   private JournalFile enqueueOpenFile() throws InterruptedException
   {
      if (trace) log.trace("enqueueOpenFile with openedFiles.size=" + openedFiles.size());
      openExecutor.execute(new Runnable()
      {
         public void run()
         {
            try
            {
               pushOpenedFile();
               if (autoReclaim)
               {
                  checkAndReclaimFiles();
               }
            }
            catch (Exception e)
            {
               log.error(e.getMessage(), e);
            }
         }
      });
      
      JournalFile nextFile = openedFiles.poll(aioTimeout, TimeUnit.SECONDS);
      
      if (nextFile == null)
      {
         throw new IllegalStateException("Timed out waiting for an opened file");
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
      this.closingExecutor.execute(new Runnable() { public void run()
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
   
   private TransactionNegPos getTransactionInfo(final long txID)
   {
      TransactionNegPos tx = transactionInfos.get(txID);
      
      if (tx == null)
      {
         tx = new TransactionNegPos();
         
         transactionInfos.put(txID, tx);
      }
      
      return tx;
   }
   
   private TransactionCallback getTransactionCallback(final long transactionId)
   {
      if (fileFactory.isSupportsCallbacks() && syncTransactional)
      {
         TransactionCallback callback = this.transactionCallbacks.get(transactionId);
         
         if (callback == null)
         {
            callback = new TransactionCallback();
            transactionCallbacks.put(transactionId, callback);
         }
         
         callback.countUp();
         return callback;
      }
      else
      {
         return null;
      }
   }
   
   // Inner classes ---------------------------------------------------------------------------
   
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
      
      public void waitCompletion(long timeout) throws InterruptedException
      {
         countLatch.waitCompletion(timeout);
         
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
            for (JournalFile jf: updateFiles)
            {
               file.incNegCount(jf);
            }
         }
      }
   }
   
   private class TransactionNegPos
   {
      private List<Pair<JournalFile, Long>> pos;
      
      private List<Pair<JournalFile, Long>> neg;
      
      private Set<JournalFile> transactionPos;
      
      void addTXPosCount(final JournalFile file)
      {
         if (transactionPos == null)
         {
            transactionPos = new HashSet<JournalFile>();
         }
         
         if (!transactionPos.contains(file))
         {
            transactionPos.add(file);
            
            //We add a pos for the transaction itself in the file - this prevents any transactional operations
            //being deleted before a commit or rollback is written
            file.incPosCount();
         }  
      }
      
      void addPos(final JournalFile file, final long id)
      {     
         addTXPosCount(file);          
         
         if (pos == null)
         {
            pos = new ArrayList<Pair<JournalFile, Long>>();
         }
         
         pos.add(new Pair<JournalFile, Long>(file, id));
      }
      
      void addNeg(final JournalFile file, final long id)
      {        
         addTXPosCount(file);    
         
         if (neg == null)
         {
            neg = new ArrayList<Pair<JournalFile, Long>>();
         }
         
         neg.add(new Pair<JournalFile, Long>(file, id));       
      }
      
      void commit(final JournalFile file)
      {        
         if (pos != null)
         {
            for (Pair<JournalFile, Long> p: pos)
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
            for (Pair<JournalFile, Long> n: neg)
            {
               PosFiles posFiles = posFilesMap.remove(n.b);
               
               if (posFiles != null)
               {
                  //throw new IllegalStateException("Cannot find add info " + n.b);
                  posFiles.addDelete(n.a);
               }
               
            }
         }
         
         //Now add negs for the pos we added in each file in which there were transactional operations
         
         for (JournalFile jf: transactionPos)
         {
            file.incNegCount(jf);
         }        
      }
      
      void rollback(JournalFile file)
      {     
         //Now add negs for the pos we added in each file in which there were transactional operations
         //Note that we do this on rollback as we do on commit, since we need to ensure the file containing
         //the rollback record doesn't get deleted before the files with the transactional operations are deleted
         //Otherwise we may run into problems especially with XA where we are just left with a prepare when the tx
         //has actually been rolled back
         
         for (JournalFile jf: transactionPos)
         {
            file.incNegCount(jf);
         }
      }
      
      void prepare(JournalFile file)
      {
         //We don't want the prepare record getting deleted before time
         
         addTXPosCount(file);
      }
      
      void forget()
      {
         //The transaction was not committed or rolled back in the file, so we reverse any pos counts we added
         
         for (JournalFile jf: transactionPos)
         {
            jf.decPosCount();
         }
      }
   }

}
