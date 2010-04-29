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

package org.hornetq.core.replication.impl;

import java.util.List;

import org.hornetq.core.journal.EncodingSupport;
import org.hornetq.core.journal.IOCompletion;
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.JournalLoadInformation;
import org.hornetq.core.journal.LoaderCallback;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.TransactionFailureCallback;
import org.hornetq.core.journal.impl.dataformat.ByteArrayEncoding;
import org.hornetq.core.logging.Logger;
import org.hornetq.core.persistence.impl.journal.JournalStorageManager;
import org.hornetq.core.replication.ReplicationManager;

/**
 * Used by the {@link JournalStorageManager} to replicate journal calls. 
 *
 * @author <mailto:clebert.suconic@jboss.org">Clebert Suconic</a>
 *
 * @see JournalStorageManager
 *
 */
public class ReplicatedJournal implements Journal
{

   // Constants -----------------------------------------------------

   private static final Logger log = Logger.getLogger(ReplicatedJournal.class);

   // Attributes ----------------------------------------------------

   private static final boolean trace = false;

   private static void trace(final String message)
   {
      System.out.println("ReplicatedJournal::" + message);
   }

   private final ReplicationManager replicationManager;

   private final Journal localJournal;

   private final byte journalID;

   public ReplicatedJournal(final byte journaID, final Journal localJournal, final ReplicationManager replicationManager)
   {
      super();
      journalID = journaID;
      this.localJournal = localJournal;
      this.replicationManager = replicationManager;
   }

   // Static --------------------------------------------------------

   // Constructors --------------------------------------------------

   // Public --------------------------------------------------------

   /**
    * @param id
    * @param recordType
    * @param record
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendAddRecord(long, byte, byte[], boolean)
    */
   public void appendAddRecord(final long id, final byte recordType, final byte[] record, final boolean sync) throws Exception
   {
      this.appendAddRecord(id, recordType, new ByteArrayEncoding(record), sync);
   }

   public void appendAddRecord(final long id, final byte recordType, final EncodingSupport record, final boolean sync) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("Append record id = " + id + " recordType = " + recordType);
      }
      replicationManager.appendAddRecord(journalID, id, recordType, record);
      localJournal.appendAddRecord(id, recordType, record, sync);
   }

   public void appendAddRecord(final long id,
                               final byte recordType,
                               final byte[] record,
                               final boolean sync,
                               final IOCompletion completionCallback) throws Exception
   {
      this.appendAddRecord(id, recordType, new ByteArrayEncoding(record), sync, completionCallback);
   }

   /**
    * @param id
    * @param recordType
    * @param record
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendAddRecord(long, byte, org.hornetq.core.journal.EncodingSupport, boolean)
    */
   public void appendAddRecord(final long id,
                               final byte recordType,
                               final EncodingSupport record,
                               final boolean sync,
                               final IOCompletion completionCallback) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("Append record id = " + id + " recordType = " + recordType);
      }
      replicationManager.appendAddRecord(journalID, id, recordType, record);
      localJournal.appendAddRecord(id, recordType, record, sync, completionCallback);
   }

   /**
    * @param txID
    * @param id
    * @param recordType
    * @param record
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendAddRecordTransactional(long, long, byte, byte[])
    */
   public void appendAddRecordTransactional(final long txID, final long id, final byte recordType, final byte[] record) throws Exception
   {
      this.appendAddRecordTransactional(txID, id, recordType, new ByteArrayEncoding(record));
   }

   /**
    * @param txID
    * @param id
    * @param recordType
    * @param record
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendAddRecordTransactional(long, long, byte, org.hornetq.core.journal.EncodingSupport)
    */
   public void appendAddRecordTransactional(final long txID,
                                            final long id,
                                            final byte recordType,
                                            final EncodingSupport record) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("Append record TXid = " + id + " recordType = " + recordType);
      }
      replicationManager.appendAddRecordTransactional(journalID, txID, id, recordType, record);
      localJournal.appendAddRecordTransactional(txID, id, recordType, record);
   }

   /**
    * @param txID
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendCommitRecord(long, boolean)
    */
   public void appendCommitRecord(final long txID, final boolean sync) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendCommit " + txID);
      }
      replicationManager.appendCommitRecord(journalID, txID);
      localJournal.appendCommitRecord(txID, sync);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.Journal#appendCommitRecord(long, boolean, org.hornetq.core.journal.IOCompletion)
    */
   public void appendCommitRecord(final long txID, final boolean sync, final IOCompletion callback) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendCommit " + txID);
      }
      replicationManager.appendCommitRecord(journalID, txID);
      localJournal.appendCommitRecord(txID, sync, callback);
   }

   /**
    * @param id
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendDeleteRecord(long, boolean)
    */
   public void appendDeleteRecord(final long id, final boolean sync) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendDelete " + id);
      }
      replicationManager.appendDeleteRecord(journalID, id);
      localJournal.appendDeleteRecord(id, sync);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.Journal#appendDeleteRecord(long, boolean, org.hornetq.core.journal.IOCompletion)
    */
   public void appendDeleteRecord(final long id, final boolean sync, final IOCompletion completionCallback) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendDelete " + id);
      }
      replicationManager.appendDeleteRecord(journalID, id);
      localJournal.appendDeleteRecord(id, sync, completionCallback);
   }

   /**
    * @param txID
    * @param id
    * @param record
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendDeleteRecordTransactional(long, long, byte[])
    */
   public void appendDeleteRecordTransactional(final long txID, final long id, final byte[] record) throws Exception
   {
      this.appendDeleteRecordTransactional(txID, id, new ByteArrayEncoding(record));
   }

   /**
    * @param txID
    * @param id
    * @param record
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendDeleteRecordTransactional(long, long, org.hornetq.core.journal.EncodingSupport)
    */
   public void appendDeleteRecordTransactional(final long txID, final long id, final EncodingSupport record) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendDelete txID=" + txID + " id=" + id);
      }
      replicationManager.appendDeleteRecordTransactional(journalID, txID, id, record);
      localJournal.appendDeleteRecordTransactional(txID, id, record);
   }

   /**
    * @param txID
    * @param id
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendDeleteRecordTransactional(long, long)
    */
   public void appendDeleteRecordTransactional(final long txID, final long id) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendDelete (noencoding) txID=" + txID + " id=" + id);
      }
      replicationManager.appendDeleteRecordTransactional(journalID, txID, id);
      localJournal.appendDeleteRecordTransactional(txID, id);
   }

   /**
    * @param txID
    * @param transactionData
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendPrepareRecord(long, byte[], boolean)
    */
   public void appendPrepareRecord(final long txID, final byte[] transactionData, final boolean sync) throws Exception
   {
      this.appendPrepareRecord(txID, new ByteArrayEncoding(transactionData), sync);
   }

   /**
    * @param txID
    * @param transactionData
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendPrepareRecord(long, org.hornetq.core.journal.EncodingSupport, boolean)
    */
   public void appendPrepareRecord(final long txID, final EncodingSupport transactionData, final boolean sync) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendPrepare txID=" + txID);
      }
      replicationManager.appendPrepareRecord(journalID, txID, transactionData);
      localJournal.appendPrepareRecord(txID, transactionData, sync);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.Journal#appendPrepareRecord(long, org.hornetq.core.journal.EncodingSupport, boolean, org.hornetq.core.journal.IOCompletion)
    */
   public void appendPrepareRecord(final long txID,
                                   final EncodingSupport transactionData,
                                   final boolean sync,
                                   final IOCompletion callback) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendPrepare txID=" + txID);
      }
      replicationManager.appendPrepareRecord(journalID, txID, transactionData);
      localJournal.appendPrepareRecord(txID, transactionData, sync, callback);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.Journal#appendPrepareRecord(long, byte[], boolean, org.hornetq.core.journal.IOCompletion)
    */
   public void appendPrepareRecord(final long txID,
                                   final byte[] transactionData,
                                   final boolean sync,
                                   final IOCompletion callback) throws Exception
   {
      this.appendPrepareRecord(txID, new ByteArrayEncoding(transactionData), sync, callback);
   }

   /**
    * @param txID
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendRollbackRecord(long, boolean)
    */
   public void appendRollbackRecord(final long txID, final boolean sync) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendRollback " + txID);
      }
      replicationManager.appendRollbackRecord(journalID, txID);
      localJournal.appendRollbackRecord(txID, sync);
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.Journal#appendRollbackRecord(long, boolean, org.hornetq.core.journal.IOCompletion)
    */
   public void appendRollbackRecord(final long txID, final boolean sync, final IOCompletion callback) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendRollback " + txID);
      }
      replicationManager.appendRollbackRecord(journalID, txID);
      localJournal.appendRollbackRecord(txID, sync, callback);
   }

   /**
    * @param id
    * @param recordType
    * @param record
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendUpdateRecord(long, byte, byte[], boolean)
    */
   public void appendUpdateRecord(final long id, final byte recordType, final byte[] record, final boolean sync) throws Exception
   {
      this.appendUpdateRecord(id, recordType, new ByteArrayEncoding(record), sync);
   }

   /**
    * @param id
    * @param recordType
    * @param record
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendUpdateRecord(long, byte, org.hornetq.core.journal.EncodingSupport, boolean)
    */
   public void appendUpdateRecord(final long id, final byte recordType, final EncodingSupport record, final boolean sync) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendUpdateRecord id = " + id + " , recordType = " + recordType);
      }
      replicationManager.appendUpdateRecord(journalID, id, recordType, record);
      localJournal.appendUpdateRecord(id, recordType, record, sync);
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
      this.appendUpdateRecord(id, recordType, new ByteArrayEncoding(record), sync, completionCallback);
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
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendUpdateRecord id = " + id + " , recordType = " + recordType);
      }
      replicationManager.appendUpdateRecord(journalID, id, recordType, record);
      localJournal.appendUpdateRecord(id, recordType, record, sync, completionCallback);
   }

   /**
    * @param txID
    * @param id
    * @param recordType
    * @param record
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendUpdateRecordTransactional(long, long, byte, byte[])
    */
   public void appendUpdateRecordTransactional(final long txID,
                                               final long id,
                                               final byte recordType,
                                               final byte[] record) throws Exception
   {
      this.appendUpdateRecordTransactional(txID, id, recordType, new ByteArrayEncoding(record));
   }

   /**
    * @param txID
    * @param id
    * @param recordType
    * @param record
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendUpdateRecordTransactional(long, long, byte, org.hornetq.core.journal.EncodingSupport)
    */
   public void appendUpdateRecordTransactional(final long txID,
                                               final long id,
                                               final byte recordType,
                                               final EncodingSupport record) throws Exception
   {
      if (ReplicatedJournal.trace)
      {
         ReplicatedJournal.trace("AppendUpdateRecord txid=" + txID + " id = " + id + " , recordType = " + recordType);
      }
      replicationManager.appendUpdateRecordTransactional(journalID, txID, id, recordType, record);
      localJournal.appendUpdateRecordTransactional(txID, id, recordType, record);
   }

   /**
    * @param committedRecords
    * @param preparedTransactions
    * @param transactionFailure
    *
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#load(java.util.List, java.util.List, org.hornetq.core.journal.TransactionFailureCallback)
    */
   public JournalLoadInformation load(final List<RecordInfo> committedRecords,
                                      final List<PreparedTransactionInfo> preparedTransactions,
                                      final TransactionFailureCallback transactionFailure) throws Exception
   {
      return localJournal.load(committedRecords, preparedTransactions, transactionFailure);
   }

   /**
    * @param reloadManager
    *
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#load(org.hornetq.core.journal.LoaderCallback)
    */
   public JournalLoadInformation load(final LoaderCallback reloadManager) throws Exception
   {
      return localJournal.load(reloadManager);
   }

   /**
    * @param pages
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#perfBlast(int)
    */
   public void perfBlast(final int pages) throws Exception
   {
      localJournal.perfBlast(pages);
   }

   /**
    * @throws Exception
    * @see org.hornetq.core.server.HornetQComponent#start()
    */
   public void start() throws Exception
   {
      localJournal.start();
   }

   /**
    * @throws Exception
    * @see org.hornetq.core.server.HornetQComponent#stop()
    */
   public void stop() throws Exception
   {
      localJournal.stop();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.Journal#getAlignment()
    */
   public int getAlignment() throws Exception
   {
      return localJournal.getAlignment();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#isStarted()
    */
   public boolean isStarted()
   {
      return localJournal.isStarted();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.Journal#loadInternalOnly()
    */
   public JournalLoadInformation loadInternalOnly() throws Exception
   {
      return localJournal.loadInternalOnly();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.Journal#getNumberOfRecords()
    */
   public int getNumberOfRecords()
   {
      return localJournal.getNumberOfRecords();
   }

   public void runDirectJournalBlast() throws Exception
   {
      localJournal.runDirectJournalBlast();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.Journal#getUserVersion()
    */
   public int getUserVersion()
   {
      return localJournal.getUserVersion();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
