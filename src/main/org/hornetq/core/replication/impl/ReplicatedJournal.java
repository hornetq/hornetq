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
import org.hornetq.core.journal.Journal;
import org.hornetq.core.journal.LoaderCallback;
import org.hornetq.core.journal.PreparedTransactionInfo;
import org.hornetq.core.journal.RecordInfo;
import org.hornetq.core.journal.TransactionFailureCallback;
import org.hornetq.core.journal.impl.JournalImpl.ByteArrayEncoding;
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

   private static final boolean trace = log.isTraceEnabled();

   private final ReplicationManager replicationManager;

   private final Journal replicatedJournal;

   private final byte journalID;

   public ReplicatedJournal(final byte journaID,
                                final Journal replicatedJournal,
                                final ReplicationManager replicationManager)
   {
      super();
      journalID = journaID;
      this.replicatedJournal = replicatedJournal;
      this.replicationManager = replicationManager;
   }

   // Static --------------------------------------------------------
   
   private static void trace(String message)
   {
      log.trace(message);
   }

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

   /**
    * @param id
    * @param recordType
    * @param record
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendAddRecord(long, byte, org.hornetq.core.journal.EncodingSupport, boolean)
    */
   public void appendAddRecord(final long id, final byte recordType, final EncodingSupport record, final boolean sync) throws Exception
   {
      if (trace)
      {
         trace("Append record id = " + id + " recordType = " + recordType);
      }
      replicationManager.appendAddRecord(journalID, id, recordType, record);
      replicatedJournal.appendAddRecord(id, recordType, record, sync);
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
      if (trace)
      {
         trace("Append record TXid = " + id + " recordType = " + recordType);
      }
      replicationManager.appendAddRecordTransactional(journalID, txID, id, recordType, record);
      replicatedJournal.appendAddRecordTransactional(txID, id, recordType, record);
   }

   /**
    * @param txID
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendCommitRecord(long, boolean)
    */
   public void appendCommitRecord(final long txID, final boolean sync) throws Exception
   {
      if (trace)
      {
         trace("AppendCommit " + txID);
      }
      replicationManager.appendCommitRecord(journalID, txID);
      replicatedJournal.appendCommitRecord(txID, sync);
   }

   /**
    * @param id
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendDeleteRecord(long, boolean)
    */
   public void appendDeleteRecord(final long id, final boolean sync) throws Exception
   {
      if (trace)
      {
         trace("AppendDelete " + id);
      }
      replicationManager.appendDeleteRecord(journalID, id);
      replicatedJournal.appendDeleteRecord(id, sync);
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
      if (trace)
      {
         trace("AppendDelete txID=" + txID + " id=" + id);
      }
      replicationManager.appendDeleteRecordTransactional(journalID, txID, id, record);
      replicatedJournal.appendDeleteRecordTransactional(txID, id, record);
   }

   /**
    * @param txID
    * @param id
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendDeleteRecordTransactional(long, long)
    */
   public void appendDeleteRecordTransactional(final long txID, final long id) throws Exception
   {
      if (trace)
      {
         trace("AppendDelete (noencoding) txID=" + txID + " id=" + id);
      }
      replicationManager.appendDeleteRecordTransactional(journalID, txID, id);
      replicatedJournal.appendDeleteRecordTransactional(txID, id);
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
      if (trace)
      {
         trace("AppendPrepare txID=" + txID);
      }
      replicationManager.appendPrepareRecord(journalID, txID, transactionData);
      replicatedJournal.appendPrepareRecord(txID, transactionData, sync);
   }

   /**
    * @param txID
    * @param sync
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#appendRollbackRecord(long, boolean)
    */
   public void appendRollbackRecord(final long txID, final boolean sync) throws Exception
   {
      if (trace)
      {
         trace("AppendRollback " + txID);
      }
      replicationManager.appendRollbackRecord(journalID, txID);
      replicatedJournal.appendRollbackRecord(txID, sync);
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
      if (trace)
      {
         trace("AppendUpdateRecord id = " + id + " , recordType = " + recordType);
      }
      replicationManager.appendUpdateRecord(journalID, id, recordType, record);
      replicatedJournal.appendUpdateRecord(id, recordType, record, sync);
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
      if (trace)
      {
         trace("AppendUpdateRecord txid=" + txID + " id = " + id + " , recordType = " + recordType);
      }
      replicationManager.appendUpdateRecordTransactional(journalID, txID, id, recordType, record);
      replicatedJournal.appendUpdateRecordTransactional(txID, id, recordType, record);
   }

   /**
    * @param committedRecords
    * @param preparedTransactions
    * @param transactionFailure
    * @return
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#load(java.util.List, java.util.List, org.hornetq.core.journal.TransactionFailureCallback)
    */
   public long load(final List<RecordInfo> committedRecords,
                    final List<PreparedTransactionInfo> preparedTransactions,
                    final TransactionFailureCallback transactionFailure) throws Exception
   {
      return replicatedJournal.load(committedRecords, preparedTransactions, transactionFailure);
   }

   /**
    * @param reloadManager
    * @return
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#load(org.hornetq.core.journal.LoaderCallback)
    */
   public long load(final LoaderCallback reloadManager) throws Exception
   {
      return replicatedJournal.load(reloadManager);
   }

   /**
    * @param pages
    * @throws Exception
    * @see org.hornetq.core.journal.Journal#perfBlast(int)
    */
   public void perfBlast(final int pages) throws Exception
   {
      replicatedJournal.perfBlast(pages);
   }

   /**
    * @throws Exception
    * @see org.hornetq.core.server.HornetQComponent#start()
    */
   public void start() throws Exception
   {
      replicatedJournal.start();
   }

   /**
    * @throws Exception
    * @see org.hornetq.core.server.HornetQComponent#stop()
    */
   public void stop() throws Exception
   {
      replicatedJournal.stop();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.journal.Journal#getAlignment()
    */
   public int getAlignment() throws Exception
   {
      return replicatedJournal.getAlignment();
   }

   /* (non-Javadoc)
    * @see org.hornetq.core.server.HornetQComponent#isStarted()
    */
   public boolean isStarted()
   {
      return replicatedJournal.isStarted();
   }

   // Package protected ---------------------------------------------

   // Protected -----------------------------------------------------

   // Private -------------------------------------------------------

   // Inner classes -------------------------------------------------

}
